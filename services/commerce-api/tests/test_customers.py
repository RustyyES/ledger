"""Customer lifecycle, including the soft-delete semantics the warehouse relies on."""

from __future__ import annotations

import uuid

import pytest


def test_create_customer_returns_201_and_echoes_fields(client, idem):
    resp = client.post(
        "/customers",
        json={
            "email": "ada@example.com",
            "name": "Ada Lovelace",
            "country_code": "gb",  # lowercase on purpose: must be upcased
            "timezone": "Europe/London",
        },
        headers=idem.fresh(),
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["country_code"] == "GB"
    assert body["deleted_at"] is None
    assert uuid.UUID(body["id"])


def test_duplicate_email_is_409_not_500(client, idem):
    payload = {"email": "dup@example.com", "name": "First", "country_code": "US"}
    assert client.post("/customers", json=payload, headers=idem.fresh()).status_code == 201
    resp = client.post("/customers", json=payload, headers=idem.fresh())
    assert resp.status_code == 409
    assert resp.json()["error"]["field"] == "email"


@pytest.mark.parametrize(
    "payload,bad_field",
    [
        ({"email": "not-an-email", "name": "X", "country_code": "US"}, "email"),
        ({"email": "a@b.com", "name": "", "country_code": "US"}, "name"),
        ({"email": "a@b.com", "name": "X", "country_code": "USA"}, "country_code"),
        (
            {"email": "a@b.com", "name": "X", "country_code": "US", "timezone": "Mars/Olympus"},
            "timezone",
        ),
    ],
)
def test_validation_returns_422_with_the_offending_field(client, idem, payload, bad_field):
    resp = client.post("/customers", json=payload, headers=idem.fresh())
    assert resp.status_code == 422, resp.text
    assert resp.json()["error"]["field"] == bad_field


def test_patch_only_touches_sent_fields(client, customer):
    original_name = customer["name"]
    resp = client.patch(f"/customers/{customer['id']}", json={"country_code": "DE"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["country_code"] == "DE"
    assert body["name"] == original_name, "PATCH must not blank unsent fields"


def test_patch_moves_updated_at(client, customer):
    """The dbt snapshot's timestamp strategy depends on this without exception."""
    before = customer["updated_at"]
    resp = client.patch(f"/customers/{customer['id']}", json={"name": "Renamed"})
    assert resp.status_code == 200
    assert resp.json()["updated_at"] > before


def test_empty_patch_body_is_rejected(client, customer):
    resp = client.patch(f"/customers/{customer['id']}", json={})
    assert resp.status_code == 422


def test_soft_delete_stamps_deleted_at_and_hides_the_row(client, customer):
    resp = client.delete(f"/customers/{customer['id']}")
    assert resp.status_code == 200
    assert resp.json()["deleted_at"] is not None

    # Invisible to an ordinary caller...
    assert client.get(f"/customers/{customer['id']}").status_code == 404
    # ...but still present, which is what reconciliation checks.
    resp = client.get(f"/customers/{customer['id']}", params={"include_deleted": True})
    assert resp.status_code == 200
    assert resp.json()["deleted_at"] is not None


def test_delete_is_idempotent(client, customer):
    """A retried DELETE must not read as a failure to the load generator."""
    first = client.delete(f"/customers/{customer['id']}")
    second = client.delete(f"/customers/{customer['id']}")
    assert first.status_code == second.status_code == 200
    assert (
        second.json()["deleted_at"] == first.json()["deleted_at"]
    ), "the retry moved deleted_at, so the delete was applied twice"


def test_unknown_customer_is_404(client):
    assert client.get(f"/customers/{uuid.uuid4()}").status_code == 404


def test_malformed_uuid_in_path_is_422(client):
    assert client.get("/customers/not-a-uuid").status_code == 422

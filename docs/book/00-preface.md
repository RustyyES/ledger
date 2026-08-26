# Preface: how to read this

This book explains one project — a data pipeline called Ledger — line by line,
decision by decision.

It assumes you know some Python and some SQL. It assumes you have **never**
built a data pipeline, and that words like *CDC*, *dbt*, *dimensional model*,
*SCD2* and *DAG* are either fuzzy or completely new. Every one of them is
explained the first time it appears, in plain language, before any code.

## What makes this different from a tutorial

Most tutorials show you *what* to type. This one is mostly about **why**, and
about the alternative you didn't pick.

Every chapter follows roughly the same shape:

> **The problem.** Stated in plain language, usually as a question a business
> person would ask.
>
> **The obvious answer.** The thing almost everyone writes first — including
> me, in several of these chapters.
>
> **Why it's wrong.** Usually with a worked example, because "it's wrong" is
> not an explanation.
>
> **What we did instead.**
>
> **What breaks if you remove it.** The most useful section. If you can't say
> what a piece of code protects you from, you don't understand it yet.
>
> **Try it.** A command you can actually run.

## A warning that matters more than the rest of this page

Roughly a third of this book is about failures that produced **no error
message**. The pipeline ran. Nothing crashed. It just returned a number that
was wrong.

That is the single most important idea in data engineering, and it is the
reason this book exists in this form. In application code, a bug usually
announces itself — a stack trace, a 500, a test going red. In data work, the
common bug is a `LEFT JOIN` that silently drops 22% of your rows, or an
incremental filter that never re-reads a row it needed to update. Everything
looks healthy. The dashboard renders. The number is just wrong, and it stays
wrong until someone reconciles it by hand months later.

Ten such bugs are documented in Chapter 18. Not one of them raised an
exception.

## Reading orders

**If you have one hour** — read Chapters 1, 2, 13 and 18. Chapter 13 is the
heart of the project; Chapter 18 is the evidence for why it's shaped that way.

**If you're learning the craft** — read straight through, and after each part,
go and build that part yourself before reading the next one. Then diff. This is
slow and it is the only way any of it sticks.

**If you're evaluating whether to use these ideas at work** — Chapters 19, 20
and 21. Nineteen tells you what each piece protects you from, twenty explains
every tool choice against its alternatives, twenty-one covers what stops
working at scale.

## A note on the code

Every code excerpt in this book is taken from the actual repository and has
actually run. Where a chapter shows you a *wrong* version — and several do —
it's labelled clearly and the difference is spelled out.

Nothing here is theoretical. When the book says "this fails", it means it was
observed failing, and Chapter 18 has the details.

## Conventions

Filenames are written as [`transform/models/marts/finance/fct_payments.sql`](../../transform/models/marts/finance/fct_payments.sql)
so you can open them alongside.

Commands are always runnable from the repository root:

```bash
make prove-lookback
```

Boxes like this one flag the thing most likely to be misunderstood:

> **The trap.** Filtering an incremental model on a business timestamp is
> correct-looking, passes every test, and is permanently wrong. Chapter 13.

---

Next: **[Chapter 1 — What we're actually building](01-what-we-are-building.md)**

"""Airflow plugins. Empty by design.

A plugin is the right tool for a genuinely reusable operator or hook. None of
the work in this project needs one: the tasks are TaskFlow functions and Bash
invocations, and wrapping `dbt run` in a custom operator would add an
abstraction whose only job is to hide a string.

The directory exists because compose mounts it and Airflow expects it.
"""

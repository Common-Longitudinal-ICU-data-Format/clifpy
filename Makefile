.PHONY: help mkdocs mo

help:
	uv run python -c "print('make docs  - start MkDocs dev server\nmake mo      - open Marimo editor')"

docs:
	uv run mkdocs serve

mo:
	uv run marimo edit

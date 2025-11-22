.PHONY: help docs mo

help:
	uv run python -c "print('make docs  - start MkDocs dev server\nmake mo      - open Marimo editor')"

docs:
	uv run mkdocs serve

mo:
	uv run marimo edit --watch

test_med_unit_conversion:
	uv run pytest tests/utils/test_unit_converter.py -vv

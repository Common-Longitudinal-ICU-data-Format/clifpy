.PHONY: help docs mo benchmark benchmark-small benchmark-memory

help:
	uv run python -c "print('make docs     - start MkDocs dev server\nmake mo       - open Marimo editor\nmake benchmark - run performance benchmark (full cohort)\nmake benchmark-small - run quick benchmark (10 IDs)\nmake benchmark-memory - run memory profiling benchmark (100 IDs)')"

docs:
	uv run mkdocs serve

mo:
	uv run marimo edit --watch

test_med_unit_conversion:
	uv run pytest tests/utils/test_unit_converter.py -vv

benchmark:
	cd dev/perf-benchmark && uv run python benchmark_simple.py

benchmark-small:
	cd dev/perf-benchmark && uv run python benchmark_simple.py -n 10

benchmark-memory:
	cd dev/perf-benchmark && memray run -o perf_profile.bin python benchmark_simple.py -n 100 && memray flamegraph perf_profile.bin

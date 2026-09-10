.PHONY: help docs mo test_med_unit_conversion benchmark benchmark-small benchmark-memory weight-impact weight-impact-pool

help:
	uv run python -c "print('make docs     - start MkDocs dev server\nmake mo       - open Marimo editor\nmake benchmark - run performance benchmark (full cohort)\nmake benchmark-small - run quick benchmark (10 IDs)\nmake benchmark-memory - run memory profiling benchmark (100 IDs)\nmake weight-impact CONFIG=<cfg.yaml> - weighted dose unit impact analysis\nmake weight-impact-pool INPUTS=<dirs> - pool sites (coordinating centre only)')"

docs:
	uv run mkdocs serve

mo:
	uv run marimo edit --watch

test_med_unit_conversion:
	uv run pytest tests/utils/med_unit_converter/ -vv

benchmark:
	cd dev/perf-benchmark && uv run python benchmark_simple.py

benchmark-small:
	cd dev/perf-benchmark && uv run python benchmark_simple.py -n 10

benchmark-memory:
	cd dev/perf-benchmark && memray run -o perf_profile.bin python benchmark_simple.py -n 100 && memray flamegraph perf_profile.bin

# ── Weighted dose unit impact analysis ───────────────────────────────
# Sites run:
#   make weight-impact CONFIG=my_site_config.yaml
# then send back output/weighted_unit_impact/<site_name>/
#
# Copy dev/weighted-unit-impact/config_template.yaml first and fill in the
# five required fields.
weight-impact:
	@test -n "$(CONFIG)" || { echo "usage: make weight-impact CONFIG=<path to your site config yaml>"; exit 2; }
	uv run python dev/weighted-unit-impact/run.py --config $(CONFIG)

# Coordinating centre only: pool the folders sites sent back.
#   make weight-impact-pool INPUTS="output/weighted_unit_impact/*"
weight-impact-pool:
	@test -n "$(INPUTS)" || { echo "usage: make weight-impact-pool INPUTS=<site output dirs>"; exit 2; }
	uv run python dev/weighted-unit-impact/pool.py --inputs $(INPUTS)

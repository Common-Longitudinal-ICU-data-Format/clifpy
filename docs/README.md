# Documentation Testing Guide

CLIFpy uses [Sybil](https://sybil.readthedocs.io/) to test code examples in our documentation, ensuring any showcased code stay accurate and functional.

Under the hood, the test parser detects any python or bash code blocks, executes them in sequence within the same .md file and with all the variables defined in earlier blocks are available in later blocks. 

At the minimum, it would test that a genuine method like `co.get_loaded_tables()` is indeed executable (regardless of the output); and that a non-existing typo-ed method like `get_loaded_table_names()` is not and would raise an error.


## Reference

If you are using a coding agent, instruct it to reference [the docs for Sybil via context7](https://context7.com/simplistix/sybil). 

For a .md file that follows all the principles listed below and pass all tests, see `docs/user-guide/quickstart.md`.


## Avoid fake path

To make any python code blocks testable, make sure you initialize any clifpy objects with the path to the demo data config. This ensures the example code are actually executable and thus testable:

````markdown
```python
from clifpy import ClifOrchestrator
co = ClifOrchestrator(config_path="config/demo_data_config.yaml")
# no more fake paths like "path/to/your/config/file"
```
````

## Testing output
When it is desirable to demonstrate the output to the users, we can also test for the output by adding `>>>` in front of the code (and nothing in front of the output lines).

````markdown
```python
>>> co.get_loaded_tables()
['patient', 'labs', 'vitals']

>>> x = 1 + 1
>>> x
2
```
````

**Important:** The `>>>` prompt must start on the **first line** of the code block (or after a `>>>` comment). Otherwise, Sybil treats it as regular Python, causing syntax errors.

✅ **Correct:**
````markdown
```python
>>> # This is a comment
>>> co.get_loaded_tables()
['patient', 'labs', 'vitals']
```
````

❌ **Incorrect:**
````markdown
```python
# This comment breaks doctest detection
>>> co.get_loaded_tables()
['patient', 'labs', 'vitals']
```
````

## Skipping code blocks if necessary
If it is desirable to show the output to users but the output is too long or complicated to test for, you can also skip testing that code block by adding a `<!-- skip: next -->` line before the code block:

````markdown
<!-- skip: next -->
```python
>>> co.validate_all()
Validating 3 table(s)...

Validating patient...
Validation completed with 5 error(s). See `errors` attribute.
```

<!-- skip: next -->
```bash
pip install clifpy
```
````

When to skip:
- Output is non-deterministic (error counts, timestamps)
- Examples require specific data/setup not available in tests
- Bash commands that would have side effects (e.g. installing packages to the virtual env)

For more complex skipping syntax: https://sybil.readthedocs.io/en/latest/markdown.html#skipping-examples


## Running tests

Finally, to test against docs, simply run it via pytest: 

```bash
# Run tests on all docs
uv run pytest docs/

# Run tests on a specific file
uv run pytest docs/user-guide/quickstart.md -v
```





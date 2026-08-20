"""
Minimal root conftest.py for project-wide Sybil markdown testing.
Test-specific fixtures are in tests/conftest.py
"""
from sybil import Sybil
from sybil.parsers.markdown import PythonCodeBlockParser, SkipParser

# Only python blocks are executed. Bash blocks in our markdown are illustrative
# (install steps, git workflow, pytest invocations), and running them shelled
# out for real: the `git add . && git commit && git push` example in
# docs/contributing.md created three "feat: add new feature" commits and pushed
# a branch of someone's venv to origin. Verifying an exit code was never worth
# that.
pytest_collect_file = Sybil(
    parsers=[
        PythonCodeBlockParser(),
        SkipParser(),
    ],
    patterns=['**/*.md'],
    path='.',
).pytest()
"""
Minimal root conftest.py for project-wide Sybil markdown testing.
Test-specific fixtures are in tests/conftest.py
"""
import subprocess
from textwrap import dedent
from sybil import Sybil
from sybil.parsers.markdown import PythonCodeBlockParser, CodeBlockParser, SkipParser
from sybil.evaluators.doctest import NUMBER
from sybil.parsers.doctest import DocTestParser

def evaluate_bash(example):
    """
    Evaluate bash code blocks by running commands and checking they succeed.
    Just checks return code, doesn't verify output.
    """
    code = dedent(example.parsed).strip()

    # Run the command and let it fail if there's an error
    # Using shell=True to support pipes, redirects, etc.
    result = subprocess.run(
        code,
        shell=True,
        capture_output=True,
        text=True,
        timeout=10
    )

    # Raise error if command failed
    if result.returncode != 0:
        raise AssertionError(
            f"Command failed with exit code {result.returncode}\n"
            f"Command: {code}\n"
            f"Stderr: {result.stderr}"
        )


pytest_collect_file = Sybil(
    parsers=[
        PythonCodeBlockParser(),
        # DocTestParser(),
        CodeBlockParser(language='bash', evaluator=evaluate_bash),
        SkipParser(),
    ],
    patterns=['**/*.md'],
    path='.',
).pytest()
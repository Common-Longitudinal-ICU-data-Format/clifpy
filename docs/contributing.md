# Contributing to CLIFpy

We welcome contributions to CLIFpy! This guide will help you get started.

## Getting Started

1. Fork the repository on GitHub
2. Clone your fork locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/CLIFpy.git
   cd CLIFpy
   ```
3. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
4. Install in development mode with all dependencies:
   ```bash
   pip install -e ".[docs]"
   ```

## Development Workflow

1. Create a new branch for your feature or fix:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. Make your changes and ensure:
   - Code follows the existing style
   - All tests pass
   - New features include tests
   - Documentation is updated

3. Run tests:
   ```bash
   pytest tests/
   ```

4. Commit your changes:
   ```bash
   git add .
   git commit -m "feat: add new feature"
   ```

5. Push to your fork:
   ```bash
   git push origin feature/your-feature-name
   ```

6. Create a Pull Request on GitHub

## Code Style

- Follow PEP 8 guidelines
- Use meaningful variable and function names
- Add type hints where appropriate
- Include docstrings for all public functions and classes

## Documentation

- Update docstrings for any API changes
- Add examples to docstrings where helpful
- Update user guide if adding new features
- Build docs locally to verify:
  ```bash
  mkdocs serve
  ```

## Testing

- Write tests for new functionality
- Ensure all tests pass before submitting PR
- Aim for high test coverage
- Use pytest fixtures for common test data

## Commit Messages

Follow conventional commits format:
- `feat:` - New feature
- `fix:` - Bug fix
- `docs:` - Documentation changes
- `test:` - Test additions or changes
- `refactor:` - Code refactoring
- `chore:` - Maintenance tasks

## Questions?

- Open an issue for bugs or feature requests
- Join discussions in existing issues
- Reach out to maintainers if you need help

Thank you for contributing to CLIFpy!
import os
from pathlib import Path

root_path = Path(__file__).parent.parent
print(f"Root path: {root_path}")

# Find Python files
python_files = []
for root, dirs, files in os.walk(root_path):
    dirs[:] = [d for d in dirs if d not in ['__pycache__', 'visualization', '.git', 'venv', 'env']]
    for file in files:
        if file.endswith('.py') and file != '__init__.py' and not file.startswith('test_'):
            file_path = os.path.join(root, file)
            relative_path = os.path.relpath(file_path, root_path)
            python_files.append(relative_path)

print(f"Found {len(python_files)} Python files:")
for f in python_files:
    print(f"  {f}")
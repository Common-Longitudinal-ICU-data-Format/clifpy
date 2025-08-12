import ast
import json
import os
from pathlib import Path
import re


def extract_imports(file_path):
    """Extract import statements from a Python file."""
    imports = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        tree = ast.parse(content)
        
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.append({
                        'module': alias.name,
                        'name': alias.name,
                        'alias': alias.asname
                    })
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ''
                for alias in node.names:
                    imports.append({
                        'module': module,
                        'name': alias.name,
                        'alias': alias.asname,
                        'from_import': True
                    })
    except Exception as e:
        print(f"Error parsing {file_path}: {e}")
    
    return imports


def extract_definitions(file_path):
    """Extract classes and functions from a Python file, including inheritance info."""
    classes = []
    functions = []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        tree = ast.parse(content)
        
        # First pass: collect all class definitions with inheritance
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                methods = []
                base_classes = []
                
                # Extract base classes
                for base in node.bases:
                    if isinstance(base, ast.Name):
                        base_classes.append(base.id)
                    elif isinstance(base, ast.Attribute):
                        # Handle cases like module.ClassName
                        # Manually construct the attribute path
                        parts = []
                        current = base
                        while isinstance(current, ast.Attribute):
                            parts.append(current.attr)
                            current = current.value
                        if isinstance(current, ast.Name):
                            parts.append(current.id)
                        base_classes.append('.'.join(reversed(parts)))
                
                # Extract methods
                for item in node.body:
                    if isinstance(item, ast.FunctionDef):
                        params = [arg.arg for arg in item.args.args]
                        methods.append({
                            'name': item.name,
                            'params': params,
                            'lineno': item.lineno
                        })
                
                classes.append({
                    'name': node.name,
                    'base_classes': base_classes,
                    'methods': methods,
                    'lineno': node.lineno
                })
            
            elif isinstance(node, ast.FunctionDef) and not any(isinstance(parent, ast.ClassDef) for parent in ast.walk(tree) if node in ast.walk(parent)):
                params = [arg.arg for arg in node.args.args]
                functions.append({
                    'name': node.name,
                    'params': params,
                    'lineno': node.lineno
                })
    
    except Exception as e:
        print(f"Error parsing {file_path}: {e}")
    
    return classes, functions


def create_folder_hierarchy(files):
    """Create a hierarchical structure from flat file list."""
    hierarchy = {}
    
    for file_data in files:
        path_parts = file_data['path'].split(os.sep)
        current = hierarchy
        
        # Build the folder structure
        for i, part in enumerate(path_parts[:-1]):
            if part not in current:
                current[part] = {
                    'type': 'folder',
                    'name': part,
                    'path': os.sep.join(path_parts[:i+1]),
                    'children': {},
                    'files': []
                }
            current = current[part]['children']
        
        # Add the file to its parent folder
        folder_path = os.sep.join(path_parts[:-1]) if len(path_parts) > 1 else ''
        if folder_path:
            # Navigate to the correct folder
            current = hierarchy
            for part in path_parts[:-1]:
                current = current[part]['children']
            parent = current
            # Get the parent dict one level up
            current = hierarchy
            for part in path_parts[:-2]:
                current = current[part]['children']
            if len(path_parts) > 1:
                current[path_parts[-2]]['files'].append(file_data)
        else:
            # Root level file
            if '.' not in hierarchy:
                hierarchy['.'] = {
                    'type': 'folder',
                    'name': 'root',
                    'path': '',
                    'children': {},
                    'files': []
                }
            hierarchy['.']['files'].append(file_data)
    
    return hierarchy


def analyze_project(root_path):
    """Analyze the entire project structure."""
    project_data = {
        'files': [],
        'folders': {},
        'connections': [],
        'hierarchy': {}
    }
    
    # Find all Python files
    python_files = []
    for root, dirs, files in os.walk(root_path):
        # Skip __pycache__ and visualization directories
        dirs[:] = [d for d in dirs if d not in ['__pycache__', 'visualization', '.git', 'venv', 'env', '.venv']]
        
        for file in files:
            # Skip __init__.py and test_* files as requested
            if file.endswith('.py') and file != '__init__.py' and not file.startswith('test_'):
                file_path = os.path.join(root, file)
                relative_path = os.path.relpath(file_path, root_path)
                python_files.append((file_path, relative_path))
    
    # Analyze each file
    file_map = {}
    for file_path, relative_path in python_files:
        imports = extract_imports(file_path)
        classes, functions = extract_definitions(file_path)
        
        # Get folder path
        folder_path = os.path.dirname(relative_path)
        
        file_data = {
            'path': relative_path,
            'name': os.path.basename(relative_path),
            'folder': folder_path,
            'imports': imports,
            'classes': classes,
            'functions': functions,
            'module': relative_path.replace('.py', '').replace(os.sep, '.')
        }
        
        project_data['files'].append(file_data)
        file_map[relative_path] = file_data
    
    # Create folder hierarchy
    project_data['hierarchy'] = create_folder_hierarchy(project_data['files'])
    
    # Analyze connections - imports and inheritance
    for file_data in project_data['files']:
        source_module = file_data['module']
        
        # Process imports
        for imp in file_data['imports']:
            imp_module = imp['module']
            imp_name = imp['name']
            
            # Try to match imports with project files
            for target_file in project_data['files']:
                target_module = target_file['module']
                
                # Various matching strategies
                matched = False
                
                # Direct module match
                if imp_module == target_module:
                    matched = True
                
                # Skip relative imports for now - they're causing issues
                elif imp_module.startswith('.'):
                    continue
                
                # Package import match (e.g., from clifpy.tables import base_table)
                elif target_module.endswith('.' + imp_name):
                    if target_module.startswith('src.clifpy.' + imp_module.replace('clifpy.', '')):
                        matched = True
                
                # Match clifpy imports to src.clifpy
                elif imp_module.startswith('clifpy'):
                    src_module = imp_module.replace('clifpy', 'src.clifpy')
                    if src_module == target_module or target_module.startswith(src_module + '.'):
                        matched = True
                
                # From import match (e.g., from .base_table import BaseTable)
                elif imp.get('from_import'):
                    if imp_module:
                        full_import = imp_module + '.' + imp_name
                        if target_module.endswith(imp_module) or target_module == full_import.replace('clifpy', 'src.clifpy'):
                            matched = True
                
                if matched:
                    connection = {
                        'source': file_data['path'],
                        'target': target_file['path'],
                        'type': 'import',
                        'relationship': 'import',
                        'import_detail': f"{imp_module}.{imp_name}" if imp_module else imp_name
                    }
                    
                    # Avoid duplicate connections
                    if not any(c['source'] == connection['source'] and c['target'] == connection['target'] 
                              and c['relationship'] == 'import'
                              for c in project_data['connections']):
                        project_data['connections'].append(connection)
        
        # Process inheritance relationships
        for cls in file_data['classes']:
            for base_class_name in cls['base_classes']:
                # Try to find the base class in the project
                for target_file in project_data['files']:
                    # Check if the base class is defined in this file
                    for target_cls in target_file['classes']:
                        if target_cls['name'] == base_class_name:
                            # Found inheritance relationship
                            connection = {
                                'source': file_data['path'],
                                'target': target_file['path'],
                                'type': 'inheritance',
                                'relationship': 'inheritance',
                                'class_name': cls['name'],
                                'base_class': base_class_name
                            }
                            
                            # Avoid duplicate connections
                            if not any(c['source'] == connection['source'] and c['target'] == connection['target'] 
                                      and c['relationship'] == 'inheritance' and c['class_name'] == cls['name']
                                      for c in project_data['connections']):
                                project_data['connections'].append(connection)
                                
                                # Mark the file as having a base class for hierarchy purposes
                                if 'is_derived' not in file_data:
                                    file_data['is_derived'] = True
                                if 'is_base' not in target_file:
                                    target_file['is_base'] = True
    
    return project_data


if __name__ == '__main__':
    root_path = Path(__file__).parent.parent  # Go up one level to reach the project root
    project_data = analyze_project(root_path)
    
    # Save to JSON
    with open('project_structure.json', 'w') as f:
        json.dump(project_data, f, indent=2)
    
    print(f"Analyzed {len(project_data['files'])} Python files")
    print(f"Found {len(project_data['connections'])} connections")
    
    # Print inheritance relationships
    inheritance_connections = [c for c in project_data['connections'] if c['relationship'] == 'inheritance']
    if inheritance_connections:
        print(f"Found {len(inheritance_connections)} inheritance relationships:")
        for conn in inheritance_connections:
            print(f"  {conn['class_name']} in {conn['source']} inherits from {conn['base_class']} in {conn['target']}")
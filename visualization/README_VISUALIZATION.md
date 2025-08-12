# PyClif Package Visualization

This is an interactive visualization of the PyClif package structure that automatically updates when the codebase changes.

## Features

- **Interactive Force-Directed Graph**: Files are displayed as nodes that can be dragged around
- **File Details Panel**: Click on any file to see its classes, methods, and functions
- **Connection Visualization**: Import relationships between files are shown as lines
- **Auto-Refresh**: The visualization automatically refreshes every 30 seconds
- **Manual Refresh**: Click the refresh button to update immediately
- **Color Coding**:
  - Blue: Core modules (__init__.py files)
  - Green: Utils/IO modules
  - Red: Tables/Data modules
  - Orange: Schema files
  - Gray: Other files

## How to Use

### Option 1: Using the Python Server (Recommended)

1. Navigate to the visualization directory:
   ```bash
   cd visualization
   ```

2. Run the visualization server:
   ```bash
   python serve_visualization.py
   ```

3. Open your browser and navigate to:
   ```
   http://localhost:8000/package_visualization.html
   ```

4. The server will automatically regenerate the project structure when you click refresh

### Option 2: Direct File Access

1. Navigate to the visualization directory:
   ```bash
   cd visualization
   ```

2. Generate the project structure:
   ```bash
   python analyze_codebase.py
   ```

3. Open `package_visualization.html` directly in your browser

4. To update, manually run the analyzer script again and refresh the page

## Interaction

- **Click** on a node to view file details in the right panel
- **Drag** nodes to rearrange the layout
- **Zoom** in/out using mouse wheel or trackpad
- **Pan** by dragging on empty space
- **Hover** over nodes to see quick stats

## Files Created

- `analyze_codebase.py`: Python script that analyzes the codebase
- `project_structure.json`: Generated data file containing the project structure
- `package_visualization.html`: The interactive visualization
- `serve_visualization.py`: Optional Python server for auto-refresh functionality

## Requirements

- Python 3.6+
- Web browser with JavaScript enabled
- Internet connection (for D3.js library)
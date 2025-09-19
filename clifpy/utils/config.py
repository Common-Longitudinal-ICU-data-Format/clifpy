"""
Configuration loading utilities for CLIF data processing.

This module provides functions to load configuration from JSON files
for consistent data loading across CLIF tables and orchestrator.
"""

import os
import json
from typing import Dict, Any, Optional
from pathlib import Path


def load_clif_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load CLIF configuration from JSON file.
    
    Parameters
    ----------
    config_path : str, optional
        Path to the configuration file.
        If None, looks for 'clif_config.json' in current directory.
    
    Returns
    -------
    dict
        Configuration dictionary with required fields validated
        
    Raises
    ------
    FileNotFoundError
        If config file doesn't exist
    ValueError
        If required fields are missing or invalid
    json.JSONDecodeError
        If config file is not valid JSON
    """
    # Determine config file path
    if config_path is None:
        config_path = os.path.join(os.getcwd(), 'clif_config.json')
    
    # Check if config file exists
    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"Configuration file not found: {config_path}\n"
            "Please either:\n"
            "  1. Create a clif_config.json file in the current directory\n"
            "  2. Provide config_path parameter pointing to your config file\n"
            "  3. Provide data_directory, filetype, and timezone parameters directly"
        )
    
    # Load configuration
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(
            f"Invalid JSON in configuration file {config_path}: {str(e)}",
            e.doc, e.pos
        )
    
    # Validate required fields
    required_fields = ['data_directory', 'filetype', 'timezone']
    missing_fields = [field for field in required_fields if field not in config]
    
    if missing_fields:
        raise ValueError(
            f"Missing required fields in configuration file {config_path}: {missing_fields}\n"
            f"Required fields are: {required_fields}"
        )
    
    # Validate data_directory exists
    data_dir = config['data_directory']
    if not os.path.exists(data_dir):
        raise ValueError(
            f"Data directory specified in config does not exist: {data_dir}\n"
            f"Please check the 'data_directory' path in {config_path}"
        )
    
    # Validate filetype
    supported_filetypes = ['csv', 'parquet']
    if config['filetype'] not in supported_filetypes:
        raise ValueError(
            f"Unsupported filetype '{config['filetype']}' in {config_path}\n"
            f"Supported filetypes are: {supported_filetypes}"
        )
    
    print(f"Configuration loaded successfully from {config_path}")
    return config


def get_config_or_params(
    config_path: Optional[str] = None,
    data_directory: Optional[str] = None,
    filetype: Optional[str] = None,
    timezone: Optional[str] = None,
    output_directory: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get configuration from either config file or direct parameters.
    
    Loading priority:
    1. If all required params provided directly → use them
    2. If config_path provided → load from that path, allow param overrides
    3. If no params and no config_path → auto-detect clif_config.json
    4. Parameters override config file values when both are provided
    
    Parameters
    ----------
    config_path : str, optional
        Path to configuration file
    data_directory : str, optional
        Direct parameter
    filetype : str, optional
        Direct parameter  
    timezone : str, optional
        Direct parameter
    output_directory : str, optional
        Direct parameter
        
    Returns
    -------
    dict
        Final configuration dictionary
        
    Raises
    ------
    ValueError
        If neither config nor required params are provided
    """
    # Check if all required params are provided directly
    required_params = [data_directory, filetype, timezone]
    if all(param is not None for param in required_params):
        # All required params provided - use them directly
        config = {
            'data_directory': data_directory,
            'filetype': filetype,
            'timezone': timezone
        }
        if output_directory is not None:
            config['output_directory'] = output_directory
        print("Using directly provided parameters")
        return config
    
    # Try to load from config file
    try:
        config = load_clif_config(config_path)
    except FileNotFoundError:
        # If no config file and incomplete params, raise helpful error
        if any(param is not None for param in required_params):
            # Some params provided but not all
            missing = []
            if data_directory is None:
                missing.append('data_directory')
            if filetype is None:
                missing.append('filetype') 
            if timezone is None:
                missing.append('timezone')
            raise ValueError(
                f"Incomplete parameters provided. Missing: {missing}\n"
                "Please either:\n"
                "  1. Provide all required parameters (data_directory, filetype, timezone)\n"
                "  2. Create a clif_config.json file\n"
                "  3. Provide a config_path parameter"
            )
        else:
            # No params and no config file - re-raise the original error
            raise
    
    # Override config values with any provided parameters
    if data_directory is not None:
        config['data_directory'] = data_directory
        print(f"Overriding data_directory from config with: {data_directory}")
    
    if filetype is not None:
        config['filetype'] = filetype
        print(f"Overriding filetype from config with: {filetype}")
        
    if timezone is not None:
        config['timezone'] = timezone
        print(f"Overriding timezone from config with: {timezone}")
        
    if output_directory is not None:
        config['output_directory'] = output_directory
        print(f"Overriding output_directory from config with: {output_directory}")
    
    return config


def create_example_config(
    data_directory: str = "./data",
    filetype: str = "parquet", 
    timezone: str = "UTC",
    output_directory: str = "./output",
    config_path: str = "./clif_config.json"
) -> None:
    """
    Create an example configuration file.
    
    Parameters
    ----------
    data_directory : str
        Path to data directory
    filetype : str
        File type (csv or parquet)
    timezone : str
        Timezone string
    output_directory : str
        Output directory path
    config_path : str
        Where to save the config file
    """
    config = {
        "data_directory": data_directory,
        "filetype": filetype,
        "timezone": timezone,
        "output_directory": output_directory
    }
    
    with open(config_path, 'w') as f:
        json.dump(config, f, indent=2)
    
    print(f"Example configuration file created at: {config_path}")
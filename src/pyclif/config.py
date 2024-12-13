# src/pyclif/config.py
import json
import os

class Config:
    def __init__(self, config_path=None):
        # Default config location
        if config_path is None:
            # Adjust to your actual config path as needed
            config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'config.json')

        if not os.path.exists(config_path):
            self.config = {}
            print("No config file found, using defaults.")
        else:
            with open(config_path, 'r') as f:
                self.config = json.load(f)
                print("Loaded configuration from", config_path)

    def get(self, key, default=None):
        return self.config.get(key, default)

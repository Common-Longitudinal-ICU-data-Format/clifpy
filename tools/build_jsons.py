import os
import json
import pandas as pd
import argparse

def generate_table_jsons(mcide_dir, json_dir):
    # Load the all_tables.json
    with open(os.path.join(mcide_dir, "all_tables.json"), "r") as f:
        all_tables = json.load(f)

    # Ensure output directory exists
    os.makedirs(json_dir, exist_ok=True)

    for table_name, columns_def in all_tables.items():
        # columns_def is a dict: {column_name: data_type, ...}

        # Determine which are category or group columns
        category_columns = [col for col in columns_def.keys() if col.endswith("_category")]
        group_columns = [col for col in columns_def.keys() if col.endswith("_group")]

        # All columns are considered required for demonstration purposes
        required_columns = list(columns_def.keys())

        columns_info = []
        for col_name, col_type in columns_def.items():
            # Determine if this column is category/group
            is_category = col_name in category_columns
            is_group = col_name in group_columns
            
            # Initialize permissible values
            permissible_values = []
            
            # If category or group, attempt to load permissible values
            if is_category:
                variable_name = col_name.rsplit("_", 1)[0]
                csv_filename = f"clif_{table_name}_{variable_name}_categories.csv"
                csv_path = os.path.join(mcide_dir, csv_filename)
                if os.path.exists(csv_path):
                    print(f"Found {csv_filename}")
                    df_cat = pd.read_csv(csv_path)
                    # Assuming the first column of the csv contains the permissible values
                    permissible_values = df_cat.iloc[:,0].dropna().unique().tolist()
                else:
                    print(f"Required CSV file {csv_filename} not found")
            
            if is_group:
                variable_name = col_name.rsplit("_", 1)[0]
                csv_filename = f"clif_{table_name}_{variable_name}_groups.csv"
                csv_path = os.path.join(mcide_dir, csv_filename)
                if os.path.exists(csv_path):
                    df_grp = pd.read_csv(csv_path)
                    permissible_values = df_grp.iloc[:,0].dropna().unique().tolist()

            # Build the column structure
            col_info = {
                "name": col_name,
                "data_type": col_type,
                "required": col_name in required_columns,
                "is_category_column": is_category,
                "is_group_column": is_group
            }

            # Add permissible values if category or group
            if is_category or is_group:
                col_info["permissible_values"] = permissible_values

            columns_info.append(col_info)

        # Create the final JSON structure for the table
        table_json = {
            "table_name": table_name,
            "columns": columns_info,
            "required_columns": required_columns,
            "category_columns": category_columns,
            "group_columns": group_columns
        }

        # Write out to a JSON file
        output_path = os.path.join(json_dir, f"{table_name}.json")
        with open(output_path, "w") as outfile:
            json.dump(table_json, outfile, indent=4)

        print(f"Generated {output_path}")

if __name__ == "__main__":
    # Assuming the data directory is always ../data
    mcide_dir = "../mCIDE"
    json_dir = "../data"
    generate_table_jsons(mcide_dir, json_dir)

# Example of how to run the script in the terminal:
# python build_jsons.py

import os
import pandas as pd
import numpy as np

def main():
    print("Loading datasets...")
    # Path resolution relative to this script inside "notebooks" directory
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    
    inst_path = os.path.join(base_dir, 'data', 'raw', 'institutions.csv')
    
    df = pd.read_csv(inst_path)
    
    print("Transforming basic columns...")
    # Standardize column names
    rename_cols = {
        'State': 'state_name',
        'Type Of Institution': 'type_of_institution',
        'Approved Intake (UOM:Number), Scaling Factor:1': 'approved_intake',
        'Institutions (UOM:Number), Scaling Factor:1': 'institutions',
        'Total Approved Institutions (UOM:Number), Scaling Factor:1': 'total_approved_institutions'
    }
    df.rename(columns=rename_cols, inplace=True)
    
    # Standardize state name to uppercase
    df['state_name'] = df['state_name'].astype(str).str.upper()
    
    # Extract numerical year from the text string
    # e.g., "Financial Year (Apr - Mar), 2021" -> 2021
    df['year'] = df['Year'].astype(str).str.extract(r'(\d{4})').astype(float)
    
    print("Handling missing values...")
    # Fill missing values in numerical fields with 0
    numeric_cols = ['approved_intake', 'institutions', 'total_approved_institutions']
    df[numeric_cols] = df[numeric_cols].fillna(0)
    
    # Fill missing categorical fields
    df['type_of_institution'] = df['type_of_institution'].fillna('Unknown')
    
    print("Aggregating data by State, Year, and Type of Institution...")
    # Group by state, year, and type of institution
    grouped = df.groupby(['state_name', 'year', 'type_of_institution'])[numeric_cols].sum().reset_index()
    
    # Optional: cast year back to Int if there are no NaNs
    if not grouped['year'].isna().any():
        grouped['year'] = grouped['year'].astype(int)
    
    # Reorder columns natively to schema definitions
    final_cols = [
        'state_name', 'year', 'type_of_institution', 
        'approved_intake', 'institutions', 'total_approved_institutions'
    ]
    grouped = grouped[final_cols]
    
    # Output
    out_dir = os.path.join(base_dir, 'data', 'silver')
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, 'silver_institutions_state_year.csv')
    
    print(f"Saving format output to: {out_path}")
    grouped.to_csv(out_path, index=False)
    print(f"Process complete. Rows generated: {len(grouped)}")

if __name__ == "__main__":
    main()

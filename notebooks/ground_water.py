import os
import pandas as pd
import numpy as np
import difflib

def calculate_contamination_index(row):
    """
    Calculates a simple 0-7 score based on Bureau of Indian Standards (BIS) limits:
    Arsenic > 0.01 mg/L
    Fluoride > 1.5 mg/L
    Iron > 1.0 mg/L
    Nitrate > 45 mg/L
    TDS > 500 mg/L
    Hardness > 200 mg/L
    pH outside of 6.5 - 8.5
    """
    score = 0
    if pd.notnull(row.get('arsenic_avg')) and row['arsenic_avg'] > 0.01:
        score += 1
    if pd.notnull(row.get('fluoride_avg')) and row['fluoride_avg'] > 1.5:
        score += 1
    if pd.notnull(row.get('iron_avg')) and row['iron_avg'] > 1.0:
        score += 1
    if pd.notnull(row.get('nitrate_avg')) and row['nitrate_avg'] > 45:
        score += 1
    if pd.notnull(row.get('tds_avg')) and row['tds_avg'] > 500:
        score += 1
    if pd.notnull(row.get('hardness_avg')) and row['hardness_avg'] > 200:
        score += 1
    if pd.notnull(row.get('ph_avg')) and (row['ph_avg'] < 6.5 or row['ph_avg'] > 8.5):
        score += 1
    return score

def main():
    print("Loading datasets...")
    # Path resolution relative to this script inside "notebooks" directory
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    
    gw_path = os.path.join(base_dir, 'data', 'raw', 'ground_water.csv')
    lgd_path = os.path.join(base_dir, 'data', 'raw', 'LGD.csv')
    
    df = pd.read_csv(gw_path)
    # Use latin1 as LGD dataset sometimes has varied encoding from government portals
    lgd = pd.read_csv(lgd_path, encoding='latin1')
    
    print("Transforming basic columns...")
    df['state_name'] = df['srcStateName'].astype(str).str.upper()
    df['district_name_raw'] = df['srcDistrictName']
    df['year'] = pd.to_numeric(df['srcYear'], errors='coerce')
    
    print("Fuzzy matching LGD district names...")
    lgd_names = lgd['District Name (In English)'].dropna().unique().tolist()
    lgd_names_lower_map = {name.lower(): name for name in lgd_names}
    
    unique_raw_districts = df['district_name_raw'].dropna().unique()
    match_dict = {}
    
    for raw in unique_raw_districts:
        raw_lower = raw.lower()
        matches = difflib.get_close_matches(raw_lower, lgd_names_lower_map.keys(), n=1, cutoff=0.6)
        if matches:
            best_match_lower = matches[0]
            best_match_original = lgd_names_lower_map[best_match_lower]
            ratio = difflib.SequenceMatcher(None, raw_lower, best_match_lower).ratio() * 100
            match_dict[raw] = {'district_name_clean': best_match_original, 'match_confidence': ratio}
        else:
            match_dict[raw] = {'district_name_clean': None, 'match_confidence': 0.0}
            
    df_match = pd.DataFrame.from_dict(match_dict, orient='index').reset_index().rename(columns={'index': 'district_name_raw'})
    df = df.merge(df_match, on='district_name_raw', how='left')
    
    lgd_subset = lgd[['District Name (In English)', 'District LGD Code']].drop_duplicates(subset=['District Name (In English)'])
    df = df.merge(lgd_subset, left_on='district_name_clean', right_on='District Name (In English)', how='left')
    df.rename(columns={'District LGD Code': 'lgd_district_code'}, inplace=True)
    
    print("Aggregating to district-year level...")
    agg_funcs = {
        'Amount of Arsenic': 'mean',
        'Amount of Fluorine': 'mean',
        'Amount of Iron': 'mean',
        'Amount of Nitrate': 'mean',
        'Amount of Total Dissolved Solids': 'mean',
        'Amount of Potential of Hydrogen': 'mean',
        'Amount of Hardness Total': 'mean',
        'Ground Water Station Name': 'count',
        'match_confidence': 'first',
        'district_name_clean': 'first',
        'lgd_district_code': 'first',
        'state_name': 'first'
    }
    
    grouped = df.groupby(['srcStateName', 'district_name_raw', 'year']).agg(agg_funcs).reset_index()
    
    rename_cols = {
        'Amount of Arsenic': 'arsenic_avg',
        'Amount of Fluorine': 'fluoride_avg',
        'Amount of Iron': 'iron_avg',
        'Amount of Nitrate': 'nitrate_avg',
        'Amount of Total Dissolved Solids': 'tds_avg',
        'Amount of Potential of Hydrogen': 'ph_avg',
        'Amount of Hardness Total': 'hardness_avg',
        'Ground Water Station Name': 'num_stations',
        'srcStateName': '_drop_src_state'
    }
    grouped.rename(columns=rename_cols, inplace=True)
    grouped.drop(columns=['_drop_src_state'], inplace=True, errors='ignore')

    print("Calculating contamination index derived metric...")
    grouped['contamination_index'] = grouped.apply(calculate_contamination_index, axis=1)
    
    # Reorder columns natively to schema definitions
    final_cols = [
        'state_name', 'district_name_raw', 'district_name_clean', 'lgd_district_code', 'year',
        'arsenic_avg', 'fluoride_avg', 'iron_avg', 'nitrate_avg', 'tds_avg', 'ph_avg', 'hardness_avg',
        'contamination_index', 'num_stations', 'match_confidence'
    ]
    grouped = grouped[final_cols]
    
    # Output
    out_dir = os.path.join(base_dir, 'data', 'silver')
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, 'silver_groundwater_district_year.csv')
    
    print(f"Saving format output to: {out_path}")
    grouped.to_csv(out_path, index=False)
    print(f"Process complete. Rows generated: {len(grouped)}")

if __name__ == "__main__":
    main()

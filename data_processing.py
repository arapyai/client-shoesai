# data_processing.py
import pandas as pd
import numpy as np
from collections import Counter

# No JSON loading here anymore. This module focuses on transforming DB query results.

def process_queried_data_for_report(df_flat_selected, df_raw_selected_reconstructed):
    """
    Calculates various metrics based on data queried from the database.
    df_flat_selected: DataFrame of joined Image, ShoeDetections, PersonDemographics for selected marathons.
    df_raw_selected_reconstructed: DataFrame primarily for counting unique images and persons.
                                   Expected columns: marathon_name, filename, has_demographics
    """
    # Default empty structure
    default_metrics = {
        "total_images_selected": 0,
        "total_shoes_detected": 0,
        "unique_brands_count": 0,
        "brand_counts_all_selected": pd.Series(dtype='int'),
        "top_brands_all_selected": pd.DataFrame(columns=['#', 'Marca', 'Count', 'Participação (%)', 'Gráfico']),
        "persons_analyzed_count": 0,
        "leader_brand_info": {"name": "N/A", "count": 0, "percentage": 0},
        "gender_brand_distribution": pd.DataFrame(),
        "race_brand_distribution": pd.DataFrame(),
        "brand_counts_by_marathon": pd.DataFrame(),
        "total_persons_by_marathon": pd.Series(dtype='int'),
        "marathon_specific_data_for_cards": {} # New: for card info
    }

    if df_flat_selected.empty and df_raw_selected_reconstructed.empty:
        return default_metrics

    # Metrics from raw_like selected data (for accurate image/person counts)
    total_images_selected = df_raw_selected_reconstructed['filename'].nunique() if 'filename' in df_raw_selected_reconstructed.columns else 0
    
    persons_analyzed_count = 0
    if 'has_demographics' in df_raw_selected_reconstructed.columns:
        # Count unique filenames where has_demographics is true
        persons_analyzed_count = df_raw_selected_reconstructed[df_raw_selected_reconstructed['has_demographics'] == 1]['filename'].nunique()
    
    total_persons_by_marathon = pd.Series(dtype='int')
    if 'marathon_name' in df_raw_selected_reconstructed.columns and 'has_demographics' in df_raw_selected_reconstructed.columns:
        total_persons_by_marathon = df_raw_selected_reconstructed[df_raw_selected_reconstructed['has_demographics'] == 1].groupby('marathon_name')['filename'].nunique()


    # Prepare marathon-specific data for cards
    marathon_specific_data_for_cards = {}
    if 'marathon_name' in df_raw_selected_reconstructed.columns:
        for marathon_name, group in df_raw_selected_reconstructed.groupby('marathon_name'):
            images_in_marathon = group['filename'].nunique()
            persons_in_marathon = group[group['has_demographics'] == 1]['filename'].nunique()
            
            # Shoe count for this specific marathon from df_flat_selected
            shoes_this_marathon_df = df_flat_selected[
                (df_flat_selected['marathon_name'] == marathon_name) &
                (df_flat_selected['shoe_brand'].notna())
            ]
            shoes_count_this_marathon = len(shoes_this_marathon_df)

            marathon_specific_data_for_cards[marathon_name] = {
                "images_count": images_in_marathon,
                "persons_count": persons_in_marathon,
                "shoes_count": shoes_count_this_marathon
            }


    # Metrics from flattened, shoe-specific data (df_flat_selected)
    df_shoes_only = df_flat_selected.dropna(subset=['shoe_brand']).copy() # Use .copy() to avoid SettingWithCopyWarning
    total_shoes_detected = len(df_shoes_only)
    
    brand_counts_all_selected = pd.Series(dtype='int')
    if not df_shoes_only.empty:
        brand_counts_all_selected = df_shoes_only['shoe_brand'].value_counts()
    unique_brands_count = len(brand_counts_all_selected)

    leader_brand_name = "N/A"
    leader_brand_count = 0
    leader_brand_percentage = 0.0
    if not brand_counts_all_selected.empty:
        leader_brand_name = brand_counts_all_selected.index[0]
        leader_brand_count = int(brand_counts_all_selected.iloc[0])
        leader_brand_percentage = (leader_brand_count / total_shoes_detected) * 100 if total_shoes_detected > 0 else 0.0

    top_n = 10
    top_brands_series = brand_counts_all_selected.head(top_n)
    top_brands_df = pd.DataFrame({
        'Marca': top_brands_series.index,
        'Count': top_brands_series.values.astype(int)
    })

    if not top_brands_df.empty:
        top_brands_df['#'] = range(1, len(top_brands_df) + 1)
        top_brands_df['Participação (%)'] = (top_brands_df['Count'] / total_shoes_detected * 100).round(1) if total_shoes_detected > 0 else 0.0
        max_count_for_bar = top_brands_df['Count'].max()
        if pd.isna(max_count_for_bar) or max_count_for_bar == 0: max_count_for_bar = 1 # Handle empty or all-zero case
        
        top_brands_df['Gráfico'] = top_brands_df['Count'].apply(
            lambda x: "█" * int(round((x / max_count_for_bar) * 10)) if max_count_for_bar > 0 and pd.notna(x) else ""
        )
        top_brands_df = top_brands_df[['#', 'Marca', 'Count', 'Participação (%)', 'Gráfico']]
    else:
        top_brands_df = pd.DataFrame(columns=['#', 'Marca', 'Count', 'Participação (%)', 'Gráfico'])

    gender_brand_dist = pd.DataFrame()
    if not df_shoes_only.empty and 'person_gender' in df_shoes_only.columns and 'shoe_brand' in df_shoes_only.columns:
        df_gender_known = df_shoes_only[(df_shoes_only['person_gender'].notna()) & (df_shoes_only['person_gender'] != 'Desconhecido') & (df_shoes_only['shoe_brand'].notna())]
        if not df_gender_known.empty:
            gender_brand_dist = df_gender_known.groupby(['person_gender', 'shoe_brand']).size().unstack(fill_value=0)

    race_brand_dist = pd.DataFrame()
    if not df_shoes_only.empty and 'person_race' in df_shoes_only.columns and 'shoe_brand' in df_shoes_only.columns:
        df_race_known = df_shoes_only[(df_shoes_only['person_race'].notna()) & (df_shoes_only['person_race'] != 'Desconhecido') & (df_shoes_only['shoe_brand'].notna())]
        if not df_race_known.empty:
            race_brand_dist = df_race_known.groupby(['person_race', 'shoe_brand']).size().unstack(fill_value=0)
    
    brand_counts_by_marathon = pd.DataFrame()
    if not df_shoes_only.empty and 'marathon_name' in df_shoes_only.columns and 'shoe_brand' in df_shoes_only.columns:
        brand_counts_by_marathon = df_shoes_only.groupby('marathon_name')['shoe_brand'].value_counts().unstack(fill_value=0)

    return {
        "total_images_selected": total_images_selected,
        "total_shoes_detected": total_shoes_detected,
        "unique_brands_count": unique_brands_count,
        "brand_counts_all_selected": brand_counts_all_selected,
        "top_brands_all_selected": top_brands_df,
        "persons_analyzed_count": persons_analyzed_count,
        "leader_brand_info": {
            "name": leader_brand_name,
            "count": int(leader_brand_count),
            "percentage": leader_brand_percentage
        },
        "gender_brand_distribution": gender_brand_dist,
        "race_brand_distribution": race_brand_dist,
        "brand_counts_by_marathon": brand_counts_by_marathon,
        "total_persons_by_marathon": total_persons_by_marathon,
        "marathon_specific_data_for_cards": marathon_specific_data_for_cards # Added this
    }
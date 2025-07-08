import pandas as pd
import numpy as np

def group_small_categories_as_others(chart_data: pd.DataFrame, column_name: str) -> pd.DataFrame:
    # Group small brands into 'Outros' category for each run_category
    min_percentage = 0.02  # Minimum percentage to consider a brand significant
    
    # Process each run_category separately
    others_rows = []
    for category in chart_data[column_name].unique():
        # Filter data for this category
        category_data = chart_data[chart_data[column_name] == category]
        
        # Find small brands in this category
        small_brands = category_data[category_data['percentage'] < min_percentage]['shoe_brand'].unique()

        if len(small_brands) > 0:
            # Create an 'Outros' row for this category
            others_row = category_data[category_data['shoe_brand'].isin(small_brands)].sum()
            others_row['shoe_brand'] = 'Outros'
            others_row[column_name] = category
            others_rows.append(others_row)
            
            # Remove small brands from this category
            chart_data = chart_data[~((chart_data[column_name] == category) & 
                                     (chart_data['shoe_brand'].isin(small_brands)))]

    # Add all 'Outros' rows to the dataframe
    if others_rows:
        chart_data = pd.concat([chart_data, pd.DataFrame(others_rows)], ignore_index=True)
    
    return chart_data
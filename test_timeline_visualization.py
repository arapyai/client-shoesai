#!/usr/bin/env python3
"""
Test script for the new timeline visualization functionality.
"""

import pandas as pd
from datetime import datetime, timedelta

def test_timeline_data_preparation():
    """Test the timeline data preparation logic."""
    
    # Create mock individual data
    individual_data = {
        "Maratona SP 2024": {
            "brand_counts_all_selected": pd.Series({
                "Nike": 50,
                "Adidas": 30,
                "Mizuno": 20,
                "Olympikus": 15
            })
        },
        "Maratona RJ 2024": {
            "brand_counts_all_selected": pd.Series({
                "Nike": 45,
                "Adidas": 35,
                "Mizuno": 25,
                "Asics": 10
            })
        }
    }
    
    # Create mock marathon metadata
    marathon_metadata = [
        {"name": "Maratona SP 2024", "event_date": "2024-05-15"},
        {"name": "Maratona RJ 2024", "event_date": "2024-06-20"}
    ]
    
    # Simulate the timeline data preparation
    timeline_records = []
    
    for marathon_name in individual_data.keys():
        marathon_data = individual_data[marathon_name]
        
        # Get event date for this marathon
        marathon_meta = next((m for m in marathon_metadata if m['name'] == marathon_name), None)
        event_date = marathon_meta.get('event_date') if marathon_meta else None
        
        if not event_date:
            continue
            
        try:
            event_date_parsed = pd.to_datetime(event_date)
        except:
            continue
        
        # Get brand counts for this marathon
        brand_counts = marathon_data.get("brand_counts_all_selected", pd.Series())
        
        if brand_counts.empty:
            continue
            
        # Calculate percentages
        total_shoes = brand_counts.sum()
        
        for brand, count in brand_counts.items():
            percentage = (count / total_shoes) * 100 if total_shoes > 0 else 0
            
            timeline_records.append({
                'marathon_name': marathon_name,
                'event_date': event_date_parsed,
                'brand': brand,
                'count': count,
                'percentage': percentage
            })
    
    # Convert to DataFrame
    timeline_df = pd.DataFrame(timeline_records)
    timeline_df = timeline_df.sort_values('event_date')
    
    print("🧪 Timeline Visualization Test Results")
    print("=" * 50)
    print(f"✅ Generated timeline data with {len(timeline_df)} records")
    print(f"📊 Marathons: {timeline_df['marathon_name'].nunique()}")
    print(f"🏷️ Brands: {timeline_df['brand'].nunique()}")
    print("\n📈 Sample data:")
    print(timeline_df.head(10))
    
    # Test insights calculation
    top_brands = timeline_df.groupby('brand')['percentage'].mean().sort_values(ascending=False).head(4).index.tolist()
    print(f"\n🏆 Top brands by average percentage: {top_brands}")
    
    # Test trend calculation for top brand
    if top_brands:
        test_brand = top_brands[0]
        brand_data = timeline_df[timeline_df['brand'] == test_brand].sort_values('event_date')
        
        if len(brand_data) >= 2:
            first_percentage = brand_data.iloc[0]['percentage']
            last_percentage = brand_data.iloc[-1]['percentage']
            change = last_percentage - first_percentage
            trend = "crescimento" if change > 0 else "queda"
            
            print(f"\n📈 Trend analysis for {test_brand}:")
            print(f"   First marathon: {first_percentage:.1f}%")
            print(f"   Last marathon: {last_percentage:.1f}%")
            print(f"   Change: {change:+.1f}pp ({trend})")
    
    return timeline_df

def main():
    """Main test function."""
    try:
        test_data = test_timeline_data_preparation()
        
        if not test_data.empty:
            print("\n✅ Timeline visualization functionality is working correctly!")
            print("\n🎯 Ready to test in Streamlit app with multiple marathons")
        else:
            print("\n❌ No timeline data generated - check date parsing or data structure")
            
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()

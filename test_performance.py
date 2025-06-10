#!/usr/bin/env python3
"""
Performance test script to demonstrate the optimization improvements.
"""

import time
import pandas as pd
from data_processing import process_queried_data_for_report, process_multiple_marathons_efficiently
from database import get_data_for_selected_marathons_db, get_marathon_list_from_db

def test_old_approach(marathon_ids):
    """Test the old approach of processing each marathon individually."""
    start_time = time.time()
    
    individual_results = {}
    for marathon_id in marathon_ids:
        df_flat, df_raw = get_data_for_selected_marathons_db([marathon_id])
        individual_results[marathon_id] = process_queried_data_for_report(df_flat, df_raw)
    
    # Combined processing
    df_flat_all, df_raw_all = get_data_for_selected_marathons_db(marathon_ids)
    combined_result = process_queried_data_for_report(df_flat_all, df_raw_all)
    
    end_time = time.time()
    return end_time - start_time, len(individual_results), combined_result

def test_new_approach(marathon_ids):
    """Test the new optimized batch approach."""
    start_time = time.time()
    
    combined_result, individual_results = process_multiple_marathons_efficiently(marathon_ids)
    
    end_time = time.time()
    return end_time - start_time, len(individual_results), combined_result

def main():
    print("🚀 Performance Test: Marathon Data Processing")
    print("=" * 50)
    
    # Get available marathons
    marathons = get_marathon_list_from_db()
    
    if len(marathons) < 2:
        print("❌ Need at least 2 marathons in database for meaningful test")
        return
    
    # Test with first few marathons (limit to avoid very long tests)
    test_marathon_ids = [m['id'] for m in marathons[:min(3, len(marathons))]]
    marathon_names = [m['name'] for m in marathons[:min(3, len(marathons))]]
    
    print(f"📊 Testing with {len(test_marathon_ids)} marathons:")
    for name in marathon_names:
        print(f"  • {name}")
    print()
    
    # Test old approach
    print("🐌 Testing OLD approach (individual processing)...")
    try:
        old_time, old_count, old_combined = test_old_approach(test_marathon_ids)
        print(f"   ✅ Completed in {old_time:.2f} seconds")
        print(f"   📈 Processed {old_count} individual marathons")
        print(f"   🎯 Combined total shoes: {old_combined.get('total_shoes_detected', 0)}")
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        old_time = float('inf')
    
    print()
    
    # Test new approach
    print("⚡ Testing NEW approach (batch processing)...")
    try:
        new_time, new_count, new_combined = test_new_approach(test_marathon_ids)
        print(f"   ✅ Completed in {new_time:.2f} seconds")
        print(f"   📈 Processed {new_count} individual marathons")
        print(f"   🎯 Combined total shoes: {new_combined.get('total_shoes_detected', 0)}")
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        new_time = float('inf')
    
    print()
    
    # Performance comparison
    if old_time != float('inf') and new_time != float('inf'):
        improvement = (old_time - new_time) / old_time * 100
        speedup = old_time / new_time
        
        print("🏆 PERFORMANCE RESULTS:")
        print(f"   ⏱️  Old approach: {old_time:.2f}s")
        print(f"   ⚡ New approach: {new_time:.2f}s")
        print(f"   📊 Improvement: {improvement:.1f}% faster")
        print(f"   🚀 Speedup: {speedup:.1f}x")
        
        if improvement > 0:
            print(f"   🎉 SUCCESS! New approach is {improvement:.1f}% faster!")
        else:
            print(f"   ⚠️  Old approach was faster by {abs(improvement):.1f}%")
    
    print("\n" + "=" * 50)
    print("✨ Test completed!")

if __name__ == "__main__":
    main()

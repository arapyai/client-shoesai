#!/usr/bin/env python3
"""
Performance test script to demonstrate the pre-computed metrics optimization.
"""

import time
import pandas as pd
from database import (
    get_marathon_list_from_db, 
    get_data_for_selected_marathons_db, 
    get_precomputed_marathon_metrics,
    get_individual_marathon_metrics
)
from data_processing import process_queried_data_for_report

def test_old_realtime_approach(marathon_ids):
    """Test the old real-time calculation approach."""
    start_time = time.time()
    
    # Individual processing
    individual_results = {}
    for marathon_id in marathon_ids:
        df_flat, df_raw = get_data_for_selected_marathons_db([marathon_id])
        individual_results[marathon_id] = process_queried_data_for_report(df_flat, df_raw)
    
    # Combined processing
    df_flat_all, df_raw_all = get_data_for_selected_marathons_db(marathon_ids)
    combined_result = process_queried_data_for_report(df_flat_all, df_raw_all)
    
    end_time = time.time()
    return end_time - start_time, len(individual_results), combined_result

def test_new_precomputed_approach(marathon_ids):
    """Test the new pre-computed metrics approach."""
    start_time = time.time()
    
    # Individual processing using pre-computed metrics
    individual_results = get_individual_marathon_metrics(marathon_ids)
    
    # Combined processing using pre-computed metrics
    combined_result = get_precomputed_marathon_metrics(marathon_ids)
    
    end_time = time.time()
    return end_time - start_time, len(individual_results), combined_result

def main():
    print("🚀 Performance Test: Pre-computed Metrics vs Real-time Calculation")
    print("=" * 65)
    
    # Get available marathons
    marathons = get_marathon_list_from_db()
    
    if len(marathons) < 2:
        print("❌ Need at least 2 marathons in database for meaningful test")
        print("💡 Import some data first using the importador page")
        return
    
    # Test with first few marathons (limit to avoid very long tests)
    test_marathon_ids = [m['id'] for m in marathons[:min(3, len(marathons))]]
    marathon_names = [m['name'] for m in marathons[:min(3, len(marathons))]]
    
    print(f"📊 Testing with {len(test_marathon_ids)} marathons:")
    for name in marathon_names:
        print(f"  • {name}")
    print()
    
    # Test old approach
    print("🐌 Testing OLD approach (real-time calculation)...")
    try:
        old_time, old_count, old_combined = test_old_realtime_approach(test_marathon_ids)
        print(f"   ✅ Completed in {old_time:.2f} seconds")
        print(f"   📈 Processed {old_count} individual marathons")
        print(f"   🎯 Combined total shoes: {old_combined.get('total_shoes_detected', 0)}")
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        old_time = float('inf')
    
    print()
    
    # Test new approach
    print("⚡ Testing NEW approach (pre-computed metrics)...")
    try:
        new_time, new_count, new_combined = test_new_precomputed_approach(test_marathon_ids)
        print(f"   ✅ Completed in {new_time:.2f} seconds")
        print(f"   📈 Processed {new_count} individual marathons")
        print(f"   🎯 Combined total shoes: {new_combined.get('total_shoes_detected', 0)}")
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        new_time = float('inf')
    
    print()
    
    # Calculate improvement
    if old_time != float('inf') and new_time != float('inf'):
        if new_time > 0:
            speedup = old_time / new_time
            improvement = ((old_time - new_time) / old_time) * 100
            print("📈 PERFORMANCE RESULTS:")
            print(f"   🚀 Speedup: {speedup:.1f}x faster")
            print(f"   📉 Time reduction: {improvement:.1f}%")
            print(f"   ⏱️  Time saved: {old_time - new_time:.2f} seconds")
            
            if speedup > 2:
                print("   🏆 EXCELLENT: Over 2x speedup achieved!")
            elif speedup > 1.5:
                print("   🎉 GREAT: Significant performance improvement!")
            elif speedup > 1.1:
                print("   👍 GOOD: Noticeable performance improvement")
            else:
                print("   ⚠️  MARGINAL: Small improvement, may need further optimization")
        else:
            print("   ⚡ INSTANT: Pre-computed metrics are nearly instantaneous!")
    else:
        print("   ❌ Could not calculate performance comparison due to errors")
    
    print()
    print("🎯 OPTIMIZATION SUMMARY:")
    print("   • Pre-computed metrics are calculated once during import")
    print("   • Report generation uses stored metrics instead of real-time calculation")
    print("   • Significant performance improvement for end users")
    print("   • Better scalability as data grows")

if __name__ == "__main__":
    main()

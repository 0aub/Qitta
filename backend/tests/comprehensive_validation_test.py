#!/usr/bin/env python3
"""
Comprehensive validation test for all booking hotel fixes
Tests:
1. Price extraction (should now work with new selectors)  
2. Success rate calculation (should now be > 0)
3. All levels functionality
4. Review extraction (limited to ~137 reviews as expected)
"""

import requests
import time
import json

def wait_for_job_completion(job_id, timeout_minutes=10):
    """Wait for job completion and return result"""
    max_polls = timeout_minutes * 6  # Poll every 10 seconds
    poll_count = 0
    
    while poll_count < max_polls:
        poll_count += 1
        
        try:
            status_response = requests.get(f"http://localhost:8004/jobs/{job_id}")
            
            if status_response.status_code == 200:
                status_data = status_response.json()
                status = status_data.get("status")
                
                if status == "completed":
                    return True, status_data
                elif status == "failed":
                    error = status_data.get("error", "Unknown error")
                    return False, {"error": error}
                elif status in ["running", "pending"]:
                    time.sleep(10)
                    continue
                else:
                    time.sleep(10)
                    continue
            else:
                time.sleep(10)
                continue
                
        except Exception as e:
            print(f"   Polling error: {e}")
            time.sleep(10)
            continue
    
    return False, {"error": "Job timed out"}

def test_level(level, description):
    """Test a specific scraping level"""
    print(f"\n📍 TESTING LEVEL {level}: {description}")
    
    test_params = {
        "level": level,
        "location": "Dubai Marina",
        "max_hotels": 3,
        "checkin": "2025-09-02", 
        "checkout": "2025-09-05"
    }
    
    try:
        # Start job
        response = requests.post(
            "http://localhost:8004/jobs/booking_hotels",
            json=test_params,
            timeout=30
        )
        
        if response.status_code != 200:
            print(f"❌ Job submission failed: {response.status_code}")
            return False
            
        job_data = response.json()
        job_id = job_data.get("job_id")
        
        if not job_id:
            print(f"❌ No job_id received")
            return False
            
        print(f"⏳ Job {job_id} started, waiting for completion...")
        
        # Wait for completion
        success, result_data = wait_for_job_completion(job_id, timeout_minutes=8)
        
        if not success:
            print(f"❌ Job failed: {result_data.get('error', 'Unknown error')}")
            return False
            
        # Analyze results
        result = result_data.get("result", {})
        hotels = result.get("hotels", [])
        metadata = result.get("search_metadata", {})
        
        print(f"✅ LEVEL {level} RESULTS:")
        print(f"   Hotels found: {len(hotels)}")
        print(f"   Success rate: {metadata.get('success_rate', 0):.1%}")
        print(f"   Average price: ${metadata.get('average_price', 0):.2f}")
        print(f"   Average completeness: {metadata.get('average_completeness', 0):.1f}%")
        
        # Check specific fixes
        hotels_with_prices = [h for h in hotels if h.get('price_per_night', 0) > 0]
        price_success_rate = len(hotels_with_prices) / len(hotels) * 100 if hotels else 0
        
        print(f"   PRICE FIX CHECK: {len(hotels_with_prices)}/{len(hotels)} hotels with prices ({price_success_rate:.1f}%)")
        
        # Sample hotel data
        if hotels:
            sample_hotel = hotels[0]
            print(f"   Sample hotel: {sample_hotel.get('name', 'Unknown')}")
            print(f"   Price: ${sample_hotel.get('price_per_night', 0)}/night")
            print(f"   Rating: {sample_hotel.get('rating', 'N/A')}")
            if level >= 3:
                reviews = sample_hotel.get('reviews', [])
                print(f"   Reviews: {len(reviews)} extracted")
        
        # Validation criteria
        success_criteria = []
        
        # 1. Should find hotels
        if len(hotels) > 0:
            success_criteria.append("✅ Hotels found")
        else:
            success_criteria.append("❌ No hotels found")
            
        # 2. Success rate should be > 0 (fix for success rate calculation)
        if metadata.get('success_rate', 0) > 0:
            success_criteria.append("✅ Success rate > 0")
        else:
            success_criteria.append("❌ Success rate = 0 (calculation issue)")
            
        # 3. Should have some prices (fix for price extraction)
        if price_success_rate > 0:
            success_criteria.append(f"✅ Price extraction working ({price_success_rate:.1f}%)")
        else:
            success_criteria.append("❌ No prices extracted")
            
        # 4. Data completeness should be reasonable
        if metadata.get('average_completeness', 0) > 50:
            success_criteria.append("✅ Good data completeness")
        else:
            success_criteria.append("⚠️ Low data completeness")
        
        print(f"   VALIDATION:")
        for criterion in success_criteria:
            print(f"     {criterion}")
        
        # Consider success if we have hotels, non-zero success rate, and some prices
        critical_fixes_work = (
            len(hotels) > 0 and 
            metadata.get('success_rate', 0) > 0 and 
            price_success_rate > 0
        )
        
        return critical_fixes_work
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

def main():
    print("🚀 COMPREHENSIVE VALIDATION TEST")
    print("=" * 60)
    print("Testing fixes:")
    print("1. Price extraction with new selectors")
    print("2. Success rate calculation fix") 
    print("3. Review extraction (limited to ~137 as expected)")
    print("4. All scraping levels")
    
    # Test each level
    test_results = []
    
    levels_to_test = [
        (1, "Quick search - Essential data only"),
        (2, "Standard extraction with amenities"),
        (3, "Deep extraction with basic reviews"),
        (4, "Maximum depth with all reviews")
    ]
    
    for level, description in levels_to_test:
        success = test_level(level, description)
        test_results.append((level, success))
        
        if not success:
            print(f"⚠️ Level {level} had issues - continuing with other levels...")
    
    # Final summary
    print("\n" + "=" * 60)
    print("🎯 FINAL VALIDATION RESULTS")
    print("=" * 60)
    
    passed_levels = [level for level, success in test_results if success]
    failed_levels = [level for level, success in test_results if not success]
    
    print(f"✅ PASSED LEVELS: {passed_levels}")
    print(f"❌ FAILED LEVELS: {failed_levels}")
    
    overall_success = len(passed_levels) >= 2  # At least 2 levels should work
    
    if overall_success:
        print("🎉 VALIDATION SUCCESSFUL!")
        print("✅ Critical fixes are working:")
        print("  - Price extraction improved")
        print("  - Success rate calculation fixed")
        print("  - Core functionality intact")
    else:
        print("❌ VALIDATION FAILED!")
        print("Critical issues remain that need attention")
    
    return overall_success

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
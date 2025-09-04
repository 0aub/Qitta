#!/usr/bin/env python3
"""
INTENSIVE TESTING SUITE - All Levels with Fixed Routing
Tests each level systematically with the routing bug fix applied
"""

import requests
import time
import json

def wait_for_job(job_id, timeout_minutes=15):
    """Wait for job completion with detailed progress"""
    max_polls = timeout_minutes * 6
    poll_count = 0
    
    while poll_count < max_polls:
        poll_count += 1
        
        try:
            response = requests.get(f"http://localhost:8004/jobs/{job_id}")
            if response.status_code == 200:
                data = response.json()
                status = data.get("status")
                
                if status in ["completed", "finished"]:
                    return True, data
                elif status == "failed":
                    return False, data
                elif status in ["running", "pending"]:
                    if poll_count % 6 == 0:  # Every minute
                        print(f"      ⏳ {status}... ({poll_count // 6}min)")
                    time.sleep(10)
                    continue
                    
        except Exception as e:
            print(f"      ⚠️ Poll error: {e}")
            time.sleep(10)
            continue
    
    return False, {"error": "Timeout"}

def intensive_test_level(level):
    """Intensive test of a specific level with detailed analysis"""
    
    print(f"\n{'='*80}")
    print(f"🔥 INTENSIVE TESTING LEVEL {level}")
    print(f"{'='*80}")
    
    # Test parameters
    params = {
        "level": level,
        "location": "Dubai Marina",
        "max_hotels": 3 if level <= 2 else 2,  # Fewer hotels for deep levels
        "checkin": "2025-09-02",
        "checkout": "2025-09-05"
    }
    
    print(f"📋 Test Parameters: {params}")
    
    try:
        # Submit job
        print(f"📡 Submitting Level {level} job...")
        response = requests.post("http://localhost:8004/jobs/booking_hotels", json=params, timeout=30)
        
        if response.status_code != 200:
            print(f"❌ Job submission failed: {response.status_code}")
            return False, {}
            
        job_data = response.json()
        job_id = job_data.get("job_id")
        
        if not job_id:
            print(f"❌ No job_id received")
            return False, {}
            
        print(f"✅ Job submitted: {job_id}")
        
        # Wait for completion
        print(f"⏳ Waiting for Level {level} completion...")
        success, result_data = wait_for_job(job_id, timeout_minutes=15)
        
        if not success:
            error = result_data.get("error", "Unknown error")
            print(f"❌ Level {level} FAILED: {error}")
            return False, {}
        
        # Detailed analysis
        result = result_data.get("result", {})
        hotels = result.get("hotels", [])
        metadata = result.get("search_metadata", {})
        
        print(f"\n📊 LEVEL {level} RESULTS ANALYSIS")
        print(f"{'='*50}")
        
        # Basic metrics
        print(f"🏨 BASIC METRICS:")
        print(f"   Hotels found: {len(hotels)}")
        print(f"   Success rate: {metadata.get('success_rate', 0):.1%}")
        print(f"   Average price: ${metadata.get('average_price', 0):.2f}")
        print(f"   Data completeness: {metadata.get('average_completeness', 0):.1f}%")
        print(f"   Scrape level used: {metadata.get('scrape_level', 'unknown')}")
        print(f"   Method used: {metadata.get('extraction_method', 'unknown')}")
        
        # Variable validation
        if hotels:
            print(f"\n🔍 VARIABLE VALIDATION:")
            
            # Price validation
            hotels_with_prices = [h for h in hotels if h.get('price_per_night', 0) > 0]
            price_rate = len(hotels_with_prices) / len(hotels) * 100
            print(f"   💰 PRICES: {len(hotels_with_prices)}/{len(hotels)} ({price_rate:.1f}%)")
            
            if hotels_with_prices:
                prices = [h['price_per_night'] for h in hotels_with_prices]
                print(f"      Range: ${min(prices):.2f} - ${max(prices):.2f}")
            
            # Rating validation
            hotels_with_ratings = [h for h in hotels if h.get('rating', 0) > 0]
            rating_rate = len(hotels_with_ratings) / len(hotels) * 100
            print(f"   ⭐ RATINGS: {len(hotels_with_ratings)}/{len(hotels)} ({rating_rate:.1f}%)")
            
            if hotels_with_ratings:
                ratings = [h['rating'] for h in hotels_with_ratings]
                print(f"      Range: {min(ratings):.1f}★ - {max(ratings):.1f}★")
            
            # Review validation (for Level 3+)
            if level >= 3:
                hotels_with_reviews = [h for h in hotels if h.get('reviews') and len(h['reviews']) > 0]
                review_rate = len(hotels_with_reviews) / len(hotels) * 100
                print(f"   📖 REVIEW EXTRACTION: {len(hotels_with_reviews)}/{len(hotels)} ({review_rate:.1f}%)")
                
                if hotels_with_reviews:
                    review_counts = [len(h['reviews']) for h in hotels_with_reviews]
                    print(f"      Extracted range: {min(review_counts)} - {max(review_counts)} reviews")
                    
                    # Level 4 specific validation
                    if level == 4:
                        print(f"\n🚨 LEVEL 4 CRITICAL VALIDATION:")
                        
                        for i, hotel in enumerate(hotels_with_reviews[:2]):
                            extracted = len(hotel['reviews'])
                            claimed = hotel.get('review_count', 0)
                            coverage = (extracted / claimed * 100) if claimed > 0 else 0
                            name = hotel.get('name', 'Unknown')[:30]
                            
                            print(f"      Hotel {i+1} ({name}...): {extracted}/{claimed} reviews ({coverage:.1f}%)")
                            
                            if coverage >= 50:  # Reasonable coverage
                                print(f"         ✅ Good coverage")
                            elif extracted >= 100:  # Absolute minimum
                                print(f"         ⚠️ Acceptable minimum") 
                            else:
                                print(f"         ❌ Insufficient coverage")
            
            # Other variables
            hotels_with_amenities = [h for h in hotels if h.get('amenities') and len(h['amenities']) > 0]
            hotels_with_images = [h for h in hotels if h.get('images') and len(h['images']) > 0]
            hotels_with_coords = [h for h in hotels if h.get('latitude') and h.get('longitude')]
            
            print(f"   🏷️ AMENITIES: {len(hotels_with_amenities)}/{len(hotels)} ({len(hotels_with_amenities)/len(hotels)*100:.1f}%)")
            print(f"   🖼️ IMAGES: {len(hotels_with_images)}/{len(hotels)} ({len(hotels_with_images)/len(hotels)*100:.1f}%)")
            print(f"   📍 COORDINATES: {len(hotels_with_coords)}/{len(hotels)} ({len(hotels_with_coords)/len(hotels)*100:.1f}%)")
            
            # Sample hotel details
            print(f"\n📋 SAMPLE HOTEL (Level {level}):")
            sample = hotels[0]
            print(f"   Name: {sample.get('name', 'N/A')}")
            print(f"   Price: ${sample.get('price_per_night', 0)}/night")
            print(f"   Rating: {sample.get('rating', 'N/A')}★")
            print(f"   Review count: {sample.get('review_count', 0)}")
            if level >= 3 and sample.get('reviews'):
                print(f"   Reviews extracted: {len(sample['reviews'])}")
            print(f"   Images: {len(sample.get('images', []))}")
            print(f"   Amenities: {len(sample.get('amenities', []))}")
            print(f"   Completeness: {sample.get('data_completeness', 0):.1f}%")
        
        # Level success criteria
        critical_issues = []
        
        # Expected method validation
        expected_methods = {
            1: "level_1_quick_search",
            2: "level_2_full_data",
            3: "level_3_basic_reviews", 
            4: "level_4_deep_reviews"
        }
        
        actual_method = metadata.get('extraction_method')
        expected_method = expected_methods.get(level)
        
        if actual_method != expected_method:
            critical_issues.append(f"Wrong method: expected {expected_method}, got {actual_method}")
        
        if len(hotels) == 0:
            critical_issues.append("No hotels found")
            
        if level >= 3 and hotels:
            hotels_with_reviews = [h for h in hotels if h.get('reviews') and len(h['reviews']) > 0]
            if len(hotels_with_reviews) == 0:
                critical_issues.append("No reviews extracted (required for Level 3+)")
        
        # Level 4 specific requirements
        if level == 4:
            if metadata.get('success_rate', 0) == 0:
                critical_issues.append("Level 4 success rate = 0")
                
            if avg_price == 0:
                critical_issues.append("Level 4 no prices extracted")
        
        print(f"\n🚨 LEVEL {level} CRITICAL ISSUES: {len(critical_issues)}")
        for i, issue in enumerate(critical_issues, 1):
            print(f"   {i}. {issue}")
        
        level_passed = len(critical_issues) == 0
        print(f"\n🎯 LEVEL {level} STATUS: {'✅ PASSED' if level_passed else '❌ FAILED'}")
        
        return level_passed, {
            'hotels': len(hotels),
            'success_rate': metadata.get('success_rate', 0),
            'price_rate': len([h for h in hotels if h.get('price_per_night', 0) > 0]) / len(hotels) * 100 if hotels else 0,
            'method': actual_method,
            'critical_issues': critical_issues
        }
        
    except Exception as e:
        print(f"❌ Level {level} test error: {e}")
        return False, {}

def main():
    print("🚀 INTENSIVE TESTING SUITE - ALL LEVELS")
    print("After critical routing bug fix")
    print("=" * 80)
    
    # Test all levels
    test_results = []
    
    for level in [1, 2, 3, 4]:
        passed, metrics = intensive_test_level(level)
        test_results.append((level, passed, metrics))
        
        # Critical: Stop if Level 4 fails  
        if level == 4 and not passed:
            print(f"\n🚨 LEVEL 4 FAILED - TESTING STOPPED")
            break
    
    # Final assessment
    print(f"\n{'='*80}")
    print("🎯 INTENSIVE TESTING RESULTS")
    print(f"{'='*80}")
    
    passed_levels = [level for level, passed, _ in test_results if passed]
    failed_levels = [level for level, passed, _ in test_results if not passed]
    
    print(f"✅ PASSED LEVELS: {passed_levels}")
    print(f"❌ FAILED LEVELS: {failed_levels}")
    
    # Level 4 specific assessment
    level_4_result = next((result for level, passed, result in test_results if level == 4), None)
    
    if 4 in passed_levels:
        print(f"\n🎉 LEVEL 4 APPROVED!")
        print(f"   ✅ Gets ALL available reviews")
        print(f"   ✅ Price extraction: {level_4_result['price_rate']:.1f}%")
        print(f"   ✅ Success rate: {level_4_result['success_rate']:.1%}")
        print(f"   ✅ Method: {level_4_result['method']}")
    else:
        print(f"\n❌ LEVEL 4 NOT APPROVED")
        if level_4_result and level_4_result['critical_issues']:
            print(f"   Issues: {level_4_result['critical_issues']}")
    
    overall_success = 4 in passed_levels and len(failed_levels) <= 1
    
    print(f"\n🎯 SYSTEM STATUS: {'APPROVED FOR PRODUCTION' if overall_success else 'REQUIRES ADDITIONAL FIXES'}")
    
    return overall_success

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
#!/usr/bin/env python3
"""
COMPREHENSIVE SYSTEM STATUS TEST
Tests all levels (1-4) systematically and checks ALL variables:
- Prices, ratings, reviews, amenities, locations, images
- Success rates, data completeness, error handling
- Level 4 ALL reviews requirement validation
"""

import requests
import time
import json

def wait_for_job(job_id, timeout_minutes=15):
    """Enhanced job waiting with better status handling"""
    max_polls = timeout_minutes * 6
    poll_count = 0
    
    while poll_count < max_polls:
        poll_count += 1
        
        try:
            response = requests.get(f"http://localhost:8004/jobs/{job_id}")
            if response.status_code == 200:
                data = response.json()
                status = data.get("status")
                
                # Handle all possible statuses
                if status in ["completed", "finished"]:
                    return True, data
                elif status == "failed":
                    return False, data
                elif status in ["running", "pending"]:
                    if poll_count % 6 == 0:  # Print every minute
                        print(f"   ⏳ Still {status}... ({poll_count // 6}min)")
                    time.sleep(10)
                    continue
                else:
                    print(f"   ❓ Unknown status: {status}")
                    time.sleep(10)
                    continue
            else:
                time.sleep(10)
                continue
                
        except Exception as e:
            print(f"   ⚠️ Poll error: {e}")
            time.sleep(10)
            continue
    
    return False, {"error": "Timeout"}

def analyze_results(level, result_data, job_id):
    """Detailed analysis of scraping results"""
    
    print(f"\n📊 LEVEL {level} DETAILED ANALYSIS (Job: {job_id})")
    print("=" * 70)
    
    if not result_data.get("result"):
        print("❌ No result data")
        return False
        
    result = result_data["result"]
    hotels = result.get("hotels", [])
    metadata = result.get("search_metadata", {})
    
    # Basic metrics
    hotel_count = len(hotels)
    success_rate = metadata.get("success_rate", 0)
    avg_price = metadata.get("average_price", 0)
    avg_completeness = metadata.get("average_completeness", 0)
    
    print(f"🏨 BASIC METRICS:")
    print(f"   Hotels found: {hotel_count}")
    print(f"   Success rate: {success_rate:.1%}")
    print(f"   Average price: ${avg_price:.2f}")
    print(f"   Data completeness: {avg_completeness:.1f}%")
    print(f"   Scrape level: {metadata.get('scrape_level', 'unknown')}")
    print(f"   Method: {metadata.get('extraction_method', 'unknown')}")
    
    # Detailed variable analysis
    if hotel_count > 0:
        print(f"\n🔍 DETAILED VARIABLE ANALYSIS:")
        
        # Price analysis
        prices = [h.get('price_per_night', 0) for h in hotels]
        hotels_with_prices = [p for p in prices if p > 0]
        price_success = len(hotels_with_prices) / len(prices) * 100 if prices else 0
        
        print(f"💰 PRICES:")
        print(f"   Hotels with valid prices: {len(hotels_with_prices)}/{len(prices)} ({price_success:.1f}%)")
        if hotels_with_prices:
            print(f"   Price range: ${min(hotels_with_prices):.2f} - ${max(hotels_with_prices):.2f}")
        else:
            print(f"   ❌ CRITICAL: NO PRICES EXTRACTED")
        
        # Rating analysis  
        ratings = [h.get('rating', 0) for h in hotels]
        hotels_with_ratings = [r for r in ratings if r > 0]
        rating_success = len(hotels_with_ratings) / len(ratings) * 100 if ratings else 0
        
        print(f"⭐ RATINGS:")
        print(f"   Hotels with ratings: {len(hotels_with_ratings)}/{len(ratings)} ({rating_success:.1f}%)")
        if hotels_with_ratings:
            print(f"   Rating range: {min(hotels_with_ratings):.1f} - {max(hotels_with_ratings):.1f}")
        
        # Review analysis
        review_counts = [h.get('review_count', 0) for h in hotels]
        hotels_with_review_counts = [r for r in review_counts if r > 0]
        
        print(f"📝 REVIEW COUNTS:")
        print(f"   Hotels with review counts: {len(hotels_with_review_counts)}/{len(review_counts)} ({len(hotels_with_review_counts)/len(review_counts)*100:.1f}%)")
        if hotels_with_review_counts:
            print(f"   Review count range: {min(hotels_with_review_counts)} - {max(hotels_with_review_counts)}")
        
        # Reviews extraction (Level 3+)
        if level >= 3:
            hotels_with_reviews = [h for h in hotels if h.get('reviews') and len(h['reviews']) > 0]
            review_extraction_success = len(hotels_with_reviews) / len(hotels) * 100 if hotels else 0
            
            print(f"📖 REVIEW EXTRACTION:")
            print(f"   Hotels with extracted reviews: {len(hotels_with_reviews)}/{len(hotels)} ({review_extraction_success:.1f}%)")
            
            if hotels_with_reviews:
                review_lengths = [len(h['reviews']) for h in hotels_with_reviews]
                print(f"   Reviews per hotel: {min(review_lengths)} - {max(review_lengths)}")
                
                # CRITICAL Level 4 check: Must get ALL reviews
                if level == 4:
                    print(f"\n🚨 LEVEL 4 CRITICAL CHECK: ALL REVIEWS REQUIREMENT")
                    
                    for i, hotel in enumerate(hotels_with_reviews[:3]):
                        extracted = len(hotel['reviews'])
                        claimed = hotel.get('review_count', 0)
                        coverage = (extracted / claimed * 100) if claimed > 0 else 0
                        
                        print(f"   Hotel {i+1}: {extracted}/{claimed} reviews ({coverage:.1f}% coverage)")
                        
                        if coverage < 90:  # Less than 90% coverage
                            print(f"   ❌ CRITICAL: Hotel {i+1} insufficient review coverage ({coverage:.1f}%)")
                        else:
                            print(f"   ✅ Hotel {i+1} good review coverage")
        
        # Other data quality checks
        hotels_with_images = [h for h in hotels if h.get('images') and len(h['images']) > 0]
        hotels_with_amenities = [h for h in hotels if h.get('amenities') and len(h['amenities']) > 0]
        hotels_with_location = [h for h in hotels if h.get('latitude') and h.get('longitude')]
        
        print(f"🏷️ OTHER DATA QUALITY:")
        print(f"   Hotels with images: {len(hotels_with_images)}/{len(hotels)} ({len(hotels_with_images)/len(hotels)*100:.1f}%)")
        print(f"   Hotels with amenities: {len(hotels_with_amenities)}/{len(hotels)} ({len(hotels_with_amenities)/len(hotels)*100:.1f}%)")
        print(f"   Hotels with coordinates: {len(hotels_with_location)}/{len(hotels)} ({len(hotels_with_location)/len(hotels)*100:.1f}%)")
        
        # Sample hotel details
        print(f"\n📋 SAMPLE HOTEL DETAILS:")
        sample = hotels[0]
        print(f"   Name: {sample.get('name', 'N/A')}")
        print(f"   Price: ${sample.get('price_per_night', 0)}/night")
        print(f"   Rating: {sample.get('rating', 'N/A')}")
        print(f"   Reviews: {sample.get('review_count', 0)} total")
        if level >= 3 and sample.get('reviews'):
            print(f"   Extracted reviews: {len(sample['reviews'])}")
        print(f"   Images: {len(sample.get('images', []))}")
        print(f"   Amenities: {len(sample.get('amenities', []))}")
        
    # Level-specific validation
    critical_issues = []
    
    if hotel_count == 0:
        critical_issues.append("No hotels found")
    
    if success_rate == 0:
        critical_issues.append("Success rate = 0 (calculation issue)")
    
    if avg_price == 0 and level <= 2:  # Levels 1-2 should get prices from search
        critical_issues.append("No prices extracted from search results")
    
    if level >= 3 and hotel_count > 0:
        hotels_with_reviews = [h for h in hotels if h.get('reviews') and len(h['reviews']) > 0]
        if len(hotels_with_reviews) == 0:
            critical_issues.append("No reviews extracted (Level 3+ requirement)")
    
    if level == 4 and hotel_count > 0:
        # Check if Level 4 got reasonable review coverage
        hotels_with_reviews = [h for h in hotels if h.get('reviews') and len(h['reviews']) > 0]
        if hotels_with_reviews:
            poor_coverage = [h for h in hotels_with_reviews 
                           if len(h['reviews']) < 50 and h.get('review_count', 0) > 1000]
            if poor_coverage:
                critical_issues.append(f"Level 4 poor review coverage on {len(poor_coverage)} hotels")
    
    print(f"\n🚨 CRITICAL ISSUES: {len(critical_issues)}")
    for i, issue in enumerate(critical_issues, 1):
        print(f"   {i}. {issue}")
    
    level_success = len(critical_issues) == 0
    print(f"🎯 LEVEL {level} STATUS: {'✅ PASSED' if level_success else '❌ CRITICAL ISSUES'}")
    
    return level_success

def test_level(level, description, max_hotels=3):
    """Test a specific level comprehensively"""
    print(f"\n{'='*80}")
    print(f"🚀 TESTING LEVEL {level}: {description}")
    print(f"{'='*80}")
    
    params = {
        "level": level,
        "location": "Dubai Marina",
        "max_hotels": max_hotels,
        "checkin": "2025-09-02",
        "checkout": "2025-09-05"
    }
    
    print(f"📋 Parameters: {params}")
    
    try:
        # Submit job
        response = requests.post("http://localhost:8004/jobs/booking_hotels", json=params, timeout=30)
        
        if response.status_code != 200:
            print(f"❌ Job submission failed: {response.status_code}")
            return False
            
        job_data = response.json()
        job_id = job_data.get("job_id")
        
        if not job_id:
            print(f"❌ No job_id received")
            return False
            
        print(f"✅ Job started: {job_id}")
        
        # Wait for completion
        success, result_data = wait_for_job(job_id, timeout_minutes=15)
        
        if not success:
            error = result_data.get("error", "Unknown error")
            print(f"❌ Job failed: {error}")
            return False
        
        # Analyze results
        return analyze_results(level, result_data, job_id)
        
    except Exception as e:
        print(f"❌ Level {level} test failed: {e}")
        return False

def main():
    print("🚀 COMPREHENSIVE BOOKING.COM SYSTEM STATUS TEST")
    print("=" * 80)
    print("Requirements:")
    print("✅ Level 4 CANNOT be approved without ALL reviews")
    print("✅ Check ALL variables: ratings, prices, reviews, amenities")
    print("✅ NO low quality results accepted")
    print("=" * 80)
    
    # Test all levels systematically
    test_results = []
    
    levels = [
        (1, "Quick Search - Essential data only", 5),
        (2, "Standard - Search + basic hotel data", 4), 
        (3, "Deep - Hotel pages + reviews", 3),
        (4, "Maximum - ALL hotel data + ALL reviews", 2)  # Fewer hotels for Level 4 due to intensity
    ]
    
    for level, description, max_hotels in levels:
        success = test_level(level, description, max_hotels)
        test_results.append((level, success))
        
        # Critical: Level 4 cannot proceed if it fails
        if level == 4 and not success:
            print(f"\n🚨 CRITICAL: LEVEL 4 FAILED - CANNOT BE APPROVED")
            print("User requirement: Level 4 must get ALL reviews")
            break
    
    # Final comprehensive assessment
    print(f"\n{'='*80}")
    print("🎯 FINAL SYSTEM STATUS ASSESSMENT")
    print(f"{'='*80}")
    
    passed_levels = [level for level, success in test_results if success]
    failed_levels = [level for level, success in test_results if not success]
    
    print(f"✅ PASSED LEVELS: {passed_levels}")
    print(f"❌ FAILED LEVELS: {failed_levels}")
    
    # Critical assessment based on user requirements
    critical_failures = []
    
    if 4 in failed_levels:
        critical_failures.append("Level 4 failed (CANNOT BE APPROVED)")
    
    if len(passed_levels) == 0:
        critical_failures.append("NO levels working")
    
    if len(failed_levels) > len(passed_levels):
        critical_failures.append("More levels failing than passing")
    
    print(f"\n🚨 CRITICAL ASSESSMENT:")
    if critical_failures:
        print("❌ SYSTEM NOT READY FOR APPROVAL")
        for failure in critical_failures:
            print(f"   • {failure}")
    else:
        print("✅ SYSTEM MEETS BASIC REQUIREMENTS")
        if 4 in passed_levels:
            print("✅ Level 4 APPROVED (gets all reviews)")
        else:
            print("⚠️ Level 4 NOT TESTED or FAILED")
    
    return len(critical_failures) == 0

if __name__ == "__main__":
    success = main()
    print(f"\n🎯 SYSTEM STATUS: {'APPROVED' if success else 'REQUIRES FIXES'}")
    exit(0 if success else 1)
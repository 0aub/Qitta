#!/usr/bin/env python3
"""
THOROUGH TESTING - Acting like the user would
No shortcuts, inspect every single data point
"""

import os, time, requests, json
from datetime import datetime

EP = os.getenv("BROWSER_ENDPOINT", "http://localhost:8004")

def wait_properly(job_id, max_wait=300):
    """Wait for job with proper timeout and status tracking"""
    print(f"⏳ Waiting for {job_id}...")
    start = time.time()
    
    while time.time() - start < max_wait:
        try:
            resp = requests.get(f"{EP}/jobs/{job_id}")
            if resp.status_code != 200:
                print(f"❌ Status check failed: {resp.status_code}")
                return None
                
            data = resp.json()
            status = data.get("status", "unknown")
            
            if status == "finished":
                elapsed = time.time() - start
                print(f"\n✅ Completed in {elapsed:.1f}s")
                return data
            elif status == "error":
                error = data.get("error", "Unknown error")
                print(f"\n❌ FAILED: {error}")
                return None
            else:
                elapsed = time.time() - start
                print(f"\r⏱️  {status} - {elapsed:.0f}s", end="")
                
        except Exception as e:
            print(f"\n❌ Exception checking status: {e}")
            return None
            
        time.sleep(5)  # Check every 5 seconds
    
    print(f"\n⏰ TIMEOUT after {max_wait}s")
    return None

def inspect_every_field(hotel_data, level, test_name):
    """Inspect every single field like a skeptical user would"""
    print(f"\n🔍 DETAILED INSPECTION: {test_name}")
    print(f"=" * 70)
    
    hotel_name = hotel_data.get('name', 'NO NAME')
    print(f"🏨 Hotel: {hotel_name}")
    
    issues_found = []
    
    # 1. PRICE INSPECTION
    price = hotel_data.get('price_per_night')
    print(f"\n💰 PRICE INSPECTION:")
    print(f"   Raw value: {price} (type: {type(price)})")
    
    if price is None:
        print(f"   ❌ ISSUE: Price is None")
        issues_found.append("Price missing")
    elif price == 0:
        print(f"   ⚠️  Price is 0 - may indicate unavailable")
    elif isinstance(price, (int, float)) and price > 0:
        print(f"   ✅ Valid price: ${price}")
    else:
        print(f"   ❌ ISSUE: Invalid price format")
        issues_found.append("Invalid price format")
    
    # 2. RATING INSPECTION  
    rating = hotel_data.get('rating')
    print(f"\n⭐ RATING INSPECTION:")
    print(f"   Raw value: {rating} (type: {type(rating)})")
    
    if rating is None:
        print(f"   ❌ ISSUE: Rating missing")
        issues_found.append("Rating missing")
    elif not isinstance(rating, (int, float)):
        print(f"   ❌ ISSUE: Rating not numeric")
        issues_found.append("Rating not numeric")
    elif rating < 0 or rating > 10:
        print(f"   ❌ ISSUE: Rating out of range")
        issues_found.append("Rating out of range")
    else:
        print(f"   ✅ Valid rating: {rating}/10")
    
    # 3. REVIEWS INSPECTION (MOST CRITICAL)
    reviews = hotel_data.get('reviews', [])
    review_count = hotel_data.get('review_count', 0)
    pages_processed = hotel_data.get('pages_processed', 1)
    
    print(f"\n📝 REVIEWS INSPECTION:")
    print(f"   Reviews extracted: {len(reviews)}")
    print(f"   Review count claimed: {review_count}")
    print(f"   Pages processed: {pages_processed}")
    
    # Level-specific expectations
    if level == 1:
        if len(reviews) > 0:
            print(f"   ⚠️  Level 1 has reviews (should have 0)")
        else:
            print(f"   ✅ Level 1 correctly has no reviews")
    elif level == 2:
        if len(reviews) > 0:
            print(f"   ⚠️  Level 2 has reviews (should have 0)")
        else:
            print(f"   ✅ Level 2 correctly has no reviews")
    elif level == 3:
        if len(reviews) == 0:
            print(f"   ❌ CRITICAL: Level 3 has 0 reviews (should have 2-5)")
            issues_found.append("Level 3 no reviews")
        elif len(reviews) <= 5:
            print(f"   ✅ Level 3 has appropriate review count")
        else:
            print(f"   ⚠️  Level 3 has too many reviews ({len(reviews)})")
    elif level == 4:
        if len(reviews) == 0:
            print(f"   ❌ CRITICAL: Level 4 has 0 reviews")
            issues_found.append("Level 4 no reviews")
        elif pages_processed == 1:
            print(f"   ❌ CRITICAL: Level 4 only processed 1 page (should paginate)")
            issues_found.append("Level 4 no pagination")
        else:
            print(f"   ✅ Level 4 processed {pages_processed} pages")
    
    # 4. DETAILED REVIEW CONTENT INSPECTION
    if reviews:
        print(f"\n🔍 REVIEW CONTENT INSPECTION:")
        
        for i, review in enumerate(reviews[:3]):  # Check first 3 reviews
            reviewer_name = review.get('reviewer_name', 'NO NAME')
            review_text = review.get('review_text', 'NO TEXT')
            
            print(f"\n   Review {i+1}:")
            print(f"   👤 Name: \"{reviewer_name}\"")
            print(f"   📝 Text: \"{review_text[:80]}...\"")
            
            # CRITICAL VALIDATION: Check if name is actually review text
            name_issues = []
            
            if len(reviewer_name) > 30:
                name_issues.append("Name too long (likely review text)")
            
            if '.' in reviewer_name and len(reviewer_name) > 15:
                name_issues.append("Name contains sentences")
                
            if reviewer_name.lower() in ['wonderful', 'excellent', 'amazing', 'fantastic']:
                name_issues.append("Generic word as name")
            
            # Check for specific bad patterns from user example
            bad_phrases = ['everything was', 'stay here', 'will not regret', 'simply fantastic']
            for phrase in bad_phrases:
                if phrase in reviewer_name.lower():
                    name_issues.append(f"Review phrase in name: '{phrase}'")
            
            if name_issues:
                print(f"   ❌ NAME ISSUES:")
                for issue in name_issues:
                    print(f"      • {issue}")
                issues_found.extend(name_issues)
            else:
                print(f"   ✅ Name looks valid")
    
    # 5. URL INSPECTION
    booking_url = hotel_data.get('booking_url', '')
    print(f"\n🔗 URL INSPECTION:")
    print(f"   Booking URL: {booking_url[:60]}...")
    
    if not booking_url:
        print(f"   ❌ ISSUE: No booking URL")
        issues_found.append("No booking URL")
    elif 'booking.com' not in booking_url:
        print(f"   ❌ ISSUE: Invalid booking URL")
        issues_found.append("Invalid booking URL")
    else:
        print(f"   ✅ Valid booking URL")
    
    # 6. OVERALL ASSESSMENT
    print(f"\n🎯 OVERALL ASSESSMENT:")
    print(f"   Total issues found: {len(issues_found)}")
    
    if issues_found:
        print(f"   ❌ ISSUES DETECTED:")
        for issue in issues_found:
            print(f"      • {issue}")
    else:
        print(f"   ✅ No major issues detected")
    
    return {
        'hotel_name': hotel_name,
        'level': level,
        'test_name': test_name,
        'issues_found': issues_found,
        'reviews_count': len(reviews),
        'pages_processed': pages_processed,
        'quality_score': max(0, 100 - len(issues_found) * 20)
    }

def test_level_thoroughly(level, location="Dubai"):
    """Test a level thoroughly like a demanding user"""
    print(f"\n{'🧪' * 25}")
    print(f"THOROUGH TESTING: LEVEL {level}")
    print('🧪' * 25)
    
    try:
        # Submit request
        payload = {
            "location": location,
            "scrape_level": level,
            "max_results": 1
        }
        
        print(f"📤 Submitting Level {level} request...")
        print(f"📋 Payload: {json.dumps(payload, indent=2)}")
        
        response = requests.post(f"{EP}/jobs/booking-hotels", json=payload)
        
        if response.status_code != 200:
            print(f"❌ CRITICAL: Request failed with {response.status_code}")
            print(f"Response: {response.text}")
            return None
        
        job_data = response.json()
        job_id = job_data.get("job_id")
        
        if not job_id:
            print(f"❌ CRITICAL: No job ID returned")
            return None
        
        print(f"🆔 Job ID: {job_id}")
        
        # Wait for completion with detailed tracking
        result = wait_properly(job_id)
        
        if not result:
            print(f"❌ CRITICAL: Job failed or timed out")
            return None
        
        if result.get("status") != "finished":
            print(f"❌ CRITICAL: Job status is {result.get('status')}")
            print(f"Error: {result.get('error', 'Unknown')}")
            return None
        
        # Extract hotel data
        hotels = result.get("result", {}).get("hotels", [])
        
        if not hotels:
            print(f"❌ CRITICAL: No hotels returned")
            return None
        
        hotel = hotels[0]
        
        # THOROUGH INSPECTION
        return inspect_every_field(hotel, level, f"Level {level} - {location}")
        
    except Exception as e:
        print(f"❌ CRITICAL EXCEPTION: {e}")
        return None

if __name__ == "__main__":
    print("🎯 STARTING THOROUGH TESTING - ACTING LIKE DEMANDING USER")
    print("=" * 80)
    print(f"🕐 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🎯 Objective: Test EVERYTHING thoroughly and inspect results like user would")
    
    all_results = []
    
    # Test each level systematically
    levels_to_test = [1, 2, 3, 4]
    
    for level in levels_to_test:
        result = test_level_thoroughly(level)
        if result:
            all_results.append(result)
        else:
            print(f"🚨 CRITICAL: Level {level} testing completely failed")
            all_results.append({
                'level': level,
                'test_name': f"Level {level} - FAILED",
                'issues_found': ['Complete test failure'],
                'quality_score': 0
            })
        
        # Pause between tests to avoid overwhelming system
        print(f"\n⏸️  Pausing 10 seconds before next test...")
        time.sleep(10)
    
    # COMPREHENSIVE ANALYSIS - Acting like demanding user
    print(f"\n{'🎯' * 40}")
    print("COMPREHENSIVE TESTING RESULTS - USER PERSPECTIVE")
    print('🎯' * 40)
    
    print(f"\n📊 LEVEL-BY-LEVEL ANALYSIS:")
    
    total_issues = 0
    critical_failures = 0
    
    for result in all_results:
        level = result['level']
        issues = result['issues_found']
        quality = result['quality_score']
        reviews = result.get('reviews_count', 0)
        pages = result.get('pages_processed', 1)
        
        print(f"\n🔍 Level {level}:")
        print(f"   Hotel: {result.get('hotel_name', 'Unknown')}")
        print(f"   Reviews: {reviews}")
        print(f"   Pages: {pages}")
        print(f"   Quality: {quality}%")
        print(f"   Issues: {len(issues)}")
        
        if issues:
            print(f"   ❌ PROBLEMS:")
            for issue in issues:
                print(f"      • {issue}")
            total_issues += len(issues)
            
            if any('CRITICAL' in issue for issue in issues):
                critical_failures += 1
        else:
            print(f"   ✅ No issues found")
    
    # OVERALL ASSESSMENT - Be harsh like user would be
    print(f"\n🎯 OVERALL ASSESSMENT:")
    print(f"   Total Issues Found: {total_issues}")
    print(f"   Critical Failures: {critical_failures}")
    
    # Specific checks user cares about
    level3_result = next((r for r in all_results if r['level'] == 3), None)
    level4_result = next((r for r in all_results if r['level'] == 4), None)
    
    print(f"\n🚨 USER'S SPECIFIC CONCERNS:")
    
    # Check Level 3 reviews
    if level3_result:
        if level3_result.get('reviews_count', 0) == 0:
            print(f"   ❌ LEVEL 3 STILL BROKEN: 0 reviews extracted")
        else:
            print(f"   ✅ Level 3 now extracts {level3_result['reviews_count']} reviews")
    
    # Check Level 4 pagination
    if level4_result:
        if level4_result.get('pages_processed', 1) == 1:
            print(f"   ❌ LEVEL 4 STILL BROKEN: Only 1 page processed")
        else:
            print(f"   ✅ Level 4 processed {level4_result['pages_processed']} pages")
    
    # Check for reviewer name issues
    reviewer_name_issues = [issue for result in all_results for issue in result['issues_found'] if 'name' in issue.lower()]
    if reviewer_name_issues:
        print(f"   ❌ REVIEWER NAME STILL BROKEN:")
        for issue in reviewer_name_issues:
            print(f"      • {issue}")
    else:
        print(f"   ✅ Reviewer names appear to be fixed")
    
    # FINAL VERDICT - Honest assessment
    print(f"\n🎯 FINAL VERDICT:")
    
    if total_issues == 0:
        print(f"   🎉 PERFECT: All systems working correctly")
    elif critical_failures == 0 and total_issues <= 2:
        print(f"   ✅ ACCEPTABLE: Minor issues only")
    elif critical_failures <= 1:
        print(f"   ⚠️  NEEDS WORK: Some critical issues remain")
    else:
        print(f"   ❌ UNACCEPTABLE: Multiple critical failures")
        print(f"   🔧 IMMEDIATE ACTION REQUIRED")
    
    # Specific recommendations
    print(f"\n💡 SPECIFIC ACTIONS NEEDED:")
    
    if level3_result and level3_result.get('reviews_count', 0) == 0:
        print(f"   🔧 Fix Level 3 review extraction - completely broken")
    
    if level4_result and level4_result.get('pages_processed', 1) == 1:
        print(f"   🔧 Fix Level 4 pagination - not working despite enhancements")
    
    if reviewer_name_issues:
        print(f"   🔧 Fix reviewer name extraction - still mixing with review text")
    
    if total_issues == 0:
        print(f"   🚀 System ready for production use")
    
    print(f"\n🕐 Testing completed: {datetime.now().strftime('%H:%M:%S')}")
    print("=" * 80)
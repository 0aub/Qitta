#!/usr/bin/env python3
"""
FOCUSED ISSUE TESTING - Target specific extraction problems
This will test the exact issues identified by the user
"""

import os, time, requests, json
from datetime import datetime

EP = os.getenv("BROWSER_ENDPOINT", "http://localhost:8004")

def test_specific_level(location, level, focus="all"):
    """Test a specific level with detailed extraction analysis"""
    print(f"\n🎯 FOCUSED TESTING: Level {level}")
    print(f"📍 Location: {location}")
    print(f"🔍 Focus: {focus}")
    
    try:
        response = requests.post(f"{EP}/jobs/booking-hotels", json={
            "location": location,
            "scrape_level": level,
            "max_results": 1  # Single hotel for detailed inspection
        })
        
        if response.status_code != 200:
            print(f"❌ Request failed: {response.status_code}")
            return None
        
        job_id = response.json()["job_id"]
        print(f"🆔 Job ID: {job_id}")
        
        # Wait for completion
        start_time = time.time()
        while time.time() - start_time < 180:  # 3 minute timeout
            time.sleep(3)
            status_response = requests.get(f"{EP}/jobs/{job_id}")
            status_data = status_response.json()
            
            if status_data["status"] == "finished":
                break
            elif status_data["status"] == "error":
                print(f"❌ Job failed: {status_data.get('error')}")
                return None
            else:
                elapsed = time.time() - start_time
                print(f"\r⏳ Running... {elapsed:.0f}s", end="")
        else:
            print(f"\n⏰ Timeout after 180s")
            return None
        
        print(f"\n✅ Job completed")
        
        # Extract and analyze results
        hotels = status_data.get("result", {}).get("hotels", [])
        if not hotels:
            print(f"❌ No hotels found")
            return None
        
        hotel = hotels[0]
        return analyze_hotel_extraction(hotel, level, focus)
        
    except Exception as e:
        print(f"❌ Exception: {e}")
        return None

def analyze_hotel_extraction(hotel, level, focus):
    """Detailed analysis of extraction issues"""
    hotel_name = hotel.get('name', 'Unknown')
    reviews = hotel.get('reviews', [])
    pages_processed = hotel.get('pages_processed', 1)
    
    print(f"\n📊 EXTRACTION ANALYSIS: {hotel_name}")
    print(f"=" * 60)
    
    analysis = {
        'hotel_name': hotel_name,
        'level': level,
        'total_reviews': len(reviews),
        'pages_processed': pages_processed,
        'issues_found': [],
        'detailed_findings': {}
    }
    
    # Issue 1: Level 3 should have reviews but doesn't
    if level == 3:
        if len(reviews) == 0:
            print(f"🚨 ISSUE: Level 3 extracted 0 reviews (should have 2-5)")
            analysis['issues_found'].append('Level 3 no reviews extracted')
        else:
            print(f"✅ Level 3 extracted {len(reviews)} reviews")
    
    # Issue 2: Level 4 should use pagination but only gets 1 page
    if level == 4:
        if pages_processed == 1:
            print(f"🚨 ISSUE: Level 4 only processed 1 page (should use pagination)")
            analysis['issues_found'].append('Level 4 single page only')
        else:
            print(f"✅ Level 4 processed {pages_processed} pages")
    
    # Issue 3: Reviewer names are wrong (mixing with review text)
    if reviews:
        print(f"\n🔍 REVIEWING SAMPLE REVIEWS:")
        bad_reviewer_names = []
        good_reviewer_names = []
        
        for i, review in enumerate(reviews[:5]):  # Check first 5 reviews
            reviewer_name = review.get('reviewer_name', '')
            review_text = review.get('review_text', '')
            
            print(f"\n   Review {i+1}:")
            print(f"   📝 Text: \"{review_text[:60]}...\"")
            print(f"   👤 Name: \"{reviewer_name}\"")
            
            # Check if reviewer name looks like review text
            if reviewer_name and len(reviewer_name) > 30:
                print(f"   🚨 ISSUE: Reviewer name too long (likely review text)")
                bad_reviewer_names.append(reviewer_name[:50])
                analysis['issues_found'].append('Long reviewer names (review text)')
            elif reviewer_name and ('.' in reviewer_name or len(reviewer_name.split()) > 5):
                print(f"   🚨 ISSUE: Reviewer name contains sentences")
                bad_reviewer_names.append(reviewer_name[:50])
                analysis['issues_found'].append('Sentence-like reviewer names')
            elif reviewer_name in ['Wonderful', 'Excellent', 'Good', 'Amazing']:
                print(f"   🚨 ISSUE: Generic word as reviewer name")
                bad_reviewer_names.append(reviewer_name)
                analysis['issues_found'].append('Generic words as names')
            elif reviewer_name and len(reviewer_name) > 3:
                print(f"   ✅ Reviewer name looks valid")
                good_reviewer_names.append(reviewer_name)
            else:
                print(f"   ⚠️ Empty or short reviewer name")
        
        analysis['detailed_findings']['bad_names'] = bad_reviewer_names
        analysis['detailed_findings']['good_names'] = good_reviewer_names
        
        if bad_reviewer_names:
            print(f"\n🚨 BAD REVIEWER NAMES FOUND:")
            for name in bad_reviewer_names:
                print(f"   • \"{name}\"")
    
    # Overall assessment
    print(f"\n🎯 ISSUE SUMMARY:")
    if analysis['issues_found']:
        print(f"   ❌ Issues found: {len(analysis['issues_found'])}")
        for issue in analysis['issues_found']:
            print(f"      • {issue}")
    else:
        print(f"   ✅ No major issues detected")
    
    return analysis

def test_extraction_selectors():
    """Test specific DOM selectors to understand extraction failures"""
    print(f"\n🔬 TESTING EXTRACTION SELECTORS")
    print(f"=" * 50)
    
    # This would require direct browser access to test selectors
    # For now, we'll rely on the extraction results analysis
    print(f"📝 Note: Selector testing requires browser access")
    print(f"   Will analyze extraction results instead")

# MAIN TESTING EXECUTION
if __name__ == "__main__":
    print("🎯 FOCUSED ISSUE TESTING")
    print("=" * 70)
    print(f"🕐 Started: {datetime.now().strftime('%H:%M:%S')}")
    print(f"🎯 Objective: Target specific extraction issues identified by user")
    
    all_results = []
    
    # Test each level with Dubai to reproduce the issues
    test_cases = [
        ("Dubai", 1, "ratings"),
        ("Dubai", 2, "full_data"),
        ("Dubai", 3, "reviews"),     # Should extract reviews but doesn't
        ("Dubai", 4, "pagination")   # Should paginate but doesn't + bad names
    ]
    
    for location, level, focus in test_cases:
        print(f"\n{'🧪' * 30}")
        print(f"FOCUSED TEST: Level {level}")
        print('🧪' * 30)
        
        result = test_specific_level(location, level, focus)
        if result:
            all_results.append(result)
        
        time.sleep(2)  # Brief pause between tests
    
    # Summary analysis
    print(f"\n{'🎯' * 30}")
    print("FOCUSED TESTING SUMMARY")
    print('🎯' * 30)
    
    total_issues = 0
    critical_issues = []
    
    for result in all_results:
        level = result['level']
        issues = result['issues_found']
        total_issues += len(issues)
        
        print(f"\nLevel {level}:")
        print(f"   Hotel: {result['hotel_name']}")
        print(f"   Reviews: {result['total_reviews']}")
        print(f"   Pages: {result['pages_processed']}")
        print(f"   Issues: {len(issues)}")
        
        for issue in issues:
            print(f"      • {issue}")
            critical_issues.append(f"Level {level}: {issue}")
    
    print(f"\n📊 OVERALL FINDINGS:")
    print(f"   Total Issues Found: {total_issues}")
    print(f"   Critical Issues: {len(critical_issues)}")
    
    if critical_issues:
        print(f"\n🚨 CRITICAL ISSUES TO FIX:")
        for i, issue in enumerate(critical_issues, 1):
            print(f"   {i}. {issue}")
        
        print(f"\n🔧 NEXT STEPS:")
        print(f"   1. Investigate Level 3 review extraction selectors")
        print(f"   2. Fix Level 4 pagination logic") 
        print(f"   3. Correct reviewer name extraction (mixing with review text)")
        print(f"   4. Add validation to prevent generic words as reviewer names")
    else:
        print(f"\n✅ No critical issues found")
    
    print(f"\n🕐 Completed: {datetime.now().strftime('%H:%M:%S')}")
    print("=" * 70)
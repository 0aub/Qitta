#!/usr/bin/env python3
"""
COMPREHENSIVE PAGINATION TESTING PLAN
=====================================

This script will systematically test the Level 4 pagination system across:
1. Hotels with different review counts (low, medium, high)
2. Different hotel types (luxury, budget, apartments, etc.)
3. Different locations (to test geographic variations)
4. Level 3 vs Level 4 performance comparison
"""

import os, time, requests, json
from datetime import datetime

EP = os.getenv("BROWSER_ENDPOINT", "http://localhost:8004")

def wait_for(job_id, every=3):
    print(f"⏳ Waiting for job {job_id}...")
    while True:
        rec = requests.get(f"{EP}/jobs/{job_id}").json()
        status = rec["status"]
        if status not in {"finished", "error"}:
            print(f"\r⏱️  {rec['status_with_elapsed']}", end="")
        else:
            print(f"\n✅ {status.upper()}")
            return rec
        time.sleep(every)

def submit(task, payload):
    print(f"🚀 Submitting {task} task...")
    r = requests.post(f"{EP}/jobs/{task}", json=payload)
    r.raise_for_status()
    jid = r.json()["job_id"]
    print(f"🆔 Job ID: {jid}")
    return wait_for(jid)

def analyze_test_result(result, test_name, expected_behavior):
    """Analyze test results and track issues"""
    print(f"\n" + "="*70)
    print(f"📊 ANALYSIS: {test_name}")
    print("="*70)
    
    issues = []
    
    if result["status"] == "error":
        print(f"❌ FAILED: {result.get('error', 'Unknown error')}")
        issues.append(f"Test failed with error: {result.get('error')}")
        return issues
    
    res = result["result"]
    hotels = res.get("hotels", [])
    metadata = res.get("search_metadata", {})
    
    if not hotels:
        print(f"❌ NO HOTELS FOUND")
        issues.append("No hotels found in search results")
        return issues
        
    hotel = hotels[0]
    reviews_extracted = len(hotel.get('reviews', []))
    review_count_claimed = hotel.get('review_count', 0)
    extraction_method = hotel.get('extraction_method', 'Unknown')
    pages_processed = hotel.get('pages_processed', 1)
    
    print(f"✅ RESULTS:")
    print(f"   🏨 Hotel: {hotel.get('name', 'Unknown')}")
    print(f"   📝 Reviews extracted: {reviews_extracted}")
    print(f"   📊 Review count claimed: {review_count_claimed}")
    print(f"   🔧 Extraction method: {extraction_method}")
    print(f"   📄 Pages processed: {pages_processed}")
    print(f"   ⏱️  Scrape level: {metadata.get('scrape_level', 'Unknown')}")
    
    # Validate against expected behavior
    print(f"\n🎯 VALIDATION:")
    
    # Check if Level 4 attempted pagination
    if metadata.get('scrape_level') == 4:
        if 'PAGINATION' in extraction_method or pages_processed > 1:
            print(f"   ✅ Level 4 pagination attempted")
        else:
            print(f"   ❌ Level 4 pagination not attempted")
            issues.append("Level 4 did not attempt pagination")
    
    # Check review extraction vs claimed count
    if reviews_extracted > 0 and review_count_claimed > 0:
        extraction_rate = (reviews_extracted / review_count_claimed) * 100
        print(f"   📊 Extraction rate: {extraction_rate:.1f}% ({reviews_extracted}/{review_count_claimed})")
        
        if extraction_rate >= 90:
            print(f"   ✅ Excellent extraction rate")
        elif extraction_rate >= 50:
            print(f"   ⚠️  Moderate extraction rate")
        else:
            print(f"   ❌ Poor extraction rate")
            if extraction_rate < 25:
                issues.append(f"Very low extraction rate: {extraction_rate:.1f}%")
    
    # Check for pagination evidence
    if pages_processed > 1:
        print(f"   ✅ Multi-page processing confirmed ({pages_processed} pages)")
    else:
        print(f"   ⚠️  Single page only")
        if expected_behavior == "multi_page":
            issues.append("Expected multi-page processing but got single page")
    
    # Check reviews quality
    if reviews_extracted > 0:
        sample_review = hotel.get('reviews', [{}])[0]
        if sample_review.get('review_text') and len(sample_review.get('review_text', '')) > 10:
            print(f"   ✅ Review quality: Valid text content")
        else:
            print(f"   ❌ Review quality: Invalid or empty content")
            issues.append("Poor review text quality")
    
    return issues

# TEST SUITE DEFINITION
TEST_CASES = [
    {
        "name": "LOW REVIEW COUNT TEST",
        "description": "Test hotel with few reviews (should extract all)",
        "location": "Budget hotel Dubai",
        "levels": [3, 4],
        "expected": "complete_extraction"
    },
    {
        "name": "MEDIUM REVIEW COUNT TEST", 
        "description": "Test hotel with moderate reviews (50-200)",
        "location": "Marriott Dubai",
        "levels": [3, 4],
        "expected": "multi_page"
    },
    {
        "name": "HIGH REVIEW COUNT TEST",
        "description": "Test hotel with many reviews (200+)",
        "location": "Burj Al Arab Dubai",
        "levels": [3, 4], 
        "expected": "multi_page"
    },
    {
        "name": "APARTMENT/RENTAL TEST",
        "description": "Test short-term rental (different review structure)",
        "location": "Dubai apartment rental",
        "levels": [4],
        "expected": "multi_page"
    },
    {
        "name": "INTERNATIONAL TEST",
        "description": "Test hotel outside UAE (different locale)",
        "location": "Hilton London",
        "levels": [4],
        "expected": "multi_page"
    }
]

def run_comprehensive_testing():
    """Execute the complete test suite"""
    print("🧪 COMPREHENSIVE PAGINATION TESTING")
    print("="*70)
    print(f"🕐 Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🎯 Total test cases: {len(TEST_CASES)}")
    
    all_issues = []
    test_results = []
    
    for i, test_case in enumerate(TEST_CASES, 1):
        print(f"\n" + "🧪"*70)
        print(f"TEST CASE {i}/{len(TEST_CASES)}: {test_case['name']}")
        print(f"📝 Description: {test_case['description']}")
        print(f"🎯 Expected behavior: {test_case['expected']}")
        print("🧪"*70)
        
        case_results = {}
        case_issues = []
        
        # Test each specified level for this test case
        for level in test_case['levels']:
            print(f"\n--- Testing Level {level} ---")
            
            payload = {
                "location": test_case['location'],
                "scrape_level": level,
                "max_results": 1
            }
            
            try:
                result = submit("booking-hotels", payload)
                issues = analyze_test_result(result, f"{test_case['name']} - Level {level}", test_case['expected'])
                
                case_results[f'level_{level}'] = {
                    'result': result,
                    'issues': issues
                }
                case_issues.extend(issues)
                
                # Brief pause between tests
                time.sleep(2)
                
            except Exception as e:
                error_msg = f"Level {level} execution failed: {str(e)}"
                print(f"❌ {error_msg}")
                case_issues.append(error_msg)
        
        # Store results for this test case
        test_results.append({
            'test_case': test_case,
            'results': case_results,
            'issues': case_issues
        })
        
        all_issues.extend(case_issues)
        
        print(f"\n📊 Test Case {i} Summary:")
        if case_issues:
            print(f"   ❌ {len(case_issues)} issues found:")
            for issue in case_issues[:3]:  # Show first 3 issues
                print(f"      • {issue}")
        else:
            print(f"   ✅ No critical issues found")
    
    return test_results, all_issues

if __name__ == "__main__":
    test_results, all_issues = run_comprehensive_testing()
    
    # FINAL SUMMARY
    print("\n" + "🎯"*70)
    print("COMPREHENSIVE TESTING COMPLETE")
    print("🎯"*70)
    
    print(f"🕐 Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📊 Total test cases: {len(test_results)}")
    print(f"⚠️  Total issues found: {len(all_issues)}")
    
    if all_issues:
        print(f"\n❌ CRITICAL ISSUES SUMMARY:")
        issue_counts = {}
        for issue in all_issues:
            issue_type = issue.split(':')[0] if ':' in issue else issue
            issue_counts[issue_type] = issue_counts.get(issue_type, 0) + 1
        
        for issue_type, count in sorted(issue_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"   • {issue_type}: {count} occurrences")
    else:
        print(f"\n✅ NO CRITICAL ISSUES FOUND!")
        print(f"🎉 Pagination system appears to be working correctly!")
    
    print(f"\n💾 Detailed results available in individual job logs")
    print("="*70)
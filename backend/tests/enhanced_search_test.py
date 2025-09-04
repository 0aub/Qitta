#!/usr/bin/env python3
"""
ENHANCED SEARCH STRATEGIES - QUICK WIN IMPLEMENTATION
====================================================
Testing with luxury hotels and major chains to find high-review properties
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
    r = requests.post(f"{EP}/jobs/{task}", json=payload)
    r.raise_for_status()
    jid = r.json()["job_id"]
    return wait_for(jid)

def enhanced_test(name, search_term, level, expected_reviews="high"):
    """Test with enhanced search strategies"""
    print(f"\n🏨 {name}")
    print(f"📍 Search: {search_term}")
    print(f"🔢 Level: {level}")
    print(f"🎯 Expected: {expected_reviews} reviews")
    
    start_time = time.time()
    
    try:
        result = submit("booking-hotels", {
            "location": search_term,
            "scrape_level": level,
            "max_results": 1
        })
        
        elapsed = time.time() - start_time
        
        if result["status"] == "finished":
            hotels = result["result"].get("hotels", [])
            if hotels:
                hotel = hotels[0]
                reviews_extracted = len(hotel.get('reviews', []))
                review_count = hotel.get('review_count', 0)
                method = hotel.get('extraction_method', 'Unknown')
                pages = hotel.get('pages_processed', 1)
                
                print(f"   ✅ Found: {hotel.get('name', 'Unknown')}")
                print(f"   📝 Reviews extracted: {reviews_extracted}")
                print(f"   📊 Review count claimed: {review_count}")
                print(f"   📄 Pages processed: {pages}")
                print(f"   🔧 Method: {method}")
                print(f"   ⏱️  Time: {elapsed:.1f}s")
                
                # Analysis
                success_indicators = []
                issues = []
                
                if pages > 1:
                    success_indicators.append(f"🎉 PAGINATION WORKING! ({pages} pages)")
                    
                if reviews_extracted >= 50:
                    success_indicators.append(f"🔥 HIGH REVIEW COUNT! ({reviews_extracted} reviews)")
                elif reviews_extracted >= 20:
                    success_indicators.append(f"📈 GOOD REVIEW COUNT ({reviews_extracted} reviews)")
                else:
                    issues.append(f"⚠️ Low review count ({reviews_extracted} reviews)")
                
                if 'PAGINATION' in method:
                    success_indicators.append("✅ Pagination method used")
                elif level == 4:
                    issues.append("❌ Level 4 not using pagination method")
                
                extraction_rate = (reviews_extracted / max(review_count, 1)) * 100
                if extraction_rate >= 90:
                    success_indicators.append(f"🎯 Excellent extraction rate ({extraction_rate:.1f}%)")
                elif extraction_rate < 50:
                    issues.append(f"⚠️ Low extraction rate ({extraction_rate:.1f}%)")
                
                # Print results
                if success_indicators:
                    print(f"   🎉 SUCCESS INDICATORS:")
                    for indicator in success_indicators:
                        print(f"      {indicator}")
                
                if issues:
                    print(f"   ⚠️ ISSUES DETECTED:")
                    for issue in issues:
                        print(f"      {issue}")
                
                return {
                    'success': True,
                    'hotel_name': hotel.get('name', 'Unknown'),
                    'reviews_extracted': reviews_extracted,
                    'review_count_claimed': review_count,
                    'pages_processed': pages,
                    'method': method,
                    'extraction_rate': extraction_rate,
                    'success_indicators': success_indicators,
                    'issues': issues,
                    'elapsed_time': elapsed
                }
        
        print(f"   ❌ Test failed: {result.get('error', 'Unknown error')}")
        return {
            'success': False,
            'error': result.get('error', 'Unknown error'),
            'elapsed_time': elapsed
        }
        
    except Exception as e:
        print(f"   ❌ Exception: {str(e)}")
        return {
            'success': False,
            'error': str(e),
            'elapsed_time': time.time() - start_time
        }

print("🚀 ENHANCED SEARCH STRATEGIES - QUICK WIN IMPLEMENTATION")
print("="*70)
print(f"🕐 Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# ENHANCED SEARCH TERMS - Targeting luxury hotels and major chains
ENHANCED_TESTS = [
    # Major luxury hotel chains in Dubai
    ("Dubai Luxury Chain 1", "Marriott Dubai Downtown", 4, "high"),
    ("Dubai Luxury Chain 2", "Hilton Dubai Creek", 4, "high"),
    ("Dubai Iconic Hotel", "Burj Al Arab Dubai", 4, "very_high"),
    ("Dubai Resort", "Atlantis The Palm Dubai", 4, "very_high"),
    
    # International luxury hotels (geographic diversity)
    ("London Luxury", "Hilton London Park Lane", 4, "high"),
    ("New York Luxury", "Marriott Times Square New York", 4, "high"),
    
    # Level 3 comparison with luxury hotel
    ("Level 3 Luxury Test", "Ritz Carlton Dubai", 3, "medium"),
    
    # Alternative Dubai luxury searches
    ("Dubai Premium", "Four Seasons Dubai", 4, "high"),
    ("Dubai Business", "Emirates Palace Abu Dhabi", 4, "high"),
]

all_results = []
total_success_indicators = []
total_issues = []

print(f"🎯 Running {len(ENHANCED_TESTS)} enhanced search tests...")

for i, (name, search_term, level, expected) in enumerate(ENHANCED_TESTS, 1):
    print(f"\n" + "🧪"*70)
    print(f"TEST {i}/{len(ENHANCED_TESTS)}")
    print("🧪"*70)
    
    result = enhanced_test(name, search_term, level, expected)
    all_results.append((name, result))
    
    if result['success']:
        total_success_indicators.extend(result.get('success_indicators', []))
        total_issues.extend(result.get('issues', []))
    else:
        total_issues.append(f"Test '{name}' failed: {result.get('error', 'Unknown')}")
    
    # Brief pause between tests
    time.sleep(2)

# COMPREHENSIVE ANALYSIS
print("\n" + "🎯"*70)
print("ENHANCED SEARCH TESTING RESULTS")
print("🎯"*70)

successful_tests = [r for name, r in all_results if r['success']]
failed_tests = [r for name, r in all_results if not r['success']]

print(f"📊 TEST SUMMARY:")
print(f"   ✅ Successful tests: {len(successful_tests)}/{len(all_results)}")
print(f"   ❌ Failed tests: {len(failed_tests)}")
print(f"   ⏱️ Average time per test: {sum(r[1]['elapsed_time'] for r in all_results) / len(all_results):.1f}s")

# Pagination analysis
pagination_working = []
high_review_hotels = []
extraction_issues = []

for name, result in all_results:
    if result['success']:
        if result.get('pages_processed', 1) > 1:
            pagination_working.append((name, result['pages_processed']))
        
        if result.get('reviews_extracted', 0) >= 50:
            high_review_hotels.append((name, result['reviews_extracted']))
        
        if result.get('extraction_rate', 0) < 50:
            extraction_issues.append((name, result['extraction_rate']))

print(f"\n📈 KEY FINDINGS:")

if pagination_working:
    print(f"   🎉 PAGINATION CONFIRMED IN {len(pagination_working)} TESTS:")
    for name, pages in pagination_working:
        print(f"      • {name}: {pages} pages")
else:
    print(f"   ⚠️ NO MULTI-PAGE PAGINATION DETECTED")

if high_review_hotels:
    print(f"   🔥 HIGH REVIEW HOTELS FOUND ({len(high_review_hotels)}):")
    for name, reviews in high_review_hotels:
        print(f"      • {name}: {reviews} reviews")
else:
    print(f"   ⚠️ NO HIGH-REVIEW HOTELS FOUND (all under 50 reviews)")

if extraction_issues:
    print(f"   ❌ EXTRACTION RATE ISSUES ({len(extraction_issues)}):")
    for name, rate in extraction_issues:
        print(f"      • {name}: {rate:.1f}% extraction rate")

# Success indicators summary
if total_success_indicators:
    print(f"\n✅ SUCCESS INDICATORS ({len(total_success_indicators)} total):")
    indicator_counts = {}
    for indicator in total_success_indicators:
        key = indicator.split('(')[0].strip()  # Group similar indicators
        indicator_counts[key] = indicator_counts.get(key, 0) + 1
    
    for indicator, count in sorted(indicator_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"   • {indicator}: {count} occurrences")

# Issues summary
if total_issues:
    print(f"\n❌ ISSUES DETECTED ({len(total_issues)} total):")
    issue_counts = {}
    for issue in total_issues:
        key = issue.split('(')[0].strip()  # Group similar issues
        issue_counts[key] = issue_counts.get(key, 0) + 1
    
    for issue, count in sorted(issue_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"   • {issue}: {count} occurrences")

# RECOMMENDATIONS
print(f"\n🎯 RECOMMENDATIONS:")

if not pagination_working:
    print(f"   🚨 CRITICAL: Pagination system not working with luxury hotels")
    print(f"   🔧 ACTION REQUIRED: Intensive investigation needed")

if not high_review_hotels:
    print(f"   ⚠️ WARNING: Enhanced search terms not finding high-review hotels")
    print(f"   💡 SUGGESTION: May need different search strategies or hotel selection")

if len(successful_tests) < len(all_results) * 0.8:
    print(f"   ⚠️ WARNING: High failure rate in tests")
    print(f"   🔧 ACTION REQUIRED: System stability investigation needed")

print(f"\n🕐 Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*70)
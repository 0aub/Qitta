#!/usr/bin/env python3
"""
FOCUSED ENHANCED SEARCH TEST - Quick validation of luxury hotel pagination
"""

import os, time, requests, json
from datetime import datetime

EP = os.getenv("BROWSER_ENDPOINT", "http://localhost:8004")

def wait_for(job_id, timeout=90):
    """Wait for job with timeout"""
    print(f"⏳ Waiting for job {job_id}...")
    start = time.time()
    
    while time.time() - start < timeout:
        rec = requests.get(f"{EP}/jobs/{job_id}").json()
        status = rec["status"]
        if status not in {"finished", "error"}:
            print(f"\r⏱️  {rec.get('status_with_elapsed', status)}", end="")
        else:
            print(f"\n✅ {status.upper()}")
            return rec
        time.sleep(3)
    
    print(f"\n⏰ TIMEOUT after {timeout}s")
    return {"status": "timeout"}

def focused_test(name, search_term, level):
    """Focused test with enhanced search"""
    print(f"\n🏨 {name}")
    print(f"📍 Search: {search_term}")
    print(f"🔢 Level: {level}")
    
    start_time = time.time()
    
    try:
        r = requests.post(f"{EP}/jobs/booking-hotels", json={
            "location": search_term,
            "scrape_level": level,
            "max_results": 1
        })
        r.raise_for_status()
        
        result = wait_for(r.json()["job_id"])
        elapsed = time.time() - start_time
        
        if result["status"] == "finished":
            hotels = result["result"].get("hotels", [])
            if hotels:
                hotel = hotels[0]
                reviews = len(hotel.get('reviews', []))
                pages = hotel.get('pages_processed', 1)
                method = hotel.get('extraction_method', 'Unknown')
                
                print(f"   ✅ Found: {hotel.get('name', 'Unknown')}")
                print(f"   📝 Reviews: {reviews}")
                print(f"   📄 Pages: {pages}")
                print(f"   🔧 Method: {method}")
                print(f"   ⏱️  Time: {elapsed:.1f}s")
                
                # Analysis
                if pages > 1:
                    print(f"   🎉 PAGINATION WORKING! ({pages} pages)")
                
                if reviews >= 30:
                    print(f"   🔥 HIGH REVIEW COUNT! ({reviews} reviews)")
                elif reviews >= 15:
                    print(f"   📈 GOOD REVIEW COUNT ({reviews} reviews)")
                else:
                    print(f"   ⚠️ Low review count ({reviews} reviews)")
                
                return {
                    'success': True,
                    'reviews': reviews,
                    'pages': pages,
                    'method': method,
                    'elapsed': elapsed
                }
        
        print(f"   ❌ Failed: {result.get('error', 'Unknown error')}")
        return {'success': False, 'error': result.get('error')}
        
    except Exception as e:
        print(f"   ❌ Exception: {str(e)}")
        return {'success': False, 'error': str(e)}

print("🚀 FOCUSED ENHANCED SEARCH TEST")
print("="*50)
print(f"🕐 Started: {datetime.now().strftime('%H:%M:%S')}")

# Focus on 3 key luxury hotels that should have high reviews
FOCUSED_TESTS = [
    ("Dubai Luxury Icon", "Burj Al Arab Dubai", 4),
    ("Dubai Resort", "Atlantis The Palm Dubai", 4), 
    ("Dubai Premium Chain", "Ritz Carlton Dubai", 4)
]

results = []
success_count = 0
pagination_found = 0
high_review_count = 0

for i, (name, search, level) in enumerate(FOCUSED_TESTS, 1):
    print(f"\n{'='*20} TEST {i}/3 {'='*20}")
    
    result = focused_test(name, search, level)
    results.append((name, result))
    
    if result['success']:
        success_count += 1
        if result['pages'] > 1:
            pagination_found += 1
        if result['reviews'] >= 30:
            high_review_count += 1

print(f"\n{'='*50}")
print("🎯 FOCUSED TEST SUMMARY")
print(f"{'='*50}")

print(f"📊 Results:")
print(f"   ✅ Successful: {success_count}/3")
print(f"   🔄 Pagination found: {pagination_found}")
print(f"   🔥 High review hotels: {high_review_count}")

# Key findings
if pagination_found > 0:
    print(f"\n🎉 SUCCESS: Pagination system working with luxury hotels!")
elif success_count >= 2:
    print(f"\n📈 PARTIAL SUCCESS: System working but no pagination detected")
    print(f"💡 May need different search strategies or hotel selection")
else:
    print(f"\n⚠️ ISSUES: System not responding reliably")

# Recommendations
print(f"\n💡 RECOMMENDATIONS:")
if high_review_count == 0:
    print(f"   🔍 Try more specific luxury hotel searches")
    print(f"   🌍 Consider different geographic locations")

if pagination_found == 0:
    print(f"   🔧 May need investigation of current pagination button detection")
    print(f"   📊 Current hotels may have limited reviews available")

print(f"\n🕐 Completed: {datetime.now().strftime('%H:%M:%S')}")
print("="*50)
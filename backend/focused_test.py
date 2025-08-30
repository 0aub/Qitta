#!/usr/bin/env python3
"""
FOCUSED PAGINATION VALIDATION TEST
==================================
Quick validation of key pagination functionality
"""

import os, time, requests, json

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

def quick_test(name, location, level, expected_reviews="any"):
    print(f"\n🧪 {name}")
    print(f"📍 Location: {location}")
    print(f"🔢 Level: {level}")
    
    result = submit("booking-hotels", {
        "location": location,
        "scrape_level": level,
        "max_results": 1
    })
    
    if result["status"] == "finished":
        hotels = result["result"].get("hotels", [])
        if hotels:
            hotel = hotels[0]
            reviews = len(hotel.get('reviews', []))
            method = hotel.get('extraction_method', 'Unknown')
            pages = hotel.get('pages_processed', 1)
            
            print(f"   ✅ Found: {hotel.get('name', 'Unknown')}")
            print(f"   📝 Reviews: {reviews}")
            print(f"   📄 Pages: {pages}")
            print(f"   🔧 Method: {method}")
            
            # Key validations
            issues = []
            if level == 4 and 'PAGINATION' not in method and pages == 1:
                issues.append("Level 4 not using pagination")
            if reviews == 0 and expected_reviews != "zero":
                issues.append("No reviews extracted")
            if pages > 1:
                print(f"   🎉 PAGINATION CONFIRMED!")
            
            return {
                'success': True,
                'reviews': reviews,
                'pages': pages,
                'method': method,
                'issues': issues
            }
    
    print(f"   ❌ Test failed")
    return {'success': False, 'issues': ['Test execution failed']}

print("🎯 FOCUSED PAGINATION VALIDATION")
print("="*50)

# Quick tests to validate core functionality
tests = [
    ("Level 3 Basic Test", "Dubai hotel", 3, "few"),
    ("Level 4 Pagination Test", "Dubai hotel", 4, "any"),
    ("High Review Hotel", "Burj Al Arab Dubai", 4, "many")
]

all_results = []
total_issues = []

for test_name, location, level, expected in tests:
    result = quick_test(test_name, location, level, expected)
    all_results.append((test_name, result))
    total_issues.extend(result.get('issues', []))

print("\n" + "="*50)
print("🎯 FOCUSED TEST SUMMARY")
print("="*50)

pagination_working = False
for test_name, result in all_results:
    if result.get('pages', 1) > 1:
        pagination_working = True
        print(f"✅ {test_name}: Pagination confirmed ({result['pages']} pages)")
    elif result.get('success'):
        print(f"⚠️  {test_name}: Single page ({result.get('reviews', 0)} reviews)")
    else:
        print(f"❌ {test_name}: Failed")

if pagination_working:
    print(f"\n🎉 PAGINATION SYSTEM WORKING!")
else:
    print(f"\n⚠️  Pagination needs investigation")

if total_issues:
    print(f"\n❌ Issues found: {len(total_issues)}")
    for issue in set(total_issues):
        print(f"   • {issue}")
else:
    print(f"\n✅ No critical issues found!")

print("="*50)
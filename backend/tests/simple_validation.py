#!/usr/bin/env python3
"""Simple validation of current system state"""

import os, requests, json

EP = os.getenv("BROWSER_ENDPOINT", "http://localhost:8004")

def test_level(location, level):
    """Quick test of a specific level"""
    print(f"\n🧪 Testing Level {level} with '{location}'")
    
    r = requests.post(f"{EP}/jobs/booking-hotels", json={
        "location": location,
        "scrape_level": level, 
        "max_results": 1
    })
    
    if r.status_code == 200:
        job_id = r.json()["job_id"]
        print(f"🆔 Job submitted: {job_id}")
        
        # Wait briefly and check status
        import time
        time.sleep(30)  # Wait 30 seconds
        
        status = requests.get(f"{EP}/jobs/{job_id}").json()
        print(f"📊 Status: {status.get('status', 'unknown')}")
        
        if status.get("status") == "finished":
            result = status.get("result", {})
            hotels = result.get("hotels", [])
            
            if hotels:
                hotel = hotels[0]
                print(f"✅ Hotel: {hotel.get('name', 'Unknown')}")
                print(f"📝 Reviews: {len(hotel.get('reviews', []))}")
                print(f"📄 Pages: {hotel.get('pages_processed', 1)}")
                print(f"🔧 Method: {hotel.get('extraction_method', 'Unknown')}")
                
                return {
                    'reviews': len(hotel.get('reviews', [])),
                    'pages': hotel.get('pages_processed', 1),
                    'method': hotel.get('extraction_method', 'Unknown')
                }
        else:
            print(f"⏳ Still running or failed: {status.get('status')}")
    
    return None

# Test different scenarios
print("🎯 SIMPLE VALIDATION TEST")
print("="*50)

# Test 1: Verify Level 3 works
result_3 = test_level("Dubai hotel", 3)

# Test 2: Verify Level 4 works  
result_4 = test_level("Dubai hotel", 4)

# Test 3: Try a well-known high-review hotel
result_luxury = test_level("Burj Al Arab Dubai", 4)

print("\n" + "="*50)
print("🎯 VALIDATION SUMMARY")
print("="*50)

results = [
    ("Level 3", result_3),
    ("Level 4", result_4), 
    ("Luxury Hotel", result_luxury)
]

working_count = 0
pagination_found = False

for name, result in results:
    if result:
        working_count += 1
        if result['pages'] > 1:
            pagination_found = True
            print(f"✅ {name}: {result['reviews']} reviews, {result['pages']} pages - PAGINATION!")
        else:
            print(f"⚠️  {name}: {result['reviews']} reviews, {result['pages']} page")
    else:
        print(f"❌ {name}: Failed or timeout")

print(f"\n📊 Results:")
print(f"   • Working tests: {working_count}/3")
print(f"   • Pagination found: {'YES' if pagination_found else 'NO'}")

if working_count >= 2:
    print(f"✅ System is functional")
else:
    print(f"❌ System needs investigation")

print("="*50)
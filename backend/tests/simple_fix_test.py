#!/usr/bin/env python3
"""Simple test to validate the specific fixes"""

import requests, json, time

EP = "http://localhost:8004"

print("🔧 SIMPLE FIX VALIDATION")

# Test Level 3 (should now extract reviews)
print(f"\n🧪 Testing Level 3...")
r = requests.post(f"{EP}/jobs/booking-hotels", json={
    "location": "Dubai", 
    "scrape_level": 3,
    "max_results": 1
})

if r.status_code == 200:
    job_id = r.json()["job_id"]
    print(f"Job: {job_id}")
    
    # Quick check
    time.sleep(60)  # Wait 1 minute
    status = requests.get(f"{EP}/jobs/{job_id}").json()
    
    if status["status"] == "finished":
        hotel = status["result"]["hotels"][0]
        reviews = hotel.get('reviews', [])
        
        print(f"📊 Level 3 Results:")
        print(f"   Reviews: {len(reviews)}")
        
        if len(reviews) > 0:
            print(f"✅ LEVEL 3 FIXED!")
            # Check first review
            review = reviews[0]
            name = review.get('reviewer_name', 'No name')
            text = review.get('review_text', 'No text')[:60]
            
            print(f"   First review:")
            print(f"     Name: '{name}'")
            print(f"     Text: '{text}...'")
            
            if len(name) < 30 and 'everything was' not in name.lower():
                print(f"✅ REVIEWER NAME FIXED!")
            else:
                print(f"❌ Reviewer name still broken")
        else:
            print(f"❌ Level 3 still extracts 0 reviews")
    else:
        print(f"⏳ Still running: {status['status']}")
else:
    print(f"❌ Request failed")
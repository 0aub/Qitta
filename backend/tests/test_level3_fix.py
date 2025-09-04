#!/usr/bin/env python3
"""Test Level 3 fix immediately"""

import requests, time, json

EP = "http://localhost:8004"

print("🧪 Testing Level 3 Navigation Fix")

# Test Level 3
r = requests.post(f"{EP}/jobs/booking-hotels", json={
    "location": "Dubai",
    "scrape_level": 3,
    "max_results": 1
})

if r.status_code == 200:
    job_id = r.json()["job_id"]
    print(f"Job: {job_id}")
    
    # Wait and check
    for i in range(20):  # 1 minute max
        time.sleep(3)
        status = requests.get(f"{EP}/jobs/{job_id}").json()
        
        if status["status"] == "finished":
            hotel = status["result"]["hotels"][0]
            reviews = hotel.get('reviews', [])
            
            print(f"\n📊 LEVEL 3 TEST RESULT:")
            print(f"   Hotel: {hotel.get('name')}")
            print(f"   Reviews: {len(reviews)}")
            
            if len(reviews) > 0:
                print(f"🎉 LEVEL 3 FIXED! Now extracts {len(reviews)} reviews")
                # Check first review
                review = reviews[0]
                name = review.get('reviewer_name', 'No name')
                text = review.get('review_text', 'No text')[:50]
                print(f"   First review: Name='{name}' Text='{text}...'")
            else:
                print(f"❌ Level 3 still broken - 0 reviews")
            break
        else:
            print(f"⏳ {status['status']}")
    
print("Done")
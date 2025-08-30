#!/usr/bin/env python3
"""Quick test of fixed enhanced pagination"""

import os, time, requests, json

EP = os.getenv("BROWSER_ENDPOINT", "http://localhost:8004")

print("🔧 TESTING FIXED ENHANCED PAGINATION")
print("=" * 50)

# Test with a luxury hotel
search_term = "Ritz Carlton Dubai"
print(f"🏨 Testing: {search_term}")

try:
    # Submit Level 4 job
    r = requests.post(f"{EP}/jobs/booking-hotels", json={
        "location": search_term,
        "scrape_level": 4,
        "max_results": 1
    })
    
    if r.status_code == 200:
        job_id = r.json()["job_id"]
        print(f"🆔 Job submitted: {job_id}")
        
        # Wait for completion
        for i in range(40):  # 2 minutes max
            time.sleep(3)
            status = requests.get(f"{EP}/jobs/{job_id}").json()
            
            if status["status"] == "finished":
                result = status.get("result", {})
                hotels = result.get("hotels", [])
                
                if hotels:
                    hotel = hotels[0]
                    reviews = hotel.get('reviews', [])
                    pages = hotel.get('pages_processed', 1)
                    
                    print(f"✅ Success!")
                    print(f"   Hotel: {hotel.get('name', 'Unknown')}")
                    print(f"   Reviews: {len(reviews)}")
                    print(f"   Pages: {pages}")
                    
                    if len(reviews) > 0:
                        print(f"🎉 FIXED! Found {len(reviews)} reviews!")
                        if pages > 1:
                            print(f"🚀 PAGINATION WORKING! {pages} pages processed!")
                    else:
                        print(f"⚠️ Still no reviews found")
                else:
                    print(f"❌ No hotels found")
                break
            elif status["status"] == "error":
                print(f"❌ Job failed: {status.get('error')}")
                break
            else:
                print(f"⏳ Status: {status['status']}")
        else:
            print(f"⏰ Timeout")
    else:
        print(f"❌ Request failed: {r.status_code}")
        
except Exception as e:
    print(f"❌ Exception: {e}")

print("=" * 50)
#!/usr/bin/env python3
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
    print(f"🚀 Submitting {task} task...")
    print(f"📝 Payload: {json.dumps(payload, indent=2)}")
    r = requests.post(f"{EP}/jobs/{task}", json=payload)
    r.raise_for_status()
    jid = r.json()["job_id"]
    print(f"🆔 Job ID: {jid}")
    return wait_for(jid)

print("🎯 TESTING WITH USER'S EXACT URL")
print("="*70)
print("🔍 Testing the exact URL from user's message:")
print("🌐 https://www.booking.com/hotel/ae/local-at-jumeirah-village-triangle-by-the-first-collection.html")

# Test with the EXACT URL the user provided (truncated for testing)
payload = {
    "location": "https://www.booking.com/hotel/ae/local-at-jumeirah-village-triangle-by-the-first-collection.html", 
    "scrape_level": 4,
    "max_results": 1
}

print(f"📝 Payload: {payload}")
result = submit("booking-hotels", payload)

if result["status"] == "finished":
    res = result["result"]
    hotels = res.get("hotels", [])
    
    if hotels:
        hotel = hotels[0]
        print(f"\n📊 DIRECT URL RESULTS:")
        print(f"   🏨 Hotel: {hotel.get('name', 'Unknown')}")
        print(f"   📝 Reviews extracted: {len(hotel.get('reviews', []))}")
        print(f"   📊 Review count from page: {hotel.get('review_count', 0)}")
        print(f"   🔧 Extraction method: {hotel.get('extraction_method', 'Unknown')}")
        print(f"   🌐 Actual URL visited: {hotel.get('booking_url', 'Unknown')}")
        
        print(f"\n🎯 ANALYSIS:")
        actual_reviews = len(hotel.get('reviews', []))
        if actual_reviews >= 200:
            print(f"   ✅ SUCCESS: Extracted {actual_reviews} reviews!")
        elif actual_reviews >= 50:
            print(f"   ⚠️  PARTIAL: Extracted {actual_reviews} reviews (progress!)")
        else:
            print(f"   ❌ LIMITED: Only extracted {actual_reviews} reviews")
            
    else:
        print("❌ NO HOTELS FOUND")
else:
    print(f"❌ TEST FAILED: {result.get('error')}")

print("\n" + "="*70)
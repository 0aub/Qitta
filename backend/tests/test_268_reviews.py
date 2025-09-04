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

print("🎯 TESTING USER'S 268 REVIEW HOTEL")
print("="*70)
print("🔍 Target: Hotel with 268 reviews (27 pages x 10 = 270)")
print("🎯 Expected: Level 4 should extract ALL 268+ reviews, not just 18")

# Test the specific hotel from user's example
payload = {
    "location": "Hotel Local Dubai at Jumeirah Village Triangle", 
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
        print(f"\n📊 ACTUAL RESULTS:")
        print(f"   🏨 Hotel: {hotel.get('name', 'Unknown')}")
        print(f"   📝 Reviews extracted: {len(hotel.get('reviews', []))}")
        print(f"   📊 Review count claimed by system: {hotel.get('review_count', 0)}")
        print(f"   🔧 Extraction method: {hotel.get('extraction_method', 'Unknown')}")
        
        print(f"\n🎯 ANALYSIS:")
        actual_reviews = len(hotel.get('reviews', []))
        if actual_reviews >= 200:
            print(f"   ✅ SUCCESS: Extracted {actual_reviews} reviews (close to expected 268)")
        elif actual_reviews >= 50:
            print(f"   ⚠️  PARTIAL: Extracted {actual_reviews} reviews (better but still missing many)")
        else:
            print(f"   ❌ FAILED: Only extracted {actual_reviews} reviews (far from 268)")
            print(f"   🚨 CRITICAL: System is not implementing proper pagination")
            print(f"   🔧 NEED: Implement 'Next page' clicking for ALL 27 pages")
            
    else:
        print("❌ NO HOTELS FOUND")
else:
    print(f"❌ TEST FAILED: {result.get('error')}")

print("\n" + "="*70)
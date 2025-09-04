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

# Specific Level 4 DEBUG test with the exact hotel the user mentioned
print("🔥 LEVEL 4 SPECIFIC DEBUG TEST")
print("="*70)
print("🎯 Target: Test user-provided selectors on ONE&ONLY hotel")

payload = {
    "location": "One&Only One Za'abeel Dubai", 
    "scrape_level": 4,
    "max_results": 1  # Single hotel for precise debugging
}

print(f"📝 Payload: {payload}")
result = submit("booking-hotels", payload)

# Enhanced analysis for debugging Level 4 review extraction
if result["status"] == "finished":
    res = result["result"]
    hotels = res.get("hotels", [])
    
    if hotels:
        hotel = hotels[0]
        print(f"\n🔍 DETAILED LEVEL 4 ANALYSIS:")
        print(f"   🏨 Hotel: {hotel.get('name', 'Unknown')}")
        print(f"   📝 Reviews extracted: {len(hotel.get('reviews', []))}")
        print(f"   📊 Review count claimed: {hotel.get('review_count', 0)}")
        print(f"   🔧 Extraction method: {hotel.get('extraction_method', 'Unknown')}")
        print(f"   ⚠️  Level 4 attempted: {hotel.get('level_4_attempted', False)}")
        
        reviews = hotel.get('reviews', [])
        if reviews:
            print(f"\n📋 EXTRACTED REVIEWS:")
            for i, review in enumerate(reviews[:3]):  # Show first 3
                print(f"   {i+1}. {review.get('reviewer_name', 'No name')}: {review.get('review_text', 'No text')[:100]}...")
        else:
            print(f"\n❌ NO REVIEWS EXTRACTED")
            print(f"   🚨 This indicates the user-provided selectors may not be working yet")
            print(f"   🔧 Need to check browser logs for selector debugging info")
            
        # Check if it fell back to Level 3
        if hotel.get('extraction_method') == 'LEVEL_4_FALLBACK_TO_LEVEL_3':
            print(f"   ⚠️  Level 4 FELL BACK to Level 3")
            print(f"   🔧 This suggests Level 4 selectors didn't find any reviews")
            
    else:
        print(f"❌ NO HOTELS FOUND")
        print(f"   🔧 Location search may have failed")
        
else:
    print(f"❌ TEST FAILED: {result.get('error', 'Unknown error')}")

print("\n" + "="*70)
print("🔍 DIAGNOSIS:")
if result["status"] == "finished" and hotels and len(hotels[0].get('reviews', [])) == 0:
    print("❌ LEVEL 4 STILL NOT EXTRACTING REVIEWS")
    print("🔧 NEXT STEPS:")
    print("   1. Check browser service logs for selector debug messages")
    print("   2. Verify user-provided selectors are being used")
    print("   3. Test individual selectors manually")
else:
    print("✅ LEVEL 4 WORKING - Reviews successfully extracted!")
print("="*70)
#!/usr/bin/env python3
"""
VALIDATION TEST - Check if the specific issues are fixed
"""

import os, time, requests, json

EP = os.getenv("BROWSER_ENDPOINT", "http://localhost:8004")

def test_level_carefully(level, expected_behavior):
    """Test a specific level with careful validation"""
    print(f"\n🎯 TESTING LEVEL {level}")
    print(f"📋 Expected: {expected_behavior}")
    
    try:
        # Submit test job
        response = requests.post(f"{EP}/jobs/booking-hotels", json={
            "location": "Dubai",
            "scrape_level": level,
            "max_results": 1
        })
        
        if response.status_code != 200:
            print(f"❌ Request failed")
            return False
        
        job_id = response.json()["job_id"]
        print(f"🆔 Job: {job_id}")
        
        # Wait for completion
        for i in range(30):  # 1.5 minutes
            time.sleep(3)
            status = requests.get(f"{EP}/jobs/{job_id}").json()
            
            if status["status"] == "finished":
                break
            elif status["status"] == "error":
                print(f"❌ Failed: {status.get('error')}")
                return False
        else:
            print(f"⏰ Timeout")
            return False
        
        # Analyze results
        hotels = status.get("result", {}).get("hotels", [])
        if not hotels:
            print(f"❌ No hotels found")
            return False
        
        hotel = hotels[0]
        reviews = hotel.get('reviews', [])
        pages = hotel.get('pages_processed', 1)
        
        print(f"✅ Hotel: {hotel.get('name', 'Unknown')}")
        print(f"📝 Reviews extracted: {len(reviews)}")
        print(f"📄 Pages processed: {pages}")
        
        # Level-specific validation
        if level == 3:
            if len(reviews) > 0:
                print(f"✅ FIXED: Level 3 now extracts reviews!")
                
                # Check reviewer names
                for i, review in enumerate(reviews[:3]):
                    name = review.get('reviewer_name', 'No name')
                    text = review.get('review_text', 'No text')[:50]
                    print(f"   Review {i+1}: Name='{name}' Text='{text}...'")
                    
                    # Check if name looks like review text
                    if len(name) > 30 or 'everything was' in name.lower():
                        print(f"   ❌ STILL BROKEN: Name looks like review text")
                        return False
                    elif name.lower() in ['wonderful', 'excellent', 'amazing']:
                        print(f"   ❌ STILL BROKEN: Generic word as name")
                        return False
                    else:
                        print(f"   ✅ Name looks valid")
                
                return True
            else:
                print(f"❌ STILL BROKEN: Level 3 extracts 0 reviews")
                return False
        
        elif level == 4:
            if pages > 1:
                print(f"✅ FIXED: Level 4 now uses pagination!")
                return True
            elif len(reviews) > 18:
                print(f"✅ IMPROVED: More reviews than before")
                return True
            else:
                print(f"❌ STILL BROKEN: Only 1 page, same review count")
                return False
        
        return True
        
    except Exception as e:
        print(f"❌ Exception: {e}")
        return False

print("🔧 VALIDATING SPECIFIC FIXES")
print("=" * 50)

# Test the specific issues
results = []

# Test 1: Level 3 should now extract reviews
level3_fixed = test_level_carefully(3, "Extract 2-5 reviews with valid reviewer names")
results.append(("Level 3 Review Extraction", level3_fixed))

# Test 2: Level 4 should now use pagination or extract more reviews
level4_fixed = test_level_carefully(4, "Use pagination or extract more reviews")
results.append(("Level 4 Pagination", level4_fixed))

# Summary
print(f"\n🎯 VALIDATION SUMMARY")
print("=" * 50)

fixed_count = 0
for test_name, result in results:
    status = "✅ FIXED" if result else "❌ STILL BROKEN"
    print(f"{status}: {test_name}")
    if result:
        fixed_count += 1

print(f"\nFixed: {fixed_count}/{len(results)} issues")

if fixed_count == len(results):
    print(f"🎉 ALL ISSUES FIXED!")
elif fixed_count > 0:
    print(f"📈 PARTIAL SUCCESS - Continue fixing remaining issues")
else:
    print(f"🚨 NO FIXES WORKING - Need deeper investigation")

print("=" * 50)
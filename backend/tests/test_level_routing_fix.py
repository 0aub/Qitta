#!/usr/bin/env python3
"""
Test that level routing fix works - each level should use correct method
"""

import requests
import time

def test_level_routing():
    print("🔍 TESTING LEVEL ROUTING FIX")
    
    levels_to_test = [1, 3, 4]  # Test key levels
    
    for level in levels_to_test:
        print(f"\n📍 TESTING LEVEL {level} ROUTING...")
        
        params = {
            "level": level,
            "location": "Dubai Marina",
            "max_hotels": 2,
            "checkin": "2025-09-02",
            "checkout": "2025-09-05"
        }
        
        response = requests.post("http://localhost:8004/jobs/booking_hotels", json=params)
        
        if response.status_code == 200:
            job_id = response.json().get("job_id")
            print(f"✅ Level {level} job started: {job_id}")
            
            # Quick poll to see what method is used
            for i in range(12):  # 2 minutes
                time.sleep(10)
                status_response = requests.get(f"http://localhost:8004/jobs/{job_id}")
                
                if status_response.status_code == 200:
                    data = status_response.json()
                    status = data.get("status")
                    
                    if status in ["completed", "finished"]:
                        metadata = data.get("result", {}).get("search_metadata", {})
                        method = metadata.get("extraction_method")
                        scrape_level = metadata.get("scrape_level")
                        
                        print(f"✅ Level {level} completed:")
                        print(f"   Requested level: {level}")
                        print(f"   Actual scrape_level: {scrape_level}")
                        print(f"   Method: {method}")
                        
                        # Verify correct mapping
                        expected_methods = {
                            1: "level_1_quick_search",
                            2: "level_2_full_data", 
                            3: "level_3_basic_reviews",
                            4: "level_4_deep_reviews"
                        }
                        
                        expected = expected_methods.get(level)
                        if method == expected:
                            print(f"   ✅ CORRECT: {method}")
                        else:
                            print(f"   ❌ WRONG: Expected {expected}, got {method}")
                        
                        break
                    elif status == "failed":
                        print(f"❌ Level {level} failed")
                        break
                    elif i % 6 == 0:
                        print(f"   ⏳ Level {level} {status}... ({i//6}min)")
            else:
                print(f"⏳ Level {level} still running after 2min")
        else:
            print(f"❌ Level {level} submission failed")

if __name__ == "__main__":
    test_level_routing()
    print("\n🎯 Level routing test complete")
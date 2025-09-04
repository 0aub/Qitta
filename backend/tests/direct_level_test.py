#!/usr/bin/env python3
"""
Direct test of level methods to see if they work independently
"""

import asyncio
import json
import requests

async def direct_level_test():
    print("🔍 DIRECT LEVEL METHOD TEST")
    
    # Test each level with explicit parameters
    levels_to_test = [
        {"level": 1, "description": "Level 1 Quick"},
        {"level": 2, "description": "Level 2 Full"}, 
        {"level": 3, "description": "Level 3 Reviews"},
        {"level": 4, "description": "Level 4 Deep Reviews"}
    ]
    
    for test_config in levels_to_test:
        level = test_config["level"]
        desc = test_config["description"]
        
        print(f"\n{'='*60}")
        print(f"🧪 TESTING {desc} (Level {level})")
        print(f"{'='*60}")
        
        # Very explicit parameters
        params = {
            "level": level,           # Primary parameter
            "scrape_level": level,    # Backup parameter 
            "location": "Dubai Marina",
            "max_hotels": 2,
            "checkin": "2025-09-02", 
            "checkout": "2025-09-05"
        }
        
        print(f"📋 Parameters: {json.dumps(params, indent=2)}")
        
        try:
            # Submit job
            response = requests.post(
                "http://localhost:8004/jobs/booking_hotels",
                json=params,
                timeout=30
            )
            
            if response.status_code == 200:
                job_data = response.json()
                job_id = job_data.get("job_id")
                print(f"✅ Job submitted: {job_id}")
                
                # Quick check - poll for 2 minutes
                for i in range(12):
                    await asyncio.sleep(10)
                    
                    status_response = requests.get(f"http://localhost:8004/jobs/{job_id}")
                    if status_response.status_code == 200:
                        data = status_response.json()
                        status = data.get("status")
                        
                        if status in ["completed", "finished"]:
                            result = data.get("result", {})
                            metadata = result.get("search_metadata", {})
                            hotels = result.get("hotels", [])
                            
                            # Key metrics
                            actual_method = metadata.get("extraction_method")
                            actual_level = metadata.get("scrape_level")
                            success_rate = metadata.get("success_rate", 0)
                            
                            print(f"✅ Level {level} completed:")
                            print(f"   Requested level: {level}")
                            print(f"   Actual scrape_level: {actual_level}")
                            print(f"   Method used: {actual_method}")
                            print(f"   Hotels: {len(hotels)}")
                            print(f"   Success rate: {success_rate:.1%}")
                            
                            # Check if method matches expectation
                            expected_methods = {
                                1: "level_1_quick_search",
                                2: "level_2_full_data", 
                                3: "level_3_basic_reviews",
                                4: "level_4_deep_reviews"
                            }
                            
                            expected = expected_methods.get(level)
                            method_correct = actual_method == expected
                            level_correct = actual_level == level
                            
                            print(f"   Method correct: {'✅' if method_correct else '❌'} (expected {expected})")
                            print(f"   Level correct: {'✅' if level_correct else '❌'} (expected {level})")
                            
                            # Check variables for this level
                            if hotels:
                                sample = hotels[0]
                                price = sample.get('price_per_night', 0)
                                rating = sample.get('rating', 0)
                                reviews = sample.get('reviews', [])
                                
                                print(f"   Sample price: ${price}")
                                print(f"   Sample rating: {rating}★")
                                if level >= 3:
                                    print(f"   Sample reviews: {len(reviews)} extracted")
                            
                            break
                            
                        elif status == "failed":
                            error = data.get("error", "Unknown")
                            print(f"❌ Level {level} failed: {error}")
                            break
                        elif i == 0 or i % 6 == 0:
                            print(f"   ⏳ {status}...")
                else:
                    print(f"⏳ Level {level} still running after 2 minutes")
            else:
                print(f"❌ Level {level} submission failed: {response.status_code}")
                
        except Exception as e:
            print(f"❌ Level {level} error: {e}")

if __name__ == "__main__":
    asyncio.run(direct_level_test())
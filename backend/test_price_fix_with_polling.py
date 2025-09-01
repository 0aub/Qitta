#!/usr/bin/env python3
"""
Test the updated price extraction fix with proper job polling
"""

import requests
import time
import json

def test_price_fix():
    print("🔍 TESTING PRICE EXTRACTION FIX...")
    
    # Test Level 1 with Dubai search
    test_params = {
        "level": 1,
        "location": "Dubai Marina",
        "max_hotels": 3,
        "checkin": "2025-09-02",
        "checkout": "2025-09-05"
    }
    
    print(f"📍 Testing Level 1 with: {test_params}")
    
    try:
        # Start the job
        response = requests.post(
            "http://localhost:8004/jobs/booking_hotels",
            json=test_params,
            timeout=30
        )
        
        print(f"📡 Job submission response: {response.status_code}")
        
        if response.status_code == 200:
            job_data = response.json()
            job_id = job_data.get("job_id")
            
            if not job_id:
                print(f"❌ No job_id received: {job_data}")
                return False
                
            print(f"✅ Job started: {job_id}")
            
            # Poll for completion
            max_polls = 30  # 5 minutes max
            poll_count = 0
            
            while poll_count < max_polls:
                poll_count += 1
                print(f"📊 Polling {poll_count}/{max_polls}...")
                
                try:
                    status_response = requests.get(f"http://localhost:8004/jobs/{job_id}")
                    
                    if status_response.status_code == 200:
                        status_data = status_response.json()
                        status = status_data.get("status")
                        
                        print(f"   Status: {status}")
                        
                        if status == "completed":
                            result = status_data.get("result", {})
                            hotels = result.get("hotels", [])
                            
                            print(f"✅ SUCCESS: Job completed with {len(hotels)} hotels")
                            
                            # Check prices
                            hotels_with_prices = [h for h in hotels if h.get('price_per_night', 0) > 0]
                            price_success_rate = len(hotels_with_prices) / len(hotels) * 100 if hotels else 0
                            
                            print(f"💰 Price extraction results:")
                            print(f"   Hotels with prices: {len(hotels_with_prices)}/{len(hotels)} ({price_success_rate:.1f}%)")
                            
                            for i, hotel in enumerate(hotels[:3]):
                                name = hotel.get('name', 'Unknown')[:30]
                                price = hotel.get('price_per_night', 0)
                                print(f"   Hotel {i+1}: {name}... -> ${price}/night")
                            
                            if price_success_rate > 50:
                                print("🎉 PRICE FIX SUCCESSFUL! Success rate > 50%")
                                return True
                            elif price_success_rate > 0:
                                print("⚠️ PARTIAL SUCCESS: Some prices extracted")
                                return True
                            else:
                                print("❌ PRICE FIX FAILED: No prices extracted")
                                return False
                                
                        elif status == "failed":
                            error = status_data.get("error", "Unknown error")
                            print(f"❌ Job failed: {error}")
                            return False
                        elif status in ["running", "pending"]:
                            time.sleep(10)  # Wait 10 seconds
                            continue
                        else:
                            print(f"🤔 Unknown status: {status}")
                            time.sleep(10)
                            continue
                    else:
                        print(f"❌ Status check failed: {status_response.status_code}")
                        time.sleep(10)
                        continue
                        
                except Exception as e:
                    print(f"❌ Polling error: {e}")
                    time.sleep(10)
                    continue
            
            print("❌ Job timed out")
            return False
        else:
            print(f"❌ Job submission failed: {response.status_code}")
            if response.text:
                print(f"   Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

if __name__ == "__main__":
    success = test_price_fix()
    print(f"🎯 Test result: {'SUCCESS' if success else 'FAILED'}")
    exit(0 if success else 1)
#!/usr/bin/env python3
"""
Quick validation of the fixes with Level 1 only
"""

import requests
import time

def quick_validation():
    print("🔍 QUICK VALIDATION TEST - Level 1 Only")
    
    test_params = {
        "level": 1,
        "location": "Dubai Marina",
        "max_hotels": 2,
        "checkin": "2025-09-02",
        "checkout": "2025-09-05"
    }
    
    try:
        # Start job
        print("📡 Starting Level 1 test job...")
        response = requests.post(
            "http://localhost:8004/jobs/booking_hotels",
            json=test_params,
            timeout=30
        )
        
        if response.status_code != 200:
            print(f"❌ Job submission failed: {response.status_code}")
            return False
            
        job_data = response.json()
        job_id = job_data.get("job_id")
        print(f"✅ Job started: {job_id}")
        
        # Poll for 3 minutes max
        for i in range(18):  # 18 * 10 seconds = 3 minutes
            time.sleep(10)
            print(f"⏳ Polling {i+1}/18...")
            
            try:
                status_response = requests.get(f"http://localhost:8004/jobs/{job_id}")
                
                if status_response.status_code == 200:
                    status_data = status_response.json()
                    status = status_data.get("status")
                    
                    if status == "completed":
                        result = status_data.get("result", {})
                        hotels = result.get("hotels", [])
                        metadata = result.get("search_metadata", {})
                        
                        print(f"🎉 SUCCESS! Job completed")
                        print(f"📊 Results: {len(hotels)} hotels found")
                        print(f"💰 Success rate: {metadata.get('success_rate', 0):.1%}")
                        print(f"💵 Average price: ${metadata.get('average_price', 0):.2f}")
                        
                        # Check fixes specifically
                        hotels_with_prices = [h for h in hotels if h.get('price_per_night', 0) > 0]
                        price_fix_works = len(hotels_with_prices) > 0
                        success_rate_fix_works = metadata.get('success_rate', 0) > 0
                        
                        print(f"🔧 FIXES VALIDATION:")
                        print(f"   Price extraction: {'✅' if price_fix_works else '❌'} ({len(hotels_with_prices)}/{len(hotels)} with prices)")
                        print(f"   Success rate calc: {'✅' if success_rate_fix_works else '❌'} ({metadata.get('success_rate', 0):.1%})")
                        
                        return price_fix_works and success_rate_fix_works
                        
                    elif status == "failed":
                        error = status_data.get("error", "Unknown error")
                        print(f"❌ Job failed: {error}")
                        return False
                    elif status in ["running", "pending"]:
                        continue
                        
            except Exception as e:
                print(f"⚠️ Poll {i+1} error: {e}")
                continue
        
        print("❌ Job timed out after 3 minutes")
        return False
        
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        return False

if __name__ == "__main__":
    success = quick_validation()
    print(f"🎯 Quick validation: {'PASSED' if success else 'FAILED'}")
    exit(0 if success else 1)
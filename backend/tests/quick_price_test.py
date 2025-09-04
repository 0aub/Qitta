#!/usr/bin/env python3
"""
Quick test of enhanced price extraction
"""

import requests
import time

def quick_price_test():
    print("🔍 QUICK PRICE EXTRACTION TEST")
    
    params = {
        "level": 1,
        "location": "Dubai Marina", 
        "max_hotels": 2,
        "checkin": "2025-09-02",
        "checkout": "2025-09-05"
    }
    
    try:
        response = requests.post("http://localhost:8004/jobs/booking_hotels", json=params)
        
        if response.status_code == 200:
            job_data = response.json()
            job_id = job_data.get("job_id")
            print(f"✅ Job started: {job_id}")
            
            # Quick polling
            for i in range(24):  # 4 minutes max
                time.sleep(10)
                
                status_response = requests.get(f"http://localhost:8004/jobs/{job_id}")
                if status_response.status_code == 200:
                    data = status_response.json()
                    status = data.get("status")
                    
                    if status in ["completed", "finished"]:
                        hotels = data.get("result", {}).get("hotels", [])
                        metadata = data.get("result", {}).get("search_metadata", {})
                        
                        print(f"✅ Quick test completed: {len(hotels)} hotels")
                        print(f"Success rate: {metadata.get('success_rate', 0):.1%}")
                        
                        # Check prices specifically
                        for j, hotel in enumerate(hotels):
                            name = hotel.get('name', 'Unknown')[:25]
                            price = hotel.get('price_per_night', 0)
                            rating = hotel.get('rating', 0)
                            print(f"  Hotel {j+1}: {name}... -> ${price}/night, {rating}★")
                        
                        prices = [h.get('price_per_night', 0) for h in hotels]
                        working_prices = [p for p in prices if p > 0]
                        price_rate = len(working_prices) / len(prices) * 100 if prices else 0
                        
                        print(f"💰 Price fix status: {len(working_prices)}/{len(prices)} ({price_rate:.1f}%)")
                        
                        return price_rate > 0
                        
                    elif status == "failed":
                        print(f"❌ Job failed: {data.get('error')}")
                        return False
                    elif i % 6 == 0:
                        print(f"   ⏳ {status}... ({i//6}min)")
            
            print("❌ Timeout")
            return False
        else:
            print(f"❌ Failed to start job: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Test error: {e}")
        return False

if __name__ == "__main__":
    success = quick_price_test()
    print(f"🎯 Price fix test: {'PASSED' if success else 'FAILED'}")
    exit(0 if success else 1)
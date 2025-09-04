#!/usr/bin/env python3
"""
Investigate why Level 4 has 100% price success while others fail
"""

import requests
import time

def test_level4_independently():
    print("🔍 INVESTIGATING LEVEL 4 SUCCESS vs OTHER LEVELS")
    
    # Test Level 4 independently
    params = {
        "level": 4,
        "location": "Dubai Marina",
        "max_hotels": 2,
        "checkin": "2025-09-02",
        "checkout": "2025-09-05"
    }
    
    print(f"📍 Testing Level 4 independently: {params}")
    
    response = requests.post("http://localhost:8004/jobs/booking_hotels", json=params)
    
    if response.status_code == 200:
        job_data = response.json()
        job_id = job_data.get("job_id")
        print(f"✅ Level 4 job started: {job_id}")
        
        # Wait for completion
        for i in range(30):  # 5 minutes
            time.sleep(10)
            status_response = requests.get(f"http://localhost:8004/jobs/{job_id}")
            
            if status_response.status_code == 200:
                data = status_response.json()
                status = data.get("status")
                
                if status in ["completed", "finished"]:
                    result = data.get("result", {})
                    hotels = result.get("hotels", [])
                    metadata = result.get("search_metadata", {})
                    
                    print(f"✅ Level 4 completed: {len(hotels)} hotels")
                    print(f"Success rate: {metadata.get('success_rate', 0):.1%}")
                    print(f"Average price: ${metadata.get('average_price', 0):.2f}")
                    print(f"Scrape level: {metadata.get('scrape_level')}")
                    print(f"Method: {metadata.get('extraction_method')}")
                    
                    # Check what makes Level 4 different
                    if hotels:
                        sample = hotels[0]
                        print(f"\n📊 Level 4 Sample Hotel:")
                        print(f"   Name: {sample.get('name')}")
                        print(f"   Price: ${sample.get('price_per_night', 0)}")
                        print(f"   Reviews extracted: {len(sample.get('reviews', []))}")
                        print(f"   Review count: {sample.get('review_count', 0)}")
                        print(f"   Source: {sample.get('source', 'unknown')}")
                    
                    # Check if Level 4 has different data sources
                    sources = set(h.get('source', 'unknown') for h in hotels)
                    print(f"   Data sources: {sources}")
                    
                    return {
                        'hotels': len(hotels),
                        'success_rate': metadata.get('success_rate', 0),
                        'price_success': len([h for h in hotels if h.get('price_per_night', 0) > 0]) / len(hotels) * 100,
                        'review_success': len([h for h in hotels if h.get('reviews') and len(h['reviews']) > 0]) / len(hotels) * 100,
                        'scrape_level': metadata.get('scrape_level'),
                        'method': metadata.get('extraction_method')
                    }
                
                elif status == "failed":
                    print(f"❌ Level 4 failed: {data.get('error')}")
                    return None
                elif i % 6 == 0:
                    print(f"   ⏳ Still {status}... ({i//6}min)")
        
        print("❌ Level 4 timed out")
        return None
    else:
        print(f"❌ Level 4 submission failed: {response.status_code}")
        return None

if __name__ == "__main__":
    result = test_level4_independently()
    if result:
        print(f"\n🎯 Level 4 Analysis Complete:")
        print(f"   Price success: {result['price_success']:.1f}%")
        print(f"   Review success: {result['review_success']:.1f}%")
        print(f"   Method: {result['method']}")
    else:
        print("\n❌ Level 4 investigation failed")
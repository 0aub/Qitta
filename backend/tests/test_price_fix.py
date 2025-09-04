#!/usr/bin/env python3
"""
Test the updated price extraction fix
"""

import asyncio
import json
import requests

async def test_price_fix():
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
        response = requests.post(
            "http://localhost:8004/jobs/booking_hotels",
            json=test_params,
            timeout=120
        )
        
        if response.status_code == 200:
            result = response.json()
            
            if result.get("status") == "completed":
                hotels = result["result"]["hotels"]
                print(f"✅ SUCCESS: Found {len(hotels)} hotels")
                
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
            else:
                print(f"❌ API Error: {result.get('error', 'Unknown error')}")
                return False
        else:
            print(f"❌ HTTP Error: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

if __name__ == "__main__":
    success = asyncio.run(test_price_fix())
    exit(0 if success else 1)
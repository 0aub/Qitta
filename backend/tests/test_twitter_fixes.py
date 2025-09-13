#!/usr/bin/env python3
"""
Quick test of Twitter scraper fixes
"""

import requests
import json
import time
import os

# Browser endpoint
EP = os.getenv("BROWSER_ENDPOINT", "http://localhost:8004")

def test_twitter_scraper():
    print("🐦 Testing Twitter scraper improvements...")
    
    # Test payload with level 1 (should be different from before)
    payload = {
        "username": "naval",
        "scrape_posts": True,
        "max_posts": 5,
        "level": 1,
        "scrape_level": 1
    }
    
    print(f"📝 Testing Level 1 extraction for @naval")
    print(f"📡 Endpoint: {EP}")
    print(f"📦 Payload: {json.dumps(payload, indent=2)}")
    
    try:
        # Submit job
        r = requests.post(f"{EP}/jobs/twitter", json=payload, timeout=30)
        r.raise_for_status()
        
        job_id = r.json()["job_id"]
        print(f"🆔 Job ID: {job_id}")
        
        # Wait for completion (with timeout)
        max_wait = 300  # 5 minutes max
        start_time = time.time()
        
        while time.time() - start_time < max_wait:
            status_r = requests.get(f"{EP}/jobs/{job_id}")
            status_r.raise_for_status()
            
            result = status_r.json()
            status = result["status"]
            
            print(f"⏱️ Status: {status}")
            
            if status == "finished":
                print("✅ COMPLETED!")
                
                # Analyze results
                data = result.get("result", {}).get("data", [])
                metadata = result.get("result", {}).get("search_metadata", {})
                
                print(f"\n📊 RESULTS ANALYSIS:")
                print(f"   📈 Success Rate: {metadata.get('success_rate', 0):.1%}")
                print(f"   🎯 Extraction Method: {metadata.get('extraction_method', 'N/A')}")
                print(f"   📊 Data Items: {len(data)}")
                print(f"   📈 Scrape Level: {metadata.get('scrape_level', 'N/A')}")
                
                if data:
                    first_item = data[0]
                    print(f"   🔍 First Item Type: {first_item.get('type', 'N/A')}")
                    print(f"   📝 Username: {first_item.get('username', 'N/A')}")
                    
                    # Check for profile data
                    profile = first_item.get('profile', {})
                    if profile:
                        print(f"   👤 Profile Display Name: {profile.get('display_name', 'MISSING')}")
                        print(f"   📊 Followers: {profile.get('followers_count', 'MISSING')}")
                        print(f"   📝 Bio: {profile.get('bio', 'MISSING')[:50]}{'...' if len(profile.get('bio', '')) > 50 else ''}")
                    
                    # Check for posts data
                    posts = first_item.get('posts', [])
                    print(f"   📝 Posts Count: {len(posts)}")
                    if posts:
                        first_post = posts[0]
                        print(f"   📄 First Post Text: {first_post.get('text', 'MISSING')[:100]}{'...' if len(first_post.get('text', '')) > 100 else ''}")
                        print(f"   📅 First Post Date: {first_post.get('date', first_post.get('timestamp', 'MISSING'))}")
                        print(f"   ❤️ First Post Likes: {first_post.get('likes', 'MISSING')}")
                        print(f"   🔄 First Post Retweets: {first_post.get('retweets', 'MISSING')}")
                
                return True
                
            elif status == "error":
                print(f"❌ FAILED: {result.get('error', 'Unknown error')}")
                return False
            
            time.sleep(3)  # Wait 3 seconds before next check
        
        print("⏰ TIMEOUT: Test took too long")
        return False
        
    except Exception as e:
        print(f"❌ TEST FAILED: {e}")
        return False

if __name__ == "__main__":
    success = test_twitter_scraper()
    if success:
        print("🎉 Test completed successfully!")
    else:
        print("💥 Test failed!")
    exit(0 if success else 1)
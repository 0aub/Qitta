#!/usr/bin/env python3
"""
Quick Twitter Metadata Extraction Test
Test the enhanced date and engagement metrics extraction after container restart
"""

import requests
import json
import time
import os
from datetime import datetime

# Configuration
EP = os.getenv("BROWSER_ENDPOINT", "http://localhost:8004")
TIMEOUT = 180  # 3 minutes for quick test

def wait_for_job(job_id, max_wait=TIMEOUT):
    """Wait for job completion."""
    print(f"⏳ Waiting for job {job_id[:8]}...")
    start_time = time.time()
    
    while time.time() - start_time < max_wait:
        try:
            response = requests.get(f"{EP}/jobs/{job_id}", timeout=30)
            if response.status_code == 404:
                print(f"⚠️ Job {job_id[:8]} not found (404)")
                time.sleep(3)
                continue
                
            response.raise_for_status()
            result = response.json()
            status = result["status"]
            
            elapsed = time.time() - start_time
            
            if status == "finished":
                print(f"✅ Job completed in {elapsed:.1f}s")
                return result
            elif status == "error":
                print(f"❌ Job failed: {result.get('error', 'Unknown error')}")
                return result
            elif elapsed % 10 < 1:  # Log every 10 seconds
                print(f"⏱️  Job: {status} ({elapsed:.0f}s elapsed)")
                
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Connection error: {e}")
            
        time.sleep(3)
    
    print(f"⏰ Job timed out after {max_wait}s")
    return {"status": "timeout", "error": "Test timed out"}

def test_metadata_extraction():
    """Test enhanced metadata extraction on a simple profile."""
    print("🧪 QUICK METADATA EXTRACTION TEST")
    print("=" * 50)
    print(f"📍 Endpoint: {EP}")
    print(f"⏰ Started: {datetime.now().strftime('%H:%M:%S')}")
    
    # Test connectivity
    try:
        health_response = requests.get(f"{EP}/healthz", timeout=10)
        if health_response.status_code == 200:
            print("✅ API connectivity confirmed")
        else:
            print(f"⚠️ API responded with status {health_response.status_code}")
    except Exception as e:
        print(f"❌ API connectivity failed: {e}")
        return False
    
    # Submit simple test job
    payload = {
        "username": "naval",  # Naval has good engagement metrics
        "scrape_posts": True,
        "max_posts": 3,  # Small number for quick test
        "level": 2,
        "scrape_level": 2
    }
    
    try:
        print(f"\n🚀 Submitting test job...")
        print(f"📝 Payload: {json.dumps(payload, indent=2)}")
        
        response = requests.post(f"{EP}/jobs/twitter", json=payload, timeout=30)
        response.raise_for_status()
        
        job_data = response.json()
        job_id = job_data["job_id"]
        print(f"🆔 Job ID: {job_id}")
        
        result = wait_for_job(job_id)
        
        if result["status"] != "finished":
            print(f"❌ Job failed: {result.get('error', 'Unknown error')}")
            return False
        
        # Analyze metadata extraction
        print(f"\n📊 METADATA ANALYSIS")
        print("=" * 30)
        
        res = result.get("result", {})
        data = res.get("data", [])
        
        if not data:
            print("❌ No data returned")
            return False
        
        first_item = data[0]
        if 'posts' not in first_item:
            print("❌ No posts found")
            return False
        
        posts = first_item['posts']
        print(f"📝 Found {len(posts)} posts")
        
        # Check metadata completeness
        posts_with_dates = 0
        posts_with_likes = 0
        posts_with_retweets = 0
        posts_with_replies = 0
        
        for i, post in enumerate(posts):
            if isinstance(post, dict):
                has_date = post.get('date') is not None
                has_likes = post.get('likes') is not None
                has_retweets = post.get('retweets') is not None
                has_replies = post.get('replies') is not None
                
                if has_date: posts_with_dates += 1
                if has_likes: posts_with_likes += 1
                if has_retweets: posts_with_retweets += 1
                if has_replies: posts_with_replies += 1
                
                print(f"  Post {i+1}: Date={has_date} | Likes={has_likes} | RT={has_retweets} | Replies={has_replies}")
                if has_date:
                    print(f"    Date: {post.get('date')}")
                if has_likes:
                    print(f"    Likes: {post.get('likes')}")
        
        # Summary
        total_posts = len(posts)
        date_pct = (posts_with_dates / total_posts) * 100 if total_posts > 0 else 0
        likes_pct = (posts_with_likes / total_posts) * 100 if total_posts > 0 else 0
        
        print(f"\n📈 METADATA SUCCESS RATES:")
        print(f"  📅 Dates: {posts_with_dates}/{total_posts} ({date_pct:.0f}%)")
        print(f"  👍 Likes: {posts_with_likes}/{total_posts} ({likes_pct:.0f}%)")
        print(f"  🔄 Retweets: {posts_with_retweets}/{total_posts} ({likes_pct:.0f}%)")
        print(f"  💬 Replies: {posts_with_replies}/{total_posts} ({likes_pct:.0f}%)")
        
        # Success criteria: At least 60% metadata extraction
        metadata_success = date_pct >= 60 and likes_pct >= 60
        
        print(f"\n🎯 RESULT: {'✅ SUCCESS' if metadata_success else '⚠️ PARTIAL'}")
        if metadata_success:
            print("Enhanced metadata extraction is working!")
        else:
            print("Metadata extraction needs further improvement")
        
        return metadata_success
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

if __name__ == "__main__":
    success = test_metadata_extraction()
    exit(0 if success else 1)
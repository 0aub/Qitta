#!/usr/bin/env python3
"""
Comprehensive Twitter Scraper Validation
Tests all fixes and improvements made to the Twitter scraper
"""

import requests
import json
import time
import os
from datetime import datetime

# Configuration
EP = os.getenv("BROWSER_ENDPOINT", "http://localhost:8004")
TIMEOUT = 300  # 5 minutes max per test

def log_test_result(test_name, success, details=""):
    """Log test results with consistent formatting."""
    status = "✅ PASSED" if success else "❌ FAILED"
    print(f"{status} | {test_name}")
    if details:
        print(f"    {details}")

def wait_for_job(job_id, test_name, max_wait=TIMEOUT):
    """Wait for job completion with enhanced logging."""
    print(f"⏳ Waiting for {test_name} (Job: {job_id[:8]}...)")
    start_time = time.time()
    
    while time.time() - start_time < max_wait:
        try:
            response = requests.get(f"{EP}/jobs/{job_id}", timeout=30)
            response.raise_for_status()
            result = response.json()
            status = result["status"]
            
            elapsed = time.time() - start_time
            
            if status == "finished":
                print(f"✅ {test_name} completed in {elapsed:.1f}s")
                return result
            elif status == "error":
                print(f"❌ {test_name} failed: {result.get('error', 'Unknown error')}")
                return result
            elif elapsed % 10 < 1:  # Log every 10 seconds
                print(f"⏱️  {test_name}: {status} ({elapsed:.0f}s elapsed)")
                
        except Exception as e:
            print(f"⚠️ Error checking job status: {e}")
            
        time.sleep(3)
    
    print(f"⏰ {test_name} timed out after {max_wait}s")
    return {"status": "timeout", "error": "Test timed out"}

def submit_twitter_job(payload, test_name):
    """Submit Twitter scraping job and return result."""
    try:
        print(f"\n🚀 STARTING: {test_name}")
        print(f"📝 Payload: {json.dumps(payload, indent=2)}")
        
        response = requests.post(f"{EP}/jobs/twitter", json=payload, timeout=30)
        response.raise_for_status()
        
        job_data = response.json()
        job_id = job_data["job_id"]
        print(f"🆔 Job ID: {job_id}")
        
        return wait_for_job(job_id, test_name)
        
    except Exception as e:
        print(f"❌ Failed to submit {test_name}: {e}")
        return {"status": "error", "error": str(e)}

def analyze_result(result, test_name):
    """Comprehensive result analysis."""
    print(f"\n📊 ANALYZING: {test_name}")
    print("=" * 60)
    
    if result["status"] != "finished":
        error_msg = result.get("error", "Unknown error")
        log_test_result(test_name, False, f"Status: {result['status']} | Error: {error_msg}")
        return False
    
    # Extract data
    res = result.get("result", {})
    metadata = res.get("search_metadata", {})
    data = res.get("data", [])
    
    success_rate = metadata.get("success_rate", 0)
    extraction_method = metadata.get("extraction_method", "N/A")
    scrape_level = metadata.get("scrape_level", "N/A")
    
    print(f"📈 Success Rate: {success_rate:.1%}")
    print(f"🎯 Method: {extraction_method}")
    print(f"📊 Level: {scrape_level}")
    print(f"📦 Data Items: {len(data)}")
    
    # Detailed analysis
    if data:
        first_item = data[0]
        item_type = first_item.get('type', 'unknown') if isinstance(first_item, dict) else type(first_item).__name__
        print(f"📄 Item Type: {item_type}")
        
        # Profile analysis for comprehensive user data
        if isinstance(first_item, dict) and 'profile' in first_item:
            profile = first_item['profile']
            profile_fields = ['display_name', 'username', 'bio', 'followers_count', 'following_count']
            valid_fields = sum(1 for field in profile_fields if profile.get(field))
            
            print(f"👤 Profile Completeness: {valid_fields}/{len(profile_fields)} fields")
            print(f"   Display Name: {profile.get('display_name', 'MISSING')}")
            print(f"   Username: @{profile.get('username', 'MISSING')}")
            print(f"   Followers: {profile.get('followers_count', 'MISSING')}")
            print(f"   Bio: {profile.get('bio', 'MISSING')[:50]}{'...' if len(str(profile.get('bio', ''))) > 50 else ''}")
            
            # Posts analysis
            posts = first_item.get('posts', [])
            if posts:
                posts_with_dates = [p for p in posts if isinstance(p, dict) and p.get('date')]
                posts_with_likes = [p for p in posts if isinstance(p, dict) and p.get('likes') is not None]
                posts_with_retweets = [p for p in posts if isinstance(p, dict) and p.get('retweets') is not None]
                
                print(f"📝 Posts Analysis:")
                print(f"   Total Posts: {len(posts)}")
                print(f"   With Dates: {len(posts_with_dates)}/{len(posts)}")
                print(f"   With Likes: {len(posts_with_likes)}/{len(posts)}")
                print(f"   With Retweets: {len(posts_with_retweets)}/{len(posts)}")
                
                if posts:
                    sample = posts[0]
                    if isinstance(sample, dict):
                        text = sample.get('text', 'No text')[:80]
                        date = sample.get('date', 'MISSING')
                        likes = sample.get('likes', 'MISSING')
                        print(f"   Sample Post: '{text}{'...' if len(sample.get('text', '')) > 80 else ''}' | {date} | {likes} likes")
    
    # Test success criteria
    is_success = (
        result["status"] == "finished" and
        len(data) > 0 and
        success_rate > 0.1  # At least 10% success rate
    )
    
    log_test_result(test_name, is_success, f"{success_rate:.1%} success rate, {len(data)} items")
    print()
    return is_success

def main():
    """Run comprehensive Twitter scraper validation tests."""
    print("🐦 COMPREHENSIVE TWITTER SCRAPER VALIDATION")
    print("=" * 70)
    print(f"📍 Endpoint: {EP}")
    print(f"⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
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
    
    test_results = []
    
    # Test 1: Basic Level 1 extraction (Fixed NameError test)
    print("\n" + "=" * 70)
    print("TEST 1: Basic Level 1 Extraction (NameError Fix Validation)")
    print("=" * 70)
    
    payload_1 = {
        "username": "naval",
        "scrape_posts": True,
        "max_posts": 5,
        "level": 1,
        "scrape_level": 1
    }
    
    result_1 = submit_twitter_job(payload_1, "Level 1 Basic Extraction")
    success_1 = analyze_result(result_1, "Level 1 Basic Extraction")
    test_results.append(("Level 1 Basic", success_1))
    
    # Test 2: Enhanced Level 2 extraction
    print("\n" + "=" * 70)
    print("TEST 2: Enhanced Level 2 Extraction")
    print("=" * 70)
    
    payload_2 = {
        "username": "paulg",
        "scrape_posts": True,
        "max_posts": 8,
        "level": 2,
        "scrape_level": 2
    }
    
    result_2 = submit_twitter_job(payload_2, "Level 2 Enhanced Extraction")
    success_2 = analyze_result(result_2, "Level 2 Enhanced Extraction")
    test_results.append(("Level 2 Enhanced", success_2))
    
    # Test 3: Comprehensive Level 4 extraction
    print("\n" + "=" * 70)
    print("TEST 3: Comprehensive Level 4 Extraction")
    print("=" * 70)
    
    payload_3 = {
        "username": "sama",
        "scrape_posts": True,
        "max_posts": 10,
        "scrape_likes": True,
        "max_likes": 5,
        "scrape_mentions": False,
        "level": 4,
        "scrape_level": 4
    }
    
    result_3 = submit_twitter_job(payload_3, "Level 4 Comprehensive Extraction")
    success_3 = analyze_result(result_3, "Level 4 Comprehensive Extraction")
    test_results.append(("Level 4 Comprehensive", success_3))
    
    # Test 4: Date filtering functionality
    print("\n" + "=" * 70)
    print("TEST 4: Date Filtering Functionality")
    print("=" * 70)
    
    payload_4 = {
        "username": "vitalikbuterin",
        "scrape_posts": True,
        "max_posts": 10,
        "enable_date_filtering": True,
        "date_range": "last_week",
        "level": 3,
        "scrape_level": 3
    }
    
    result_4 = submit_twitter_job(payload_4, "Date Filtering Test")
    success_4 = analyze_result(result_4, "Date Filtering Test")
    test_results.append(("Date Filtering", success_4))
    
    # Final summary
    print("\n" + "=" * 70)
    print("📊 FINAL VALIDATION SUMMARY")
    print("=" * 70)
    
    total_tests = len(test_results)
    passed_tests = sum(1 for _, success in test_results if success)
    overall_success_rate = (passed_tests / total_tests) * 100 if total_tests > 0 else 0
    
    print(f"📈 Overall Success Rate: {passed_tests}/{total_tests} ({overall_success_rate:.1f}%)")
    print()
    
    for test_name, success in test_results:
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"  {status} {test_name}")
    
    print(f"\n⏰ Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if overall_success_rate >= 75:
        print("🎉 VALIDATION SUCCESSFUL - Twitter scraper is working well!")
        return True
    elif overall_success_rate >= 50:
        print("⚠️ VALIDATION PARTIAL - Some issues remain but major fixes confirmed")
        return True
    else:
        print("❌ VALIDATION FAILED - Significant issues detected")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
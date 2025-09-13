#!/usr/bin/env python3
"""
Debug Extraction Test
Quick test to see the actual extraction errors with enhanced logging
"""

import requests
import json
import time
import os

EP = os.getenv("BROWSER_ENDPOINT", "http://localhost:8004")

def quick_debug_test():
    print("🔍 DEBUG EXTRACTION TEST")
    print("=" * 40)
    
    payload = {
        "username": "naval",
        "scrape_posts": True,
        "max_posts": 2,  # Very small for quick test
        "level": 1,
        "scrape_level": 1
    }
    
    try:
        print("🚀 Submitting debug job...")
        response = requests.post(f"{EP}/jobs/twitter", json=payload, timeout=30)
        response.raise_for_status()
        
        job_data = response.json()
        job_id = job_data["job_id"]
        print(f"🆔 Job ID: {job_id}")
        
        # Wait briefly then check status
        time.sleep(30)
        
        status_response = requests.get(f"{EP}/jobs/{job_id}", timeout=10)
        status_response.raise_for_status()
        result = status_response.json()
        
        print(f"📊 Status: {result['status']}")
        if result['status'] == 'finished':
            print("✅ Job completed")
        else:
            print(f"⏳ Job still running: {result['status']}")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

if __name__ == "__main__":
    quick_debug_test()
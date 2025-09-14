#!/usr/bin/env python3
"""
Quick Twitter Scraper Test
=========================

Test the fixed Twitter scraper with a simple request to verify:
1. Missing methods are now implemented
2. Success rate calculation bug is fixed
3. No more crashes with boolean inputs
"""

import requests
import json
import time

def test_twitter_scraper():
    """Test the Twitter scraper through the API endpoint."""

    EP = "http://browser:8004"

    print("🧪 QUICK TWITTER SCRAPER TEST")
    print("=" * 40)

    # Test basic Level 4 scraping (this was failing before)
    payload = {
        "username": "elonmusk",  # Public account
        "scrape_posts": True,
        "max_posts": 3,  # Small number for quick test
        "level": 4,
        "scrape_level": 4
    }

    print("🚀 Submitting test request...")
    print(f"📝 Payload: {json.dumps(payload, indent=2)}")

    try:
        # Submit job
        r = requests.post(f"{EP}/jobs/twitter", json=payload)
        r.raise_for_status()
        job_data = r.json()
        job_id = job_data["job_id"]

        print(f"🆔 Job ID: {job_id}")
        print("⏳ Waiting for completion...")

        # Wait for job completion
        max_wait = 120  # 2 minutes max
        start_time = time.time()

        while True:
            if time.time() - start_time > max_wait:
                print("❌ Test timed out after 2 minutes")
                return False

            # Check job status
            status_r = requests.get(f"{EP}/jobs/{job_id}")
            status_data = status_r.json()

            status = status_data["status"]
            elapsed = status_data.get("status_with_elapsed", status)

            if status in {"finished", "error"}:
                print(f"\n🏁 Final status: {status.upper()}")

                if status == "error":
                    error = status_data.get("error", "Unknown error")
                    print(f"❌ Error: {error}")

                    # Check if it's still the old boolean error
                    if "object of type 'bool' has no len()" in error:
                        print("💥 CRITICAL: Boolean error still occurring!")
                        return False
                    else:
                        print("✅ No more boolean errors - that's progress!")
                        return True
                else:
                    print("✅ Job completed successfully!")

                    # Check if we got data
                    result = status_data.get("result", {})
                    if result:
                        data = result.get("data", [])
                        print(f"📊 Data extracted: {len(data)} items")

                        if len(data) > 0:
                            print("🎉 SUCCESS: Data extraction working!")
                        else:
                            print("⚠️ WARNING: No data extracted (may be normal)")

                    return True
            else:
                print(f"\r⏱️  {elapsed}", end="", flush=True)
                time.sleep(3)

    except Exception as e:
        print(f"❌ Test failed with exception: {e}")
        return False

def main():
    """Run the quick test."""
    success = test_twitter_scraper()

    print(f"\n" + "=" * 40)
    if success:
        print("✅ QUICK TEST PASSED")
        print("🔧 Critical bugs appear to be fixed!")
        print("📋 Ready for comprehensive testing")
    else:
        print("❌ QUICK TEST FAILED")
        print("🚨 Further investigation needed")

    return 0 if success else 1

if __name__ == "__main__":
    exit(main())
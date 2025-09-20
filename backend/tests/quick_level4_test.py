#!/usr/bin/env python3
"""
Quick Level 4 Test - Validate Level 4 fixes without external dependencies
"""
import json
import urllib.request
import urllib.parse
import time

def test_level4():
    """Quick test of Level 4 improvements"""
    print("🧪 LEVEL 4 VALIDATION TEST")
    print("=" * 50)

    endpoint = "http://localhost:8004"

    # Test Level 4 with small request (should force large-scale)
    payload = {
        "username": "naval",
        "scrape_posts": True,
        "max_posts": 8,  # Small request - should be overridden by Level 4
        "scrape_level": 4
    }

    print(f"🚀 Testing Level 4 with max_posts=8 (should force 50+ extraction)")
    print(f"📝 Payload: {json.dumps(payload, indent=2)}")

    try:
        # Submit job
        data = json.dumps(payload).encode('utf-8')
        req = urllib.request.Request(
            f"{endpoint}/jobs/twitter",
            data=data,
            headers={'Content-Type': 'application/json'}
        )

        with urllib.request.urlopen(req, timeout=30) as response:
            result = json.loads(response.read().decode('utf-8'))
            job_id = result["job_id"]
            print(f"🆔 Job ID: {job_id}")

        # Wait for completion
        print(f"⏳ Waiting for job completion...")
        for i in range(60):  # Wait up to 3 minutes
            try:
                with urllib.request.urlopen(f"{endpoint}/jobs/{job_id}", timeout=10) as response:
                    result = json.loads(response.read().decode('utf-8'))
                    status = result["status"]

                    if status in ["finished", "error"]:
                        break

                print(f"\\r⏱️  Status: {status} ({i*3}s)", end="")
                time.sleep(3)

            except Exception as e:
                print(f"\\n❌ Error checking status: {e}")
                return

        print(f"\\n✅ Job completed with status: {status}")

        if status == "error":
            print(f"❌ Job failed: {result.get('error', 'Unknown error')}")
            return

        # Analyze results
        if "result" not in result:
            print(f"❌ No result data found")
            return

        res = result["result"]
        metadata = res.get("search_metadata", {})
        data = res.get("data", [])

        print(f"\\n📊 LEVEL 4 TEST RESULTS:")
        print(f"   Target: @{metadata.get('target_username', 'N/A')}")
        print(f"   Level: {metadata.get('scrape_level', 'N/A')}")
        print(f"   Method: {metadata.get('extraction_method', 'N/A')}")
        print(f"   Success Rate: {metadata.get('success_rate', 0):.1%}")

        if data and len(data) > 0:
            first_item = data[0]
            if 'posts' in first_item:
                posts = first_item['posts']
                posts_count = len(posts)
                print(f"   Posts Extracted: {posts_count}")

                # Check for validation metadata
                validated = sum(1 for p in posts if 'extraction_method' in p and 'validated' in p.get('extraction_method', ''))
                print(f"   Validated Posts: {validated}/{posts_count}")

                # Test success criteria
                print(f"\\n🎯 LEVEL 4 FIX VALIDATION:")
                if posts_count >= 50:
                    print(f"   ✅ LARGE SCALE: {posts_count} posts (target: 50+)")
                elif posts_count >= 20:
                    print(f"   ⚡ IMPROVED: {posts_count} posts (better than previous 5-8)")
                else:
                    print(f"   ❌ NEEDS WORK: Only {posts_count} posts (expected 50+)")

                if validated > 0:
                    print(f"   ✅ VALIDATION: {validated} posts have validation metadata")
                else:
                    print(f"   ❌ VALIDATION: No validation metadata found")

                # Sample validation method
                sample_post = next((p for p in posts if p.get('extraction_method')), None)
                if sample_post:
                    print(f"   📝 Sample Method: {sample_post.get('extraction_method', 'N/A')}")
                    print(f"   📝 Sample Confidence: {sample_post.get('validation_confidence', 'N/A')}")

                # Check for NEW comprehensive data types
                print(f"\\n🔍 COMPREHENSIVE DATA CHECK:")
                reposts = first_item.get('reposts', [])
                followers = first_item.get('followers', [])
                following = first_item.get('following', [])
                media = first_item.get('media', [])

                print(f"   Reposts: {len(reposts)} items {'✅' if len(reposts) > 0 else '❌'}")
                print(f"   Followers: {len(followers)} items {'✅' if len(followers) > 0 else '❌'}")
                print(f"   Following: {len(following)} items {'✅' if len(following) > 0 else '❌'}")
                print(f"   Media: {len(media)} items {'✅' if len(media) > 0 else '❌'}")

            else:
                print(f"   ❌ Unexpected data structure")
        else:
            print(f"   ❌ No data extracted")

    except Exception as e:
        print(f"❌ Test failed: {e}")

if __name__ == "__main__":
    test_level4()
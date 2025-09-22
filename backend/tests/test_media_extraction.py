#!/usr/bin/env python3
"""
Test 1.1.A: Media Extraction Validation
Test media extraction from high-media accounts like @elonmusk
"""
import json
import urllib.request
import urllib.parse
import time

def test_media_extraction():
    """Test 1.1.A: Extract from @elonmusk (high media usage) - should find 5+ posts with embedded media"""
    print("🧪 TEST 1.1.A: MEDIA EXTRACTION VALIDATION")
    print("=" * 60)

    endpoint = "http://localhost:8004"

    # Test with elonmusk (known for high media usage)
    payload = {
        "username": "elonmusk",
        "scrape_posts": True,
        "max_posts": 15,  # Get enough posts to find media
        "scrape_level": 4
    }

    print(f"🚀 Testing media extraction with @elonmusk (high media account)")
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
        for i in range(40):  # Wait up to 2 minutes
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

        # Analyze results for media extraction
        if "result" not in result:
            print(f"❌ No result data found")
            return

        res = result["result"]
        data = res.get("data", [])

        print(f"\\n📊 MEDIA EXTRACTION TEST RESULTS:")

        if data and len(data) > 0:
            first_item = data[0]
            if 'posts' in first_item:
                posts = first_item['posts']
                posts_count = len(posts)
                print(f"   Total Posts: {posts_count}")

                # Count posts with media
                posts_with_media = 0
                total_media_items = 0
                media_types = {}

                for i, post in enumerate(posts):
                    post_media = post.get('media', [])
                    has_media = post.get('has_media', False)

                    if post_media and len(post_media) > 0:
                        posts_with_media += 1
                        total_media_items += len(post_media)

                        print(f"\\n   📷 Post {i+1} Media:")
                        print(f"      Text: {post.get('text', '')[:60]}...")
                        print(f"      Media Count: {len(post_media)}")

                        for media_item in post_media:
                            media_type = media_item.get('type', 'unknown')
                            media_url = media_item.get('url', 'no-url')
                            alt_text = media_item.get('alt_text', 'no-alt')

                            media_types[media_type] = media_types.get(media_type, 0) + 1

                            print(f"        - {media_type}: {media_url[:50]}...")
                            if alt_text:
                                print(f"          Alt: {alt_text}")

                print(f"\\n🎯 TEST 1.1.A VALIDATION RESULTS:")
                print(f"   Posts with Media: {posts_with_media}/{posts_count} ({posts_with_media/posts_count*100:.1f}%)")
                print(f"   Total Media Items: {total_media_items}")
                print(f"   Media Types Found: {media_types}")

                # Test success criteria
                if posts_with_media >= 5:
                    print(f"   ✅ SUCCESS: Found {posts_with_media} posts with media (target: 5+)")
                elif posts_with_media >= 2:
                    print(f"   ⚠️ PARTIAL: Found {posts_with_media} posts with media (target: 5+)")
                else:
                    print(f"   ❌ FAILED: Only {posts_with_media} posts with media (target: 5+)")

                if total_media_items >= 10:
                    print(f"   ✅ RICH MEDIA: {total_media_items} total media items detected")
                elif total_media_items >= 3:
                    print(f"   ⚠️ SOME MEDIA: {total_media_items} total media items detected")
                else:
                    print(f"   ❌ POOR MEDIA: Only {total_media_items} total media items detected")

                if len(media_types) >= 2:
                    print(f"   ✅ DIVERSE: Multiple media types found: {list(media_types.keys())}")
                else:
                    print(f"   ⚠️ LIMITED: Few media types found: {list(media_types.keys())}")

            else:
                print(f"   ❌ Unexpected data structure")
        else:
            print(f"   ❌ No data extracted")

    except Exception as e:
        print(f"❌ Test failed: {e}")

if __name__ == "__main__":
    test_media_extraction()
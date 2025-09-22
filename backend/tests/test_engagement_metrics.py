#!/usr/bin/env python3
"""
Test 1.2.A: Engagement Metrics Validation
Test engagement metrics extraction from high-engagement accounts
"""
import json
import urllib.request
import time

def test_engagement_metrics():
    """Test 1.2.A: Extract from @sama (high engagement) - verify metrics parsing"""
    print("🧪 TEST 1.2.A: ENGAGEMENT METRICS VALIDATION")
    print("=" * 60)

    endpoint = "http://localhost:8004"

    # Test with sama (known for high engagement on AI posts)
    payload = {
        "username": "sama",
        "scrape_posts": True,
        "max_posts": 10,  # Small test for quick results
        "scrape_level": 4
    }

    print(f"🚀 Testing engagement metrics with @sama (high engagement account)")
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

        # Analyze results for engagement metrics
        if "result" not in result:
            print(f"❌ No result data found")
            return

        res = result["result"]
        data = res.get("data", [])

        print(f"\\n📊 ENGAGEMENT METRICS TEST RESULTS:")

        if data and len(data) > 0:
            first_item = data[0]
            if 'posts' in first_item:
                posts = first_item['posts']
                posts_count = len(posts)
                print(f"   Total Posts: {posts_count}")

                # Count posts with engagement metrics
                posts_with_likes = 0
                posts_with_retweets = 0
                posts_with_replies = 0
                posts_with_views = 0

                total_likes = 0
                total_retweets = 0
                total_replies = 0
                total_views = 0

                for i, post in enumerate(posts):
                    likes = post.get('likes', None)
                    retweets = post.get('retweets', None)
                    replies = post.get('replies', None)
                    views = post.get('views', None)

                    if likes is not None:
                        posts_with_likes += 1
                        total_likes += likes
                    if retweets is not None:
                        posts_with_retweets += 1
                        total_retweets += retweets
                    if replies is not None:
                        posts_with_replies += 1
                        total_replies += replies
                    if views is not None:
                        posts_with_views += 1
                        total_views += views

                    # Show details for first few posts
                    if i < 3:
                        print(f"\\n   📊 Post {i+1} Engagement:")
                        print(f"      Text: {post.get('text', '')[:50]}...")
                        print(f"      Likes: {likes if likes is not None else 'missing'}")
                        print(f"      Retweets: {retweets if retweets is not None else 'missing'}")
                        print(f"      Replies: {replies if replies is not None else 'missing'}")
                        print(f"      Views: {views if views is not None else 'missing'}")

                print(f"\\n🎯 TEST 1.2.A VALIDATION RESULTS:")
                print(f"   Posts with Likes: {posts_with_likes}/{posts_count} ({posts_with_likes/posts_count*100:.1f}%)")
                print(f"   Posts with Retweets: {posts_with_retweets}/{posts_count} ({posts_with_retweets/posts_count*100:.1f}%)")
                print(f"   Posts with Replies: {posts_with_replies}/{posts_count} ({posts_with_replies/posts_count*100:.1f}%)")
                print(f"   Posts with Views: {posts_with_views}/{posts_count} ({posts_with_views/posts_count*100:.1f}%)")

                print(f"\\n📈 ENGAGEMENT TOTALS:")
                print(f"   Total Likes: {total_likes:,}")
                print(f"   Total Retweets: {total_retweets:,}")
                print(f"   Total Replies: {total_replies:,}")
                print(f"   Total Views: {total_views:,}")

                # Test success criteria
                engagement_coverage = (posts_with_likes + posts_with_retweets + posts_with_replies) / (posts_count * 3) * 100

                if engagement_coverage >= 80:
                    print(f"   ✅ EXCELLENT: {engagement_coverage:.1f}% engagement coverage")
                elif engagement_coverage >= 50:
                    print(f"   ⚠️ GOOD: {engagement_coverage:.1f}% engagement coverage")
                elif engagement_coverage >= 20:
                    print(f"   ⚠️ FAIR: {engagement_coverage:.1f}% engagement coverage")
                else:
                    print(f"   ❌ POOR: {engagement_coverage:.1f}% engagement coverage")

            else:
                print(f"   ❌ Unexpected data structure")
        else:
            print(f"   ❌ No data extracted")

    except Exception as e:
        print(f"❌ Test failed: {e}")

if __name__ == "__main__":
    test_engagement_metrics()
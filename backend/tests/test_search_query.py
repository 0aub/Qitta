#!/usr/bin/env python3
"""
Test 2.2.A: Search Query Scraping Validation
Test comprehensive search functionality including keywords, hashtags, and mentions
"""
import json
import urllib.request
import time

def test_search_query_scraping():
    """Test 2.2.A: Search query scraping with Phase 1 enhancements - test various query types"""
    print("🧪 TEST 2.2.A: SEARCH QUERY SCRAPING VALIDATION")
    print("=" * 60)

    endpoint = "http://localhost:8004"

    # Test cases for different search types
    test_cases = [
        {
            "name": "Keyword Search",
            "payload": {
                "search_query": "artificial intelligence",
                "max_tweets": 8,
                "result_type": "recent",
                "scrape_level": 4
            }
        },
        {
            "name": "Hashtag Search",
            "payload": {
                "search_query": "#AI",
                "max_tweets": 6,
                "result_type": "popular",
                "scrape_level": 4
            }
        },
        {
            "name": "Complex Query",
            "payload": {
                "search_query": "machine learning OR ML",
                "max_tweets": 5,
                "result_type": "mixed",
                "scrape_level": 4
            }
        }
    ]

    for test_case in test_cases:
        print(f"\n🔍 Testing {test_case['name']}")
        print(f"📝 Query: '{test_case['payload']['search_query']}'")
        print(f"🎯 Type: {test_case['payload']['result_type']}")

        try:
            # Submit job
            data = json.dumps(test_case['payload']).encode('utf-8')
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

                    if i % 5 == 0:  # Print status every 15 seconds
                        print(f"\\r⏱️  Status: {status} ({i*3}s)", end="")
                    time.sleep(3)

                except Exception as e:
                    print(f"\\n❌ Error checking status: {e}")
                    continue

            print(f"\\n✅ Job completed with status: {status}")

            if status == "error":
                print(f"❌ Job failed: {result.get('error', 'Unknown error')}")
                continue

            # Analyze results for search functionality
            if "result" not in result:
                print(f"❌ No result data found")
                continue

            res = result["result"]
            search_metadata = res.get("search_metadata", {})
            data = res.get("data", [])

            print(f"\\n📊 SEARCH RESULTS - {test_case['name']}:")

            if data and len(data) > 0:
                search_result = data[0]
                posts = search_result.get('posts', [])
                stats = search_result.get('extraction_stats', {})
                search_params = search_result.get('search_parameters', {})

                print(f"   Total Posts: {len(posts)}")
                print(f"   Search Method: {stats.get('extraction_method', 'N/A')}")
                print(f"   Average Relevance: {stats.get('avg_relevance', 0):.2f}")
                print(f"   Posts with Media: {stats.get('with_media', 0)}")
                print(f"   Posts with Engagement: {stats.get('with_engagement', 0)}")
                print(f"   Posts with Threads: {stats.get('with_threads', 0)}")

                # Show sample search results with Phase 1 enhancements
                for i, post in enumerate(posts[:2]):
                    print(f"\\n   🔍 Search Result {i+1}:")
                    print(f"      Text: {post.get('text', '')[:80]}...")
                    print(f"      Relevance: {post.get('relevance_score', 0):.2f}")
                    print(f"      Search Context: {post.get('search_context', 'N/A')}")

                    # Check Phase 1 enhancements
                    if post.get('has_media'):
                        print(f"      📷 Media: {len(post.get('media', []))} items")

                    # Engagement metrics
                    engagement = []
                    for metric in ['likes', 'retweets', 'replies', 'views']:
                        if post.get(metric) is not None:
                            engagement.append(f"{metric}:{post[metric]}")
                    if engagement:
                        print(f"      📊 Engagement: {engagement}")

                    # Thread information
                    thread_info = post.get('thread_info', {})
                    if thread_info.get('is_thread'):
                        indicators = thread_info.get('thread_indicators', [])
                        print(f"      🧵 Thread: {indicators}")

                    # Author information
                    print(f"      👤 Author: {post.get('author', 'N/A')}")

                # Test success criteria
                print(f"\\n🎯 {test_case['name']} VALIDATION:")
                if len(posts) >= 3:
                    print(f"   ✅ GOOD RESULTS: Found {len(posts)} posts")
                elif len(posts) >= 1:
                    print(f"   ⚠️ LIMITED RESULTS: Found {len(posts)} posts")
                else:
                    print(f"   ❌ NO RESULTS: Found {len(posts)} posts")

                if stats.get('avg_relevance', 0) >= 0.5:
                    print(f"   ✅ HIGH RELEVANCE: {stats.get('avg_relevance', 0):.2f} average")
                elif stats.get('avg_relevance', 0) >= 0.2:
                    print(f"   ⚠️ MEDIUM RELEVANCE: {stats.get('avg_relevance', 0):.2f} average")
                else:
                    print(f"   ❌ LOW RELEVANCE: {stats.get('avg_relevance', 0):.2f} average")

                # Phase 1 enhancement validation
                enhancement_score = (
                    (stats.get('with_media', 0) / max(len(posts), 1) * 30) +
                    (stats.get('with_engagement', 0) / max(len(posts), 1) * 40) +
                    (stats.get('with_threads', 0) / max(len(posts), 1) * 30)
                )

                if enhancement_score >= 60:
                    print(f"   🏆 EXCELLENT PHASE 1 INTEGRATION: {enhancement_score:.1f}% enhanced")
                elif enhancement_score >= 30:
                    print(f"   ✅ GOOD PHASE 1 INTEGRATION: {enhancement_score:.1f}% enhanced")
                else:
                    print(f"   ⚠️ LIMITED PHASE 1 INTEGRATION: {enhancement_score:.1f}% enhanced")

            else:
                print(f"   ❌ No search data extracted")

        except Exception as e:
            print(f"❌ {test_case['name']} test failed: {e}")

        print(f"\\n{'='*50}")

    print(f"\\n🏁 Search Query Testing Complete!")

if __name__ == "__main__":
    test_search_query_scraping()
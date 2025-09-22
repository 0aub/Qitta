#!/usr/bin/env python3
"""
Test 1.3.A: Thread Detection and Reconstruction Validation
Test thread detection from accounts known for creating threads like @naval
"""
import json
import urllib.request
import time

def test_thread_detection():
    """Test 1.3.A: Extract from @naval (known for threads) - should detect and reconstruct thread sequences"""
    print("🧪 TEST 1.3.A: THREAD DETECTION AND RECONSTRUCTION VALIDATION")
    print("=" * 60)

    endpoint = "http://localhost:8004"

    # Test with naval (known for thread creation)
    payload = {
        "username": "naval",
        "scrape_posts": True,
        "max_posts": 20,  # Get enough posts to find threads
        "scrape_level": 4
    }

    print(f"🚀 Testing thread detection with @naval (known for threads)")
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

        # Analyze results for thread detection
        if "result" not in result:
            print(f"❌ No result data found")
            return

        res = result["result"]
        data = res.get("data", [])

        print(f"\\n📊 THREAD DETECTION TEST RESULTS:")

        if data and len(data) > 0:
            first_item = data[0]
            if 'posts' in first_item:
                posts = first_item['posts']
                posts_count = len(posts)
                print(f"   Total Posts: {posts_count}")

                # Count posts with thread information
                posts_with_threads = 0
                thread_starters = 0
                thread_continuations = 0
                posts_with_numbering = 0
                total_threads_detected = 0

                thread_ids = set()

                for i, post in enumerate(posts):
                    thread_info = post.get('thread_info', {})

                    if thread_info.get('is_thread', False):
                        posts_with_threads += 1

                        # Count thread metadata
                        if thread_info.get('is_thread_starter', False):
                            thread_starters += 1

                        if thread_info.get('has_continuation', False):
                            thread_continuations += 1

                        if thread_info.get('thread_position') is not None:
                            posts_with_numbering += 1

                        # Track unique threads
                        thread_id = thread_info.get('thread_id')
                        if thread_id:
                            thread_ids.add(thread_id)

                        # Show details for first few thread posts
                        if i < 3 and thread_info.get('is_thread'):
                            print(f"\\n   🧵 Thread Post {i+1}:")
                            print(f"      Text: {post.get('text', '')[:80]}...")
                            print(f"      Thread ID: {thread_info.get('thread_id', 'N/A')}")
                            print(f"      Position: {thread_info.get('thread_position', 'N/A')}")
                            print(f"      Size: {thread_info.get('thread_size', 'N/A')}")
                            print(f"      Is Starter: {thread_info.get('is_thread_starter', False)}")
                            print(f"      Has Continuation: {thread_info.get('has_continuation', False)}")
                            print(f"      Indicators: {thread_info.get('thread_indicators', [])}")

                            # Check for reconstruction metadata
                            if thread_info.get('is_reconstructed'):
                                print(f"      Reconstructed Position: {thread_info.get('reconstructed_position', 'N/A')}")
                                print(f"      Reconstructed Size: {thread_info.get('reconstructed_thread_size', 'N/A')}")

                total_threads_detected = len(thread_ids)

                print(f"\\n🎯 TEST 1.3.A VALIDATION RESULTS:")
                print(f"   Posts with Thread Info: {posts_with_threads}/{posts_count} ({posts_with_threads/posts_count*100:.1f}%)")
                print(f"   Thread Starters: {thread_starters}")
                print(f"   Thread Continuations: {thread_continuations}")
                print(f"   Posts with Numbering: {posts_with_numbering}")
                print(f"   Unique Threads Detected: {total_threads_detected}")

                print(f"\\n🧵 THREAD ANALYSIS:")
                print(f"   Thread IDs Found: {sorted(list(thread_ids))}")

                # Test success criteria
                if posts_with_threads >= 3:
                    print(f"   ✅ EXCELLENT: Found {posts_with_threads} posts with thread detection")
                elif posts_with_threads >= 1:
                    print(f"   ⚠️ GOOD: Found {posts_with_threads} posts with thread detection")
                else:
                    print(f"   ❌ POOR: Only {posts_with_threads} posts with thread detection")

                if total_threads_detected >= 2:
                    print(f"   ✅ DIVERSE: Multiple unique threads detected ({total_threads_detected})")
                elif total_threads_detected >= 1:
                    print(f"   ⚠️ LIMITED: Single thread detected")
                else:
                    print(f"   ❌ FAILED: No threads detected")

                if posts_with_numbering >= 1:
                    print(f"   ✅ NUMBERED: Found {posts_with_numbering} posts with thread numbering")
                else:
                    print(f"   ⚠️ NO NUMBERING: No numbered threads found")

                # Overall assessment
                thread_detection_score = (
                    (posts_with_threads / posts_count * 40) +
                    (min(total_threads_detected, 3) / 3 * 30) +
                    (min(posts_with_numbering, 2) / 2 * 30)
                )

                if thread_detection_score >= 80:
                    print(f"   🏆 OUTSTANDING: {thread_detection_score:.1f}% thread detection effectiveness")
                elif thread_detection_score >= 60:
                    print(f"   ✅ EXCELLENT: {thread_detection_score:.1f}% thread detection effectiveness")
                elif thread_detection_score >= 40:
                    print(f"   ⚠️ GOOD: {thread_detection_score:.1f}% thread detection effectiveness")
                else:
                    print(f"   ❌ NEEDS IMPROVEMENT: {thread_detection_score:.1f}% thread detection effectiveness")

            else:
                print(f"   ❌ Unexpected data structure")
        else:
            print(f"   ❌ No data extracted")

    except Exception as e:
        print(f"❌ Test failed: {e}")

if __name__ == "__main__":
    test_thread_detection()
#!/usr/bin/env python3
"""
Test Phase 4.2: Performance and Anti-Detection Optimization Validation
Test performance metrics, caching, stealth measures, and optimization features
"""
import json
import urllib.request
import time

def test_phase42_performance_optimization():
    """Test Phase 4.2: Performance and anti-detection optimization - test metrics, caching, and stealth"""
    print("🧪 TEST PHASE 4.2: PERFORMANCE AND ANTI-DETECTION OPTIMIZATION")
    print("=" * 75)

    endpoint = "http://localhost:8004"

    # Test with performance monitoring enabled
    payload = {
        "username": "naval",
        "scrape_posts": True,
        "max_posts": 5,  # Small test for fast evaluation
        "scrape_level": 4
    }

    print(f"🚀 Testing Phase 4.2 Performance Optimization")
    print(f"📝 Payload: {json.dumps(payload, indent=2)}")

    try:
        # Submit job with timing measurement
        start_time = time.time()

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

        # Wait for completion with performance tracking
        print(f"⏳ Waiting for Phase 4.2 test completion...")
        for i in range(30):  # Wait up to 1.5 minutes
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

        job_duration = time.time() - start_time
        print(f"\\n✅ Job completed with status: {status}")
        print(f"⏱️ Total Job Duration: {job_duration:.2f}s")

        if status == "error":
            print(f"❌ Job failed: {result.get('error', 'Unknown error')}")
            return

        # Analyze Phase 4.2 performance results
        if "result" not in result:
            print(f"❌ No result data found")
            return

        res = result["result"]

        print(f"\\n📊 PHASE 4.2 PERFORMANCE ANALYSIS:")

        # Check for performance metrics (Phase 4.2 feature)
        performance_metrics = res.get("performance_metrics", {})
        if performance_metrics:
            print(f"\\n🚀 PERFORMANCE METRICS FOUND:")
            print(f"   Total Requests: {performance_metrics.get('total_requests', 0)}")
            print(f"   Cache Hits: {performance_metrics.get('cache_hits', 0)}")
            print(f"   Cache Hit Rate: {performance_metrics.get('cache_hit_rate', '0%')}")
            print(f"   Total Extraction Time: {performance_metrics.get('total_extraction_time', '0s')}")
            print(f"   Avg Extraction Time: {performance_metrics.get('avg_extraction_time', '0s')}")
            print(f"   Anti-Detection Actions: {performance_metrics.get('anti_detection_actions', 0)}")
            print(f"   Performance Mode: {performance_metrics.get('performance_mode', False)}")
            print(f"   Anti-Detection Enabled: {performance_metrics.get('anti_detection_enabled', False)}")
        else:
            print(f"   ❌ Performance metrics not found")

        # Analyze posts data
        data = res.get("data", [])
        if data and len(data) > 0:
            first_item = data[0]
            if 'posts' in first_item:
                posts = first_item['posts']
                posts_count = len(posts)
                print(f"\\n📝 POSTS ANALYSIS:")
                print(f"   Total Posts: {posts_count}")

                # Check extraction quality and completeness
                complete_posts = 0
                posts_with_enhancements = 0

                for post in posts:
                    # Check if post has all basic fields
                    if all(field in post for field in ['text', 'author', 'url']):
                        complete_posts += 1

                    # Check for Phase enhancements
                    has_enhancements = any([
                        post.get('media'),  # Phase 1.1
                        post.get('likes') is not None,  # Phase 1.2
                        post.get('thread_info', {}).get('is_thread'),  # Phase 1.3
                        post.get('classification')  # Phase 3.2
                    ])
                    if has_enhancements:
                        posts_with_enhancements += 1

                completion_rate = (complete_posts / posts_count) * 100 if posts_count > 0 else 0
                enhancement_rate = (posts_with_enhancements / posts_count) * 100 if posts_count > 0 else 0

                print(f"   Complete Posts: {complete_posts}/{posts_count} ({completion_rate:.1f}%)")
                print(f"   Enhanced Posts: {posts_with_enhancements}/{posts_count} ({enhancement_rate:.1f}%)")

                # Performance assessment
                print(f"\\n🎯 PHASE 4.2 VALIDATION RESULTS:")

                # Performance metrics validation
                if performance_metrics:
                    print(f"   ✅ METRICS: Performance tracking implemented")

                    cache_hit_rate = float(performance_metrics.get('cache_hit_rate', '0%').replace('%', ''))
                    if cache_hit_rate > 0:
                        print(f"   ✅ CACHING: {cache_hit_rate:.1f}% cache hit rate")
                    else:
                        print(f"   ⚠️ CACHING: No cache hits detected")

                    anti_detection_actions = performance_metrics.get('anti_detection_actions', 0)
                    if anti_detection_actions > 0:
                        print(f"   ✅ ANTI-DETECTION: {anti_detection_actions} stealth actions performed")
                    else:
                        print(f"   ⚠️ ANTI-DETECTION: No stealth actions recorded")
                else:
                    print(f"   ❌ METRICS: Performance tracking not detected")

                # Overall performance assessment
                if completion_rate >= 80:
                    print(f"   ✅ EXTRACTION QUALITY: {completion_rate:.1f}% completion rate")
                elif completion_rate >= 60:
                    print(f"   ⚠️ EXTRACTION QUALITY: {completion_rate:.1f}% completion rate")
                else:
                    print(f"   ❌ EXTRACTION QUALITY: {completion_rate:.1f}% completion rate")

                # Speed assessment
                if job_duration <= 60:
                    print(f"   ✅ PERFORMANCE SPEED: {job_duration:.1f}s extraction time")
                elif job_duration <= 120:
                    print(f"   ⚠️ PERFORMANCE SPEED: {job_duration:.1f}s extraction time")
                else:
                    print(f"   ❌ PERFORMANCE SPEED: {job_duration:.1f}s extraction time")

                # Overall Phase 4.2 score
                performance_score = 0
                if performance_metrics:
                    performance_score += 30
                if completion_rate >= 80:
                    performance_score += 30
                if job_duration <= 60:
                    performance_score += 20
                if enhancement_rate >= 60:
                    performance_score += 20

                if performance_score >= 80:
                    print(f"   🏆 OUTSTANDING: {performance_score}% Phase 4.2 optimization effectiveness")
                elif performance_score >= 60:
                    print(f"   ✅ EXCELLENT: {performance_score}% Phase 4.2 optimization effectiveness")
                elif performance_score >= 40:
                    print(f"   ⚠️ GOOD: {performance_score}% Phase 4.2 optimization effectiveness")
                else:
                    print(f"   ❌ NEEDS IMPROVEMENT: {performance_score}% Phase 4.2 optimization effectiveness")

            else:
                print(f"   ❌ Unexpected data structure")
        else:
            print(f"   ❌ No data extracted")

    except Exception as e:
        print(f"❌ Phase 4.2 test failed: {e}")

    print(f"\\n🏁 Phase 4.2 Performance and Anti-Detection Testing Complete!")

if __name__ == "__main__":
    test_phase42_performance_optimization()
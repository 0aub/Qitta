#!/usr/bin/env python3
"""
Test Phase 4.1: Export Format Extensions Validation
Test multiple export formats including JSON, CSV, XML, Markdown, and more
"""
import json
import urllib.request
import time
import os

def test_phase41_export_formats():
    """Test Phase 4.1: Multi-format export system - test JSON, CSV, XML, and Markdown formats"""
    print("🧪 TEST PHASE 4.1: EXPORT FORMAT EXTENSIONS VALIDATION")
    print("=" * 70)

    endpoint = "http://localhost:8004"

    # Test cases for different export formats
    test_cases = [
        {
            "name": "JSON Export (Enhanced)",
            "payload": {
                "username": "naval",
                "scrape_posts": True,
                "max_posts": 3,
                "scrape_level": 4,
                "export_formats": ["json"]
            }
        },
        {
            "name": "Multiple Formats Export",
            "payload": {
                "username": "naval",
                "scrape_posts": True,
                "max_posts": 3,
                "scrape_level": 4,
                "export_formats": ["json", "csv", "xml", "markdown"]
            }
        },
        {
            "name": "High-Performance Formats",
            "payload": {
                "username": "naval",
                "scrape_posts": True,
                "max_posts": 3,
                "scrape_level": 4,
                "export_formats": ["json", "parquet", "excel"]
            }
        }
    ]

    for test_case in test_cases:
        print(f"\n🔍 Testing {test_case['name']}")
        print(f"📊 Formats: {test_case['payload']['export_formats']}")

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
            print(f"⏳ Waiting for Phase 4.1 export test completion...")
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

            # Analyze Phase 4.1 export results
            if "result" not in result:
                print(f"❌ No result data found")
                continue

            res = result["result"]
            data = res.get("data", [])

            print(f"\\n📊 PHASE 4.1 EXPORT ANALYSIS - {test_case['name']}:")

            if data and len(data) > 0:
                first_item = data[0]
                if 'posts' in first_item:
                    posts = first_item['posts']
                    posts_count = len(posts)
                    print(f"   Total Posts: {posts_count}")

                    # Check for Phase 4.1 enhancements in the data
                    posts_with_phase_features = 0
                    formats_requested = test_case['payload']['export_formats']

                    for post in posts:
                        has_phase_features = any([
                            post.get('media'),  # Phase 1.1
                            post.get('likes') is not None,  # Phase 1.2
                            post.get('thread_info', {}).get('is_thread'),  # Phase 1.3
                            post.get('classification')  # Phase 3.2
                        ])
                        if has_phase_features:
                            posts_with_phase_features += 1

                    # Check export metadata
                    export_metadata = res.get("export_metadata")
                    if export_metadata:
                        print(f"   📦 Export Metadata Found: ✅")
                        print(f"      Format: {export_metadata.get('format')}")
                        print(f"      Posts Count: {export_metadata.get('posts_count')}")
                        print(f"      Phase Enhancements: {export_metadata.get('phase_enhancements', [])}")
                        print(f"      Exported At: {export_metadata.get('exported_at')}")
                    else:
                        print(f"   📦 Export Metadata: ❌ Not found")

                    # Sample post analysis
                    if posts:
                        sample_post = posts[0]
                        print(f"\\n   📝 Sample Post Analysis:")
                        print(f"      Text: {sample_post.get('text', '')[:60]}...")

                        # Check Phase enhancements
                        enhancements = []
                        if sample_post.get('media'):
                            enhancements.append(f"Media: {len(sample_post['media'])} items")
                        if sample_post.get('likes') is not None:
                            enhancements.append(f"Engagement: ❤️{sample_post.get('likes', 0)}")
                        if sample_post.get('thread_info', {}).get('is_thread'):
                            enhancements.append(f"Thread: pos {sample_post['thread_info'].get('thread_position', '?')}")
                        if sample_post.get('classification'):
                            cls = sample_post['classification']
                            enhancements.append(f"Classification: {cls.get('content_type', '?')} | {cls.get('sentiment', '?')}")

                        if enhancements:
                            print(f"      Enhancements: {' | '.join(enhancements)}")
                        else:
                            print(f"      Enhancements: ❌ None detected")

                    print(f"\\n🎯 PHASE 4.1 VALIDATION RESULTS:")
                    print(f"   Formats Requested: {len(formats_requested)} ({', '.join(formats_requested)})")
                    print(f"   Posts with Enhanced Features: {posts_with_phase_features}/{posts_count} ({posts_with_phase_features/posts_count*100:.1f}%)")

                    # Test success criteria for Phase 4.1
                    if export_metadata and export_metadata.get('phase_enhancements'):
                        print(f"   ✅ EXPORT METADATA: Phase enhancements tracked")
                    else:
                        print(f"   ⚠️ EXPORT METADATA: Missing or incomplete")

                    if posts_with_phase_features >= posts_count * 0.8:
                        print(f"   ✅ ENHANCED DATA: {posts_with_phase_features}/{posts_count} posts with Phase features")
                    elif posts_with_phase_features >= posts_count * 0.5:
                        print(f"   ⚠️ PARTIAL ENHANCEMENT: {posts_with_phase_features}/{posts_count} posts with Phase features")
                    else:
                        print(f"   ❌ LOW ENHANCEMENT: Only {posts_with_phase_features}/{posts_count} posts with Phase features")

                    # Overall Phase 4.1 assessment
                    export_score = (
                        (50 if export_metadata else 0) +
                        (posts_with_phase_features / posts_count * 50)
                    )

                    if export_score >= 80:
                        print(f"   🏆 OUTSTANDING: {export_score:.1f}% Phase 4.1 export effectiveness")
                    elif export_score >= 60:
                        print(f"   ✅ EXCELLENT: {export_score:.1f}% Phase 4.1 export effectiveness")
                    elif export_score >= 40:
                        print(f"   ⚠️ GOOD: {export_score:.1f}% Phase 4.1 export effectiveness")
                    else:
                        print(f"   ❌ NEEDS IMPROVEMENT: {export_score:.1f}% Phase 4.1 export effectiveness")

                else:
                    print(f"   ❌ Unexpected data structure")
            else:
                print(f"   ❌ No data extracted")

        except Exception as e:
            print(f"❌ {test_case['name']} test failed: {e}")

        print(f"\\n{'='*50}")

    print(f"\\n🏁 Phase 4.1 Export Format Testing Complete!")

    # Check for exported files in the data directory
    print(f"\\n📁 Checking for exported files in ../storage/scraped_data/...")
    try:
        data_dir = "../storage/scraped_data"
        if os.path.exists(data_dir):
            files = os.listdir(data_dir)
            export_files = [f for f in files if any(ext in f for ext in ['.json', '.csv', '.xml', '.md', '.parquet', '.xlsx'])]
            if export_files:
                print(f"   ✅ Found {len(export_files)} export files:")
                for file in export_files[-5:]:  # Show last 5 files
                    print(f"      📄 {file}")
            else:
                print(f"   ⚠️ No export files found in {data_dir}")
        else:
            print(f"   ❌ Data directory {data_dir} not found")
    except Exception as e:
        print(f"   ❌ Error checking files: {e}")

if __name__ == "__main__":
    test_phase41_export_formats()
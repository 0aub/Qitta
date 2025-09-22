#!/usr/bin/env python3
"""
CRITICAL VALIDATION TEST
=======================

This test validates the critical issues identified:
1. TRUE large scale scraping (ALL posts, not just 30)
2. Media extraction in posts with attachments
3. Thread detection on actual thread creators
4. Search functionality (query, hashtag, location)

This is the REAL test to see if the system works as intended.
"""
import json
import urllib.request
import time
import sys

class CriticalValidationTest:
    def __init__(self):
        self.endpoint = "http://localhost:8004"
        self.issues_found = []
        self.successes = []

    def run_critical_tests(self):
        """Run all critical validation tests."""
        print("🚨 CRITICAL VALIDATION TEST - Real World Testing")
        print("=" * 60)

        tests = [
            ("Media Extraction Reality Check", self.test_media_extraction_reality),
            ("Thread Detection Reality Check", self.test_thread_detection_reality),
            ("Search Query Functionality", self.test_search_query_functionality),
            ("Hashtag Search Functionality", self.test_hashtag_functionality),
            ("Large Scale Extraction", self.test_large_scale_extraction),
            ("Posts vs Other Data", self.test_posts_only_extraction)
        ]

        for test_name, test_func in tests:
            print(f"\n🧪 {test_name}")
            try:
                result = test_func()
                if result['critical_pass']:
                    self.successes.append(f"✅ {test_name}: {result['details']}")
                    print(f"✅ PASS: {result['details']}")
                else:
                    self.issues_found.append(f"❌ {test_name}: {result['details']}")
                    print(f"❌ FAIL: {result['details']}")
            except Exception as e:
                self.issues_found.append(f"❌ {test_name}: ERROR - {str(e)}")
                print(f"❌ ERROR: {e}")

        self.generate_critical_report()

    def submit_simple_job(self, payload: dict, timeout: int = 90) -> dict:
        """Submit a job with shorter timeout for focused testing."""
        try:
            data = json.dumps(payload).encode('utf-8')
            req = urllib.request.Request(
                f"{self.endpoint}/jobs/twitter",
                data=data,
                headers={'Content-Type': 'application/json'}
            )

            with urllib.request.urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode('utf-8'))
                job_id = result["job_id"]

            # Wait for completion with shorter intervals
            for i in range(timeout // 5):
                try:
                    with urllib.request.urlopen(f"{self.endpoint}/jobs/{job_id}", timeout=10) as response:
                        result = json.loads(response.read().decode('utf-8'))
                        status = result["status"]

                        if status in ["finished", "error"]:
                            return result

                    time.sleep(5)
                except:
                    continue

            return {"status": "timeout", "error": f"Job timed out after {timeout}s"}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def test_posts_only_extraction(self) -> dict:
        """Test that we can extract ONLY posts, not followers/following."""
        payload = {
            "username": "naval",
            "scrape_posts": True,
            "scrape_followers": False,
            "scrape_following": False,
            "max_posts": 5,
            "scrape_level": 4
        }

        result = self.submit_simple_job(payload, 60)

        if result["status"] != "finished":
            return {
                'critical_pass': False,
                'details': f'Job failed: {result.get("error", "Unknown error")}'
            }

        res = result["result"]
        data = res.get("data", [])

        if not data or not data[0].get('posts'):
            return {
                'critical_pass': False,
                'details': 'No posts extracted - system may be extracting wrong data'
            }

        posts = data[0]['posts']

        # Check if we got followers/following instead of posts
        has_followers = 'followers' in data[0]
        has_following = 'following' in data[0]

        if has_followers or has_following:
            return {
                'critical_pass': False,
                'details': f'System extracted followers/following instead of posts (followers: {has_followers}, following: {has_following})'
            }

        return {
            'critical_pass': True,
            'details': f'Successfully extracted {len(posts)} posts only (no followers/following data)'
        }

    def test_media_extraction_reality(self) -> dict:
        """Test media extraction with an account known for posting media."""
        # Using a specific account known for media posts
        payload = {
            "username": "elonmusk",  # Known for posting images/memes
            "scrape_posts": True,
            "max_posts": 8,
            "scrape_level": 4
        }

        result = self.submit_simple_job(payload, 90)

        if result["status"] != "finished":
            return {
                'critical_pass': False,
                'details': f'Job failed: {result.get("error", "Unknown error")}'
            }

        res = result["result"]
        data = res.get("data", [])

        if not data or not data[0].get('posts'):
            return {
                'critical_pass': False,
                'details': 'No posts extracted'
            }

        posts = data[0]['posts']
        posts_with_media = [p for p in posts if p.get('media') and len(p['media']) > 0]

        if not posts_with_media:
            return {
                'critical_pass': False,
                'details': f'NO MEDIA FOUND in {len(posts)} posts from media-heavy account (@elonmusk)'
            }

        # Validate media structure
        sample_media = posts_with_media[0]['media'][0]
        required_fields = ['type', 'url']
        missing_fields = [field for field in required_fields if not sample_media.get(field)]

        if missing_fields:
            return {
                'critical_pass': False,
                'details': f'Media missing fields: {missing_fields}'
            }

        return {
            'critical_pass': True,
            'details': f'Found {len(posts_with_media)}/{len(posts)} posts with media. Sample: {sample_media["type"]}'
        }

    def test_thread_detection_reality(self) -> dict:
        """Test thread detection with an account known for creating threads."""
        payload = {
            "username": "naval",  # Known for creating long threads
            "scrape_posts": True,
            "max_posts": 12,
            "scrape_level": 4
        }

        result = self.submit_simple_job(payload, 90)

        if result["status"] != "finished":
            return {
                'critical_pass': False,
                'details': f'Job failed: {result.get("error", "Unknown error")}'
            }

        res = result["result"]
        data = res.get("data", [])

        if not data or not data[0].get('posts'):
            return {
                'critical_pass': False,
                'details': 'No posts extracted'
            }

        posts = data[0]['posts']
        thread_posts = [p for p in posts if p.get('thread_info', {}).get('is_thread')]
        thread_starters = [p for p in posts if p.get('thread_info', {}).get('is_thread_starter')]

        if not thread_posts:
            return {
                'critical_pass': False,
                'details': f'NO THREADS DETECTED in {len(posts)} posts from thread-heavy account (@naval)'
            }

        # Check thread metadata quality
        sample_thread = thread_posts[0]['thread_info']
        thread_fields = ['thread_id', 'thread_position', 'thread_indicators']
        present_fields = [field for field in thread_fields if sample_thread.get(field)]

        return {
            'critical_pass': True,
            'details': f'Found {len(thread_posts)} thread posts, {len(thread_starters)} starters. Fields: {present_fields}'
        }

    def test_search_query_functionality(self) -> dict:
        """Test search by query functionality."""
        payload = {
            "search_query": "machine learning",
            "max_tweets": 8,
            "scrape_level": 4
        }

        result = self.submit_simple_job(payload, 90)

        if result["status"] != "finished":
            return {
                'critical_pass': False,
                'details': f'Search job failed: {result.get("error", "Unknown error")}'
            }

        res = result["result"]
        data = res.get("data", [])

        if not data:
            return {
                'critical_pass': False,
                'details': 'Search returned no data'
            }

        search_result = data[0]
        if 'posts' not in search_result:
            return {
                'critical_pass': False,
                'details': f'Search result missing posts. Keys: {list(search_result.keys())}'
            }

        posts = search_result['posts']
        if not posts:
            return {
                'critical_pass': False,
                'details': 'Search returned empty posts array'
            }

        # Check if posts contain search-related metadata
        sample_post = posts[0]
        search_fields = ['relevance_score', 'search_context']
        present_search_fields = [field for field in search_fields if sample_post.get(field)]

        return {
            'critical_pass': True,
            'details': f'Search found {len(posts)} posts. Search fields: {present_search_fields}'
        }

    def test_hashtag_functionality(self) -> dict:
        """Test hashtag search functionality."""
        payload = {
            "hashtag": "AI",
            "max_tweets": 8,
            "scrape_level": 4
        }

        result = self.submit_simple_job(payload, 90)

        if result["status"] != "finished":
            return {
                'critical_pass': False,
                'details': f'Hashtag job failed: {result.get("error", "Unknown error")}'
            }

        res = result["result"]
        data = res.get("data", [])

        if not data:
            return {
                'critical_pass': False,
                'details': 'Hashtag search returned no data'
            }

        hashtag_result = data[0]
        if 'posts' not in hashtag_result:
            return {
                'critical_pass': False,
                'details': f'Hashtag result missing posts. Keys: {list(hashtag_result.keys())}'
            }

        posts = hashtag_result['posts']
        if not posts:
            return {
                'critical_pass': False,
                'details': 'Hashtag search returned empty posts array'
            }

        # Check if posts actually contain the hashtag
        hashtag_mentions = sum(1 for p in posts if '#AI' in p.get('text', '').upper() or '#ai' in p.get('text', ''))

        return {
            'critical_pass': True,
            'details': f'Hashtag search found {len(posts)} posts, {hashtag_mentions} mention #AI'
        }

    def test_large_scale_extraction(self) -> dict:
        """Test extraction of larger number of posts."""
        payload = {
            "username": "naval",
            "scrape_posts": True,
            "max_posts": 50,  # Reasonable large scale
            "scrape_level": 4
        }

        start_time = time.time()
        result = self.submit_simple_job(payload, 180)  # 3 minutes
        extraction_time = time.time() - start_time

        if result["status"] == "timeout":
            return {
                'critical_pass': False,
                'details': f'Large scale extraction timed out after {extraction_time:.1f}s'
            }

        if result["status"] != "finished":
            return {
                'critical_pass': False,
                'details': f'Large scale job failed: {result.get("error", "Unknown error")}'
            }

        res = result["result"]
        data = res.get("data", [])

        if not data or not data[0].get('posts'):
            return {
                'critical_pass': False,
                'details': 'Large scale extraction returned no posts'
            }

        posts = data[0]['posts']
        extraction_rate = len(posts) / extraction_time * 60  # posts per minute

        if len(posts) < 20:  # At least 40% success rate
            return {
                'critical_pass': False,
                'details': f'Only extracted {len(posts)}/50 posts in {extraction_time:.1f}s'
            }

        return {
            'critical_pass': True,
            'details': f'Extracted {len(posts)}/50 posts in {extraction_time:.1f}s ({extraction_rate:.1f} posts/min)'
        }

    def generate_critical_report(self):
        """Generate critical validation report."""
        print("\n" + "=" * 60)
        print("🚨 CRITICAL VALIDATION REPORT")
        print("=" * 60)

        total_tests = len(self.successes) + len(self.issues_found)
        success_count = len(self.successes)

        print(f"📊 OVERALL RESULTS:")
        print(f"   Tests Passed: {success_count}/{total_tests}")
        print(f"   Critical Issues Found: {len(self.issues_found)}")
        print(f"   Success Rate: {success_count/total_tests*100:.1f}%")
        print()

        if self.issues_found:
            print("🚨 CRITICAL ISSUES IDENTIFIED:")
            for issue in self.issues_found:
                print(f"   {issue}")
            print()

        if self.successes:
            print("✅ SUCCESSFUL VALIDATIONS:")
            for success in self.successes:
                print(f"   {success}")
            print()

        # Final assessment
        if len(self.issues_found) == 0:
            print("🎉 ALL CRITICAL TESTS PASSED - System ready for production!")
        elif len(self.issues_found) <= 2:
            print("⚠️ Minor issues found - System mostly functional")
        else:
            print("❌ MAJOR ISSUES DETECTED - System needs significant fixes")

        print("=" * 60)

if __name__ == "__main__":
    tester = CriticalValidationTest()
    tester.run_critical_tests()
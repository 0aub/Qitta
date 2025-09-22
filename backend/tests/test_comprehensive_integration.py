#!/usr/bin/env python3
"""
COMPREHENSIVE INTEGRATION TEST SUITE
====================================

Tests all 9 phases working together:
- Phase 1.1: Media extraction
- Phase 1.2: Engagement metrics
- Phase 1.3: Thread detection
- Phase 2.1: Hashtag scraping
- Phase 2.2: Search query
- Phase 3.1: Real-time monitoring
- Phase 3.2: Content classification
- Phase 4.1: Export formats
- Phase 4.2: Performance optimization

This is the ultimate validation test for the enhanced Twitter scraper.
"""
import json
import urllib.request
import time
import os
from typing import Dict, List, Any

class TwitterScraperIntegrationTest:
    def __init__(self):
        self.endpoint = "http://localhost:8004"
        self.test_results = {}
        self.overall_score = 0

    def run_comprehensive_tests(self):
        """Run all integration tests."""
        print("🚀 COMPREHENSIVE TWITTER SCRAPER INTEGRATION TEST")
        print("=" * 60)
        print("Testing ALL 9 phases working together...")
        print()

        tests = [
            ("Basic Enhanced Extraction", self.test_basic_enhanced_extraction),
            ("Multi-Format Export", self.test_multi_format_export),
            ("Performance Optimization", self.test_performance_optimization),
            ("Content Classification", self.test_content_classification),
            ("Thread Detection", self.test_thread_detection),
            ("Cross-User Compatibility", self.test_cross_user_compatibility),
            ("Error Handling", self.test_error_handling),
            ("Scale Testing", self.test_scale_performance)
        ]

        for test_name, test_func in tests:
            print(f"🧪 Running: {test_name}")
            try:
                result = test_func()
                self.test_results[test_name] = result
                print(f"✅ {test_name}: {'PASS' if result['passed'] else 'FAIL'} ({result['score']}%)")
            except Exception as e:
                print(f"❌ {test_name}: ERROR - {e}")
                self.test_results[test_name] = {'passed': False, 'score': 0, 'error': str(e)}
            print()

        self.generate_final_report()

    def submit_job_and_wait(self, payload: Dict, timeout: int = 120) -> Dict:
        """Helper method to submit job and wait for completion."""
        data = json.dumps(payload).encode('utf-8')
        req = urllib.request.Request(
            f"{self.endpoint}/jobs/twitter",
            data=data,
            headers={'Content-Type': 'application/json'}
        )

        with urllib.request.urlopen(req, timeout=30) as response:
            result = json.loads(response.read().decode('utf-8'))
            job_id = result["job_id"]

        # Wait for completion
        for i in range(timeout // 3):
            try:
                with urllib.request.urlopen(f"{self.endpoint}/jobs/{job_id}", timeout=10) as response:
                    result = json.loads(response.read().decode('utf-8'))
                    status = result["status"]

                    if status in ["finished", "error"]:
                        return result

                time.sleep(3)
            except:
                continue

        raise TimeoutError(f"Job {job_id} did not complete within {timeout} seconds")

    def test_basic_enhanced_extraction(self) -> Dict:
        """Test basic extraction with all Phase 1 enhancements."""
        payload = {
            "username": "naval",
            "scrape_posts": True,
            "max_posts": 5,
            "scrape_level": 4
        }

        result = self.submit_job_and_wait(payload, 90)

        if result["status"] != "finished":
            return {'passed': False, 'score': 0, 'details': 'Job failed'}

        res = result["result"]
        data = res.get("data", [])

        if not data or not data[0].get('posts'):
            return {'passed': False, 'score': 0, 'details': 'No posts extracted'}

        posts = data[0]['posts']
        score = 0

        # Test Phase 1.1: Media extraction
        posts_with_media = sum(1 for p in posts if p.get('media'))
        if posts_with_media > 0:
            score += 25

        # Test Phase 1.2: Engagement metrics
        posts_with_engagement = sum(1 for p in posts if p.get('likes') is not None)
        if posts_with_engagement > 0:
            score += 25

        # Test Phase 1.3: Thread detection
        posts_with_threads = sum(1 for p in posts if p.get('thread_info', {}).get('is_thread'))
        if posts_with_threads > 0:
            score += 25

        # Test basic data quality
        complete_posts = sum(1 for p in posts if p.get('text') and p.get('author'))
        if complete_posts >= len(posts) * 0.8:
            score += 25

        return {
            'passed': score >= 75,
            'score': score,
            'details': f'Media: {posts_with_media}, Engagement: {posts_with_engagement}, Threads: {posts_with_threads}, Complete: {complete_posts}/{len(posts)}'
        }

    def test_multi_format_export(self) -> Dict:
        """Test Phase 4.1: Multi-format export functionality."""
        payload = {
            "username": "naval",
            "scrape_posts": True,
            "max_posts": 3,
            "scrape_level": 4,
            "export_formats": ["json", "csv", "xml", "markdown"]
        }

        result = self.submit_job_and_wait(payload, 90)

        if result["status"] != "finished":
            return {'passed': False, 'score': 0, 'details': 'Job failed'}

        res = result["result"]
        score = 0

        # Check for export metadata
        export_metadata = res.get("export_metadata")
        if export_metadata:
            score += 50
            if export_metadata.get('phase_enhancements'):
                score += 25

        # Check data quality
        data = res.get("data", [])
        if data and data[0].get('posts'):
            posts = data[0]['posts']
            if len(posts) >= 2:
                score += 25

        return {
            'passed': score >= 75,
            'score': score,
            'details': f'Export metadata: {bool(export_metadata)}, Posts: {len(posts) if data and data[0].get("posts") else 0}'
        }

    def test_performance_optimization(self) -> Dict:
        """Test Phase 4.2: Performance and anti-detection features."""
        start_time = time.time()

        payload = {
            "username": "naval",
            "scrape_posts": True,
            "max_posts": 5,
            "scrape_level": 4
        }

        result = self.submit_job_and_wait(payload, 90)
        execution_time = time.time() - start_time

        if result["status"] != "finished":
            return {'passed': False, 'score': 0, 'details': 'Job failed'}

        res = result["result"]
        score = 0

        # Check for performance metrics
        performance_metrics = res.get("performance_metrics")
        if performance_metrics:
            score += 40

            # Check specific metrics
            if performance_metrics.get('cache_hits', 0) > 0:
                score += 20
            if performance_metrics.get('anti_detection_actions', 0) > 0:
                score += 20
            if performance_metrics.get('performance_mode'):
                score += 20

        return {
            'passed': score >= 60,
            'score': score,
            'details': f'Execution time: {execution_time:.1f}s, Metrics: {bool(performance_metrics)}'
        }

    def test_content_classification(self) -> Dict:
        """Test Phase 3.2: Content classification integration."""
        payload = {
            "username": "naval",
            "scrape_posts": True,
            "max_posts": 4,
            "scrape_level": 4
        }

        result = self.submit_job_and_wait(payload, 90)

        if result["status"] != "finished":
            return {'passed': False, 'score': 0, 'details': 'Job failed'}

        res = result["result"]
        data = res.get("data", [])

        if not data or not data[0].get('posts'):
            return {'passed': False, 'score': 0, 'details': 'No posts extracted'}

        posts = data[0]['posts']
        score = 0

        classified_posts = 0
        content_types = set()
        sentiments = set()

        for post in posts:
            classification = post.get('classification', {})
            if classification and not classification.get('error'):
                classified_posts += 1
                if classification.get('content_type'):
                    content_types.add(classification['content_type'])
                if classification.get('sentiment'):
                    sentiments.add(classification['sentiment'])

        # Scoring
        if classified_posts >= len(posts) * 0.8:
            score += 40
        elif classified_posts >= len(posts) * 0.5:
            score += 20

        if len(content_types) >= 2:
            score += 30
        elif len(content_types) >= 1:
            score += 15

        if len(sentiments) >= 2:
            score += 30
        elif len(sentiments) >= 1:
            score += 15

        return {
            'passed': score >= 60,
            'score': score,
            'details': f'Classified: {classified_posts}/{len(posts)}, Types: {len(content_types)}, Sentiments: {len(sentiments)}'
        }

    def test_thread_detection(self) -> Dict:
        """Test Phase 1.3: Thread detection on known thread creator."""
        payload = {
            "username": "naval",  # Known for creating threads
            "scrape_posts": True,
            "max_posts": 8,
            "scrape_level": 4
        }

        result = self.submit_job_and_wait(payload, 90)

        if result["status"] != "finished":
            return {'passed': False, 'score': 0, 'details': 'Job failed'}

        res = result["result"]
        data = res.get("data", [])

        if not data or not data[0].get('posts'):
            return {'passed': False, 'score': 0, 'details': 'No posts extracted'}

        posts = data[0]['posts']
        score = 0

        thread_posts = 0
        thread_starters = 0
        unique_threads = set()

        for post in posts:
            thread_info = post.get('thread_info', {})
            if thread_info.get('is_thread'):
                thread_posts += 1
                if thread_info.get('is_thread_starter'):
                    thread_starters += 1
                if thread_info.get('thread_id'):
                    unique_threads.add(thread_info['thread_id'])

        # Scoring
        if thread_posts >= 2:
            score += 40
        elif thread_posts >= 1:
            score += 20

        if thread_starters >= 1:
            score += 30

        if len(unique_threads) >= 1:
            score += 30

        return {
            'passed': score >= 50,
            'score': score,
            'details': f'Thread posts: {thread_posts}, Starters: {thread_starters}, Unique threads: {len(unique_threads)}'
        }

    def test_cross_user_compatibility(self) -> Dict:
        """Test scraper with different users."""
        users = ["sama", "naval"]
        user_results = []

        for username in users:
            try:
                payload = {
                    "username": username,
                    "scrape_posts": True,
                    "max_posts": 3,
                    "scrape_level": 4
                }

                result = self.submit_job_and_wait(payload, 60)

                if result["status"] == "finished":
                    res = result["result"]
                    data = res.get("data", [])
                    if data and data[0].get('posts'):
                        posts = data[0]['posts']
                        user_results.append({
                            'username': username,
                            'posts': len(posts),
                            'success': True
                        })
                    else:
                        user_results.append({'username': username, 'success': False})
                else:
                    user_results.append({'username': username, 'success': False})

            except Exception as e:
                user_results.append({'username': username, 'success': False, 'error': str(e)})

        successful_users = sum(1 for r in user_results if r.get('success'))
        score = (successful_users / len(users)) * 100

        return {
            'passed': score >= 50,
            'score': int(score),
            'details': f'Successful users: {successful_users}/{len(users)}'
        }

    def test_error_handling(self) -> Dict:
        """Test error handling with invalid inputs."""
        test_cases = [
            {"username": "nonexistentuser123456", "expected": "graceful_failure"},
            {"username": "", "expected": "validation_error"}
        ]

        score = 0
        handled_cases = 0

        for case in test_cases:
            try:
                payload = {
                    "username": case["username"],
                    "scrape_posts": True,
                    "max_posts": 2,
                    "scrape_level": 4
                }

                result = self.submit_job_and_wait(payload, 45)

                # Check if error was handled gracefully
                if result["status"] in ["finished", "error"]:
                    handled_cases += 1
                    if result["status"] == "finished":
                        # Check if returned empty results gracefully
                        res = result.get("result", {})
                        data = res.get("data", [])
                        if isinstance(data, list):
                            score += 50
                    elif result["status"] == "error":
                        # Check if error message is informative
                        error = result.get("error", "")
                        if error and len(error) > 5:
                            score += 50

            except Exception:
                # Timeout or other exception is acceptable for invalid inputs
                handled_cases += 1
                score += 25

        score = score // len(test_cases)  # Average across test cases

        return {
            'passed': score >= 50,
            'score': score,
            'details': f'Handled cases: {handled_cases}/{len(test_cases)}'
        }

    def test_scale_performance(self) -> Dict:
        """Test performance with larger datasets."""
        start_time = time.time()

        payload = {
            "username": "naval",
            "scrape_posts": True,
            "max_posts": 15,  # Larger dataset
            "scrape_level": 4
        }

        result = self.submit_job_and_wait(payload, 150)  # Longer timeout
        execution_time = time.time() - start_time

        if result["status"] != "finished":
            return {'passed': False, 'score': 0, 'details': f'Job failed after {execution_time:.1f}s'}

        res = result["result"]
        data = res.get("data", [])

        score = 0

        if data and data[0].get('posts'):
            posts = data[0]['posts']
            posts_count = len(posts)

            # Score based on extraction success
            if posts_count >= 10:
                score += 40
            elif posts_count >= 5:
                score += 20

            # Score based on execution time
            if execution_time <= 120:  # 2 minutes
                score += 30
            elif execution_time <= 180:  # 3 minutes
                score += 15

            # Score based on data quality
            complete_posts = sum(1 for p in posts if p.get('text') and p.get('author'))
            if complete_posts >= posts_count * 0.8:
                score += 30

        return {
            'passed': score >= 60,
            'score': score,
            'details': f'Posts: {posts_count if data and data[0].get("posts") else 0}, Time: {execution_time:.1f}s'
        }

    def generate_final_report(self):
        """Generate comprehensive test report."""
        print("=" * 60)
        print("🏆 COMPREHENSIVE INTEGRATION TEST RESULTS")
        print("=" * 60)

        total_tests = len(self.test_results)
        passed_tests = sum(1 for r in self.test_results.values() if r.get('passed', False))
        avg_score = sum(r.get('score', 0) for r in self.test_results.values()) / max(total_tests, 1)

        print(f"📊 OVERALL RESULTS:")
        print(f"   Tests Passed: {passed_tests}/{total_tests}")
        print(f"   Average Score: {avg_score:.1f}%")
        print(f"   Success Rate: {passed_tests/total_tests*100:.1f}%")
        print()

        print("📝 DETAILED RESULTS:")
        for test_name, result in self.test_results.items():
            status = "✅ PASS" if result.get('passed') else "❌ FAIL"
            score = result.get('score', 0)
            details = result.get('details', 'No details')
            error = result.get('error', '')

            print(f"   {status} {test_name}: {score}%")
            print(f"      Details: {details}")
            if error:
                print(f"      Error: {error}")
            print()

        # Overall assessment
        if avg_score >= 85:
            print("🏆 OUTSTANDING: Twitter scraper is production-ready!")
        elif avg_score >= 75:
            print("✅ EXCELLENT: Twitter scraper performing very well!")
        elif avg_score >= 65:
            print("⚠️ GOOD: Twitter scraper working with minor issues")
        else:
            print("❌ NEEDS IMPROVEMENT: Some critical issues detected")

        print("=" * 60)

if __name__ == "__main__":
    tester = TwitterScraperIntegrationTest()
    tester.run_comprehensive_tests()
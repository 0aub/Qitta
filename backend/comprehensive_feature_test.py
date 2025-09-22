#!/usr/bin/env python3
"""
Comprehensive Feature Testing Suite for Twitter Scraper System
Tests all features systematically and identifies real issues
"""

import json
import urllib.request
import time
import sys
from typing import Dict, List, Any

class TwitterScraperTester:
    def __init__(self):
        self.endpoint = "http://localhost:8004"
        self.results = {}
        self.issues = []

    def submit_job(self, payload: Dict) -> str:
        """Submit a job and return job ID"""
        try:
            data = json.dumps(payload).encode('utf-8')
            req = urllib.request.Request(
                f'{self.endpoint}/jobs/twitter',
                data=data,
                headers={'Content-Type': 'application/json'}
            )

            with urllib.request.urlopen(req, timeout=10) as response:
                result = json.loads(response.read().decode('utf-8'))
                return result['job_id']
        except Exception as e:
            self.issues.append(f"Job submission failed: {e}")
            return None

    def wait_for_completion(self, job_id: str, timeout_minutes: int = 3) -> Dict:
        """Wait for job completion and return result"""
        if not job_id:
            return {"status": "failed", "error": "No job ID"}

        start_time = time.time()
        timeout_seconds = timeout_minutes * 60

        while time.time() - start_time < timeout_seconds:
            try:
                with urllib.request.urlopen(f'{self.endpoint}/jobs/{job_id}', timeout=10) as response:
                    result = json.loads(response.read().decode('utf-8'))

                    if result['status'] in ['finished', 'error']:
                        return result

                time.sleep(3)
            except Exception as e:
                continue

        return {"status": "timeout", "error": f"Job timed out after {timeout_minutes} minutes"}

    def analyze_result(self, result: Dict, test_name: str) -> Dict:
        """Analyze job result and extract key metrics"""
        analysis = {
            "test_name": test_name,
            "status": result.get("status"),
            "execution_time": 0,
            "posts_found": 0,
            "has_media": 0,
            "has_engagement": 0,
            "has_classification": 0,
            "extraction_method": "unknown",
            "issues": []
        }

        # Calculate execution time
        if result.get("started_at") and result.get("finished_at"):
            try:
                from datetime import datetime
                start = datetime.fromisoformat(result["started_at"].replace('Z', '+00:00'))
                end = datetime.fromisoformat(result["finished_at"].replace('Z', '+00:00'))
                analysis["execution_time"] = (end - start).total_seconds()
            except:
                pass

        if result["status"] == "finished" and "result" in result:
            res_data = result["result"]

            # Extract metadata
            search_metadata = res_data.get("search_metadata", {})
            analysis["extraction_method"] = search_metadata.get("extraction_method", "unknown")

            # Extract posts data
            data_results = res_data.get("data", [])
            if data_results and len(data_results) > 0:
                first_result = data_results[0]
                if "posts" in first_result:
                    posts = first_result["posts"]
                    analysis["posts_found"] = len(posts)

                    # Analyze post quality
                    for post in posts:
                        if post.get("media") and len(post["media"]) > 0:
                            analysis["has_media"] += 1
                        if any([post.get("likes"), post.get("retweets"), post.get("replies")]):
                            analysis["has_engagement"] += 1
                        if post.get("classification"):
                            analysis["has_classification"] += 1

                else:
                    analysis["issues"].append("No 'posts' key in result data")
            else:
                analysis["issues"].append("No data results found")

        elif result["status"] == "error":
            analysis["issues"].append(f"Job failed: {result.get('error', 'Unknown error')}")
        elif result["status"] == "timeout":
            analysis["issues"].append("Job timed out")

        return analysis

    def test_user_profiles(self) -> List[Dict]:
        """Test user profile extraction across different accounts"""
        print("🧪 TESTING USER PROFILE EXTRACTION")
        print("=" * 50)

        test_accounts = [
            {"username": "naval", "posts": 5},
            {"username": "sama", "posts": 5},
            {"username": "elonmusk", "posts": 3},
            {"username": "sundarpichai", "posts": 3},
            {"username": "jeffbezos", "posts": 3}
        ]

        results = []

        for account in test_accounts:
            print(f"\n📱 Testing @{account['username']} ({account['posts']} posts)")

            payload = {
                "username": account["username"],
                "scrape_posts": True,
                "max_posts": account["posts"],
                "scrape_level": 4
            }

            job_id = self.submit_job(payload)
            if job_id:
                print(f"   Job submitted: {job_id}")
                result = self.wait_for_completion(job_id, timeout_minutes=2)
                analysis = self.analyze_result(result, f"user_{account['username']}")
                results.append(analysis)

                # Print quick summary
                if analysis["status"] == "finished":
                    print(f"   ✅ SUCCESS: {analysis['posts_found']} posts in {analysis['execution_time']:.1f}s")
                    print(f"      Media: {analysis['has_media']}, Engagement: {analysis['has_engagement']}")
                else:
                    print(f"   ❌ FAILED: {analysis['status']} - {analysis.get('issues', ['Unknown'])}")
            else:
                results.append({"test_name": f"user_{account['username']}", "status": "submission_failed"})
                print(f"   ❌ SUBMISSION FAILED")

        return results

    def test_search_functionality(self) -> List[Dict]:
        """Test search functionality (hashtag and query searches)"""
        print("\n🔍 TESTING SEARCH FUNCTIONALITY")
        print("=" * 50)

        search_tests = [
            {"type": "hashtag", "hashtag": "AI", "max_tweets": 5},
            {"type": "hashtag", "hashtag": "bitcoin", "max_tweets": 5},
            {"type": "query", "search_query": "machine learning", "max_tweets": 5},
            {"type": "query", "search_query": "crypto", "max_tweets": 5}
        ]

        results = []

        for test in search_tests:
            search_term = test.get("hashtag", test.get("search_query"))
            print(f"\n🏷️ Testing {test['type']}: {search_term}")

            payload = {
                "scrape_level": 4,
                "max_tweets": test["max_tweets"]
            }

            if test["type"] == "hashtag":
                payload["hashtag"] = test["hashtag"]
            else:
                payload["search_query"] = test["search_query"]

            job_id = self.submit_job(payload)
            if job_id:
                print(f"   Job submitted: {job_id}")
                result = self.wait_for_completion(job_id, timeout_minutes=2)
                analysis = self.analyze_result(result, f"search_{test['type']}_{search_term}")
                results.append(analysis)

                # Print quick summary
                if analysis["status"] == "finished":
                    print(f"   ✅ SUCCESS: {analysis['posts_found']} posts via {analysis['extraction_method']}")
                else:
                    print(f"   ❌ FAILED: {analysis['status']} - {analysis.get('issues', ['Unknown'])}")
            else:
                results.append({"test_name": f"search_{test['type']}_{search_term}", "status": "submission_failed"})
                print(f"   ❌ SUBMISSION FAILED")

        return results

    def test_scale_performance(self) -> List[Dict]:
        """Test performance across different scales"""
        print("\n⚡ TESTING SCALE PERFORMANCE")
        print("=" * 50)

        scale_tests = [
            {"name": "small", "username": "naval", "max_posts": 5},
            {"name": "medium", "username": "naval", "max_posts": 15},
            {"name": "large", "username": "naval", "max_posts": 30}
        ]

        results = []

        for test in scale_tests:
            print(f"\n📊 Testing {test['name']} scale: {test['max_posts']} posts")

            payload = {
                "username": test["username"],
                "scrape_posts": True,
                "max_posts": test["max_posts"],
                "scrape_level": 4
            }

            job_id = self.submit_job(payload)
            if job_id:
                print(f"   Job submitted: {job_id}")
                result = self.wait_for_completion(job_id, timeout_minutes=4)
                analysis = self.analyze_result(result, f"scale_{test['name']}")
                results.append(analysis)

                # Print performance analysis
                if analysis["status"] == "finished":
                    extraction_rate = analysis["posts_found"] / test["max_posts"] * 100
                    speed = analysis["posts_found"] / max(analysis["execution_time"], 1) * 60
                    print(f"   ✅ SUCCESS: {analysis['posts_found']}/{test['max_posts']} posts ({extraction_rate:.1f}%)")
                    print(f"      Performance: {speed:.1f} posts/min, Method: {analysis['extraction_method']}")
                else:
                    print(f"   ❌ FAILED: {analysis['status']} - {analysis.get('issues', ['Unknown'])}")
            else:
                results.append({"test_name": f"scale_{test['name']}", "status": "submission_failed"})
                print(f"   ❌ SUBMISSION FAILED")

        return results

    def test_edge_cases(self) -> List[Dict]:
        """Test edge cases and error handling"""
        print("\n🚨 TESTING EDGE CASES")
        print("=" * 50)

        edge_tests = [
            {"name": "invalid_user", "username": "nonexistentuser12345", "max_posts": 3},
            {"name": "private_user", "username": "test", "max_posts": 3},  # Likely private
            {"name": "zero_posts", "username": "naval", "max_posts": 0},
            {"name": "large_request", "username": "naval", "max_posts": 100}
        ]

        results = []

        for test in edge_tests:
            print(f"\n🔍 Testing {test['name']}")

            payload = {
                "username": test["username"],
                "scrape_posts": True,
                "max_posts": test["max_posts"],
                "scrape_level": 4
            }

            job_id = self.submit_job(payload)
            if job_id:
                print(f"   Job submitted: {job_id}")
                result = self.wait_for_completion(job_id, timeout_minutes=3)
                analysis = self.analyze_result(result, f"edge_{test['name']}")
                results.append(analysis)

                # Print edge case results
                if analysis["status"] == "finished":
                    print(f"   📊 RESULT: {analysis['posts_found']} posts found")
                elif analysis["status"] == "error":
                    print(f"   ⚠️ ERROR (expected): {analysis.get('issues', ['Unknown'])}")
                else:
                    print(f"   ❌ UNEXPECTED: {analysis['status']}")
            else:
                results.append({"test_name": f"edge_{test['name']}", "status": "submission_failed"})
                print(f"   ❌ SUBMISSION FAILED")

        return results

    def run_comprehensive_tests(self):
        """Run all tests and generate comprehensive report"""
        print("🚀 COMPREHENSIVE TWITTER SCRAPER TESTING")
        print("=" * 60)

        # Run all test suites
        user_results = self.test_user_profiles()
        search_results = self.test_search_functionality()
        scale_results = self.test_scale_performance()
        edge_results = self.test_edge_cases()

        # Combine all results
        all_results = user_results + search_results + scale_results + edge_results

        # Generate comprehensive report
        self.generate_report(all_results)

        return all_results

    def generate_report(self, results: List[Dict]):
        """Generate comprehensive test report"""
        print("\n" + "=" * 60)
        print("📊 COMPREHENSIVE TEST RESULTS")
        print("=" * 60)

        # Overall statistics
        total_tests = len(results)
        successful_tests = sum(1 for r in results if r.get("status") == "finished" and r.get("posts_found", 0) > 0)
        failed_tests = total_tests - successful_tests

        print(f"\n📈 OVERALL STATISTICS:")
        print(f"   Total tests: {total_tests}")
        print(f"   Successful: {successful_tests} ({successful_tests/total_tests*100:.1f}%)")
        print(f"   Failed: {failed_tests} ({failed_tests/total_tests*100:.1f}%)")

        # Category breakdown
        categories = {}
        for result in results:
            category = result["test_name"].split("_")[0]
            if category not in categories:
                categories[category] = {"total": 0, "success": 0, "posts": 0}
            categories[category]["total"] += 1
            if result.get("status") == "finished" and result.get("posts_found", 0) > 0:
                categories[category]["success"] += 1
                categories[category]["posts"] += result.get("posts_found", 0)

        print(f"\n📊 CATEGORY BREAKDOWN:")
        for category, stats in categories.items():
            success_rate = stats["success"] / stats["total"] * 100
            print(f"   {category.upper()}: {stats['success']}/{stats['total']} ({success_rate:.1f}%) - {stats['posts']} total posts")

        # Detailed failures
        failures = [r for r in results if r.get("status") != "finished" or r.get("posts_found", 0) == 0]
        if failures:
            print(f"\n❌ DETAILED FAILURES:")
            for failure in failures:
                print(f"   {failure['test_name']}: {failure.get('status', 'unknown')} - {failure.get('issues', ['No details'])}")

        # Performance metrics
        successful_results = [r for r in results if r.get("status") == "finished" and r.get("posts_found", 0) > 0]
        if successful_results:
            avg_time = sum(r.get("execution_time", 0) for r in successful_results) / len(successful_results)
            total_posts = sum(r.get("posts_found", 0) for r in successful_results)

            print(f"\n⚡ PERFORMANCE METRICS:")
            print(f"   Average execution time: {avg_time:.1f}s")
            print(f"   Total posts extracted: {total_posts}")
            print(f"   Average posts per test: {total_posts/len(successful_results):.1f}")

        # Issues summary
        all_issues = []
        for result in results:
            all_issues.extend(result.get("issues", []))

        if all_issues:
            print(f"\n🚨 ISSUES IDENTIFIED:")
            unique_issues = list(set(all_issues))
            for i, issue in enumerate(unique_issues, 1):
                print(f"   {i}. {issue}")
        else:
            print(f"\n✅ NO CRITICAL ISSUES IDENTIFIED")

if __name__ == "__main__":
    tester = TwitterScraperTester()
    results = tester.run_comprehensive_tests()
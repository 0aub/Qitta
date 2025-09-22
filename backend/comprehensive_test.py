#!/usr/bin/env python3
"""
COMPREHENSIVE TWITTER SCRAPER FEATURE TESTING
============================================

Tests all system features systematically to identify issues:
1. User post extraction (multiple accounts, different scales)
2. Search functionality (hashtag + query searches)
3. Media extraction and Phase B filtering
4. Performance across different request sizes
5. Error handling and edge cases
"""

import json
import urllib.request
import time
import sys
from typing import Dict, List, Any

class ComprehensiveTwitterTest:
    def __init__(self):
        self.endpoint = "http://localhost:8004"
        self.test_results = {}
        self.issues_found = []
        self.successes = []

    def run_all_tests(self):
        """Run comprehensive test suite."""
        print("🧪 COMPREHENSIVE TWITTER SCRAPER TESTING")
        print("=" * 60)
        print("Testing ALL features to identify issues before production deployment")
        print()

        tests = [
            ("User Post Extraction", self.test_user_posts),
            ("Search Functionality", self.test_search_features),
            ("Media Extraction", self.test_media_extraction),
            ("Performance Scaling", self.test_performance_scaling),
            ("Error Handling", self.test_error_handling),
            ("Edge Cases", self.test_edge_cases)
        ]

        for test_name, test_func in tests:
            print(f"\n{'=' * 20} {test_name} {'=' * 20}")
            try:
                results = test_func()
                self.test_results[test_name] = results
                self.analyze_test_results(test_name, results)
            except Exception as e:
                error_msg = f"Test framework error: {e}"
                self.issues_found.append(f"❌ {test_name}: {error_msg}")
                print(f"💥 TEST FRAMEWORK ERROR: {e}")

        self.generate_comprehensive_report()

    def submit_job_and_wait(self, payload: Dict, timeout: int = 90) -> Dict:
        """Submit job and wait for completion."""
        try:
            data = json.dumps(payload).encode('utf-8')
            req = urllib.request.Request(
                f"{self.endpoint}/jobs/twitter",
                data=data,
                headers={'Content-Type': 'application/json'}
            )

            with urllib.request.urlopen(req, timeout=15) as response:
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

            return {"status": "timeout", "job_id": job_id}

        except Exception as e:
            return {"status": "exception", "error": str(e)}

    def test_user_posts(self) -> Dict:
        """Test user post extraction with multiple accounts and scales."""
        print("🔍 Testing user post extraction...")

        test_cases = [
            # Different account types
            {"username": "sama", "max_posts": 3, "scrape_level": 4, "desc": "Tech leader"},
            {"username": "naval", "max_posts": 3, "scrape_level": 4, "desc": "Previously failing"},
            {"username": "elonmusk", "max_posts": 3, "scrape_level": 4, "desc": "High-profile"},
            {"username": "openai", "max_posts": 3, "scrape_level": 4, "desc": "Corporate"},

            # Different scales with same account
            {"username": "sama", "max_posts": 1, "scrape_level": 4, "desc": "Minimal request"},
            {"username": "sama", "max_posts": 5, "scrape_level": 4, "desc": "Small request"},
            {"username": "sama", "max_posts": 15, "scrape_level": 4, "desc": "Medium request"},
            {"username": "sama", "max_posts": 25, "scrape_level": 4, "desc": "Large request"},

            # Different levels
            {"username": "sama", "max_posts": 5, "scrape_level": 1, "desc": "Level 1"},
            {"username": "sama", "max_posts": 5, "scrape_level": 2, "desc": "Level 2"},
            {"username": "sama", "max_posts": 5, "scrape_level": 3, "desc": "Level 3"},
        ]

        results = {}

        for case in test_cases:
            test_key = f"{case['username']}_L{case['scrape_level']}_{case['max_posts']}posts"
            print(f"  Testing: @{case['username']} - {case['desc']} ({case['max_posts']} posts, Level {case['scrape_level']})")

            payload = {
                "username": case["username"],
                "scrape_posts": True,
                "max_posts": case["max_posts"],
                "scrape_level": case["scrape_level"]
            }

            start_time = time.time()
            result = self.submit_job_and_wait(payload, 90)
            execution_time = time.time() - start_time

            analysis = self.analyze_user_post_result(result, case, execution_time)
            results[test_key] = analysis

            # Print immediate feedback
            if analysis["status"] == "SUCCESS":
                print(f"    ✅ SUCCESS: {analysis['posts_count']} posts in {execution_time:.1f}s")
            else:
                print(f"    ❌ FAILED: {analysis['issue']}")

        return results

    def test_search_features(self) -> Dict:
        """Test search functionality (hashtag and query)."""
        print("🔍 Testing search functionality...")

        test_cases = [
            # Hashtag searches
            {"hashtag": "AI", "max_tweets": 5, "scrape_level": 4, "desc": "Popular hashtag"},
            {"hashtag": "bitcoin", "max_tweets": 5, "scrape_level": 4, "desc": "Crypto hashtag"},
            {"hashtag": "python", "max_tweets": 3, "scrape_level": 4, "desc": "Tech hashtag"},

            # Query searches
            {"search_query": "machine learning", "max_tweets": 5, "scrape_level": 4, "desc": "ML query"},
            {"search_query": "artificial intelligence", "max_tweets": 3, "scrape_level": 4, "desc": "AI query"},
            {"search_query": "startup", "max_tweets": 3, "scrape_level": 4, "desc": "Business query"},

            # Different levels
            {"hashtag": "AI", "max_tweets": 3, "scrape_level": 1, "desc": "Hashtag Level 1"},
            {"search_query": "AI", "max_tweets": 3, "scrape_level": 2, "desc": "Query Level 2"},
        ]

        results = {}

        for case in test_cases:
            search_type = "hashtag" if "hashtag" in case else "query"
            search_term = case.get("hashtag", case.get("search_query"))
            test_key = f"{search_type}_{search_term.replace(' ', '_')}_L{case['scrape_level']}"

            print(f"  Testing: {search_type} '{search_term}' - {case['desc']}")

            start_time = time.time()
            result = self.submit_job_and_wait(case, 90)
            execution_time = time.time() - start_time

            analysis = self.analyze_search_result(result, case, execution_time)
            results[test_key] = analysis

            # Print immediate feedback
            if analysis["status"] == "SUCCESS":
                print(f"    ✅ SUCCESS: {analysis['posts_count']} posts, method: {analysis['extraction_method']}")
            else:
                print(f"    ❌ FAILED: {analysis['issue']}")

        return results

    def test_media_extraction(self) -> Dict:
        """Test Phase B media extraction improvements."""
        print("🔍 Testing media extraction and Phase B filtering...")

        # Test with accounts likely to have media
        test_cases = [
            {"username": "elonmusk", "max_posts": 5, "scrape_level": 4, "desc": "Media-heavy account"},
            {"username": "sama", "max_posts": 5, "scrape_level": 4, "desc": "Mixed content"},
            {"username": "openai", "max_posts": 3, "scrape_level": 4, "desc": "Corporate media"},
        ]

        results = {}

        for case in test_cases:
            test_key = f"media_{case['username']}"
            print(f"  Testing: @{case['username']} - {case['desc']}")

            payload = {
                "username": case["username"],
                "scrape_posts": True,
                "max_posts": case["max_posts"],
                "scrape_level": case["scrape_level"]
            }

            result = self.submit_job_and_wait(payload, 90)
            analysis = self.analyze_media_result(result, case)
            results[test_key] = analysis

            # Print immediate feedback
            if analysis["total_media"] > 0:
                confidence = analysis["avg_confidence"]
                signal_rate = analysis["high_confidence_rate"]
                print(f"    📷 MEDIA: {analysis['total_media']} items, avg confidence: {confidence:.2f}, signal rate: {signal_rate:.1f}%")

                if confidence >= 0.6:
                    print(f"    ✅ Phase B working well")
                else:
                    print(f"    ⚠️ Phase B needs improvement")
            else:
                print(f"    ℹ️ No media found")

        return results

    def test_performance_scaling(self) -> Dict:
        """Test performance across different request scales."""
        print("🔍 Testing performance scaling...")

        test_cases = [
            {"username": "sama", "max_posts": 1, "desc": "Minimal (1 post)"},
            {"username": "sama", "max_posts": 5, "desc": "Small (5 posts)"},
            {"username": "sama", "max_posts": 15, "desc": "Medium (15 posts)"},
            {"username": "sama", "max_posts": 30, "desc": "Large (30 posts)"},
        ]

        results = {}

        for case in test_cases:
            test_key = f"perf_{case['max_posts']}posts"
            print(f"  Testing: {case['desc']}")

            payload = {
                "username": case["username"],
                "scrape_posts": True,
                "max_posts": case["max_posts"],
                "scrape_level": 4
            }

            start_time = time.time()
            result = self.submit_job_and_wait(payload, 180)  # Longer timeout for large requests
            execution_time = time.time() - start_time

            analysis = self.analyze_performance_result(result, case, execution_time)
            results[test_key] = analysis

            # Print immediate feedback
            if analysis["status"] == "SUCCESS":
                posts_per_min = analysis["posts_extracted"] / (execution_time / 60)
                print(f"    ✅ {analysis['posts_extracted']} posts in {execution_time:.1f}s ({posts_per_min:.1f} posts/min)")
                print(f"    Method: {analysis['extraction_method']}")
            else:
                print(f"    ❌ FAILED: {analysis['issue']}")

        return results

    def test_error_handling(self) -> Dict:
        """Test error handling with invalid inputs."""
        print("🔍 Testing error handling...")

        test_cases = [
            {"username": "nonexistentuser12345", "max_posts": 3, "desc": "Non-existent user"},
            {"username": "", "max_posts": 3, "desc": "Empty username"},
            {"username": "sama", "max_posts": 0, "desc": "Zero posts requested"},
            {"username": "sama", "max_posts": -1, "desc": "Negative posts"},
            {"hashtag": "", "max_tweets": 3, "desc": "Empty hashtag"},
            {"search_query": "", "max_tweets": 3, "desc": "Empty query"},
        ]

        results = {}

        for case in test_cases:
            test_key = f"error_{case['desc'].replace(' ', '_').replace('-', '_')}"
            print(f"  Testing: {case['desc']}")

            payload = dict(case)
            payload.pop("desc")
            if "max_posts" not in payload:
                payload["scrape_level"] = 4

            result = self.submit_job_and_wait(payload, 60)
            analysis = self.analyze_error_result(result, case)
            results[test_key] = analysis

            # Print immediate feedback
            if analysis["handled_gracefully"]:
                print(f"    ✅ Error handled gracefully: {analysis['status']}")
            else:
                print(f"    ❌ Poor error handling: {analysis['issue']}")

        return results

    def test_edge_cases(self) -> Dict:
        """Test edge cases and unusual scenarios."""
        print("🔍 Testing edge cases...")

        test_cases = [
            {"username": "sama", "max_posts": 1000, "scrape_level": 4, "desc": "Very large request"},
            {"username": "sama", "scrape_posts": True, "scrape_followers": True, "max_posts": 3, "desc": "Multiple data types"},
            {"hashtag": "AI", "max_tweets": 100, "scrape_level": 4, "desc": "Large search"},
        ]

        results = {}

        for case in test_cases:
            test_key = f"edge_{case['desc'].replace(' ', '_')}"
            print(f"  Testing: {case['desc']}")

            payload = dict(case)
            payload.pop("desc")

            start_time = time.time()
            result = self.submit_job_and_wait(payload, 300)  # 5 minute timeout
            execution_time = time.time() - start_time

            analysis = self.analyze_edge_case_result(result, case, execution_time)
            results[test_key] = analysis

            # Print immediate feedback
            print(f"    Status: {analysis['status']} in {execution_time:.1f}s")
            if "issue" in analysis:
                print(f"    Issue: {analysis['issue']}")

        return results

    def analyze_user_post_result(self, result: Dict, case: Dict, execution_time: float) -> Dict:
        """Analyze user post extraction results."""
        analysis = {
            "case": case,
            "execution_time": execution_time,
            "status": "UNKNOWN"
        }

        if result.get("status") == "finished":
            res = result["result"]
            data = res.get("data", [])

            if data and "posts" in data[0]:
                posts = data[0]["posts"]
                posts_count = len(posts)

                analysis.update({
                    "status": "SUCCESS" if posts_count > 0 else "NO_POSTS",
                    "posts_count": posts_count,
                    "extraction_method": res.get("search_metadata", {}).get("extraction_method", "unknown"),
                    "success_rate": res.get("search_metadata", {}).get("success_rate", 0)
                })

                if posts_count == 0:
                    analysis["issue"] = "0 posts extracted despite success status"
            else:
                analysis.update({
                    "status": "NO_DATA_STRUCTURE",
                    "issue": "No posts data structure in result"
                })
        elif result.get("status") == "error":
            analysis.update({
                "status": "ERROR",
                "issue": result.get("error", "Unknown error")
            })
        else:
            analysis.update({
                "status": "TIMEOUT_OR_EXCEPTION",
                "issue": result.get("error", "Job did not complete")
            })

        return analysis

    def analyze_search_result(self, result: Dict, case: Dict, execution_time: float) -> Dict:
        """Analyze search functionality results."""
        analysis = {
            "case": case,
            "execution_time": execution_time,
            "status": "UNKNOWN"
        }

        if result.get("status") == "finished":
            res = result["result"]
            search_metadata = res.get("search_metadata", {})
            extraction_method = search_metadata.get("extraction_method", "unknown")

            data = res.get("data", [])
            posts_count = 0

            if data and "posts" in data[0]:
                posts = data[0]["posts"]
                posts_count = len(posts)

            # Check if routing is working correctly
            expected_method = "hashtag_scraping" if "hashtag" in case else "query_scraping"
            routing_correct = expected_method in extraction_method or "search" in extraction_method

            analysis.update({
                "status": "SUCCESS" if posts_count > 0 and routing_correct else "ROUTING_ISSUE",
                "posts_count": posts_count,
                "extraction_method": extraction_method,
                "routing_correct": routing_correct
            })

            if posts_count == 0:
                analysis["issue"] = f"0 posts found for search (method: {extraction_method})"
            elif not routing_correct:
                analysis["issue"] = f"Wrong routing: expected {expected_method}, got {extraction_method}"

        elif result.get("status") == "error":
            analysis.update({
                "status": "ERROR",
                "issue": result.get("error", "Unknown error")
            })
        else:
            analysis.update({
                "status": "TIMEOUT",
                "issue": "Search job did not complete"
            })

        return analysis

    def analyze_media_result(self, result: Dict, case: Dict) -> Dict:
        """Analyze Phase B media extraction results."""
        analysis = {
            "case": case,
            "total_media": 0,
            "high_confidence_media": 0,
            "avg_confidence": 0,
            "priority_breakdown": {},
            "high_confidence_rate": 0
        }

        if result.get("status") == "finished":
            res = result["result"]
            data = res.get("data", [])

            if data and "posts" in data[0]:
                posts = data[0]["posts"]
                confidence_scores = []

                for post in posts:
                    media = post.get("media", [])
                    analysis["total_media"] += len(media)

                    for media_item in media:
                        confidence = media_item.get("confidence_score", 0)
                        priority = media_item.get("priority_level", "UNKNOWN")

                        confidence_scores.append(confidence)
                        analysis["priority_breakdown"][priority] = analysis["priority_breakdown"].get(priority, 0) + 1

                        if confidence >= 0.7:
                            analysis["high_confidence_media"] += 1

                if confidence_scores:
                    analysis["avg_confidence"] = sum(confidence_scores) / len(confidence_scores)
                    analysis["high_confidence_rate"] = analysis["high_confidence_media"] / analysis["total_media"] * 100

        return analysis

    def analyze_performance_result(self, result: Dict, case: Dict, execution_time: float) -> Dict:
        """Analyze performance scaling results."""
        analysis = {
            "case": case,
            "execution_time": execution_time,
            "status": "UNKNOWN"
        }

        if result.get("status") == "finished":
            res = result["result"]
            search_metadata = res.get("search_metadata", {})

            data = res.get("data", [])
            posts_count = 0

            if data and "posts" in data[0]:
                posts = data[0]["posts"]
                posts_count = len(posts)

            analysis.update({
                "status": "SUCCESS",
                "posts_extracted": posts_count,
                "extraction_method": search_metadata.get("extraction_method", "unknown"),
                "posts_per_second": posts_count / execution_time if execution_time > 0 else 0
            })

        elif result.get("status") == "error":
            analysis.update({
                "status": "ERROR",
                "issue": result.get("error", "Unknown error")
            })
        else:
            analysis.update({
                "status": "TIMEOUT",
                "issue": "Performance test did not complete"
            })

        return analysis

    def analyze_error_result(self, result: Dict, case: Dict) -> Dict:
        """Analyze error handling results."""
        analysis = {
            "case": case,
            "handled_gracefully": False,
            "status": result.get("status", "unknown")
        }

        # Good error handling should either:
        # 1. Return error status with clear message
        # 2. Return finished with empty results but no crash

        if result.get("status") == "error":
            error_msg = result.get("error", "")
            analysis.update({
                "handled_gracefully": True,
                "error_message": error_msg
            })
        elif result.get("status") == "finished":
            # Check if it returned empty results gracefully
            res = result.get("result", {})
            data = res.get("data", [])

            if not data or not data[0].get("posts"):
                analysis.update({
                    "handled_gracefully": True,
                    "result": "Empty results returned gracefully"
                })
            else:
                analysis.update({
                    "handled_gracefully": False,
                    "issue": "Invalid input produced valid results"
                })
        else:
            analysis.update({
                "handled_gracefully": False,
                "issue": f"Unexpected status: {result.get('status')}"
            })

        return analysis

    def analyze_edge_case_result(self, result: Dict, case: Dict, execution_time: float) -> Dict:
        """Analyze edge case results."""
        analysis = {
            "case": case,
            "execution_time": execution_time,
            "status": result.get("status", "unknown")
        }

        if result.get("status") == "finished":
            res = result["result"]
            data = res.get("data", [])

            if data:
                posts_count = len(data[0].get("posts", []))
                analysis["posts_extracted"] = posts_count

                if case["desc"] == "Very large request":
                    # For large requests, expect either good results or graceful handling
                    if posts_count >= 20:  # Got reasonable amount
                        analysis["status"] = "SUCCESS"
                    elif posts_count > 0:
                        analysis["status"] = "PARTIAL_SUCCESS"
                        analysis["issue"] = f"Only {posts_count} posts for large request"
                    else:
                        analysis["status"] = "FAILED"
                        analysis["issue"] = "No posts for large request"
        elif result.get("status") == "error":
            analysis["issue"] = result.get("error", "Unknown error")
        elif result.get("status") == "timeout":
            analysis["issue"] = "Edge case test timed out"

        return analysis

    def analyze_test_results(self, test_name: str, results: Dict):
        """Analyze results and categorize issues."""
        success_count = 0
        total_count = len(results)

        for test_key, result in results.items():
            if result.get("status") in ["SUCCESS", "PARTIAL_SUCCESS"] or result.get("handled_gracefully"):
                success_count += 1
                self.successes.append(f"✅ {test_name}: {test_key} - {result.get('status', 'SUCCESS')}")
            else:
                issue = result.get("issue", "Unknown issue")
                self.issues_found.append(f"❌ {test_name}: {test_key} - {issue}")

        success_rate = success_count / total_count * 100 if total_count > 0 else 0
        print(f"\n📊 {test_name} Summary: {success_count}/{total_count} passed ({success_rate:.1f}%)")

    def generate_comprehensive_report(self):
        """Generate final comprehensive test report."""
        print("\n" + "=" * 80)
        print("🧪 COMPREHENSIVE TESTING RESULTS")
        print("=" * 80)

        total_issues = len(self.issues_found)
        total_successes = len(self.successes)
        total_tests = total_issues + total_successes

        print(f"📊 OVERALL RESULTS:")
        print(f"   Total tests run: {total_tests}")
        print(f"   Successful tests: {total_successes}")
        print(f"   Issues found: {total_issues}")
        print(f"   Success rate: {total_successes/total_tests*100:.1f}%" if total_tests > 0 else "   Success rate: 0%")
        print()

        if self.issues_found:
            print("🚨 CRITICAL ISSUES IDENTIFIED:")
            for issue in self.issues_found[:10]:  # Show first 10
                print(f"   {issue}")

            if len(self.issues_found) > 10:
                print(f"   ... and {len(self.issues_found) - 10} more issues")
            print()

        if self.successes:
            print("✅ SUCCESSFUL TESTS:")
            for success in self.successes[:10]:  # Show first 10
                print(f"   {success}")

            if len(self.successes) > 10:
                print(f"   ... and {len(self.successes) - 10} more successes")
            print()

        # Final assessment
        if total_issues == 0:
            print("🎉 ALL TESTS PASSED - System ready for production!")
        elif total_issues <= total_tests * 0.2:  # Less than 20% issues
            print("⚠️ MINOR ISSUES FOUND - System mostly ready with some tweaks needed")
        else:
            print("❌ SIGNIFICANT ISSUES FOUND - System needs more work before production")

        print("=" * 80)

if __name__ == "__main__":
    tester = ComprehensiveTwitterTest()
    tester.run_all_tests()
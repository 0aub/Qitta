#!/usr/bin/env python3
"""
🐦 COMPREHENSIVE TWITTER SCRAPER TEST SUITE
==========================================

Complete testing and validation suite for Twitter scraper functionality.
Provides detailed analysis, validation, and performance metrics in a single output.
"""

import os, time, pathlib, pprint, requests, json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

# Configuration - Auto-detect correct endpoint
def find_browser_endpoint():
    """Auto-detect the correct browser endpoint."""
    import urllib.request

    # Try different possible endpoints
    endpoints = [
        "http://browser:8001",
        "http://browser:8004",
        "http://localhost:8001",
        "http://localhost:8004"
    ]

    for endpoint in endpoints:
        try:
            with urllib.request.urlopen(f"{endpoint}/healthz", timeout=2) as response:
                if response.status == 200:
                    print(f"🔍 Auto-detected browser endpoint: {endpoint}")
                    return endpoint
        except:
            continue

    # Default fallback
    return "http://localhost:8001"

EP = find_browser_endpoint()  # Auto-detect correct endpoint
SCRAPED = pathlib.Path("/storage/scraped_data")

# Test accounts with different characteristics
TEST_ACCOUNTS = {
    "naval": "High-quality tweets, philosophy",
    "elonmusk": "High activity, mixed content",
    "paulg": "Startup advice, essays",
    "sama": "AI/tech commentary",
    "vitalikbuterin": "Crypto/blockchain content"
}

# Test configurations
TEST_CONFIGS = {
    "basic": {
        "scrape_posts": True,
        "max_posts": 10,
        "scrape_level": 1
    },
    "enhanced": {
        "scrape_posts": True,
        "max_posts": 15,
        "scrape_likes": True,
        "max_likes": 5,
        "scrape_level": 2
    },
    "comprehensive": {
        "scrape_posts": True,
        "max_posts": 20,
        "scrape_likes": True,
        "max_likes": 10,
        "scrape_mentions": True,
        "max_mentions": 5,
        "scrape_media": True,
        "max_media": 5,
        "scrape_level": 4
    },
    "date_filtered": {
        "scrape_posts": True,
        "max_posts": 25,
        "enable_date_filtering": True,
        "date_range": "last_week",
        "scrape_level": 4
    }
}

class TwitterTestSuite:
    """Comprehensive Twitter scraper test suite."""

    def __init__(self):
        self.results = {}
        self.performance_metrics = {}
        self.validation_results = {}
        self.total_tests = 0
        self.successful_tests = 0
        self.failed_tests = 0
        self.data_quality_scores = []

    def wait_for_job(self, job_id: str, every: int = 3) -> Dict[str, Any]:
        """Wait for job completion and return result."""
        print(f"⏳ Waiting for job {job_id}...")
        start_time = time.time()

        while True:
            try:
                rec = requests.get(f"{EP}/jobs/{job_id}", timeout=10).json()
                status = rec["status"]

                if status not in {"finished", "error"}:
                    elapsed = time.time() - start_time
                    print(f"\r⏱️  {rec.get('status_with_elapsed', status)} ({elapsed:.0f}s)", end="")
                else:
                    elapsed = time.time() - start_time
                    print(f"\n✅ {status.upper()} in {elapsed:.1f}s")
                    return rec

                time.sleep(every)

            except Exception as e:
                print(f"\n❌ Error checking job status: {e}")
                return {"status": "error", "error": str(e)}

    def submit_job(self, task: str, payload: Dict[str, Any], test_name: str) -> Dict[str, Any]:
        """Submit job and wait for completion."""
        print(f"\n🚀 SUBMITTING: {test_name}")
        print(f"📝 Payload: {json.dumps(payload, indent=2)}")

        try:
            r = requests.post(f"{EP}/jobs/{task}", json=payload, timeout=30)
            r.raise_for_status()
            jid = r.json()["job_id"]
            print(f"🆔 Job ID: {jid}")

            result = self.wait_for_job(jid)
            return result

        except Exception as e:
            print(f"❌ Job submission failed: {e}")
            return {"status": "error", "error": str(e)}

    def analyze_extraction_result(self, result: Dict[str, Any], test_name: str) -> Dict[str, Any]:
        """Comprehensive analysis of extraction results."""
        print(f"\n" + "="*80)
        print(f"🔍 ANALYZING: {test_name}")
        print("="*80)

        analysis = {
            "test_name": test_name,
            "status": result.get("status", "unknown"),
            "job_id": result.get("job_id", "N/A"),
            "execution_time": 0,
            "data_extracted": False,
            "total_items": 0,
            "data_types": {},
            "quality_score": 0,
            "issues": [],
            "success": False
        }

        if result["status"] == "error":
            error_msg = result.get('error', 'Unknown error')
            print(f"❌ FAILED: {error_msg}")
            analysis["issues"].append(f"Job failed: {error_msg}")
            return analysis

        if "result" not in result:
            print(f"❌ No result data found")
            analysis["issues"].append("No result data in response")
            return analysis

        res = result["result"]
        metadata = res.get("search_metadata", {})
        data = res.get("data", [])

        # Basic extraction info
        print(f"✅ STATUS: Task completed successfully")
        print(f"🎯 TARGET: @{metadata.get('target_username', 'N/A')}")
        print(f"📊 EXTRACTION METHOD: {metadata.get('extraction_method', 'N/A')}")
        print(f"📈 SCRAPE LEVEL: {metadata.get('scrape_level', 'N/A')}")
        print(f"📈 SUCCESS RATE: {metadata.get('success_rate', 0):.1%}")

        # Update analysis
        analysis["success"] = True
        analysis["total_items"] = len(data)
        analysis["data_extracted"] = len(data) > 0

        if not data:
            print(f"⚠️ NO DATA EXTRACTED - 0 items returned")
            analysis["issues"].append("No data extracted")
            return analysis

        print(f"✅ TOTAL EXTRACTED: {len(data)} items")

        # Analyze data structure
        first_item = data[0] if data else {}

        # Check if comprehensive user data (profile + posts structure)
        if isinstance(first_item, dict) and 'profile' in first_item:
            self._analyze_comprehensive_data(first_item, analysis)
        else:
            self._analyze_direct_posts(data, analysis)

        # Calculate quality score
        quality_score = self._calculate_quality_score(data, analysis)
        analysis["quality_score"] = quality_score

        print(f"\n📊 DATA QUALITY SCORE: {quality_score:.0f}%")
        if quality_score >= 80:
            print(f"🎉 EXCELLENT data quality")
        elif quality_score >= 60:
            print(f"✅ GOOD data quality")
        elif quality_score >= 40:
            print(f"⚠️ FAIR data quality")
        else:
            print(f"❌ POOR data quality")

        return analysis

    def _analyze_comprehensive_data(self, data: Dict[str, Any], analysis: Dict[str, Any]):
        """Analyze comprehensive user data structure."""
        print(f"\n📋 COMPREHENSIVE USER DATA ANALYSIS:")

        # Profile analysis
        profile = data.get('profile', {})
        if profile:
            print(f"   👤 Profile: Name='{profile.get('display_name', 'N/A')}' | Bio={len(profile.get('bio', ''))} chars")
            print(f"   📊 Stats: {profile.get('followers_count', 'N/A')} followers | {profile.get('following_count', 'N/A')} following")
            analysis["data_types"]["profile"] = 1

        # Data types analysis
        data_types = ['posts', 'likes', 'mentions', 'media', 'followers', 'following']
        total_items = 0

        print(f"\n📊 EXTRACTED DATA BREAKDOWN:")
        for data_type in data_types:
            items = data.get(data_type, [])
            if items and isinstance(items, list):
                count = len(items)
                total_items += count
                analysis["data_types"][data_type] = count

                emoji = self._get_emoji(data_type)
                print(f"   {emoji} {data_type.title()}: {count} items")

                # Show sample if available
                if count > 0 and isinstance(items[0], dict):
                    sample = items[0]
                    sample_text = sample.get('text', sample.get('content', str(sample)))[:60]
                    print(f"      📝 Sample: {sample_text}{'...' if len(str(sample)) > 60 else ''}")

        analysis["total_items"] = total_items
        print(f"\n📈 TOTAL DATA ITEMS: {total_items} across all categories")

    def _analyze_direct_posts(self, data: List[Dict], analysis: Dict[str, Any]):
        """Analyze direct posts/tweets data."""
        print(f"\n📝 DIRECT POSTS ANALYSIS:")
        print(f"   📊 Total Posts: {len(data)}")

        analysis["data_types"]["posts"] = len(data)

        # Count different data attributes
        posts_with_text = sum(1 for p in data if isinstance(p, dict) and p.get('text'))
        posts_with_dates = sum(1 for p in data if isinstance(p, dict) and p.get('date'))
        posts_with_metrics = sum(1 for p in data if isinstance(p, dict) and any(k in p for k in ['likes', 'retweets', 'replies']))

        print(f"   📝 With text: {posts_with_text}/{len(data)} ({posts_with_text/len(data)*100:.0f}%)")
        print(f"   📅 With dates: {posts_with_dates}/{len(data)} ({posts_with_dates/len(data)*100:.0f}%)")
        print(f"   📊 With metrics: {posts_with_metrics}/{len(data)} ({posts_with_metrics/len(data)*100:.0f}%)")

        # Show samples
        sample_count = min(3, len(data))
        for i in range(sample_count):
            post = data[i]
            if isinstance(post, dict):
                text = post.get('text', 'No text')[:100]
                date = post.get('date', 'No date')
                likes = post.get('likes', 'N/A')
                print(f"   🐦 Post {i+1}: {text}{'...' if len(post.get('text', '')) > 100 else ''}")
                print(f"      📅 {date} | ❤️ {likes}")

    def _calculate_quality_score(self, data: List[Dict], analysis: Dict[str, Any]) -> float:
        """Calculate data quality score based on completeness and structure."""
        if not data:
            return 0.0

        score = 0
        max_score = 100

        # Basic data presence (40 points)
        if len(data) > 0:
            score += 20
        if len(data) >= 5:
            score += 20

        # Data structure quality (60 points)
        if isinstance(data[0], dict):
            # Check for key attributes
            first_item = data[0]

            if 'profile' in first_item:
                # Comprehensive data structure
                profile = first_item['profile']
                if profile.get('display_name'): score += 10
                if profile.get('username'): score += 10
                if profile.get('bio'): score += 5
                if profile.get('followers_count') is not None: score += 10

                # Data types presence
                if first_item.get('posts'): score += 15
                if first_item.get('likes'): score += 5
                if first_item.get('media'): score += 5
            else:
                # Direct posts structure
                posts_with_text = sum(1 for p in data if isinstance(p, dict) and p.get('text'))
                if posts_with_text > 0: score += 25
                if posts_with_text / len(data) > 0.8: score += 15

                posts_with_dates = sum(1 for p in data if isinstance(p, dict) and p.get('date'))
                if posts_with_dates > 0: score += 10

                posts_with_metrics = sum(1 for p in data if isinstance(p, dict) and any(k in p for k in ['likes', 'retweets']))
                if posts_with_metrics > 0: score += 10

        return min(score, max_score)

    def _get_emoji(self, data_type: str) -> str:
        """Get emoji for data type."""
        emojis = {
            'posts': '📝', 'likes': '❤️', 'mentions': '@️⃣',
            'media': '🖼️', 'followers': '👥', 'following': '➡️'
        }
        return emojis.get(data_type, '📊')

    def run_comprehensive_tests(self):
        """Run complete test suite."""
        print("🐦 TWITTER SCRAPER COMPREHENSIVE TEST SUITE")
        print("="*80)
        print(f"📍 API Endpoint: {EP}")
        print(f"📁 Storage Path: {SCRAPED}")
        print(f"🎯 Test Accounts: {list(TEST_ACCOUNTS.keys())}")
        print(f"🔧 Test Configurations: {list(TEST_CONFIGS.keys())}")

        # Check API connectivity
        try:
            test_response = requests.get(f"{EP}/healthz", timeout=5)
            if test_response.status_code == 200:
                print(f"✅ API connectivity: Connected to browser service")
            else:
                print(f"⚠️ API connectivity: Unexpected response {test_response.status_code}")
        except Exception as e:
            print(f"❌ API connectivity: Failed - {e}")
            return

        # Test 1: Basic extraction across multiple accounts
        print(f"\n" + "="*80)
        print(f"🧪 TEST PHASE 1: BASIC EXTRACTION ACROSS ACCOUNTS")
        print("="*80)

        accounts_to_test = ["naval", "paulg", "sama"]
        for account in accounts_to_test:
            self.total_tests += 1

            payload = {
                "username": account,
                **TEST_CONFIGS["basic"]
            }

            test_name = f"Basic Extraction - @{account}"
            result = self.submit_job("twitter", payload, test_name)

            analysis = self.analyze_extraction_result(result, test_name)
            self.results[test_name] = analysis

            if analysis["success"]:
                self.successful_tests += 1
                if analysis["data_extracted"]:
                    self.data_quality_scores.append(analysis["quality_score"])
            else:
                self.failed_tests += 1

            time.sleep(2)  # Brief pause between tests

        # Test 2: Level comparison on single account
        print(f"\n" + "="*80)
        print(f"🧪 TEST PHASE 2: SCRAPE LEVEL COMPARISON")
        print("="*80)

        test_account = "vitalikbuterin"
        for level in [1, 2, 3, 4]:
            self.total_tests += 1

            payload = {
                "username": test_account,
                "scrape_posts": True,
                "max_posts": 8,
                "scrape_level": level,
                "level": level
            }

            test_name = f"Level {level} Extraction - @{test_account}"
            result = self.submit_job("twitter", payload, test_name)

            analysis = self.analyze_extraction_result(result, test_name)
            self.results[test_name] = analysis

            if analysis["success"]:
                self.successful_tests += 1
                if analysis["data_extracted"]:
                    self.data_quality_scores.append(analysis["quality_score"])
            else:
                self.failed_tests += 1

        # Test 3: Comprehensive extraction with all features
        print(f"\n" + "="*80)
        print(f"🧪 TEST PHASE 3: COMPREHENSIVE EXTRACTION")
        print("="*80)

        self.total_tests += 1

        payload = {
            "username": "naval",
            **TEST_CONFIGS["comprehensive"]
        }

        test_name = "Comprehensive Extraction - @naval"
        result = self.submit_job("twitter", payload, test_name)

        analysis = self.analyze_extraction_result(result, test_name)
        self.results[test_name] = analysis

        if analysis["success"]:
            self.successful_tests += 1
            if analysis["data_extracted"]:
                self.data_quality_scores.append(analysis["quality_score"])
        else:
            self.failed_tests += 1

        # Test 4: Date filtering performance
        print(f"\n" + "="*80)
        print(f"🧪 TEST PHASE 4: DATE FILTERING PERFORMANCE")
        print("="*80)

        date_ranges = ["last_day", "last_week"]
        for date_range in date_ranges:
            self.total_tests += 1

            payload = {
                "username": "sama",
                "scrape_posts": True,
                "max_posts": 15,
                "enable_date_filtering": True,
                "date_range": date_range,
                "scrape_level": 4
            }

            test_name = f"Date Filter ({date_range}) - @sama"
            result = self.submit_job("twitter", payload, test_name)

            analysis = self.analyze_extraction_result(result, test_name)
            self.results[test_name] = analysis

            if analysis["success"]:
                self.successful_tests += 1
                if analysis["data_extracted"]:
                    self.data_quality_scores.append(analysis["quality_score"])
            else:
                self.failed_tests += 1

        # Generate final report
        self.generate_final_report()

    def generate_final_report(self):
        """Generate comprehensive final report."""
        print(f"\n" + "="*80)
        print(f"📊 COMPREHENSIVE TEST RESULTS & ANALYSIS")
        print("="*80)

        # Overall statistics
        success_rate = (self.successful_tests / self.total_tests * 100) if self.total_tests > 0 else 0
        avg_quality = sum(self.data_quality_scores) / len(self.data_quality_scores) if self.data_quality_scores else 0

        print(f"📈 OVERALL STATISTICS:")
        print(f"   🎯 Total Tests: {self.total_tests}")
        print(f"   ✅ Successful: {self.successful_tests} ({success_rate:.1f}%)")
        print(f"   ❌ Failed: {self.failed_tests}")
        print(f"   📊 Tests with Data: {len(self.data_quality_scores)}")
        print(f"   🏆 Average Quality Score: {avg_quality:.1f}%")

        # Detailed results breakdown
        print(f"\n📋 DETAILED TEST RESULTS:")
        for test_name, analysis in self.results.items():
            status = "✅" if analysis["success"] else "❌"
            data_status = f"({analysis['total_items']} items)" if analysis["data_extracted"] else "(no data)"
            quality = f"Q:{analysis['quality_score']:.0f}%" if analysis["quality_score"] > 0 else "Q:0%"

            print(f"   {status} {test_name}: {data_status} {quality}")

            # Show issues if any
            if analysis["issues"]:
                for issue in analysis["issues"]:
                    print(f"      ⚠️ {issue}")

        # Data extraction analysis
        tests_with_data = sum(1 for a in self.results.values() if a["data_extracted"])
        total_items_extracted = sum(a["total_items"] for a in self.results.values())

        print(f"\n📊 DATA EXTRACTION ANALYSIS:")
        print(f"   📈 Tests with Data: {tests_with_data}/{self.total_tests} ({tests_with_data/self.total_tests*100:.1f}%)")
        print(f"   📊 Total Items Extracted: {total_items_extracted}")
        print(f"   📈 Average Items per Successful Test: {total_items_extracted/tests_with_data:.1f}" if tests_with_data > 0 else "   📈 Average Items: 0")

        # Data types breakdown
        data_type_counts = {}
        for analysis in self.results.values():
            for data_type, count in analysis.get("data_types", {}).items():
                data_type_counts[data_type] = data_type_counts.get(data_type, 0) + count

        if data_type_counts:
            print(f"\n📊 DATA TYPES EXTRACTED:")
            for data_type, count in sorted(data_type_counts.items()):
                emoji = self._get_emoji(data_type)
                print(f"   {emoji} {data_type.title()}: {count} total items")

        # Performance assessment
        print(f"\n🎯 OVERALL ASSESSMENT:")
        if success_rate >= 90 and avg_quality >= 70:
            print(f"🎉 EXCELLENT - Twitter scraper is working perfectly!")
        elif success_rate >= 75 and avg_quality >= 50:
            print(f"✅ GOOD - Twitter scraper is working well with minor issues")
        elif success_rate >= 50:
            print(f"⚠️ FAIR - Twitter scraper has issues that need attention")
        else:
            print(f"❌ POOR - Twitter scraper needs significant fixes")

        # Storage information
        twitter_dir = SCRAPED / "twitter"
        if twitter_dir.exists():
            recent_jobs = sorted([d.name for d in twitter_dir.iterdir() if d.is_dir()], reverse=True)[:5]
            print(f"\n📁 RECENT JOB DATA:")
            for job_id in recent_jobs:
                job_path = twitter_dir / job_id
                files = list(job_path.glob("*")) if job_path.exists() else []
                print(f"   📂 {job_id}: {len(files)} files")

        print(f"\n🔗 Raw data files: {SCRAPED}/twitter/")
        print(f"🎉 COMPREHENSIVE TESTING COMPLETED!")

# Initialize and run comprehensive test suite
if __name__ == "__main__":
    test_suite = TwitterTestSuite()
    test_suite.run_comprehensive_tests()
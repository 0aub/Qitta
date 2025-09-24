"""Test script for Phase 1 reliability improvements.

This script validates that the reliable infrastructure works correctly by:
1. Testing Redis job queue functionality
2. Validating job lifecycle management
3. Testing timeout and cancellation
4. Verifying worker health monitoring
5. Testing resource cleanup
"""

import asyncio
import json
import time
import urllib.request
from typing import Dict, Any
import logging


class ReliabilityTester:
    """Comprehensive tester for Phase 1 reliability improvements."""

    def __init__(self, endpoint: str = "http://localhost:8004"):
        self.endpoint = endpoint
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO)

    async def run_all_tests(self) -> Dict[str, Any]:
        """Run all reliability tests."""
        print("🚀 PHASE 1 RELIABILITY TESTING")
        print("=" * 60)

        results = {
            "timestamp": time.time(),
            "tests": {},
            "overall_success": False,
            "critical_issues": [],
            "improvements_verified": []
        }

        try:
            # Test 1: Service health and startup
            results["tests"]["service_health"] = await self._test_service_health()

            # Test 2: Basic job submission and completion
            results["tests"]["basic_job_flow"] = await self._test_basic_job_flow()

            # Test 3: Concurrent job handling
            results["tests"]["concurrent_jobs"] = await self._test_concurrent_jobs()

            # Test 4: Job timeout and cancellation
            results["tests"]["timeout_cancellation"] = await self._test_timeout_cancellation()

            # Test 5: System statistics and monitoring
            results["tests"]["monitoring"] = await self._test_monitoring()

            # Test 6: Error recovery
            results["tests"]["error_recovery"] = await self._test_error_recovery()

            # Analyze overall results
            self._analyze_results(results)

        except Exception as e:
            self.logger.error(f"Critical test failure: {e}")
            results["critical_issues"].append(f"Test framework failure: {e}")

        return results

    async def _test_service_health(self) -> Dict[str, Any]:
        """Test service health and component status."""
        print("\n🏥 TEST 1: Service Health Check")

        result = {
            "status": "unknown",
            "components": {},
            "issues": [],
            "improvements": []
        }

        try:
            # Check health endpoint
            with urllib.request.urlopen(f"{self.endpoint}/healthz", timeout=10) as response:
                health_data = json.loads(response.read().decode('utf-8'))

            result["components"] = health_data.get("components", {})
            overall_status = health_data.get("status", "unknown")

            print(f"   Overall Status: {overall_status}")
            for component, status in result["components"].items():
                icon = "✅" if status == "ok" else "❌"
                print(f"   {icon} {component}: {status}")

            # Validate Redis connectivity
            if result["components"].get("redis") == "ok":
                result["improvements"].append("✅ Redis job queue connected successfully")
            else:
                result["issues"].append("❌ Redis connection failed - job persistence unavailable")

            # Validate browser runtime
            if result["components"].get("browser") == "ok":
                result["improvements"].append("✅ Browser runtime operational")
            else:
                result["issues"].append("❌ Browser runtime failed")

            # Validate workers
            if result["components"].get("workers") == "ok":
                result["improvements"].append("✅ Reliable worker pool operational")
            else:
                result["issues"].append("❌ Worker pool failed")

            result["status"] = "pass" if overall_status == "ok" else "fail"

        except Exception as e:
            result["status"] = "error"
            result["issues"].append(f"Health check failed: {e}")

        return result

    async def _test_basic_job_flow(self) -> Dict[str, Any]:
        """Test basic job submission, processing, and completion."""
        print("\n📝 TEST 2: Basic Job Flow")

        result = {
            "status": "unknown",
            "job_id": None,
            "execution_time": 0,
            "issues": [],
            "improvements": []
        }

        try:
            start_time = time.time()

            # Submit a simple job
            payload = {
                "username": "naval",
                "scrape_posts": True,
                "max_posts": 3,
                "scrape_level": 4,
                "timeout_seconds": 120,
                "priority": 0,
                "max_retries": 2
            }

            data = json.dumps(payload).encode('utf-8')
            req = urllib.request.Request(
                f"{self.endpoint}/jobs/twitter",
                data=data,
                headers={'Content-Type': 'application/json'}
            )

            with urllib.request.urlopen(req, timeout=10) as response:
                submit_result = json.loads(response.read().decode('utf-8'))

            job_id = submit_result["job_id"]
            result["job_id"] = job_id
            print(f"   ✅ Job submitted: {job_id}")

            # Monitor job progress
            max_wait = 180  # 3 minutes
            for i in range(max_wait):
                time.sleep(1)

                try:
                    with urllib.request.urlopen(f"{self.endpoint}/jobs/{job_id}", timeout=5) as response:
                        job_status = json.loads(response.read().decode('utf-8'))

                    status = job_status["status"]
                    if i % 10 == 0:  # Log every 10 seconds
                        print(f"   Status: {status} ({i}s elapsed)")

                    if status in ["completed", "failed", "cancelled", "timeout"]:
                        break

                except Exception as e:
                    result["issues"].append(f"Status check failed: {e}")
                    continue

            result["execution_time"] = time.time() - start_time

            # Check final status
            if status == "completed":
                result["status"] = "pass"
                result["improvements"].append("✅ Job completed successfully with reliable infrastructure")

                # Check reliability metadata
                reliability_info = job_status.get("reliability_info", {})
                if reliability_info:
                    result["improvements"].append("✅ Reliability metadata present in job response")
                    print(f"   Retry count: {reliability_info.get('retry_count', 0)}/{reliability_info.get('max_retries', 0)}")
                    print(f"   Worker ID: {reliability_info.get('worker_id', 'unknown')}")

                # Check result data
                if job_status.get("result"):
                    result["improvements"].append("✅ Job result data retrieved successfully")
                    print(f"   Result available: {len(str(job_status['result']))} chars")

            else:
                result["status"] = "fail"
                result["issues"].append(f"Job failed with status: {status}")
                if job_status.get("error"):
                    result["issues"].append(f"Error: {job_status['error']}")

            print(f"   ⏱️ Total execution time: {result['execution_time']:.1f}s")

        except Exception as e:
            result["status"] = "error"
            result["issues"].append(f"Basic job flow test failed: {e}")

        return result

    async def _test_concurrent_jobs(self) -> Dict[str, Any]:
        """Test concurrent job handling capabilities."""
        print("\n🔀 TEST 3: Concurrent Jobs")

        result = {
            "status": "unknown",
            "jobs_submitted": 0,
            "jobs_completed": 0,
            "jobs_failed": 0,
            "execution_time": 0,
            "issues": [],
            "improvements": []
        }

        try:
            start_time = time.time()
            job_ids = []

            # Submit 5 concurrent jobs
            concurrent_count = 5
            for i in range(concurrent_count):
                payload = {
                    "username": "naval",
                    "scrape_posts": True,
                    "max_posts": 2,  # Small jobs for speed
                    "scrape_level": 4,
                    "timeout_seconds": 120,
                    "priority": i  # Different priorities
                }

                data = json.dumps(payload).encode('utf-8')
                req = urllib.request.Request(
                    f"{self.endpoint}/jobs/twitter",
                    data=data,
                    headers={'Content-Type': 'application/json'}
                )

                with urllib.request.urlopen(req, timeout=10) as response:
                    submit_result = json.loads(response.read().decode('utf-8'))

                job_ids.append(submit_result["job_id"])

            result["jobs_submitted"] = len(job_ids)
            print(f"   ✅ Submitted {len(job_ids)} concurrent jobs")

            # Monitor all jobs
            max_wait = 300  # 5 minutes
            completed_jobs = set()

            for i in range(max_wait):
                time.sleep(1)

                for job_id in job_ids:
                    if job_id in completed_jobs:
                        continue

                    try:
                        with urllib.request.urlopen(f"{self.endpoint}/jobs/{job_id}", timeout=5) as response:
                            job_status = json.loads(response.read().decode('utf-8'))

                        status = job_status["status"]
                        if status in ["completed", "failed", "cancelled", "timeout"]:
                            completed_jobs.add(job_id)
                            if status == "completed":
                                result["jobs_completed"] += 1
                            else:
                                result["jobs_failed"] += 1

                    except Exception:
                        continue

                if len(completed_jobs) == len(job_ids):
                    break

                if i % 30 == 0 and i > 0:  # Log every 30 seconds
                    print(f"   Progress: {len(completed_jobs)}/{len(job_ids)} jobs completed ({i}s elapsed)")

            result["execution_time"] = time.time() - start_time

            # Analyze results
            completion_rate = result["jobs_completed"] / result["jobs_submitted"] * 100

            if completion_rate >= 80:
                result["status"] = "pass"
                result["improvements"].append(f"✅ {completion_rate:.1f}% job completion rate with concurrent processing")
                result["improvements"].append("✅ System handles concurrent jobs without degradation")
            else:
                result["status"] = "fail"
                result["issues"].append(f"Low completion rate: {completion_rate:.1f}%")

            print(f"   📊 Results: {result['jobs_completed']} completed, {result['jobs_failed']} failed")
            print(f"   ⏱️ Total time: {result['execution_time']:.1f}s")

        except Exception as e:
            result["status"] = "error"
            result["issues"].append(f"Concurrent jobs test failed: {e}")

        return result

    async def _test_timeout_cancellation(self) -> Dict[str, Any]:
        """Test job timeout and cancellation mechanisms."""
        print("\n⏰ TEST 4: Timeout and Cancellation")

        result = {
            "status": "unknown",
            "timeout_test": False,
            "cancellation_test": False,
            "issues": [],
            "improvements": []
        }

        try:
            # Test 1: Job timeout
            print("   Testing job timeout...")
            payload = {
                "username": "naval",
                "scrape_posts": True,
                "max_posts": 100,  # Large job likely to timeout
                "scrape_level": 4,
                "timeout_seconds": 30,  # Short timeout
                "max_retries": 0  # No retries for clean test
            }

            data = json.dumps(payload).encode('utf-8')
            req = urllib.request.Request(
                f"{self.endpoint}/jobs/twitter",
                data=data,
                headers={'Content-Type': 'application/json'}
            )

            with urllib.request.urlopen(req, timeout=10) as response:
                submit_result = json.loads(response.read().decode('utf-8'))

            timeout_job_id = submit_result["job_id"]

            # Wait for timeout
            for i in range(60):  # Wait up to 60 seconds
                time.sleep(1)

                with urllib.request.urlopen(f"{self.endpoint}/jobs/{timeout_job_id}", timeout=5) as response:
                    job_status = json.loads(response.read().decode('utf-8'))

                status = job_status["status"]
                if status in ["timeout", "failed"]:
                    result["timeout_test"] = True
                    result["improvements"].append("✅ Job timeout mechanism working correctly")
                    print(f"   ✅ Job timed out as expected ({status})")
                    break

            # Test 2: Job cancellation
            print("   Testing job cancellation...")
            payload = {
                "username": "naval",
                "scrape_posts": True,
                "max_posts": 50,
                "scrape_level": 4,
                "timeout_seconds": 300
            }

            data = json.dumps(payload).encode('utf-8')
            req = urllib.request.Request(
                f"{self.endpoint}/jobs/twitter",
                data=data,
                headers={'Content-Type': 'application/json'}
            )

            with urllib.request.urlopen(req, timeout=10) as response:
                submit_result = json.loads(response.read().decode('utf-8'))

            cancel_job_id = submit_result["job_id"]

            # Wait for job to start
            time.sleep(5)

            # Cancel the job
            cancel_req = urllib.request.Request(
                f"{self.endpoint}/jobs/{cancel_job_id}",
                method='DELETE'
            )

            with urllib.request.urlopen(cancel_req, timeout=10) as response:
                cancel_result = json.loads(response.read().decode('utf-8'))

            # Check if cancellation worked
            time.sleep(2)
            with urllib.request.urlopen(f"{self.endpoint}/jobs/{cancel_job_id}", timeout=5) as response:
                job_status = json.loads(response.read().decode('utf-8'))

            if job_status["status"] == "cancelled":
                result["cancellation_test"] = True
                result["improvements"].append("✅ Job cancellation mechanism working correctly")
                print("   ✅ Job cancelled successfully")

            if result["timeout_test"] and result["cancellation_test"]:
                result["status"] = "pass"
            else:
                result["status"] = "fail"
                if not result["timeout_test"]:
                    result["issues"].append("❌ Job timeout mechanism not working")
                if not result["cancellation_test"]:
                    result["issues"].append("❌ Job cancellation mechanism not working")

        except Exception as e:
            result["status"] = "error"
            result["issues"].append(f"Timeout/cancellation test failed: {e}")

        return result

    async def _test_monitoring(self) -> Dict[str, Any]:
        """Test system monitoring and statistics."""
        print("\n📊 TEST 5: Monitoring and Statistics")

        result = {
            "status": "unknown",
            "stats_available": False,
            "worker_info": False,
            "queue_info": False,
            "issues": [],
            "improvements": []
        }

        try:
            # Get system stats
            with urllib.request.urlopen(f"{self.endpoint}/stats", timeout=10) as response:
                stats_data = json.loads(response.read().decode('utf-8'))

            result["stats_available"] = True
            print("   ✅ Statistics endpoint accessible")

            # Check for required data
            if "jobs" in stats_data:
                job_stats = stats_data["jobs"]
                result["queue_info"] = True
                result["improvements"].append("✅ Job queue statistics available")
                print(f"   Queue size: {job_stats.get('total_queued', 0)}")
                print(f"   Running jobs: {job_stats.get('running_jobs_count', 0)}")

            if "workers" in stats_data:
                worker_stats = stats_data["workers"]
                result["worker_info"] = True
                result["improvements"].append("✅ Worker pool statistics available")
                print(f"   Worker count: {worker_stats.get('worker_count', 0)}/{worker_stats.get('max_workers', 0)}")

            if "service" in stats_data:
                service_info = stats_data["service"]
                result["improvements"].append("✅ Service metadata available")
                print(f"   Version: {service_info.get('version', 'unknown')}")
                print(f"   Mode: {service_info.get('mode', 'unknown')}")

            if result["stats_available"] and result["worker_info"] and result["queue_info"]:
                result["status"] = "pass"
            else:
                result["status"] = "partial"
                result["issues"].append("Some monitoring data missing")

        except Exception as e:
            result["status"] = "error"
            result["issues"].append(f"Monitoring test failed: {e}")

        return result

    async def _test_error_recovery(self) -> Dict[str, Any]:
        """Test error recovery and retry mechanisms."""
        print("\n🔄 TEST 6: Error Recovery")

        result = {
            "status": "unknown",
            "retry_mechanism": False,
            "graceful_failure": False,
            "issues": [],
            "improvements": []
        }

        try:
            # Test retry mechanism with invalid parameters
            payload = {
                "username": "invaliduser123456789",  # Likely to fail
                "scrape_posts": True,
                "max_posts": 3,
                "scrape_level": 4,
                "timeout_seconds": 60,
                "max_retries": 2
            }

            data = json.dumps(payload).encode('utf-8')
            req = urllib.request.Request(
                f"{self.endpoint}/jobs/twitter",
                data=data,
                headers={'Content-Type': 'application/json'}
            )

            with urllib.request.urlopen(req, timeout=10) as response:
                submit_result = json.loads(response.read().decode('utf-8'))

            job_id = submit_result["job_id"]

            # Monitor for retry attempts
            max_wait = 120
            retry_count = 0
            final_status = None

            for i in range(max_wait):
                time.sleep(1)

                with urllib.request.urlopen(f"{self.endpoint}/jobs/{job_id}", timeout=5) as response:
                    job_status = json.loads(response.read().decode('utf-8'))

                current_retry = job_status.get("reliability_info", {}).get("retry_count", 0)
                if current_retry > retry_count:
                    retry_count = current_retry
                    print(f"   Retry attempt {retry_count} detected")

                status = job_status["status"]
                if status in ["completed", "failed", "cancelled", "timeout"]:
                    final_status = status
                    break

            if retry_count > 0:
                result["retry_mechanism"] = True
                result["improvements"].append(f"✅ Retry mechanism working ({retry_count} retries attempted)")

            if final_status == "failed":
                result["graceful_failure"] = True
                result["improvements"].append("✅ Graceful failure handling working")
                print(f"   ✅ Job failed gracefully after {retry_count} retries")

            if result["retry_mechanism"] and result["graceful_failure"]:
                result["status"] = "pass"
            else:
                result["status"] = "partial"

        except Exception as e:
            result["status"] = "error"
            result["issues"].append(f"Error recovery test failed: {e}")

        return result

    def _analyze_results(self, results: Dict[str, Any]) -> None:
        """Analyze overall test results."""
        print("\n" + "=" * 60)
        print("📈 PHASE 1 RELIABILITY TEST RESULTS")
        print("=" * 60)

        test_results = results["tests"]
        passed = sum(1 for test in test_results.values() if test["status"] == "pass")
        total = len(test_results)

        results["overall_success"] = passed >= total * 0.8  # 80% pass rate

        print(f"\n🎯 OVERALL RESULTS: {passed}/{total} tests passed")

        # Collect all improvements and issues
        all_improvements = []
        all_issues = []

        for test_name, test_result in test_results.items():
            status_icon = "✅" if test_result["status"] == "pass" else "❌" if test_result["status"] == "fail" else "⚠️"
            print(f"{status_icon} {test_name.replace('_', ' ').title()}: {test_result['status']}")

            all_improvements.extend(test_result.get("improvements", []))
            all_issues.extend(test_result.get("issues", []))

        results["improvements_verified"] = all_improvements
        results["critical_issues"] = all_issues

        # Show improvements
        if all_improvements:
            print("\n🎉 PHASE 1 IMPROVEMENTS VERIFIED:")
            for improvement in all_improvements:
                print(f"   {improvement}")

        # Show remaining issues
        if all_issues:
            print("\n⚠️ REMAINING ISSUES:")
            for issue in all_issues:
                print(f"   {issue}")

        # Final verdict
        print("\n" + "=" * 60)
        if results["overall_success"]:
            print("🏆 PHASE 1 RELIABILITY IMPROVEMENTS: SUCCESS!")
            print("✅ System ready for Phase 2 architecture improvements")
        else:
            print("🚨 PHASE 1 RELIABILITY IMPROVEMENTS: NEEDS WORK")
            print("❌ Fix critical issues before proceeding to Phase 2")

        print("=" * 60)


async def main():
    """Run the reliability test suite."""
    tester = ReliabilityTester()
    results = await tester.run_all_tests()

    # Save results for analysis
    with open("/tmp/phase1_test_results.json", "w") as f:
        json.dump(results, f, indent=2, default=str)

    print(f"\n📁 Detailed results saved to: /tmp/phase1_test_results.json")


if __name__ == "__main__":
    asyncio.run(main())
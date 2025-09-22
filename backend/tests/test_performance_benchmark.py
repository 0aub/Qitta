#!/usr/bin/env python3
"""
PERFORMANCE BENCHMARK TEST SUITE
================================

Tests the Twitter scraper under various load conditions:
- Small scale (5 posts)
- Medium scale (15 posts)
- Large scale (30 posts)
- Multiple users
- Concurrent requests

Measures:
- Execution time
- Memory efficiency
- Data quality
- Error rates
- Performance metrics
"""
import json
import urllib.request
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List

class TwitterPerformanceBenchmark:
    def __init__(self):
        self.endpoint = "http://localhost:8004"
        self.benchmark_results = {}

    def run_benchmarks(self):
        """Run all performance benchmarks."""
        print("⚡ TWITTER SCRAPER PERFORMANCE BENCHMARK")
        print("=" * 50)

        benchmarks = [
            ("Small Scale (5 posts)", self.benchmark_small_scale),
            ("Medium Scale (15 posts)", self.benchmark_medium_scale),
            ("Large Scale (30 posts)", self.benchmark_large_scale),
            ("Multi-User Test", self.benchmark_multi_user),
            ("Concurrent Requests", self.benchmark_concurrent_requests),
            ("Export Format Performance", self.benchmark_export_formats)
        ]

        for name, benchmark_func in benchmarks:
            print(f"\n🧪 Running: {name}")
            try:
                result = benchmark_func()
                self.benchmark_results[name] = result
                print(f"✅ {name}: {result['status']} ({result['score']}/100)")
                print(f"   Time: {result['execution_time']:.1f}s, Quality: {result['data_quality']:.1f}%")
            except Exception as e:
                print(f"❌ {name}: ERROR - {e}")
                self.benchmark_results[name] = {'status': 'FAILED', 'error': str(e), 'score': 0}

        self.generate_benchmark_report()

    def submit_job_and_measure(self, payload: Dict, timeout: int = 120) -> Dict:
        """Submit job and measure performance metrics."""
        start_time = time.time()
        memory_start = self.get_estimated_memory_usage()

        data = json.dumps(payload).encode('utf-8')
        req = urllib.request.Request(
            f"{self.endpoint}/jobs/twitter",
            data=data,
            headers={'Content-Type': 'application/json'}
        )

        with urllib.request.urlopen(req, timeout=30) as response:
            result = json.loads(response.read().decode('utf-8'))
            job_id = result["job_id"]

        submit_time = time.time() - start_time

        # Wait for completion
        wait_start = time.time()
        for i in range(timeout // 3):
            try:
                with urllib.request.urlopen(f"{self.endpoint}/jobs/{job_id}", timeout=10) as response:
                    result = json.loads(response.read().decode('utf-8'))
                    status = result["status"]

                    if status in ["finished", "error"]:
                        break

                time.sleep(3)
            except:
                continue

        wait_time = time.time() - wait_start
        total_time = time.time() - start_time
        memory_end = self.get_estimated_memory_usage()

        return {
            'result': result,
            'submit_time': submit_time,
            'wait_time': wait_time,
            'total_time': total_time,
            'memory_delta': memory_end - memory_start,
            'job_id': job_id
        }

    def get_estimated_memory_usage(self) -> float:
        """Estimate memory usage (simplified)."""
        try:
            import psutil
            process = psutil.Process()
            return process.memory_info().rss / 1024 / 1024  # MB
        except:
            return 0.0

    def analyze_result_quality(self, result: Dict) -> Dict:
        """Analyze the quality of extraction results."""
        if result["status"] != "finished":
            return {'data_quality': 0, 'posts_count': 0, 'enhancement_rate': 0}

        res = result["result"]
        data = res.get("data", [])

        if not data or not data[0].get('posts'):
            return {'data_quality': 0, 'posts_count': 0, 'enhancement_rate': 0}

        posts = data[0]['posts']
        posts_count = len(posts)

        # Calculate data quality metrics
        complete_posts = sum(1 for p in posts if p.get('text') and p.get('author') and p.get('url'))
        enhanced_posts = sum(1 for p in posts if any([
            p.get('media'),
            p.get('likes') is not None,
            p.get('thread_info', {}).get('is_thread'),
            p.get('classification')
        ]))

        data_quality = (complete_posts / max(posts_count, 1)) * 100
        enhancement_rate = (enhanced_posts / max(posts_count, 1)) * 100

        return {
            'data_quality': data_quality,
            'posts_count': posts_count,
            'enhancement_rate': enhancement_rate,
            'complete_posts': complete_posts,
            'enhanced_posts': enhanced_posts
        }

    def benchmark_small_scale(self) -> Dict:
        """Benchmark small scale extraction (5 posts)."""
        payload = {
            "username": "naval",
            "scrape_posts": True,
            "max_posts": 5,
            "scrape_level": 4
        }

        measurement = self.submit_job_and_measure(payload, 60)
        quality = self.analyze_result_quality(measurement['result'])

        # Scoring
        score = 0
        if measurement['total_time'] <= 30:
            score += 40
        elif measurement['total_time'] <= 60:
            score += 20

        if quality['data_quality'] >= 80:
            score += 30
        elif quality['data_quality'] >= 60:
            score += 15

        if quality['enhancement_rate'] >= 60:
            score += 30
        elif quality['enhancement_rate'] >= 30:
            score += 15

        return {
            'status': 'PASS' if score >= 60 else 'FAIL',
            'score': score,
            'execution_time': measurement['total_time'],
            'data_quality': quality['data_quality'],
            'posts_extracted': quality['posts_count'],
            'enhancement_rate': quality['enhancement_rate']
        }

    def benchmark_medium_scale(self) -> Dict:
        """Benchmark medium scale extraction (15 posts)."""
        payload = {
            "username": "naval",
            "scrape_posts": True,
            "max_posts": 15,
            "scrape_level": 4
        }

        measurement = self.submit_job_and_measure(payload, 120)
        quality = self.analyze_result_quality(measurement['result'])

        # Scoring
        score = 0
        if measurement['total_time'] <= 90:
            score += 35
        elif measurement['total_time'] <= 120:
            score += 20

        if quality['posts_count'] >= 10:
            score += 25
        elif quality['posts_count'] >= 7:
            score += 15

        if quality['data_quality'] >= 75:
            score += 25
        elif quality['data_quality'] >= 60:
            score += 15

        if quality['enhancement_rate'] >= 50:
            score += 15

        return {
            'status': 'PASS' if score >= 60 else 'FAIL',
            'score': score,
            'execution_time': measurement['total_time'],
            'data_quality': quality['data_quality'],
            'posts_extracted': quality['posts_count'],
            'enhancement_rate': quality['enhancement_rate']
        }

    def benchmark_large_scale(self) -> Dict:
        """Benchmark large scale extraction (30 posts)."""
        payload = {
            "username": "naval",
            "scrape_posts": True,
            "max_posts": 30,
            "scrape_level": 4
        }

        measurement = self.submit_job_and_measure(payload, 180)
        quality = self.analyze_result_quality(measurement['result'])

        # Scoring
        score = 0
        if measurement['total_time'] <= 150:
            score += 30
        elif measurement['total_time'] <= 180:
            score += 15

        if quality['posts_count'] >= 20:
            score += 30
        elif quality['posts_count'] >= 15:
            score += 20
        elif quality['posts_count'] >= 10:
            score += 10

        if quality['data_quality'] >= 70:
            score += 25
        elif quality['data_quality'] >= 50:
            score += 15

        if quality['enhancement_rate'] >= 40:
            score += 15

        return {
            'status': 'PASS' if score >= 50 else 'FAIL',
            'score': score,
            'execution_time': measurement['total_time'],
            'data_quality': quality['data_quality'],
            'posts_extracted': quality['posts_count'],
            'enhancement_rate': quality['enhancement_rate']
        }

    def benchmark_multi_user(self) -> Dict:
        """Test performance across multiple users."""
        users = ["naval", "sama", "elonmusk"]
        user_results = []
        total_time = 0

        for username in users:
            try:
                payload = {
                    "username": username,
                    "scrape_posts": True,
                    "max_posts": 5,
                    "scrape_level": 4
                }

                measurement = self.submit_job_and_measure(payload, 90)
                quality = self.analyze_result_quality(measurement['result'])

                user_results.append({
                    'username': username,
                    'success': measurement['result']['status'] == 'finished',
                    'time': measurement['total_time'],
                    'posts': quality['posts_count'],
                    'quality': quality['data_quality']
                })
                total_time += measurement['total_time']

            except Exception as e:
                user_results.append({
                    'username': username,
                    'success': False,
                    'error': str(e),
                    'time': 0,
                    'posts': 0,
                    'quality': 0
                })

        successful_users = sum(1 for r in user_results if r.get('success'))
        avg_quality = sum(r.get('quality', 0) for r in user_results) / max(len(user_results), 1)

        # Scoring
        score = 0
        if successful_users == len(users):
            score += 50
        elif successful_users >= len(users) * 0.7:
            score += 30

        if avg_quality >= 70:
            score += 30
        elif avg_quality >= 50:
            score += 20

        if total_time <= 150:
            score += 20
        elif total_time <= 200:
            score += 10

        return {
            'status': 'PASS' if score >= 60 else 'FAIL',
            'score': score,
            'execution_time': total_time,
            'data_quality': avg_quality,
            'successful_users': f"{successful_users}/{len(users)}",
            'user_results': user_results
        }

    def benchmark_concurrent_requests(self) -> Dict:
        """Test performance with concurrent requests."""
        def submit_concurrent_job(job_id):
            payload = {
                "username": "naval",
                "scrape_posts": True,
                "max_posts": 3,
                "scrape_level": 4
            }

            try:
                measurement = self.submit_job_and_measure(payload, 90)
                quality = self.analyze_result_quality(measurement['result'])
                return {
                    'job_id': job_id,
                    'success': measurement['result']['status'] == 'finished',
                    'time': measurement['total_time'],
                    'quality': quality['data_quality'],
                    'posts': quality['posts_count']
                }
            except Exception as e:
                return {
                    'job_id': job_id,
                    'success': False,
                    'error': str(e),
                    'time': 0,
                    'quality': 0,
                    'posts': 0
                }

        start_time = time.time()

        # Submit 3 concurrent jobs
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(submit_concurrent_job, i) for i in range(3)]
            results = [future.result() for future in as_completed(futures)]

        total_time = time.time() - start_time

        successful_jobs = sum(1 for r in results if r.get('success'))
        avg_quality = sum(r.get('quality', 0) for r in results) / max(len(results), 1)

        # Scoring
        score = 0
        if successful_jobs == len(results):
            score += 40
        elif successful_jobs >= len(results) * 0.7:
            score += 25

        if total_time <= 120:
            score += 30
        elif total_time <= 180:
            score += 20

        if avg_quality >= 70:
            score += 30
        elif avg_quality >= 50:
            score += 20

        return {
            'status': 'PASS' if score >= 60 else 'FAIL',
            'score': score,
            'execution_time': total_time,
            'data_quality': avg_quality,
            'successful_jobs': f"{successful_jobs}/{len(results)}",
            'concurrent_results': results
        }

    def benchmark_export_formats(self) -> Dict:
        """Test performance with multiple export formats."""
        payload = {
            "username": "naval",
            "scrape_posts": True,
            "max_posts": 5,
            "scrape_level": 4,
            "export_formats": ["json", "csv", "xml", "markdown"]
        }

        measurement = self.submit_job_and_measure(payload, 90)
        quality = self.analyze_result_quality(measurement['result'])

        # Check export metadata
        res = measurement['result'].get('result', {})
        export_metadata = res.get('export_metadata')

        # Scoring
        score = 0
        if measurement['total_time'] <= 60:
            score += 30
        elif measurement['total_time'] <= 90:
            score += 20

        if export_metadata:
            score += 40

        if quality['data_quality'] >= 75:
            score += 30
        elif quality['data_quality'] >= 60:
            score += 20

        return {
            'status': 'PASS' if score >= 60 else 'FAIL',
            'score': score,
            'execution_time': measurement['total_time'],
            'data_quality': quality['data_quality'],
            'export_metadata': bool(export_metadata),
            'posts_extracted': quality['posts_count']
        }

    def generate_benchmark_report(self):
        """Generate comprehensive benchmark report."""
        print("\n" + "=" * 50)
        print("⚡ PERFORMANCE BENCHMARK RESULTS")
        print("=" * 50)

        total_benchmarks = len(self.benchmark_results)
        passed_benchmarks = sum(1 for r in self.benchmark_results.values() if r.get('status') == 'PASS')
        avg_score = sum(r.get('score', 0) for r in self.benchmark_results.values()) / max(total_benchmarks, 1)

        print(f"📊 OVERALL PERFORMANCE:")
        print(f"   Benchmarks Passed: {passed_benchmarks}/{total_benchmarks}")
        print(f"   Average Score: {avg_score:.1f}/100")
        print(f"   Success Rate: {passed_benchmarks/total_benchmarks*100:.1f}%")
        print()

        print("📝 DETAILED BENCHMARK RESULTS:")
        for name, result in self.benchmark_results.items():
            status_icon = "✅" if result.get('status') == 'PASS' else "❌"
            score = result.get('score', 0)
            exec_time = result.get('execution_time', 0)
            quality = result.get('data_quality', 0)

            print(f"   {status_icon} {name}: {score}/100")
            print(f"      Time: {exec_time:.1f}s, Quality: {quality:.1f}%")

            if 'posts_extracted' in result:
                print(f"      Posts: {result['posts_extracted']}")
            if 'enhancement_rate' in result:
                print(f"      Enhanced: {result['enhancement_rate']:.1f}%")
            if 'successful_users' in result:
                print(f"      Users: {result['successful_users']}")
            if 'successful_jobs' in result:
                print(f"      Concurrent Jobs: {result['successful_jobs']}")
            print()

        # Performance assessment
        if avg_score >= 85:
            print("🏆 OUTSTANDING PERFORMANCE: Production-ready at scale!")
        elif avg_score >= 75:
            print("✅ EXCELLENT PERFORMANCE: Very good scalability!")
        elif avg_score >= 65:
            print("⚠️ GOOD PERFORMANCE: Acceptable with minor optimizations needed")
        else:
            print("❌ PERFORMANCE ISSUES: Significant optimization required")

        print("=" * 50)

if __name__ == "__main__":
    benchmark = TwitterPerformanceBenchmark()
    benchmark.run_benchmarks()
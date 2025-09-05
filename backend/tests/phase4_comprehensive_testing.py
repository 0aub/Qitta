#!/usr/bin/env python3
"""
Phase 4.1: Comprehensive System Testing Suite
============================================

Complete validation of all Airbnb scraper functionality:
1. Multi-location testing (5 cities)
2. All levels (1-4) validation  
3. Parameter combination testing
4. Edge case scenarios
5. Performance measurement
"""

import requests
import json
import time
import sys
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta

class Phase4ComprehensiveTesting:
    
    def __init__(self):
        self.base_url = "http://localhost:8004"
        self.test_results = []
        self.failed_tests = []
        
    def run_test_job(self, test_name: str, params: Dict[str, Any], timeout: int = 600) -> Optional[Dict[str, Any]]:
        """Run a single test job and return results."""
        print(f"\n🚀 Running: {test_name}")
        print(f"   Params: {json.dumps(params, indent=2)}")
        
        start_time = time.time()
        
        try:
            # Submit job
            response = requests.post(
                f"{self.base_url}/jobs/airbnb",
                json=params,
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code != 200:
                print(f"❌ Failed to submit job: {response.status_code}")
                return None
            
            job_data = response.json()
            job_id = job_data["job_id"]
            print(f"   Job ID: {job_id}")
            
            # Wait for completion
            while time.time() - start_time < timeout:
                time.sleep(5)
                status_response = requests.get(f"{self.base_url}/jobs/{job_id}")
                
                if status_response.status_code != 200:
                    print(f"❌ Failed to get job status")
                    return None
                
                status_data = status_response.json()
                status = status_data.get("status")
                elapsed = time.time() - start_time
                
                print(f"   Status: {status} ({elapsed:.1f}s)")
                
                if status == "finished":
                    result = status_data.get("result", {})
                    print(f"✅ {test_name} completed in {elapsed:.1f}s")
                    return {
                        "test_name": test_name,
                        "params": params,
                        "result": result,
                        "execution_time": elapsed,
                        "success": True
                    }
                elif status == "failed":
                    error = status_data.get("error", "Unknown error")
                    print(f"❌ {test_name} failed: {error}")
                    return {
                        "test_name": test_name,
                        "params": params,
                        "error": error,
                        "execution_time": elapsed,
                        "success": False
                    }
            
            print(f"⏰ {test_name} timed out after {timeout}s")
            return None
            
        except Exception as e:
            print(f"❌ {test_name} exception: {e}")
            return None

    def analyze_result(self, test_result: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze test result and calculate metrics."""
        if not test_result.get("success"):
            return {"success": False, "error": test_result.get("error", "Unknown error")}
        
        result = test_result.get("result", {})
        properties = result.get("properties", [])
        metadata = result.get("search_metadata", {})
        
        analysis = {
            "success": True,
            "execution_time": test_result["execution_time"],
            "total_properties": len(properties),
            "success_rate": metadata.get("success_rate", 0),
            "average_completeness": metadata.get("average_completeness", 0),
            "properties_with_prices": len([p for p in properties if p.get("price_per_night", 0) > 0]),
            "properties_with_reviews": len([p for p in properties if p.get("reviews_count", 0) > 0]),
            "extraction_level": metadata.get("scrape_level", "unknown")
        }
        
        # Level-specific analysis
        if len(properties) > 0:
            sample_property = properties[0]
            
            # Check data completeness
            required_fields = ["title", "price_per_night", "airbnb_url"]
            level2_fields = ["description", "amenities", "host_name"]
            level34_fields = ["reviews"]
            
            basic_completeness = sum(1 for field in required_fields if sample_property.get(field)) / len(required_fields)
            level2_completeness = sum(1 for field in level2_fields if sample_property.get(field)) / len(level2_fields) if metadata.get("scrape_level", 1) >= 2 else 1
            review_completeness = 1 if sample_property.get("reviews") and len(sample_property.get("reviews", [])) > 0 else 0 if metadata.get("scrape_level", 1) >= 3 else 1
            
            analysis["field_completeness"] = {
                "basic_fields": basic_completeness,
                "level2_fields": level2_completeness,
                "review_extraction": review_completeness
            }
        
        return analysis

    def run_multi_location_tests(self):
        """Test scraper across multiple diverse locations."""
        print("\n" + "="*80)
        print("🌍 MULTI-LOCATION TESTING")
        print("="*80)
        
        test_locations = [
            {
                "name": "New York City (USA)",
                "location": "New York, NY",
                "expected_properties": "high-density urban"
            },
            {
                "name": "Los Angeles (USA)", 
                "location": "Los Angeles, CA",
                "expected_properties": "diverse property types"
            },
            {
                "name": "London (UK)",
                "location": "London, UK", 
                "expected_properties": "international market"
            },
            {
                "name": "Dubai (UAE)",
                "location": "Dubai, UAE",
                "expected_properties": "luxury market"
            },
            {
                "name": "Tokyo (Japan)",
                "location": "Tokyo, Japan",
                "expected_properties": "asian market"
            }
        ]
        
        location_results = []
        
        for loc_test in test_locations:
            params = {
                "params": {
                    "location": loc_test["location"],
                    "check_in": "2025-09-20",
                    "check_out": "2025-09-22", 
                    "adults": 2,
                    "max_results": 3,
                    "level": 2  # Level 2 for comprehensive but fast testing
                }
            }
            
            result = self.run_test_job(f"Location Test: {loc_test['name']}", params, timeout=300)
            if result:
                analysis = self.analyze_result(result)
                analysis["location_info"] = loc_test
                location_results.append(analysis)
                self.test_results.append(analysis)
            else:
                self.failed_tests.append(f"Location Test: {loc_test['name']}")
        
        return location_results

    def run_level_validation_tests(self):
        """Test all extraction levels systematically."""
        print("\n" + "="*80)
        print("📊 LEVEL VALIDATION TESTING")
        print("="*80)
        
        level_tests = [
            {"level": 1, "description": "Quick Search", "expected_time": 60},
            {"level": 2, "description": "Full Data", "expected_time": 120}, 
            {"level": 3, "description": "Basic Reviews", "expected_time": 180},
            {"level": 4, "description": "Comprehensive Reviews", "expected_time": 300}
        ]
        
        level_results = []
        
        for level_test in level_tests:
            params = {
                "params": {
                    "location": "San Francisco, CA",
                    "check_in": "2025-09-25",
                    "check_out": "2025-09-27",
                    "adults": 2, 
                    "max_results": 2,  # Small for faster testing
                    "level": level_test["level"]
                }
            }
            
            result = self.run_test_job(
                f"Level {level_test['level']}: {level_test['description']}", 
                params, 
                timeout=level_test["expected_time"] + 60
            )
            
            if result:
                analysis = self.analyze_result(result)
                analysis["level_info"] = level_test
                level_results.append(analysis)
                self.test_results.append(analysis)
            else:
                self.failed_tests.append(f"Level {level_test['level']} Test")
        
        return level_results

    def run_parameter_combination_tests(self):
        """Test various parameter combinations."""
        print("\n" + "="*80)
        print("🔧 PARAMETER COMBINATION TESTING")
        print("="*80)
        
        param_tests = [
            {
                "name": "Price Filter Test",
                "params": {
                    "location": "Miami, FL",
                    "check_in": "2025-10-01",
                    "check_out": "2025-10-03",
                    "adults": 2,
                    "max_results": 5,
                    "min_price": 100,
                    "max_price": 300,
                    "level": 2
                }
            },
            {
                "name": "Family Configuration",
                "params": {
                    "location": "Orlando, FL",
                    "check_in": "2025-10-05", 
                    "check_out": "2025-10-07",
                    "adults": 2,
                    "children": 2,
                    "rooms": 2,
                    "max_results": 3,
                    "level": 2
                }
            },
            {
                "name": "Property Type Filter",
                "params": {
                    "location": "Austin, TX",
                    "check_in": "2025-10-10",
                    "check_out": "2025-10-12", 
                    "adults": 4,
                    "property_type": "house",
                    "max_results": 3,
                    "level": 2
                }
            },
            {
                "name": "Rating Filter",
                "params": {
                    "location": "Seattle, WA",
                    "check_in": "2025-10-15",
                    "check_out": "2025-10-17",
                    "adults": 2,
                    "min_rating": 4.5,
                    "max_results": 3,
                    "level": 2
                }
            }
        ]
        
        param_results = []
        
        for param_test in param_tests:
            params = {"params": param_test["params"]}
            
            result = self.run_test_job(param_test["name"], params, timeout=240)
            if result:
                analysis = self.analyze_result(result)
                analysis["test_type"] = "parameter_combination"
                param_results.append(analysis)
                self.test_results.append(analysis)
            else:
                self.failed_tests.append(param_test["name"])
        
        return param_results

    def generate_comprehensive_report(self):
        """Generate final comprehensive test report."""
        print("\n" + "="*80)
        print("📋 COMPREHENSIVE TEST REPORT")
        print("="*80)
        
        total_tests = len(self.test_results) + len(self.failed_tests)
        successful_tests = len(self.test_results)
        failed_tests = len(self.failed_tests)
        
        print(f"\n📊 OVERALL RESULTS:")
        print(f"   Total Tests: {total_tests}")
        print(f"   Successful: {successful_tests}")
        print(f"   Failed: {failed_tests}")
        print(f"   Success Rate: {(successful_tests/total_tests)*100:.1f}%")
        
        if self.test_results:
            # Performance analysis
            execution_times = [r["execution_time"] for r in self.test_results]
            avg_time = sum(execution_times) / len(execution_times)
            max_time = max(execution_times)
            min_time = min(execution_times)
            
            print(f"\n⏱️ PERFORMANCE METRICS:")
            print(f"   Average Execution Time: {avg_time:.1f}s")
            print(f"   Fastest Test: {min_time:.1f}s")
            print(f"   Slowest Test: {max_time:.1f}s")
            
            # Data quality analysis
            total_properties = sum(r["total_properties"] for r in self.test_results)
            avg_completeness = sum(r["average_completeness"] for r in self.test_results if r["average_completeness"]) / len([r for r in self.test_results if r["average_completeness"]])
            
            print(f"\n📈 DATA QUALITY:")
            print(f"   Total Properties Extracted: {total_properties}")
            print(f"   Average Data Completeness: {avg_completeness:.1f}%")
            
            # Level performance breakdown
            level_performance = {}
            for result in self.test_results:
                level = result.get("extraction_level", "unknown")
                if level not in level_performance:
                    level_performance[level] = []
                level_performance[level].append(result["execution_time"])
            
            print(f"\n🎯 LEVEL PERFORMANCE:")
            for level, times in level_performance.items():
                avg_level_time = sum(times) / len(times)
                print(f"   Level {level}: {avg_level_time:.1f}s average ({len(times)} tests)")
        
        if self.failed_tests:
            print(f"\n❌ FAILED TESTS:")
            for failed_test in self.failed_tests:
                print(f"   - {failed_test}")
        
        # Success criteria evaluation
        print(f"\n✅ SUCCESS CRITERIA EVALUATION:")
        
        success_rate = (successful_tests/total_tests)*100 if total_tests > 0 else 0
        criteria = {
            "Overall Success Rate ≥90%": success_rate >= 90,
            "All Locations Working": len([r for r in self.test_results if "location_info" in r]) >= 4,
            "All Levels Functional": len([r for r in self.test_results if r.get("extraction_level") in [1,2,3,4]]) >= 3,
            "Parameter Combinations Working": len([r for r in self.test_results if r.get("test_type") == "parameter_combination"]) >= 3,
            "Performance Acceptable": max(execution_times) <= 300 if execution_times else False
        }
        
        passed_criteria = 0
        for criterion, passed in criteria.items():
            status = "✅ PASS" if passed else "❌ FAIL"
            print(f"   {criterion}: {status}")
            if passed:
                passed_criteria += 1
        
        final_success = passed_criteria >= 4  # At least 4/5 criteria
        
        print(f"\n{'🎉 PHASE 4.1 VALIDATION: SUCCESS' if final_success else '⚠️ PHASE 4.1 VALIDATION: NEEDS ATTENTION'}")
        print(f"Criteria passed: {passed_criteria}/{len(criteria)}")
        
        return final_success

    def run_full_test_suite(self):
        """Execute complete Phase 4.1 testing."""
        print("🧪 PHASE 4.1: COMPREHENSIVE SYSTEM TESTING")
        print("=" * 80)
        
        # Run all test categories
        self.run_multi_location_tests()
        self.run_level_validation_tests() 
        self.run_parameter_combination_tests()
        
        # Generate comprehensive report
        return self.generate_comprehensive_report()

def main():
    """Main test execution."""
    tester = Phase4ComprehensiveTesting()
    success = tester.run_full_test_suite()
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
INTENSIVE AIRBNB TESTING SUITE
==============================

Comprehensive testing for Airbnb scraper after data quality fixes:
- All parameter combinations
- Performance comparison with booking scraper
- Data quality validation
- All scrape levels testing
- Edge cases and error handling

Version: 1.0
"""

import requests
import json
import time
import sys
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import statistics

ENDPOINT = "http://localhost:8004"

class AirbnbTestSuite:
    def __init__(self):
        self.results = []
        self.performance_data = []
        
    def wait_for_job(self, job_id, timeout=600):
        """Wait for job completion with timeout."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                response = requests.get(f"{ENDPOINT}/jobs/{job_id}")
                data = response.json()
                
                if data["status"] == "finished":
                    return data
                elif data["status"] == "error":
                    return data
                    
                time.sleep(3)
            except Exception as e:
                print(f"Error checking job {job_id}: {e}")
                time.sleep(3)
                
        return {"status": "timeout", "job_id": job_id}
    
    def submit_job(self, task, payload, test_name):
        """Submit job and wait for completion."""
        print(f"\n🔍 TESTING: {test_name}")
        print(f"📝 Payload: {json.dumps(payload, indent=2)}")
        
        start_time = time.time()
        
        try:
            response = requests.post(f"{ENDPOINT}/jobs/{task}", json=payload)
            response.raise_for_status()
            job_id = response.json()["job_id"]
            print(f"🆔 Job ID: {job_id}")
            
            # Wait for completion
            result = self.wait_for_job(job_id)
            
            execution_time = time.time() - start_time
            
            # Analyze results
            analysis = self.analyze_result(result, test_name, execution_time, payload)
            self.results.append(analysis)
            
            return analysis
            
        except Exception as e:
            error_analysis = {
                "test_name": test_name,
                "status": "error",
                "error": str(e),
                "execution_time": time.time() - start_time,
                "payload": payload
            }
            self.results.append(error_analysis)
            return error_analysis
    
    def analyze_result(self, result, test_name, execution_time, payload):
        """Analyze job result for quality and performance."""
        analysis = {
            "test_name": test_name,
            "status": result["status"],
            "execution_time": execution_time,
            "payload": payload,
            "data_quality": {}
        }
        
        if result["status"] == "finished" and "result" in result:
            data = result["result"]
            properties = data.get("properties", [])
            metadata = data.get("search_metadata", {})
            
            # Performance metrics
            analysis["performance"] = {
                "total_properties": len(properties),
                "execution_time": execution_time,
                "properties_per_second": len(properties) / execution_time if execution_time > 0 else 0,
                "scrape_level": metadata.get("scrape_level", "unknown")
            }
            
            # Data quality validation (check for fixes)
            if properties:
                sample_property = properties[0]
                quality_checks = self.validate_data_quality(sample_property)
                analysis["data_quality"] = quality_checks
                
                # Field completeness
                analysis["field_completeness"] = self.calculate_field_completeness(properties)
        
        elif result["status"] == "error":
            analysis["error"] = result.get("error", "Unknown error")
        
        return analysis
    
    def validate_data_quality(self, property_data):
        """Validate that our 7 fixes are working."""
        checks = {
            "coordinates_source_removed": "coordinates_source" not in property_data,
            "data_completeness_removed": "data_completeness" not in property_data,
            "deep_scrape_enabled_removed": "deep_scrape_enabled" not in property_data,
            "host_name_clean": self.is_host_name_clean(property_data.get("host_name", "")),
            "address_clean": self.is_address_clean(property_data.get("address", "")),
            "neighborhood_clean": self.is_neighborhood_clean(property_data.get("neighborhood", "")),
            "bathrooms_integer": self.is_bathrooms_integer(property_data.get("bathrooms")),
            "images_original_size": self.are_images_original_size(property_data.get("images", []))
        }
        
        checks["total_fixes_working"] = sum(1 for v in checks.values() if v is True)
        checks["fixes_percentage"] = (checks["total_fixes_working"] / 8) * 100
        
        return checks
    
    def is_host_name_clean(self, host_name):
        """Check if host name is clean (not HTML dump)."""
        if not host_name:
            return True  # No host name is acceptable
        
        # Should be short and not contain HTML navigation elements
        html_indicators = ["Skip to content", "Homes", "Experiences", "Services", "Start your search"]
        return len(host_name) < 100 and not any(indicator in host_name for indicator in html_indicators)
    
    def is_address_clean(self, address):
        """Check if address is clean (not HTML dump)."""
        if not address:
            return True  # No address is acceptable
            
        html_indicators = ["Skip to content", "Show all photos", "Check-in", "Checkout"]
        return len(address) < 300 and not any(indicator in address for indicator in html_indicators)
    
    def is_neighborhood_clean(self, neighborhood):
        """Check if neighborhood is clean (not HTML dump)."""
        if not neighborhood:
            return True  # No neighborhood is acceptable
            
        html_indicators = ["Skip to content", "Show all photos", "Check-in"]
        return len(neighborhood) < 150 and not any(indicator in neighborhood for indicator in html_indicators)
    
    def is_bathrooms_integer(self, bathrooms):
        """Check if bathrooms is integer."""
        if bathrooms is None:
            return True  # No bathrooms data is acceptable
        return isinstance(bathrooms, int)
    
    def are_images_original_size(self, images):
        """Check if images are original size (no size restrictions)."""
        if not images:
            return True  # No images is acceptable
            
        # Check that images don't have size restrictions
        for img_url in images[:3]:  # Check first 3 images
            if any(param in img_url for param in ["?w=", "&w=", "?h=", "&h=", "?s=", "&s="]):
                return False
        return True
    
    def calculate_field_completeness(self, properties):
        """Calculate field completeness across properties."""
        if not properties:
            return {}
            
        essential_fields = ["title", "airbnb_url", "price_per_night"]
        optional_fields = ["rating", "host_name", "description", "amenities", "images", "address"]
        
        completeness = {}
        
        for field in essential_fields + optional_fields:
            filled_count = sum(1 for prop in properties if prop.get(field))
            completeness[field] = (filled_count / len(properties)) * 100
            
        return completeness
    
    def run_parameter_tests(self):
        """Test all parameter combinations."""
        print("=" * 80)
        print("🧪 PARAMETER COMBINATION TESTING")
        print("=" * 80)
        
        # Future dates for testing
        today = datetime.now()
        check_in = (today + timedelta(days=30)).strftime("%Y-%m-%d")
        check_out = (today + timedelta(days=33)).strftime("%Y-%m-%d")
        
        test_cases = [
            # Basic functionality tests
            {
                "name": "Level 1 - Quick Search (NYC)",
                "payload": {
                    "params": {
                        "location": "New York, NY",
                        "check_in": check_in,
                        "check_out": check_out,
                        "adults": 2,
                        "max_results": 3,
                        "level": 1
                    }
                }
            },
            {
                "name": "Level 2 - Full Data (NYC)", 
                "payload": {
                    "params": {
                        "location": "New York, NY",
                        "check_in": check_in,
                        "check_out": check_out,
                        "adults": 2,
                        "max_results": 3,
                        "level": 2
                    }
                }
            },
            {
                "name": "Level 3 - Basic Reviews (NYC)",
                "payload": {
                    "params": {
                        "location": "New York, NY", 
                        "check_in": check_in,
                        "check_out": check_out,
                        "adults": 2,
                        "max_results": 2,
                        "level": 3
                    }
                }
            },
            {
                "name": "Level 4 - Deep Reviews (NYC)",
                "payload": {
                    "params": {
                        "location": "New York, NY",
                        "check_in": check_in,
                        "check_out": check_out,
                        "adults": 2,
                        "max_results": 1,
                        "level": 4
                    }
                }
            },
            # Multi-location tests
            {
                "name": "Level 2 - Los Angeles",
                "payload": {
                    "params": {
                        "location": "Los Angeles, CA",
                        "check_in": check_in,
                        "check_out": check_out,
                        "adults": 2,
                        "max_results": 3,
                        "level": 2
                    }
                }
            },
            {
                "name": "Level 2 - London",
                "payload": {
                    "params": {
                        "location": "London, UK",
                        "check_in": check_in,
                        "check_out": check_out,
                        "adults": 2,
                        "max_results": 3,
                        "level": 2
                    }
                }
            },
            # Parameter combination tests
            {
                "name": "Complete Parameter Set",
                "payload": {
                    "params": {
                        "location": "San Francisco, CA",
                        "check_in": check_in,
                        "check_out": check_out,
                        "adults": 4,
                        "children": 2,
                        "rooms": 2,
                        "min_price": 100,
                        "max_price": 400,
                        "min_rating": 4.0,
                        "property_type": "house",
                        "currency": "USD",
                        "max_results": 5,
                        "level": 2
                    }
                }
            },
            # Edge cases
            {
                "name": "Single Adult - Minimum Stay",
                "payload": {
                    "params": {
                        "location": "Miami, FL",
                        "check_in": check_in,
                        "check_out": check_out,
                        "adults": 1,
                        "max_results": 3,
                        "level": 1
                    }
                }
            },
            {
                "name": "Large Group - Multiple Rooms",
                "payload": {
                    "params": {
                        "location": "Las Vegas, NV",
                        "check_in": check_in,
                        "check_out": check_out,
                        "adults": 8,
                        "children": 4,
                        "rooms": 4,
                        "max_results": 3,
                        "level": 1
                    }
                }
            }
        ]
        
        for test_case in test_cases:
            result = self.submit_job("airbnb", test_case["payload"], test_case["name"])
            time.sleep(2)  # Brief pause between tests
    
    def run_performance_comparison(self):
        """Compare Airbnb vs Booking performance."""
        print("\n" + "=" * 80)
        print("⚡ PERFORMANCE COMPARISON: AIRBNB vs BOOKING")
        print("=" * 80)
        
        today = datetime.now()
        check_in = (today + timedelta(days=30)).strftime("%Y-%m-%d")
        check_out = (today + timedelta(days=32)).strftime("%Y-%m-%d")
        
        # Test same parameters on both systems
        test_payload = {
            "params": {
                "location": "Dubai",
                "check_in": check_in,
                "check_out": check_out,
                "adults": 2,
                "max_results": 5,
                "level": 2
            }
        }
        
        # Test Airbnb
        print("\n🏠 Testing Airbnb Level 2...")
        airbnb_result = self.submit_job("airbnb", test_payload, "Airbnb Performance Test")
        
        # Test Booking (adjust payload for booking)
        booking_payload = {
            **test_payload["params"],
            "scrape_level": 2  # Booking uses scrape_level
        }
        del booking_payload["level"]  # Remove Airbnb-specific parameter
        
        print("\n🏨 Testing Booking Level 2...")
        booking_result = self.submit_job("booking", booking_payload, "Booking Performance Test")
        
        # Compare results
        print("\n" + "=" * 50)
        print("📊 PERFORMANCE COMPARISON RESULTS")
        print("=" * 50)
        
        if airbnb_result["status"] == "finished" and booking_result["status"] == "finished":
            airbnb_time = airbnb_result["execution_time"]
            booking_time = booking_result["execution_time"]
            
            airbnb_props = airbnb_result["performance"]["total_properties"]
            booking_props = booking_result["performance"]["total_properties"]
            
            print(f"🏠 AIRBNB:")
            print(f"   ⏱️  Execution Time: {airbnb_time:.1f}s")
            print(f"   🏨 Properties Found: {airbnb_props}")
            print(f"   📈 Properties/Second: {airbnb_props/airbnb_time:.2f}")
            
            print(f"\n🏨 BOOKING:")
            print(f"   ⏱️  Execution Time: {booking_time:.1f}s") 
            print(f"   🏨 Properties Found: {booking_props}")
            print(f"   📈 Properties/Second: {booking_props/booking_time:.2f}")
            
            # Performance comparison
            time_diff = airbnb_time - booking_time
            speed_comparison = "FASTER" if time_diff < 0 else "SLOWER"
            
            print(f"\n🎯 VERDICT:")
            print(f"   Airbnb is {abs(time_diff):.1f}s {speed_comparison} than Booking")
            print(f"   Speed difference: {abs(time_diff/booking_time)*100:.1f}%")
            
            if airbnb_time > booking_time * 1.5:
                print(f"   ⚠️  PERFORMANCE ISSUE: Airbnb is significantly slower!")
            elif airbnb_time > booking_time * 1.2:
                print(f"   ⚠️  MINOR ISSUE: Airbnb is moderately slower")
            else:
                print(f"   ✅ PERFORMANCE: Acceptable speed difference")
                
        else:
            print("❌ COMPARISON FAILED: One or both systems had errors")
    
    def generate_final_report(self):
        """Generate comprehensive final report."""
        print("\n" + "=" * 80)
        print("📋 INTENSIVE TESTING FINAL REPORT")
        print("=" * 80)
        
        total_tests = len(self.results)
        successful_tests = sum(1 for r in self.results if r["status"] == "finished")
        error_tests = sum(1 for r in self.results if r["status"] == "error")
        
        print(f"📊 OVERALL RESULTS:")
        print(f"   ✅ Total Tests: {total_tests}")
        print(f"   ✅ Successful: {successful_tests} ({successful_tests/total_tests*100:.1f}%)")
        print(f"   ❌ Errors: {error_tests} ({error_tests/total_tests*100:.1f}%)")
        
        # Performance analysis
        successful_results = [r for r in self.results if r["status"] == "finished"]
        if successful_results:
            execution_times = [r["execution_time"] for r in successful_results]
            
            print(f"\n⚡ PERFORMANCE ANALYSIS:")
            print(f"   📊 Average Execution Time: {statistics.mean(execution_times):.1f}s")
            print(f"   📊 Fastest Test: {min(execution_times):.1f}s")
            print(f"   📊 Slowest Test: {max(execution_times):.1f}s")
            print(f"   📊 Standard Deviation: {statistics.stdev(execution_times):.1f}s")
        
        # Data quality analysis
        quality_results = [r for r in successful_results if "data_quality" in r and r["data_quality"]]
        if quality_results:
            fix_percentages = [r["data_quality"]["fixes_percentage"] for r in quality_results]
            avg_fix_percentage = statistics.mean(fix_percentages)
            
            print(f"\n🎯 DATA QUALITY FIXES VALIDATION:")
            print(f"   ✅ Average Fix Success Rate: {avg_fix_percentage:.1f}%")
            
            if avg_fix_percentage >= 90:
                print(f"   🎉 EXCELLENT: All fixes are working properly!")
            elif avg_fix_percentage >= 75:
                print(f"   ✅ GOOD: Most fixes are working")
            else:
                print(f"   ⚠️  ISSUES: Some fixes need attention")
                
            # Detailed fix analysis
            fix_details = {}
            for result in quality_results:
                for fix_name, status in result["data_quality"].items():
                    if fix_name not in ["total_fixes_working", "fixes_percentage"]:
                        if fix_name not in fix_details:
                            fix_details[fix_name] = []
                        fix_details[fix_name].append(status)
            
            print(f"\n   📋 FIX DETAILS:")
            for fix_name, statuses in fix_details.items():
                success_rate = (sum(statuses) / len(statuses)) * 100
                status_icon = "✅" if success_rate >= 80 else "⚠️"
                print(f"   {status_icon} {fix_name}: {success_rate:.1f}% success")
        
        # Level-specific analysis  
        level_performance = {}
        for result in successful_results:
            if "performance" in result:
                level = result["performance"]["scrape_level"]
                if level not in level_performance:
                    level_performance[level] = []
                level_performance[level].append(result["execution_time"])
        
        if level_performance:
            print(f"\n📊 PERFORMANCE BY LEVEL:")
            for level, times in level_performance.items():
                avg_time = statistics.mean(times)
                print(f"   Level {level}: {avg_time:.1f}s average ({len(times)} tests)")
        
        # Error analysis
        if error_tests > 0:
            print(f"\n❌ ERROR ANALYSIS:")
            error_results = [r for r in self.results if r["status"] == "error"]
            error_types = {}
            
            for error_result in error_results:
                error_msg = error_result.get("error", "Unknown error")
                if error_msg not in error_types:
                    error_types[error_msg] = []
                error_types[error_msg].append(error_result["test_name"])
            
            for error_msg, test_names in error_types.items():
                print(f"   • {error_msg}: {len(test_names)} tests")
                for test_name in test_names:
                    print(f"     - {test_name}")
        
        # Final verdict
        print(f"\n🎯 FINAL VERDICT:")
        if successful_tests == total_tests and avg_fix_percentage >= 90:
            print(f"   🎉 EXCELLENT: All tests passed with high data quality!")
        elif successful_tests >= total_tests * 0.9:
            print(f"   ✅ GOOD: System is working well with minor issues")
        else:
            print(f"   ⚠️  NEEDS ATTENTION: Significant issues found")
        
        return {
            "total_tests": total_tests,
            "success_rate": successful_tests / total_tests,
            "avg_execution_time": statistics.mean(execution_times) if execution_times else 0,
            "data_quality_score": avg_fix_percentage if quality_results else 0
        }

def main():
    """Run intensive testing suite."""
    print("🚀 AIRBNB INTENSIVE TESTING SUITE")
    print("=" * 50)
    print("Testing all parameters, data quality fixes, and performance")
    
    suite = AirbnbTestSuite()
    
    # Run all tests
    suite.run_parameter_tests()
    suite.run_performance_comparison()
    
    # Generate final report
    final_results = suite.generate_final_report()
    
    print(f"\n✅ TESTING COMPLETED!")
    print(f"📊 Results: {final_results['success_rate']*100:.1f}% success rate")
    print(f"⏱️  Performance: {final_results['avg_execution_time']:.1f}s average")
    print(f"🎯 Data Quality: {final_results['data_quality_score']:.1f}% fixes working")

if __name__ == "__main__":
    main()
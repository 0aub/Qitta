#!/usr/bin/env python3
"""
INTENSIVE DATA INSPECTION - Comprehensive validation of all extracted values
This system will meticulously check every single data point we extract for accuracy and completeness.
"""

import os, time, requests, json, re
from datetime import datetime
from typing import Dict, List, Any, Optional
from urllib.parse import urlparse

EP = os.getenv("BROWSER_ENDPOINT", "http://localhost:8004")

class DataInspector:
    """Comprehensive data validation and inspection system"""
    
    def __init__(self):
        self.issues = []
        self.warnings = []
        self.successes = []
        self.detailed_findings = {}
        
    def log_issue(self, category: str, severity: str, message: str, data: Any = None):
        """Log a data quality issue with full context"""
        issue = {
            'category': category,
            'severity': severity,  # CRITICAL, HIGH, MEDIUM, LOW
            'message': message,
            'timestamp': datetime.now().isoformat(),
            'data': data
        }
        
        if severity in ['CRITICAL', 'HIGH']:
            self.issues.append(issue)
        elif severity == 'MEDIUM':
            self.warnings.append(issue)
        else:
            self.successes.append(issue)
            
        print(f"🔍 [{severity}] {category}: {message}")
        
    def inspect_price_data(self, hotel: Dict[str, Any], hotel_name: str) -> Dict[str, Any]:
        """Comprehensive price data inspection"""
        price_findings = {
            'price_per_night': hotel.get('price_per_night'),
            'issues': [],
            'warnings': [],
            'status': 'unknown'
        }
        
        price = hotel.get('price_per_night')
        
        # Check if price exists
        if price is None:
            self.log_issue('PRICE', 'HIGH', f"{hotel_name}: Price is None/missing", price)
            price_findings['issues'].append('Price missing')
            price_findings['status'] = 'missing'
        elif price == 0:
            self.log_issue('PRICE', 'MEDIUM', f"{hotel_name}: Price is 0 (may indicate unavailability)", price)
            price_findings['warnings'].append('Price is zero')
            price_findings['status'] = 'zero'
        elif isinstance(price, (int, float)) and price > 0:
            # Validate reasonable price range
            if price > 10000:
                self.log_issue('PRICE', 'MEDIUM', f"{hotel_name}: Extremely high price ${price}", price)
                price_findings['warnings'].append('Extremely high price')
            elif price < 10:
                self.log_issue('PRICE', 'MEDIUM', f"{hotel_name}: Extremely low price ${price}", price)
                price_findings['warnings'].append('Extremely low price')
            else:
                self.log_issue('PRICE', 'LOW', f"{hotel_name}: Valid price ${price}", price)
                price_findings['status'] = 'valid'
        else:
            self.log_issue('PRICE', 'HIGH', f"{hotel_name}: Invalid price format: {type(price)} {price}", price)
            price_findings['issues'].append('Invalid price format')
            price_findings['status'] = 'invalid'
            
        return price_findings
    
    def inspect_review_data(self, hotel: Dict[str, Any], hotel_name: str) -> Dict[str, Any]:
        """Comprehensive review data inspection"""
        reviews = hotel.get('reviews', [])
        review_count = hotel.get('review_count', 0)
        
        review_findings = {
            'reviews_extracted': len(reviews),
            'review_count_claimed': review_count,
            'sample_reviews': [],
            'issues': [],
            'warnings': [],
            'status': 'unknown'
        }
        
        # Check review count consistency
        if len(reviews) == 0:
            if review_count > 0:
                self.log_issue('REVIEWS', 'HIGH', f"{hotel_name}: Claims {review_count} reviews but extracted 0", {
                    'claimed': review_count, 'extracted': 0
                })
                review_findings['issues'].append('Review count mismatch - extracted none')
                review_findings['status'] = 'missing'
            else:
                self.log_issue('REVIEWS', 'LOW', f"{hotel_name}: No reviews available", None)
                review_findings['status'] = 'empty'
        else:
            # Inspect review quality
            valid_reviews = 0
            for i, review in enumerate(reviews[:5]):  # Check first 5 reviews
                review_text = review.get('review_text', '')
                reviewer_name = review.get('reviewer_name', '')
                
                if review_text and len(review_text.strip()) > 10:
                    valid_reviews += 1
                    review_findings['sample_reviews'].append({
                        'text_length': len(review_text),
                        'has_name': bool(reviewer_name),
                        'text_preview': review_text[:100] + '...' if len(review_text) > 100 else review_text
                    })
                else:
                    self.log_issue('REVIEWS', 'MEDIUM', f"{hotel_name}: Review {i+1} has insufficient content", review)
            
            if valid_reviews == 0:
                self.log_issue('REVIEWS', 'HIGH', f"{hotel_name}: No valid review content found", len(reviews))
                review_findings['issues'].append('No valid review content')
                review_findings['status'] = 'invalid'
            elif valid_reviews < len(reviews) * 0.5:
                self.log_issue('REVIEWS', 'MEDIUM', f"{hotel_name}: Low quality reviews - {valid_reviews}/{len(reviews)} valid", None)
                review_findings['warnings'].append('Low quality reviews')
                review_findings['status'] = 'low_quality'
            else:
                self.log_issue('REVIEWS', 'LOW', f"{hotel_name}: Good review quality - {valid_reviews}/{len(reviews)} valid", None)
                review_findings['status'] = 'valid'
                
        return review_findings
    
    def inspect_rating_data(self, hotel: Dict[str, Any], hotel_name: str) -> Dict[str, Any]:
        """Comprehensive rating data inspection"""
        rating = hotel.get('rating')
        
        rating_findings = {
            'rating_value': rating,
            'issues': [],
            'warnings': [],
            'status': 'unknown'
        }
        
        if rating is None:
            self.log_issue('RATING', 'MEDIUM', f"{hotel_name}: Rating is missing", rating)
            rating_findings['issues'].append('Rating missing')
            rating_findings['status'] = 'missing'
        elif not isinstance(rating, (int, float)):
            self.log_issue('RATING', 'HIGH', f"{hotel_name}: Invalid rating format: {type(rating)} {rating}", rating)
            rating_findings['issues'].append('Invalid rating format')
            rating_findings['status'] = 'invalid'
        elif rating < 0 or rating > 10:
            self.log_issue('RATING', 'HIGH', f"{hotel_name}: Rating out of range: {rating}", rating)
            rating_findings['issues'].append('Rating out of range')
            rating_findings['status'] = 'invalid'
        elif rating == 0:
            self.log_issue('RATING', 'MEDIUM', f"{hotel_name}: Zero rating (may be unrated)", rating)
            rating_findings['warnings'].append('Zero rating')
            rating_findings['status'] = 'zero'
        else:
            self.log_issue('RATING', 'LOW', f"{hotel_name}: Valid rating {rating}/10", rating)
            rating_findings['status'] = 'valid'
            
        return rating_findings
    
    def inspect_hotel_metadata(self, hotel: Dict[str, Any], hotel_name: str) -> Dict[str, Any]:
        """Comprehensive hotel metadata inspection"""
        metadata_findings = {
            'name': hotel.get('name'),
            'address': hotel.get('address'),
            'description': hotel.get('description'),
            'amenities': hotel.get('amenities', []),
            'issues': [],
            'warnings': [],
            'status': 'unknown'
        }
        
        # Check name
        name = hotel.get('name', '')
        if not name or len(name.strip()) < 3:
            self.log_issue('METADATA', 'HIGH', f"{hotel_name}: Invalid or missing name", name)
            metadata_findings['issues'].append('Invalid hotel name')
        
        # Check address
        address = hotel.get('address', '')
        if not address or len(address.strip()) < 10:
            self.log_issue('METADATA', 'MEDIUM', f"{hotel_name}: Invalid or missing address", address)
            metadata_findings['warnings'].append('Invalid address')
        
        # Check description
        description = hotel.get('description', '')
        if not description or len(description.strip()) < 50:
            self.log_issue('METADATA', 'MEDIUM', f"{hotel_name}: Short or missing description", len(description) if description else 0)
            metadata_findings['warnings'].append('Short description')
        
        # Check amenities
        amenities = hotel.get('amenities', [])
        if not amenities:
            self.log_issue('METADATA', 'MEDIUM', f"{hotel_name}: No amenities listed", len(amenities))
            metadata_findings['warnings'].append('No amenities')
        elif len(amenities) < 3:
            self.log_issue('METADATA', 'MEDIUM', f"{hotel_name}: Few amenities listed ({len(amenities)})", amenities)
            metadata_findings['warnings'].append('Few amenities')
        
        # Overall status
        if len(metadata_findings['issues']) == 0:
            if len(metadata_findings['warnings']) == 0:
                metadata_findings['status'] = 'excellent'
            else:
                metadata_findings['status'] = 'good'
        else:
            metadata_findings['status'] = 'poor'
            
        return metadata_findings
    
    def inspect_url_data(self, hotel: Dict[str, Any], hotel_name: str) -> Dict[str, Any]:
        """Comprehensive URL data inspection"""
        booking_url = hotel.get('booking_url', '')
        google_maps_url = hotel.get('google_maps_url', '')
        images = hotel.get('images', [])
        
        url_findings = {
            'booking_url_valid': False,
            'maps_url_valid': False,
            'image_urls_valid': 0,
            'total_images': len(images),
            'issues': [],
            'warnings': [],
            'status': 'unknown'
        }
        
        # Check booking URL
        if not booking_url:
            self.log_issue('URLS', 'HIGH', f"{hotel_name}: Missing booking URL", booking_url)
            url_findings['issues'].append('Missing booking URL')
        else:
            parsed_url = urlparse(booking_url)
            if parsed_url.netloc and 'booking.com' in parsed_url.netloc:
                url_findings['booking_url_valid'] = True
                self.log_issue('URLS', 'LOW', f"{hotel_name}: Valid booking URL", booking_url[:50] + '...')
            else:
                self.log_issue('URLS', 'HIGH', f"{hotel_name}: Invalid booking URL format", booking_url)
                url_findings['issues'].append('Invalid booking URL')
        
        # Check Google Maps URL
        if google_maps_url:
            if 'google.com/maps' in google_maps_url or 'maps.google.com' in google_maps_url:
                url_findings['maps_url_valid'] = True
                self.log_issue('URLS', 'LOW', f"{hotel_name}: Valid Google Maps URL", None)
            else:
                self.log_issue('URLS', 'MEDIUM', f"{hotel_name}: Invalid Google Maps URL", google_maps_url)
                url_findings['warnings'].append('Invalid Maps URL')
        
        # Check image URLs
        valid_images = 0
        for img_url in images[:10]:  # Check first 10 images
            if img_url and urlparse(img_url).netloc:
                valid_images += 1
        
        url_findings['image_urls_valid'] = valid_images
        
        if valid_images == 0 and len(images) > 0:
            self.log_issue('URLS', 'MEDIUM', f"{hotel_name}: No valid image URLs found", len(images))
            url_findings['warnings'].append('Invalid image URLs')
        elif valid_images < len(images) * 0.5:
            self.log_issue('URLS', 'MEDIUM', f"{hotel_name}: Many invalid image URLs ({valid_images}/{len(images)})", None)
            url_findings['warnings'].append('Some invalid image URLs')
        
        # Overall status
        if url_findings['booking_url_valid'] and valid_images > 0:
            url_findings['status'] = 'good'
        elif url_findings['booking_url_valid']:
            url_findings['status'] = 'acceptable'
        else:
            url_findings['status'] = 'poor'
            
        return url_findings
    
    def inspect_hotel_comprehensive(self, hotel: Dict[str, Any], test_name: str) -> Dict[str, Any]:
        """Comprehensive inspection of a single hotel"""
        hotel_name = hotel.get('name', 'Unknown Hotel')
        
        print(f"\n🔍 INSPECTING: {hotel_name} ({test_name})")
        print("=" * 60)
        
        inspection_results = {
            'hotel_name': hotel_name,
            'test_name': test_name,
            'price_inspection': self.inspect_price_data(hotel, hotel_name),
            'review_inspection': self.inspect_review_data(hotel, hotel_name),
            'rating_inspection': self.inspect_rating_data(hotel, hotel_name),
            'metadata_inspection': self.inspect_hotel_metadata(hotel, hotel_name),
            'url_inspection': self.inspect_url_data(hotel, hotel_name),
            'overall_quality_score': 0,
            'critical_issues_count': 0,
            'recommendations': []
        }
        
        # Calculate overall quality score
        quality_factors = [
            inspection_results['price_inspection']['status'] in ['valid', 'zero'],
            inspection_results['review_inspection']['status'] in ['valid', 'empty'],
            inspection_results['rating_inspection']['status'] in ['valid', 'zero'],
            inspection_results['metadata_inspection']['status'] in ['excellent', 'good'],
            inspection_results['url_inspection']['status'] in ['good', 'acceptable']
        ]
        
        inspection_results['overall_quality_score'] = sum(quality_factors) / len(quality_factors) * 100
        
        # Count critical issues
        for category in ['price_inspection', 'review_inspection', 'rating_inspection', 'metadata_inspection', 'url_inspection']:
            inspection_results['critical_issues_count'] += len(inspection_results[category]['issues'])
        
        # Generate recommendations
        if inspection_results['overall_quality_score'] < 60:
            inspection_results['recommendations'].append('Major data quality improvements needed')
        if inspection_results['critical_issues_count'] > 2:
            inspection_results['recommendations'].append('Address critical data extraction issues')
        if inspection_results['review_inspection']['status'] in ['missing', 'invalid']:
            inspection_results['recommendations'].append('Improve review extraction logic')
            
        return inspection_results

def wait_for_job(job_id: str, timeout: int = 180) -> Dict[str, Any]:
    """Wait for job completion with detailed progress tracking"""
    print(f"⏳ Waiting for job {job_id} (timeout: {timeout}s)...")
    
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            response = requests.get(f"{EP}/jobs/{job_id}")
            if response.status_code == 200:
                data = response.json()
                status = data.get("status", "unknown")
                
                if status == "finished":
                    print(f"✅ Job completed successfully")
                    return data
                elif status == "error":
                    error = data.get("error", "Unknown error")
                    print(f"❌ Job failed: {error}")
                    return data
                else:
                    elapsed = time.time() - start_time
                    print(f"\r⏱️  {status} - {elapsed:.0f}s", end="")
            
            time.sleep(3)
        except Exception as e:
            print(f"❌ Error checking job status: {e}")
            break
    
    print(f"\n⏰ Job timed out after {timeout}s")
    return {"status": "timeout", "error": "Job timed out"}

def run_comprehensive_test(test_name: str, location: str, scrape_level: int) -> Optional[Dict[str, Any]]:
    """Run a comprehensive test and return results"""
    print(f"\n🧪 RUNNING TEST: {test_name}")
    print(f"📍 Location: {location}")
    print(f"🔢 Level: {scrape_level}")
    
    try:
        # Submit job
        response = requests.post(f"{EP}/jobs/booking-hotels", json={
            "location": location,
            "scrape_level": scrape_level,
            "max_results": 1
        })
        
        if response.status_code != 200:
            print(f"❌ Failed to submit job: {response.status_code}")
            return None
        
        job_id = response.json()["job_id"]
        print(f"🆔 Job ID: {job_id}")
        
        # Wait for completion
        result = wait_for_job(job_id)
        
        if result.get("status") == "finished":
            hotels = result.get("result", {}).get("hotels", [])
            if hotels:
                return hotels[0]
            else:
                print(f"❌ No hotels found in results")
                return None
        else:
            print(f"❌ Job did not complete successfully")
            return None
            
    except Exception as e:
        print(f"❌ Test failed with exception: {e}")
        return None

# COMPREHENSIVE TEST EXECUTION
if __name__ == "__main__":
    print("🚀 STARTING INTENSIVE DATA INSPECTION")
    print("=" * 80)
    print(f"🕐 Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🎯 Objective: Comprehensive validation of ALL extracted data values")
    
    inspector = DataInspector()
    
    # Define comprehensive test matrix
    TEST_MATRIX = [
        # Different hotel types and locations
        ("Luxury Dubai Hotel", "Burj Al Arab Dubai", 4),
        ("Resort Dubai", "Atlantis The Palm Dubai", 4), 
        ("Business Dubai Hotel", "Ritz Carlton Dubai", 3),
        ("Budget Dubai Option", "Dubai Hotel", 2),
        ("International Luxury", "Hilton London Park Lane", 4),
        
        # Cross-level comparison for same location
        ("Dubai Baseline L1", "Dubai hotels", 1),
        ("Dubai Enhanced L2", "Dubai hotels", 2),
        ("Dubai Deep L3", "Dubai hotels", 3),
        ("Dubai Maximum L4", "Dubai hotels", 4),
    ]
    
    all_inspection_results = []
    
    print(f"\n📊 EXECUTING {len(TEST_MATRIX)} COMPREHENSIVE TESTS...")
    
    for i, (test_name, location, level) in enumerate(TEST_MATRIX, 1):
        print(f"\n{'🔬' * 40}")
        print(f"TEST {i}/{len(TEST_MATRIX)}: COMPREHENSIVE DATA INSPECTION") 
        print('🔬' * 40)
        
        # Run test
        hotel_data = run_comprehensive_test(test_name, location, level)
        
        if hotel_data:
            # Comprehensive inspection
            inspection_result = inspector.inspect_hotel_comprehensive(hotel_data, test_name)
            all_inspection_results.append(inspection_result)
            
            # Summary for this test
            quality_score = inspection_result['overall_quality_score']
            critical_issues = inspection_result['critical_issues_count']
            
            print(f"\n📊 INSPECTION SUMMARY:")
            print(f"   Quality Score: {quality_score:.1f}%")
            print(f"   Critical Issues: {critical_issues}")
            
            if quality_score >= 80:
                print(f"   Status: ✅ EXCELLENT")
            elif quality_score >= 60:
                print(f"   Status: ⚠️ ACCEPTABLE")  
            else:
                print(f"   Status: ❌ POOR")
        else:
            print(f"❌ Test failed - no data to inspect")
            
        # Brief pause between tests
        time.sleep(2)
    
    # COMPREHENSIVE FINAL ANALYSIS
    print(f"\n{'🎯' * 40}")
    print("INTENSIVE DATA INSPECTION RESULTS")
    print('🎯' * 40)
    
    if all_inspection_results:
        # Calculate overall statistics
        total_tests = len(all_inspection_results)
        avg_quality_score = sum(r['overall_quality_score'] for r in all_inspection_results) / total_tests
        total_critical_issues = sum(r['critical_issues_count'] for r in all_inspection_results)
        
        excellent_tests = sum(1 for r in all_inspection_results if r['overall_quality_score'] >= 80)
        acceptable_tests = sum(1 for r in all_inspection_results if 60 <= r['overall_quality_score'] < 80)
        poor_tests = sum(1 for r in all_inspection_results if r['overall_quality_score'] < 60)
        
        print(f"📊 OVERALL STATISTICS:")
        print(f"   Total Tests Completed: {total_tests}")
        print(f"   Average Quality Score: {avg_quality_score:.1f}%")
        print(f"   Total Critical Issues: {total_critical_issues}")
        print(f"   Excellent Data Quality: {excellent_tests} tests")
        print(f"   Acceptable Data Quality: {acceptable_tests} tests") 
        print(f"   Poor Data Quality: {poor_tests} tests")
        
        # Category-specific analysis
        print(f"\n🔍 CATEGORY ANALYSIS:")
        
        categories = ['price_inspection', 'review_inspection', 'rating_inspection', 'metadata_inspection', 'url_inspection']
        
        for category in categories:
            category_issues = []
            category_name = category.replace('_inspection', '').upper()
            
            for result in all_inspection_results:
                category_data = result[category]
                category_issues.extend(category_data.get('issues', []))
            
            print(f"   {category_name}: {len(category_issues)} issues across all tests")
        
        # Most common issues
        all_issues = []
        for result in all_inspection_results:
            for category in categories:
                all_issues.extend(result[category].get('issues', []))
        
        if all_issues:
            print(f"\n❌ MOST COMMON ISSUES:")
            issue_counts = {}
            for issue in all_issues:
                issue_counts[issue] = issue_counts.get(issue, 0) + 1
            
            for issue, count in sorted(issue_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
                print(f"   • {issue}: {count} occurrences")
        
        # Recommendations
        print(f"\n💡 INSPECTION RECOMMENDATIONS:")
        
        if avg_quality_score < 70:
            print(f"   🚨 CRITICAL: Overall data quality below acceptable threshold")
            print(f"   🔧 ACTION REQUIRED: Systematic improvements needed across all extraction levels")
        
        if total_critical_issues > total_tests:
            print(f"   ⚠️ HIGH: More than 1 critical issue per test on average")
            print(f"   🔧 RECOMMENDED: Focus on eliminating critical data extraction failures")
        
        if poor_tests > total_tests * 0.3:
            print(f"   ⚠️ MEDIUM: High percentage of poor quality results ({poor_tests}/{total_tests})")
            print(f"   🔧 SUGGESTED: Review extraction selectors and validation logic")
        
        print(f"\n🎯 DETAILED FINDINGS SAVED FOR FURTHER ANALYSIS")
        
        # Overall assessment
        if avg_quality_score >= 80 and total_critical_issues <= total_tests:
            print(f"\n🎉 OVERALL ASSESSMENT: ✅ SYSTEM DATA QUALITY IS ACCEPTABLE")
        elif avg_quality_score >= 60:
            print(f"\n⚠️ OVERALL ASSESSMENT: ⚠️ SYSTEM DATA QUALITY NEEDS IMPROVEMENT")
        else:
            print(f"\n❌ OVERALL ASSESSMENT: 🚨 SYSTEM DATA QUALITY IS POOR - URGENT ATTENTION REQUIRED")
    else:
        print(f"❌ NO SUCCESSFUL TESTS - UNABLE TO PERFORM DATA INSPECTION")
        print(f"🔧 SYSTEM ISSUES NEED TO BE RESOLVED BEFORE DATA QUALITY ASSESSMENT")
    
    print(f"\n🕐 Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
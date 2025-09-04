#!/usr/bin/env python3
"""
PERFECT TESTING - Acting exactly like the user would
Taking time, being thorough, inspecting every detail
"""

import os, time, requests, json
from datetime import datetime

EP = os.getenv("BROWSER_ENDPOINT", "http://localhost:8004")

class PerfectTester:
    """Testing class that acts like a demanding, thorough user"""
    
    def __init__(self):
        self.critical_issues = []
        self.warnings = []
        self.successes = []
        self.detailed_evidence = {}
    
    def log_finding(self, level, category, severity, message, evidence=None):
        """Log findings with full evidence"""
        finding = {
            'level': level,
            'category': category,
            'severity': severity,
            'message': message,
            'evidence': evidence,
            'timestamp': datetime.now().isoformat()
        }
        
        if severity == 'CRITICAL':
            self.critical_issues.append(finding)
            print(f"🚨 [CRITICAL] Level {level} {category}: {message}")
        elif severity == 'WARNING':
            self.warnings.append(finding)
            print(f"⚠️  [WARNING] Level {level} {category}: {message}")
        else:
            self.successes.append(finding)
            print(f"✅ [SUCCESS] Level {level} {category}: {message}")
    
    def wait_for_job_like_user(self, job_id, max_wait=300):
        """Wait for job like an impatient user would"""
        print(f"⏳ Waiting for job {job_id} (max {max_wait}s)...")
        start_time = time.time()
        
        while time.time() - start_time < max_wait:
            try:
                response = requests.get(f"{EP}/jobs/{job_id}")
                if response.status_code != 200:
                    print(f"❌ API Error: {response.status_code}")
                    return None
                
                data = response.json()
                status = data.get("status", "unknown")
                elapsed = time.time() - start_time
                
                if status == "finished":
                    print(f"\n✅ Completed in {elapsed:.1f}s")
                    return data
                elif status == "error":
                    error = data.get("error", "Unknown error")
                    print(f"\n❌ JOB FAILED: {error}")
                    return None
                else:
                    print(f"\r⏱️  {status} - {elapsed:.0f}s", end="")
                
            except Exception as e:
                print(f"\n❌ Exception: {e}")
                return None
            
            time.sleep(5)  # Check every 5 seconds like an impatient user
        
        print(f"\n⏰ TIMEOUT - Taking too long!")
        return None
    
    def test_level_thoroughly(self, level, location="Dubai"):
        """Test a level with extreme thoroughness"""
        print(f"\n{'🧪' * 30}")
        print(f"TESTING LEVEL {level} - BEING EXTREMELY THOROUGH")
        print('🧪' * 30)
        
        # Submit job
        try:
            payload = {
                "location": location,
                "scrape_level": level,
                "max_results": 1
            }
            
            print(f"📤 Submitting Level {level} job...")
            print(f"📋 Payload: {json.dumps(payload, indent=2)}")
            
            response = requests.post(f"{EP}/jobs/booking-hotels", json=payload)
            
            if response.status_code != 200:
                self.log_finding(level, "SYSTEM", "CRITICAL", f"Request failed: {response.status_code}")
                return None
            
            job_data = response.json()
            job_id = job_data.get("job_id")
            
            if not job_id:
                self.log_finding(level, "SYSTEM", "CRITICAL", "No job ID returned")
                return None
            
            print(f"🆔 Job ID: {job_id}")
            
            # Wait for completion with user-like impatience
            result = self.wait_for_job_like_user(job_id)
            
            if not result:
                self.log_finding(level, "SYSTEM", "CRITICAL", "Job failed or timed out")
                return None
            
            if result.get("status") != "finished":
                self.log_finding(level, "SYSTEM", "CRITICAL", f"Job status: {result.get('status')}")
                return None
            
            # Extract and inspect hotel data
            hotels = result.get("result", {}).get("hotels", [])
            
            if not hotels:
                self.log_finding(level, "DATA", "CRITICAL", "No hotels returned")
                return None
            
            hotel = hotels[0]
            return self.inspect_hotel_like_user(hotel, level)
            
        except Exception as e:
            self.log_finding(level, "SYSTEM", "CRITICAL", f"Exception: {e}")
            return None
    
    def inspect_hotel_like_user(self, hotel, level):
        """Inspect hotel data like a demanding user would"""
        hotel_name = hotel.get('name', 'NO NAME FOUND')
        
        print(f"\n🔍 INSPECTING: {hotel_name}")
        print(f"📊 Level {level} Extraction Results:")
        print("=" * 70)
        
        inspection_result = {
            'level': level,
            'hotel_name': hotel_name,
            'all_fields': {},
            'issues': [],
            'critical_issues': [],
            'review_analysis': {}
        }
        
        # 1. BASIC FIELDS INSPECTION
        price = hotel.get('price_per_night')
        rating = hotel.get('rating')
        review_count = hotel.get('review_count', 0)
        
        print(f"💰 Price: ${price} (type: {type(price)})")
        print(f"⭐ Rating: {rating}/10 (type: {type(rating)})")
        print(f"📊 Review Count Claimed: {review_count}")
        
        inspection_result['all_fields'] = {
            'price': price,
            'rating': rating,
            'review_count_claimed': review_count
        }
        
        # Validate basic fields like a user would
        if price is None or price == 0:
            self.log_finding(level, "PRICE", "WARNING", f"Price is {price}")
        elif not isinstance(price, (int, float)):
            self.log_finding(level, "PRICE", "CRITICAL", f"Invalid price type: {type(price)}")
            inspection_result['critical_issues'].append('Invalid price')
        
        if rating is None:
            self.log_finding(level, "RATING", "CRITICAL", "No rating found")
            inspection_result['critical_issues'].append('No rating')
        elif not isinstance(rating, (int, float)) or rating < 0 or rating > 10:
            self.log_finding(level, "RATING", "CRITICAL", f"Invalid rating: {rating}")
            inspection_result['critical_issues'].append('Invalid rating')
        
        # 2. REVIEWS INSPECTION (MOST CRITICAL)
        reviews = hotel.get('reviews', [])
        pages_processed = hotel.get('pages_processed', 1)
        
        print(f"\n📝 REVIEWS DETAILED INSPECTION:")
        print(f"   Reviews Extracted: {len(reviews)}")
        print(f"   Pages Processed: {pages_processed}")
        
        inspection_result['review_analysis'] = {
            'reviews_extracted': len(reviews),
            'pages_processed': pages_processed,
            'reviewer_name_issues': [],
            'review_text_issues': []
        }
        
        # Level-specific expectations (acting like demanding user)
        if level == 1 or level == 2:
            if len(reviews) > 0:
                self.log_finding(level, "REVIEWS", "WARNING", f"Level {level} shouldn't have reviews but has {len(reviews)}")
            else:
                self.log_finding(level, "REVIEWS", "SUCCESS", f"Level {level} correctly has no reviews")
        
        elif level == 3:
            if len(reviews) == 0:
                self.log_finding(level, "REVIEWS", "CRITICAL", "Level 3 extracts 0 reviews (should extract 2-5)")
                inspection_result['critical_issues'].append('Level 3 no reviews')
            elif len(reviews) < 2:
                self.log_finding(level, "REVIEWS", "WARNING", f"Level 3 only {len(reviews)} review (should be 2-5)")
            elif len(reviews) > 5:
                self.log_finding(level, "REVIEWS", "WARNING", f"Level 3 too many reviews ({len(reviews)}, should be 2-5)")
            else:
                self.log_finding(level, "REVIEWS", "SUCCESS", f"Level 3 extracted {len(reviews)} reviews (appropriate count)")
        
        elif level == 4:
            if len(reviews) == 0:
                self.log_finding(level, "REVIEWS", "CRITICAL", "Level 4 extracts 0 reviews")
                inspection_result['critical_issues'].append('Level 4 no reviews')
            elif pages_processed == 1:
                self.log_finding(level, "PAGINATION", "CRITICAL", "Level 4 only processed 1 page (should paginate)")
                inspection_result['critical_issues'].append('Level 4 no pagination')
            else:
                self.log_finding(level, "PAGINATION", "SUCCESS", f"Level 4 processed {pages_processed} pages")
        
        # 3. DETAILED REVIEW CONTENT INSPECTION
        if reviews:
            print(f"\n🔍 INDIVIDUAL REVIEW INSPECTION:")
            
            for i, review in enumerate(reviews[:5]):  # Check first 5 like a thorough user
                reviewer_name = review.get('reviewer_name', 'NO NAME')
                review_text = review.get('review_text', 'NO TEXT')
                
                print(f"\n   📝 Review {i+1}:")
                print(f"      👤 Reviewer Name: \"{reviewer_name}\"")
                print(f"      📄 Review Text: \"{review_text[:100]}...\"")
                
                # CRITICAL: Check for specific issues user identified
                name_issues = []
                text_issues = []
                
                # Check reviewer name issues
                if reviewer_name == 'NO NAME':
                    name_issues.append("Missing reviewer name")
                elif reviewer_name in ['Wonderful', 'Excellent', 'Amazing', 'Fantastic', 'Exceptional']:
                    name_issues.append(f"Generic word as name: '{reviewer_name}'")
                elif len(reviewer_name) > 30:
                    name_issues.append(f"Name too long (likely review text): '{reviewer_name[:30]}...'")
                elif '.' in reviewer_name and len(reviewer_name) > 15:
                    name_issues.append(f"Name contains sentences: '{reviewer_name}'")
                elif any(phrase in reviewer_name.lower() for phrase in ['everything was', 'stay here', 'will not regret']):
                    name_issues.append(f"Review phrase in name: '{reviewer_name}'")
                
                # Check review text issues
                if review_text == 'NO TEXT':
                    text_issues.append("Missing review text")
                elif len(review_text.strip()) < 10:
                    text_issues.append("Review text too short")
                
                # Record issues
                if name_issues:
                    print(f"      ❌ NAME ISSUES:")
                    for issue in name_issues:
                        print(f"         • {issue}")
                    inspection_result['review_analysis']['reviewer_name_issues'].extend(name_issues)
                else:
                    print(f"      ✅ Reviewer name looks valid")
                
                if text_issues:
                    print(f"      ❌ TEXT ISSUES:")
                    for issue in text_issues:
                        print(f"         • {issue}")
                    inspection_result['review_analysis']['review_text_issues'].extend(text_issues)
                else:
                    print(f"      ✅ Review text looks valid")
        
        return inspection_result
    
    def final_assessment(self, all_results):
        """Final assessment like a demanding user"""
        print(f"\n{'🎯' * 40}")
        print("FINAL ASSESSMENT - USER PERSPECTIVE")
        print('🎯' * 40)
        
        total_critical = len(self.critical_issues)
        total_warnings = len(self.warnings)
        
        print(f"📊 OVERALL STATISTICS:")
        print(f"   Critical Issues: {total_critical}")
        print(f"   Warnings: {total_warnings}")
        print(f"   Successes: {len(self.successes)}")
        
        # Check specific issues user cares about
        level3_working = False
        level4_pagination = False
        reviewer_names_fixed = False
        
        for result in all_results:
            if result and result['level'] == 3:
                if result['review_analysis']['reviews_extracted'] > 0:
                    level3_working = True
            
            if result and result['level'] == 4:
                if result['review_analysis']['pages_processed'] > 1:
                    level4_pagination = True
                
                if len(result['review_analysis']['reviewer_name_issues']) == 0:
                    reviewer_names_fixed = True
        
        print(f"\n🚨 USER'S SPECIFIC ISSUES:")
        print(f"   Level 3 extracts reviews: {'✅ FIXED' if level3_working else '❌ STILL BROKEN'}")
        print(f"   Level 4 uses pagination: {'✅ FIXED' if level4_pagination else '❌ STILL BROKEN'}")
        print(f"   Reviewer names clean: {'✅ FIXED' if reviewer_names_fixed else '❌ STILL BROKEN'}")
        
        # Overall verdict like user would give
        if total_critical == 0 and level3_working and reviewer_names_fixed:
            print(f"\n🎉 VERDICT: SYSTEM IS NOW WORKING CORRECTLY")
        elif total_critical <= 1:
            print(f"\n⚠️  VERDICT: SYSTEM MOSTLY WORKING BUT NEEDS MINOR FIXES")
        else:
            print(f"\n❌ VERDICT: SYSTEM STILL HAS MAJOR PROBLEMS")
            print(f"🔧 IMMEDIATE ACTION REQUIRED")
        
        return {
            'level3_working': level3_working,
            'level4_pagination': level4_pagination,
            'reviewer_names_fixed': reviewer_names_fixed,
            'total_critical': total_critical,
            'overall_success': total_critical == 0 and level3_working and reviewer_names_fixed
        }

# EXECUTE PERFECT TESTING
if __name__ == "__main__":
    print("🎯 STARTING PERFECT TESTING - ACTING LIKE DEMANDING USER")
    print("=" * 80)
    print(f"🕐 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🎯 Objective: Test EVERYTHING perfectly, inspect every detail")
    print(f"⚠️  Taking time to be thorough - no rushing")
    
    tester = PerfectTester()
    all_results = []
    
    # Test each level systematically
    levels = [1, 2, 3, 4]
    
    for level in levels:
        print(f"\n" + "=" * 80)
        print(f"STARTING LEVEL {level} PERFECT TESTING")
        print("=" * 80)
        
        # Update todo status
        print(f"🔄 Starting Level {level} testing...")
        
        result = tester.test_level_thoroughly(level)
        all_results.append(result)
        
        if result:
            # Immediate feedback on this level
            critical_count = len(result['critical_issues'])
            review_count = result['review_analysis']['reviews_extracted']
            
            print(f"\n📊 LEVEL {level} IMMEDIATE ASSESSMENT:")
            print(f"   Critical Issues: {critical_count}")
            print(f"   Reviews Extracted: {review_count}")
            
            if level == 3 and review_count == 0:
                print(f"   🚨 LEVEL 3 STILL BROKEN!")
            elif level == 3 and review_count > 0:
                print(f"   🎉 LEVEL 3 FIXED!")
            
            if level == 4:
                name_issues = len(result['review_analysis']['reviewer_name_issues'])
                pages = result['review_analysis']['pages_processed']
                
                if name_issues > 0:
                    print(f"   🚨 REVIEWER NAMES STILL BROKEN!")
                else:
                    print(f"   🎉 REVIEWER NAMES FIXED!")
                
                if pages == 1:
                    print(f"   🚨 PAGINATION STILL BROKEN!")
                else:
                    print(f"   🎉 PAGINATION WORKING!")
        else:
            print(f"   ❌ LEVEL {level} COMPLETELY FAILED")
        
        # Pause between levels like a methodical user
        if level < 4:
            print(f"\n⏸️  Pausing 15 seconds before next level...")
            time.sleep(15)
    
    # FINAL COMPREHENSIVE ASSESSMENT
    final_verdict = tester.final_assessment(all_results)
    
    # Save detailed results for evidence
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    results_file = f"/home/aub/boo/Qitta/backend/PERFECT_TEST_RESULTS_{timestamp}.json"
    
    with open(results_file, 'w') as f:
        json.dump({
            'test_timestamp': datetime.now().isoformat(),
            'all_results': all_results,
            'critical_issues': tester.critical_issues,
            'warnings': tester.warnings,
            'successes': tester.successes,
            'final_verdict': final_verdict
        }, f, indent=2)
    
    print(f"\n📁 Detailed results saved to: {results_file}")
    print(f"🕐 Perfect testing completed: {datetime.now().strftime('%H:%M:%S')}")
    print("=" * 80)
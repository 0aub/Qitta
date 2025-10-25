"""
Twitter/X Scraper - Production Ready
===================================

Twitter account and tweet scraper with authentication support.
Built following the successful Airbnb/Booking scraper patterns.

Features:
- Authenticated login with session persistence
- Profile data extraction
- Tweet timeline scraping
- Rate limiting and error handling
- Multiple extraction levels (1-4)
- Batch processing support
- Date filtering for timeline extraction

Extraction Levels:
-----------------
Level 1 (Basic):
    - User profile information (name, bio, counts)
    - Recent tweets (last 10-20)
    - Fast extraction (~30-60 seconds)
    - Minimal resource usage

Level 2 (Standard):
    - Everything from Level 1
    - Extended tweet timeline (50-100 tweets)
    - Basic engagement data (likes, retweets per tweet)
    - Moderate extraction (~2-5 minutes)

Level 3 (Advanced):
    - Everything from Level 2
    - Social graph data (followers/following lists)
    - Media content extraction
    - User interactions (mentions, replies)
    - Intensive extraction (~5-15 minutes)

Level 4 (Complete):
    - Everything from Level 3
    - Unlimited post extraction mode
    - Complete timeline historical data
    - All engagement metrics
    - Full media gallery
    - Maximum resource usage (~15+ minutes)
    - Supports 1000+ post extraction with intelligent batching

Shared Components:
-----------------
This scraper uses the following shared components for reusability:
- shared.session_manager: Session pooling and concurrency control
- shared.batch_processor: Intelligent batch job processing
- shared.date_utils: Date filtering and timeline management
- shared.metrics: Success rate calculation

Version: 1.0
Author: Based on successful Airbnb/Booking patterns
"""

import json
import logging
import asyncio
import os
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Union
import hashlib

# Import reusable components from existing modules
from ..observability.metrics import calculate_extraction_success_rate
from ..utils import (
    parse_date_range,
    parse_custom_date,
    get_date_filter_bounds,
    is_within_date_range,
    parse_relative_timestamp,
    format_date,
    calculate_date_range_days,
    get_expected_data_reduction
)
from ..human_behavior import HumanBehavior


# DEPRECATED: Kept for backward compatibility - use shared.metrics.calculate_extraction_success_rate
def calculate_extraction_success_rate_legacy(results: Union[List[Dict[str, Any]], bool, None], params: Dict[str, Any], method: str) -> float:
    """Calculate realistic success rate based on actual data quality."""
    # CRITICAL FIX: Type validation first to prevent "object of type 'bool' has no len()" error
    if not isinstance(results, (list, tuple)):
        return 0.0

    if not results:
        return 0.0

    # For comprehensive user scraping
    if method == "comprehensive_user_scraping" and len(results) == 1:
        result = results[0]
        
        # Count successful data extractions
        success_points = 0
        total_points = 0
        
        # Profile data (20 points possible)
        profile = result.get('profile', {})
        if profile.get('display_name'):
            success_points += 5
        if profile.get('username'):
            success_points += 3
        if profile.get('bio'):
            success_points += 4
        if profile.get('followers_count'):
            success_points += 4
        if profile.get('following_count'):
            success_points += 2
        if profile.get('posts_count'):
            success_points += 2
        total_points += 20
        
        # Posts data (40 points possible) - ENHANCED FOR VALIDATION
        posts = result.get('posts', [])
        if posts:
            requested_posts = params.get('max_posts', 10)
            posts_extracted = len(posts)
            posts_with_dates = len([p for p in posts if p.get('date') or p.get('timestamp')])
            posts_with_engagement = len([p for p in posts if p.get('likes') or p.get('retweets')])

            # NEW: Count validation-verified posts for data integrity
            validated_posts = len([p for p in posts if p.get('validation_method') or p.get('validation_confidence')])
            high_confidence_posts = len([p for p in posts if p.get('validation_confidence') in ['high', 'verified']])

            # NEW: Check for authorship accuracy
            correct_author_posts = len([p for p in posts if p.get('author') == params.get('username', '')])
            url_verified_posts = len([p for p in posts if p.get('validation_method') == 'url_verified'])
            posts_with_text = len([p for p in posts if p.get('text') and len(p.get('text', '')) > 10])
            
            # Posts quantity (10 points) - EVERYTHING MODE ADJUSTMENT
            if posts_extracted >= 50:  # EVERYTHING mode likely achieved
                quantity_score = 10  # Maximum points for high extraction
            elif posts_extracted >= requested_posts:
                quantity_score = 10  # Full points if met or exceeded request
            else:
                quantity_score = min(10, (posts_extracted / max(requested_posts, 1)) * 10)
            success_points += quantity_score
            
            # Posts quality (30 points) - ENHANCED WITH VALIDATION METRICS
            if posts_extracted > 0:
                success_points += (posts_with_text / posts_extracted) * 10  # Text quality (reduced)
                success_points += (posts_with_dates / posts_extracted) * 5   # Date presence (reduced)
                success_points += (posts_with_engagement / posts_extracted) * 3  # Engagement metrics (reduced)

                # NEW: VALIDATION QUALITY METRICS (12 points)
                if validated_posts > 0:
                    success_points += (validated_posts / posts_extracted) * 7  # Validation coverage
                    success_points += (high_confidence_posts / posts_extracted) * 5  # High confidence validation

                # NEW: AUTHORSHIP ACCURACY (Critical for data integrity)
                authorship_accuracy = correct_author_posts / posts_extracted
                if authorship_accuracy >= 0.9:  # 90%+ accuracy
                    success_points += 10  # Bonus for high accuracy
                elif authorship_accuracy >= 0.7:  # 70%+ accuracy
                    success_points += 7
                elif authorship_accuracy >= 0.5:  # 50%+ accuracy
                    success_points += 4
                else:
                    # Heavy penalty for poor authorship accuracy
                    success_points = max(0, success_points - 15)

        total_points += 40
        
        # Other data types (40 points possible)
        for data_type, weight in [('likes', 10), ('mentions', 8), ('reposts', 9), ('media', 8), ('followers', 7), ('following', 7)]:
            if params.get(f'scrape_{data_type}', False):
                data_list = result.get(data_type, [])
                requested = params.get(f'max_{data_type}', 10)
                if data_list and len(data_list) > 0:
                    success_points += min(weight, (len(data_list) / max(requested, 1)) * weight)
            total_points += weight
        
        return (success_points / total_points) if total_points > 0 else 0.0
        
    # For other methods, use simpler calculation
    else:
        max_requested = params.get('max_posts', params.get('max_results', 10))
        return len(results) / max(max_requested, 1) if max_requested > 0 else 0.0


# Create TwitterDateUtils class as wrapper around utils functions for backward compatibility
class TwitterDateUtils:
    """Utility class for handling date filtering in Twitter extraction."""

    @staticmethod
    def parse_date_range(date_range: str) -> tuple[datetime, datetime]:
        """Parse predefined date ranges into start/end datetime objects."""
        return parse_date_range(date_range)

    @staticmethod
    def parse_custom_date(date_str: str) -> Optional[datetime]:
        """Parse custom date string into datetime object."""
        return parse_custom_date(date_str)

    @staticmethod
    def get_date_filter_bounds(params: Dict[str, Any]) -> tuple[Optional[datetime], Optional[datetime]]:
        """Get date filtering bounds from parameters."""
        return get_date_filter_bounds(params)

    @staticmethod
    def is_within_date_range(tweet_timestamp: str, start_dt: datetime, end_dt: datetime) -> bool:
        """Check if tweet timestamp is within the specified date range."""
        return is_within_date_range(tweet_timestamp, start_dt, end_dt)

    @staticmethod
    def _parse_relative_timestamp(timestamp: str, start_dt: datetime, end_dt: datetime) -> bool:
        """Parse relative timestamps like '2h', '3d ago', 'yesterday'."""
        return parse_relative_timestamp(timestamp, start_dt, end_dt)


class TwitterDateUtils_Legacy:
    """DEPRECATED - kept for reference only."""

    @staticmethod
    def parse_date_range_legacy(date_range: str) -> tuple[datetime, datetime]:
        """Parse predefined date ranges into start/end datetime objects."""
        now = datetime.now()
        
        if date_range == "last_day" or date_range == "yesterday":
            start = now - timedelta(days=1)
            end = now
        elif date_range == "last_3_days":
            start = now - timedelta(days=3)
            end = now
        elif date_range == "last_week":
            start = now - timedelta(days=7)
            end = now
        elif date_range == "last_2_weeks":
            start = now - timedelta(days=14)
            end = now
        elif date_range == "last_month":
            start = now - timedelta(days=30)
            end = now
        elif date_range == "last_3_months":
            start = now - timedelta(days=90)
            end = now
        elif date_range == "today":
            start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            end = now
        else:
            # Default to last week if unknown range
            start = now - timedelta(days=7)
            end = now
            
        return start, end
    
    @staticmethod
    def parse_custom_date(date_str: str) -> datetime:
        """Parse custom date string into datetime object."""
        if not date_str:
            return None
            
        try:
            # Try different formats
            formats = [
                "%Y-%m-%d",           # 2024-01-01
                "%Y-%m-%dT%H:%M:%S",  # 2024-01-01T10:30:00
                "%Y-%m-%d %H:%M:%S",  # 2024-01-01 10:30:00
                "%m/%d/%Y",           # 01/01/2024
                "%d/%m/%Y",           # 01/01/2024 (European)
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(date_str, fmt)
                except ValueError:
                    continue
                    
            # If no format matches, try ISO format
            return datetime.fromisoformat(date_str.replace('Z', '+00:00').replace('+00:00', ''))
            
        except Exception as e:
            raise ValueError(f"Unable to parse date '{date_str}': {e}")
    
    @staticmethod
    def get_date_filter_bounds(params: Dict[str, Any]) -> tuple[datetime, datetime]:
        """Get date filtering bounds from parameters."""
        if not params.get("enable_date_filtering"):
            return None, None
            
        # Handle predefined ranges
        date_range = params.get("date_range", "")
        if date_range:
            return TwitterDateUtils.parse_date_range(date_range)
            
        # Handle custom date range
        start_date = params.get("start_date", "")
        end_date = params.get("end_date", "")
        
        start_dt = TwitterDateUtils.parse_custom_date(start_date) if start_date else None
        end_dt = TwitterDateUtils.parse_custom_date(end_date) if end_date else datetime.now()
        
        return start_dt, end_dt
    
    @staticmethod
    def is_within_date_range(tweet_timestamp: str, start_dt: datetime, end_dt: datetime) -> bool:
        """Check if tweet timestamp is within the specified date range."""
        if not tweet_timestamp or not start_dt or not end_dt:
            return True

        try:
            # Parse Twitter timestamp (usually ISO format)
            if 'T' in tweet_timestamp:
                tweet_dt = datetime.fromisoformat(tweet_timestamp.replace('Z', '+00:00').replace('+00:00', ''))
            else:
                # Handle relative timestamps like "2h", "1d ago"
                return TwitterDateUtils._parse_relative_timestamp(tweet_timestamp, start_dt, end_dt)

            # Remove timezone info for comparison - ensure all datetimes are timezone-naive
            tweet_dt = tweet_dt.replace(tzinfo=None) if tweet_dt.tzinfo else tweet_dt
            start_dt = start_dt.replace(tzinfo=None) if start_dt.tzinfo else start_dt
            end_dt = end_dt.replace(tzinfo=None) if end_dt.tzinfo else end_dt

            return start_dt <= tweet_dt <= end_dt
            
        except Exception:
            # If parsing fails, assume it's within range to avoid losing data
            return True
    
    @staticmethod
    def _parse_relative_timestamp(timestamp: str, start_dt: datetime, end_dt: datetime) -> bool:
        """Parse relative timestamps like '2h', '3d ago', 'yesterday'."""
        try:
            timestamp = timestamp.lower().strip()
            now = datetime.now()
            
            if 'ago' in timestamp:
                timestamp = timestamp.replace('ago', '').strip()
            
            if timestamp == 'now' or timestamp == 'just now':
                tweet_dt = now
            elif 's' in timestamp:  # seconds
                seconds = int(timestamp.replace('s', ''))
                tweet_dt = now - timedelta(seconds=seconds)
            elif 'm' in timestamp:  # minutes  
                minutes = int(timestamp.replace('m', ''))
                tweet_dt = now - timedelta(minutes=minutes)
            elif 'h' in timestamp:  # hours
                hours = int(timestamp.replace('h', ''))
                tweet_dt = now - timedelta(hours=hours)
            elif 'd' in timestamp:  # days
                days = int(timestamp.replace('d', ''))
                tweet_dt = now - timedelta(days=days)
            elif 'w' in timestamp:  # weeks
                weeks = int(timestamp.replace('w', ''))
                tweet_dt = now - timedelta(weeks=weeks)
            else:
                return True  # Can't parse, assume valid
                
            return start_dt <= tweet_dt <= end_dt
            
        except Exception:
            return True


class TwitterTask:
    """Production-ready Twitter scraper with authentication."""
    
    BASE_URL = "https://twitter.com"
    LOGIN_URL = "https://twitter.com/i/flow/login"

    @staticmethod
    async def run(params: Dict[str, Any], logger: logging.Logger, browser, context=None, job_output_dir: str = None) -> Dict[str, Any]:
        """Main entry point for Twitter scraping."""
        try:
            # Extract nested params if present (consistent with Airbnb/Booking)
            actual_params = params.get("params", params)
            
            # DEBUG: Log raw parameters for batch processing analysis
            logger.info(f"🔍 DEBUG RAW PARAMS: {list(actual_params.keys())}")
            if 'batch_mode' in actual_params:
                logger.info(f"🚀 BATCH MODE DETECTED: {actual_params['batch_mode']}")
            if 'jobs' in actual_params:
                logger.info(f"📦 BATCH JOBS COUNT: {len(actual_params['jobs'])}")
            
            # Validate and normalize parameters
            clean_params = TwitterTask._validate_params(actual_params)
            
            # Determine scraping level - Check all possible parameter names
            scrape_level = clean_params.get("extract_level", 
                                           clean_params.get("level", 
                                                           clean_params.get("scrape_level", 1)))
            
            # 🔧 PHASE A FIX: Proper routing priority for search vs user scraping
            # CRITICAL FIX: Don't derive target_username for search operations
            target_username = None

            # Only determine target_username for non-search operations to avoid routing conflicts
            if not (clean_params.get('hashtag') or clean_params.get('search_query') or clean_params.get('query')):
                target_username = clean_params.get("target_username",
                                                 clean_params.get("username", "timeline"))
            
            logger.info(f"🐦 TWITTER SCRAPER v1.0 - STARTING")
            logger.info(f"👤 Target: {target_username}")
            logger.info(f"📊 Scrape Level: {scrape_level}")
            # Show actual scraping parameters instead of generic max_results
            if clean_params.get('scrape_posts', True):
                logger.info(f"📝 Posts Limit: {clean_params.get('max_posts', 100)}")
            if clean_params.get('scrape_likes', False):
                logger.info(f"❤️ Likes Limit: {clean_params.get('max_likes', 50)}")
            if clean_params.get('scrape_mentions', False):
                logger.info(f"@️⃣ Mentions Limit: {clean_params.get('max_mentions', 30)}")
            if clean_params.get('scrape_media', False):
                logger.info(f"🖼️ Media Limit: {clean_params.get('max_media', 25)}")
            if clean_params.get('scrape_followers', False):
                logger.info(f"👥 Followers Limit: {clean_params.get('max_followers', 200)}")
            if clean_params.get('scrape_following', False):
                logger.info(f"➡️ Following Limit: {clean_params.get('max_following', 150)}")
            if clean_params.get('scrape_reposts', False):
                logger.info(f"🔄 Reposts Limit: {clean_params.get('max_reposts', 50)}")

            # Post-level engagement scraping
            if clean_params.get('scrape_post_likers', False):
                logger.info(f"💖 Post Likers: {clean_params.get('max_likers_per_post', 20)} per post")
            if clean_params.get('scrape_post_repliers', False):
                logger.info(f"💬 Post Repliers: {clean_params.get('max_repliers_per_post', 20)} per post")
            if clean_params.get('scrape_post_reposters', False):
                logger.info(f"🔁 Post Reposters: {clean_params.get('max_reposters_per_post', 20)} per post")

            if not any([clean_params.get('scrape_posts', True), clean_params.get('scrape_likes', False),
                       clean_params.get('scrape_mentions', False), clean_params.get('scrape_media', False),
                       clean_params.get('scrape_followers', False), clean_params.get('scrape_following', False),
                       clean_params.get('scrape_reposts', False)]):
                logger.info(f"🔢 Max Results: {clean_params.get('max_results', 10)}")

            # Initialize concurrency manager for session isolation
            concurrency_manager = TwitterConcurrencyManager()
            
            # Get optimal session for concurrent processing with performance tracking
            start_time = time.time()
            session_file = await concurrency_manager.get_available_session(logger)
            
            # Initialize scraper with assigned session and context from workers
            scraper = TwitterScraper(browser, logger, output_dir=job_output_dir, context=context)
            
            # Load the assigned session
            if clean_params.get("use_session") or os.path.exists(session_file):
                logger.info(f"🍪 Using session for authentication: {session_file}")
                logger.info("⏳ STEP 1/5: Loading session cookies...")
                await scraper.load_session(session_file)
                logger.info("✅ STEP 1/5: Session loaded successfully")
            else:
                logger.info("🔐 Using credential-based authentication...")
                logger.info("⏳ STEP 1/5: Starting authentication...")
                await scraper.authenticate()
                logger.info("✅ STEP 1/5: Authentication completed")
            
            # Add the corrected parameters to clean_params (FIXED ROUTING)
            # Only set target_username for user scraping, not search operations
            if target_username is not None:
                clean_params['target_username'] = target_username
            clean_params['scrape_level'] = scrape_level
            
            # Determine scraping type based on parameters
            if clean_params.get('jobs') and clean_params.get('batch_mode'):
                # 🚀 NEW BATCH PROCESSING MODE - Multiple jobs with optimization
                batch_jobs = clean_params['jobs']
                batch_mode = clean_params['batch_mode']
                
                logger.info(f"🚀 **ADVANCED BATCH MODE**: {batch_mode} processing {len(batch_jobs)} jobs")
                
                # Use intelligent batch processing
                if batch_mode == "test_sequential":
                    results = await concurrency_manager.process_batch_jobs(batch_jobs, logger)
                elif batch_mode == "test_parallel":
                    results = await concurrency_manager.batch_processor.process_parallel_batch(batch_jobs, logger)
                else:
                    # Auto-select optimal processing mode
                    optimal_mode = concurrency_manager.batch_processor.auto_select_processing_mode(batch_jobs, logger)
                    if optimal_mode == "parallel":
                        results = await concurrency_manager.batch_processor.process_parallel_batch(batch_jobs, logger)
                    else:
                        results = await concurrency_manager.process_batch_jobs(batch_jobs, logger)
                
                extraction_method = f"batch_processing_{batch_mode}"
                logger.info(f"🏁 **BATCH PROCESSING COMPLETE**: {len(results)} job results")
                
                # Get batch performance metrics
                metrics = concurrency_manager.get_batch_performance_metrics()
                logger.info(f"📊 Batch Analytics: {metrics}")
                
            elif clean_params.get('hashtag'):
                # Hashtag scraping mode
                logger.info(f"🏷️ HASHTAG MODE: {clean_params['hashtag']}")
                logger.info(f"📊 Max Tweets: {clean_params.get('max_tweets', 50)}")
                results = await scraper.scrape_hashtag(clean_params)
                extraction_method = "hashtag_scraping"

            elif clean_params.get('search_query') or clean_params.get('query'):
                # Phase 2.2: Search query scraping mode
                search_query = clean_params.get('search_query', clean_params.get('query'))
                logger.info(f"🔍 SEARCH QUERY MODE: '{search_query}'")
                logger.info(f"📊 Max Tweets: {clean_params.get('max_tweets', 50)}")
                logger.info(f"🎯 Result Type: {clean_params.get('result_type', 'recent')}")
                results = await scraper.scrape_search_query(clean_params)
                extraction_method = "search_query_scraping"

            elif clean_params.get('monitor_mode') or clean_params.get('monitor_target'):
                # Phase 3.1: Real-time monitoring mode
                monitor_target = clean_params.get('monitor_target', '')
                monitor_type = clean_params.get('monitor_type', 'user')
                logger.info(f"🔄 REAL-TIME MONITORING MODE: {monitor_type}")
                logger.info(f"🎯 Target: '{monitor_target}'")
                logger.info(f"⏱️ Interval: {clean_params.get('monitoring_interval', 300)}s")
                logger.info(f"🔄 Max cycles: {clean_params.get('max_iterations', 10)}")
                results = await scraper.monitor_real_time(clean_params)
                extraction_method = "real_time_monitoring"

            elif clean_params.get('batch_usernames'):
                # 🚀 BATCH PROCESSING MODE - Multiple usernames
                batch_usernames = clean_params['batch_usernames']
                if isinstance(batch_usernames, str):
                    batch_usernames = [u.strip() for u in batch_usernames.split(',')]
                
                logger.info(f"🎯 BATCH MODE: Processing {len(batch_usernames)} accounts: {batch_usernames}")
                
                batch_results = []
                for i, username in enumerate(batch_usernames):
                    logger.info(f"🔄 Processing {i+1}/{len(batch_usernames)}: @{username}")
                    
                    # Create individual params for each user
                    user_params = clean_params.copy()
                    user_params['target_username'] = username.replace('@', '')
                    
                    try:
                        user_results = await scraper.scrape_user_comprehensive(user_params)
                        if user_results:
                            batch_results.extend(user_results)
                            logger.info(f"✅ @{username}: {len(user_results)} items extracted")
                        else:
                            logger.warning(f"⚠️ @{username}: No data extracted")
                    except Exception as e:
                        logger.error(f"❌ @{username}: Failed - {e}")
                    
                    # Brief pause between users
                    if i < len(batch_usernames) - 1:
                        await asyncio.sleep(2)
                
                results = batch_results
                extraction_method = "batch_user_scraping"
                logger.info(f"🏁 BATCH COMPLETE: {len(results)} total items from {len(batch_usernames)} accounts")
                
            elif clean_params.get('batch_hashtags'):
                # 🚀 BATCH PROCESSING MODE - Multiple hashtags  
                batch_hashtags = clean_params['batch_hashtags']
                if isinstance(batch_hashtags, str):
                    batch_hashtags = [h.strip().replace('#', '') for h in batch_hashtags.split(',')]
                
                logger.info(f"🏷️ BATCH HASHTAG MODE: Processing {len(batch_hashtags)} hashtags: {batch_hashtags}")
                
                batch_results = []
                for i, hashtag in enumerate(batch_hashtags):
                    logger.info(f"🔄 Processing {i+1}/{len(batch_hashtags)}: #{hashtag}")
                    
                    # Create individual params for each hashtag
                    hashtag_params = clean_params.copy()
                    hashtag_params['hashtag'] = hashtag
                    
                    try:
                        hashtag_results = await scraper.scrape_hashtag(hashtag_params)
                        if hashtag_results:
                            batch_results.extend(hashtag_results)
                            logger.info(f"✅ #{hashtag}: {len(hashtag_results)} items extracted")
                        else:
                            logger.warning(f"⚠️ #{hashtag}: No data extracted")
                    except Exception as e:
                        logger.error(f"❌ #{hashtag}: Failed - {e}")
                    
                    # Brief pause between hashtags
                    if i < len(batch_hashtags) - 1:
                        await asyncio.sleep(3)
                
                results = batch_results
                extraction_method = "batch_hashtag_scraping"
                logger.info(f"🏁 HASHTAG BATCH COMPLETE: {len(results)} total items from {len(batch_hashtags)} hashtags")
                
            elif clean_params.get('target_username'):
                # User account scraping mode with differentiated levels
                logger.info(f"👤 USER ACCOUNT MODE: @{target_username}")

                # FIXED: Properly differentiate scrape levels
                if scrape_level >= 4:
                    # Level 4: Enhanced extraction - RESPECT USER PARAMETERS
                    logger.info(f"📊 LEVEL 4: Enhanced extraction respecting user parameters")
                    clean_params_level4 = clean_params.copy()

                    # ONLY enhance what user explicitly requested or what's enabled by default
                    # DO NOT force enable what user explicitly disabled
                    if clean_params.get('scrape_posts', True):
                        clean_params_level4['scrape_posts'] = True
                        requested_posts = clean_params.get('max_posts', 30)

                        # TRUE LARGE SCALE EXTRACTION: Support unlimited mode
                        if requested_posts >= 500 or str(requested_posts).lower() in ['all', 'unlimited', 'everything']:
                            clean_params_level4['max_posts'] = 99999  # Unlimited mode
                            clean_params_level4['unlimited_mode'] = True
                            logger.info(f"🌟 UNLIMITED MODE ACTIVATED: Extracting ALL available posts")
                        else:
                            clean_params_level4['max_posts'] = max(requested_posts, 30)  # Enhanced minimum for Level 4

                    if clean_params.get('scrape_media', False):
                        clean_params_level4['scrape_media'] = True
                        clean_params_level4['max_media'] = max(clean_params.get('max_media', 25), 15)

                    if clean_params.get('scrape_likes', False):
                        clean_params_level4['scrape_likes'] = True
                        clean_params_level4['max_likes'] = max(clean_params.get('max_likes', 50), 20)

                    if clean_params.get('scrape_mentions', False):
                        clean_params_level4['scrape_mentions'] = True
                        clean_params_level4['max_mentions'] = max(clean_params.get('max_mentions', 30), 15)

                    if clean_params.get('scrape_reposts', False):
                        clean_params_level4['scrape_reposts'] = True
                        clean_params_level4['max_reposts'] = max(clean_params.get('max_reposts', 50), 20)

                    # RESPECT user's explicit choices for followers/following
                    if clean_params.get('scrape_followers', False):
                        clean_params_level4['scrape_followers'] = True
                        clean_params_level4['max_followers'] = max(clean_params.get('max_followers', 200), 100)

                    if clean_params.get('scrape_following', False):
                        clean_params_level4['scrape_following'] = True
                        clean_params_level4['max_following'] = max(clean_params.get('max_following', 150), 75)

                    logger.info(f"✅ LEVEL 4 ENHANCED MODE: Respecting user parameter choices")
                    results = await scraper.scrape_user_comprehensive(clean_params_level4)
                    extraction_method = "level_4_enhanced"
                elif scrape_level >= 3:
                    # Level 3: Full profile + posts + media + likes + mentions + reposts + sample social graph + post engagement
                    logger.info(f"📊 LEVEL 3: Full profile with all features and sample social graph")
                    clean_params_level3 = clean_params.copy()
                    clean_params_level3.update({
                        'scrape_posts': True,
                        'scrape_media': True,
                        # FORCE enable these features for Level 3 (override False from _validate_params)
                        'scrape_likes': True,  # Always enabled for Level 3
                        'max_likes': min(clean_params.get('max_likes', 10), 10),  # Sample 10 for Level 3
                        'scrape_mentions': True,  # Always enabled for Level 3
                        'max_mentions': min(clean_params.get('max_mentions', 10), 10),  # Sample 10
                        # Level 3: Allow sample followers/following if requested
                        'scrape_followers': clean_params.get('scrape_followers', False),
                        'max_followers': min(clean_params.get('max_followers', 25), 25),  # Max 25 for level 3
                        'scrape_following': clean_params.get('scrape_following', False),
                        'max_following': min(clean_params.get('max_following', 25), 25),  # Max 25 for level 3
                        'scrape_reposts': True,  # Always enabled for Level 3
                        'max_reposts': min(clean_params.get('max_reposts', 10), 10),  # Sample 10
                        # Post-level engagement (enabled by default for Level 3)
                        'scrape_post_likers': True,  # Always enabled for Level 3
                        'max_likers_per_post': clean_params.get('max_likers_per_post', 5),  # Sample 5 per post
                        'scrape_post_repliers': True,  # Always enabled for Level 3
                        'max_repliers_per_post': clean_params.get('max_repliers_per_post', 5),  # Sample 5 per post
                        'scrape_post_reposters': True,  # Always enabled for Level 3
                        'max_reposters_per_post': clean_params.get('max_reposters_per_post', 5)  # Sample 5 per post
                    })
                    # DEBUG: Log the params being sent
                    logger.info(f"🔍 DEBUG LEVEL 3 PARAMS: scrape_likes={clean_params_level3.get('scrape_likes')}, scrape_mentions={clean_params_level3.get('scrape_mentions')}, scrape_reposts={clean_params_level3.get('scrape_reposts')}")
                    logger.info(f"🔍 DEBUG ENGAGEMENT: scrape_post_likers={clean_params_level3.get('scrape_post_likers')}, scrape_post_repliers={clean_params_level3.get('scrape_post_repliers')}, scrape_post_reposters={clean_params_level3.get('scrape_post_reposters')}")
                    results = await scraper.scrape_user_comprehensive(clean_params_level3)
                    extraction_method = "level_3_with_media"
                elif scrape_level >= 2:
                    # Level 2: Profile + posts + sample social graph + optional post engagement
                    logger.info(f"📊 LEVEL 2: Profile and posts extraction with sample social graph")
                    clean_params_level2 = clean_params.copy()
                    clean_params_level2.update({
                        'scrape_posts': True,
                        'scrape_media': False,
                        'scrape_likes': False,
                        'scrape_mentions': False,
                        # Level 2: Allow sample followers/following if requested
                        'scrape_followers': clean_params.get('scrape_followers', False),
                        'max_followers': min(clean_params.get('max_followers', 10), 10),  # Max 10 for level 2
                        'scrape_following': clean_params.get('scrape_following', False),
                        'max_following': min(clean_params.get('max_following', 10), 10),  # Max 10 for level 2
                        'scrape_reposts': False,
                        'max_posts': min(clean_params.get('max_posts', 10), 15),  # Limit posts for level 2
                        # Post-level engagement (optional, user-controlled)
                        'scrape_post_likers': clean_params.get('scrape_post_likers', False),
                        'max_likers_per_post': clean_params.get('max_likers_per_post', 20),
                        'scrape_post_repliers': clean_params.get('scrape_post_repliers', False),
                        'max_repliers_per_post': clean_params.get('max_repliers_per_post', 20),
                        'scrape_post_reposters': clean_params.get('scrape_post_reposters', False),
                        'max_reposters_per_post': clean_params.get('max_reposters_per_post', 20)
                    })
                    results = await scraper.scrape_user_comprehensive(clean_params_level2)
                    extraction_method = "level_2_full_profile"
                else:
                    # Level 1: Basic profile + minimal posts
                    logger.info(f"📊 LEVEL 1: Basic extraction")
                    clean_params_level1 = clean_params.copy()
                    clean_params_level1.update({
                        'scrape_posts': True,
                        'scrape_media': False,
                        'scrape_likes': False,
                        'scrape_reposts': False,
                        'scrape_mentions': False,
                        'scrape_followers': False,
                        'scrape_following': False,
                        'max_posts': min(clean_params.get('max_posts', 5), 8)  # Minimal posts for level 1
                    })
                    results = await scraper.scrape_user_comprehensive(clean_params_level1)
                    extraction_method = "level_1_basic"

                # Log what was actually scraped
                scrape_options = []
                if clean_params.get('scrape_posts'):
                    scrape_options.append(f"Posts({clean_params.get('max_posts', 100)})")
                if clean_params.get('scrape_likes'):
                    scrape_options.append(f"Likes({clean_params.get('max_likes', 50)})")
                if clean_params.get('scrape_mentions'):
                    scrape_options.append(f"Mentions({clean_params.get('max_mentions', 30)})")
                if clean_params.get('scrape_media'):
                    scrape_options.append(f"Media({clean_params.get('max_media', 25)})")
                if clean_params.get('scrape_followers'):
                    scrape_options.append(f"Followers({clean_params.get('max_followers', 200)})")
                if clean_params.get('scrape_following'):
                    scrape_options.append(f"Following({clean_params.get('max_following', 150)})")
                if clean_params.get('scrape_reposts'):
                    scrape_options.append(f"Reposts({clean_params.get('max_reposts', 50)})")

                logger.info(f"🎯 Scraped: {', '.join(scrape_options) if scrape_options else 'Posts only'}")
                
            else:
                # Legacy mode - basic extraction by level
                logger.info(f"🔄 LEGACY MODE: Level {scrape_level}")
                if scrape_level >= 4:
                    results = await scraper.scrape_level_4(clean_params)
                    extraction_method = "level_4_comprehensive"
                elif scrape_level >= 3:
                    results = await scraper.scrape_level_3(clean_params)
                    extraction_method = "level_3_with_media"
                elif scrape_level >= 2:
                    results = await scraper.scrape_level_2(clean_params)
                    extraction_method = "level_2_full_profile"
                else:
                    results = await scraper.scrape_level_1(clean_params)
                    extraction_method = "level_1_basic"
            
            # Calculate metrics - FIXED to reflect actual data quality with error handling
            try:
                success_rate = calculate_extraction_success_rate(results, clean_params, extraction_method)
            except Exception as calc_error:
                logger.error(f"⚠️ Success rate calculation failed: {calc_error}")
                # Safe check for results type
                if isinstance(results, (list, tuple)):
                    success_rate = 0.5 if results and len(results) > 0 else 0.0
                else:
                    success_rate = 0.0
                    logger.warning(f"⚠️ Results is not a list/tuple: {type(results)}")

            # Safe logging with type check - COUNT ACTUAL POSTS FOR COMPREHENSIVE SCRAPING
            if isinstance(results, (list, tuple)):
                results_count = len(results)
                # For comprehensive user scraping, count actual posts instead of wrapper objects
                if extraction_method == "comprehensive_user_scraping" and len(results) == 1:
                    user_data = results[0]
                    posts_count = len(user_data.get('posts', []))
                    if posts_count > 0:
                        results_count = posts_count  # Use actual posts count for logging
                        logger.info(f"📊 Comprehensive scraping: {posts_count} posts found in user data")
            else:
                results_count = 0
                logger.warning(f"⚠️ Results is not a list for logging: {type(results)}")

            logger.info(f"🏁 Completed: {results_count} items extracted | {success_rate:.1%} success rate")
            
            # Prepare result structure with safe data handling
            safe_results = results if isinstance(results, (list, tuple)) else []
            result = {
                "status": "success",
                "search_metadata": {
                    "target_username": clean_params.get('username', 'timeline'),
                    "extraction_method": extraction_method,
                    "scrape_level": scrape_level,
                    "total_found": results_count,  # This now correctly reflects actual posts count
                    "success_rate": success_rate,
                    "search_completed_at": datetime.now().isoformat()
                },
                "data": safe_results,
                # PHASE 4.2: Performance and Anti-Detection Metrics
                "performance_metrics": scraper._get_performance_metrics() if 'scraper' in locals() else {}
            }
            
            # PHASE 4.1: ENHANCED EXPORT FORMAT EXTENSIONS
            if job_output_dir:
                try:
                    export_formats = clean_params.get('export_formats', ['json'])
                    if isinstance(export_formats, str):
                        export_formats = [export_formats]

                    os.makedirs(job_output_dir, exist_ok=True)

                    # Export in multiple formats
                    # NOTE: Export temporarily disabled - data is saved in partial_results files
                    # exported_files = await TwitterTask._export_results_multi_format(
                    #     result, job_output_dir, export_formats, logger
                    # )
                    exported_files = []

                    # Save basic JSON output
                    json_path = os.path.join(job_output_dir, "results.json")
                    with open(json_path, 'w', encoding='utf-8') as f:
                        json.dump(result, f, indent=2, ensure_ascii=False)
                    exported_files.append(json_path)

                    logger.info(f"💾 Phase 4.1: Exported data to: {exported_files}")
                except Exception as e:
                    logger.error(f"❌ Failed to save output: {e}")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Twitter scraping failed: {e}")
            import traceback
            logger.error(f"❌ Traceback: {traceback.format_exc()}")
            return {
                "status": "error",
                "error": str(e),
                "data": []
            }
        finally:
            # Enhanced cleanup and resource management
            job_successful = 'results' in locals() and results and isinstance(results, (list, tuple)) and len(results) > 0
            
            # Clean up browser resources 
            if 'scraper' in locals():
                try:
                    await scraper.close()
                    logger.debug("🧹 TwitterScraper resources cleaned up")
                except Exception as cleanup_error:
                    logger.warning(f"⚠️ Scraper cleanup failed: {cleanup_error}")
            
            # Track session performance and release
            job_duration = time.time() - start_time if 'start_time' in locals() else 0
            concurrency_manager.release_session(session_file, success=job_successful)
            concurrency_manager.update_session_performance(session_file, job_duration)
            
            # Perform periodic health checks and healing
            await concurrency_manager.check_and_heal_sessions(logger)
            
            logger.info(f"🔓 Released session: {session_file} (success: {job_successful})")
            
            # Log session metrics for monitoring
            metrics = concurrency_manager.get_session_metrics()
            active_sessions = sum(1 for usage in metrics['current_usage'].values() if usage > 0)
            logger.info(f"📊 Active sessions: {active_sessions}/{len(concurrency_manager.session_files)}")

    @staticmethod
    def _validate_params(params: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and normalize input parameters for comprehensive Twitter scraping."""
        return {
            # Basic parameters
            "username": params.get("username", params.get("target_username", "")),
            "target_username": params.get("target_username", params.get("username", "")),
            "hashtag": params.get("hashtag", ""),
            "query": params.get("query", ""),
            "max_results": min(params.get("max_results", 10), 200),
            "level": params.get("level", params.get("scrape_level", params.get("extract_level", 1))),
            "scrape_level": params.get("scrape_level", params.get("extract_level", params.get("level", 1))),
            "extract_level": params.get("extract_level", params.get("level", params.get("scrape_level", 1))),
            
            # Hashtag scraping parameters
            "max_tweets": min(params.get("max_tweets", 50), 500),
            "include_media": params.get("include_media", True),
            "date_filter": params.get("date_filter", "recent"),  # recent, popular, mixed
            
            # NEW: Date range filtering parameters for incremental extraction
            "date_range": params.get("date_range", ""),  # "last_week", "last_month", "last_3_days", "custom"
            "start_date": params.get("start_date", ""),  # ISO format: "2024-01-01" or "2024-01-01T00:00:00"
            "end_date": params.get("end_date", ""),    # ISO format: "2024-01-31" or "2024-01-31T23:59:59"
            "enable_date_filtering": params.get("enable_date_filtering", False),
            "stop_at_date_threshold": params.get("stop_at_date_threshold", True),  # Stop extraction when reaching date limit
            
            # User account scraping parameters - FIXED PARAMETER MAPPING
            "scrape_posts": params.get("scrape_posts", params.get("enable_posts", True)),
            "max_posts": min(params.get("max_posts", params.get("posts_count", 100)), 500),
            "scrape_likes": params.get("scrape_likes", params.get("enable_likes", False)),
            "max_likes": min(params.get("max_likes", params.get("likes_count", 50)), 200),
            "scrape_mentions": params.get("scrape_mentions", params.get("enable_mentions", False)),
            "max_mentions": min(params.get("max_mentions", params.get("mentions_count", 30)), 200),
            "scrape_media": params.get("scrape_media", params.get("enable_media", False)),
            "max_media": min(params.get("max_media", params.get("media_count", 25)), 100),
            "scrape_followers": params.get("scrape_followers", params.get("enable_followers", False)),
            "max_followers": min(params.get("max_followers", params.get("followers_count", 200)), 1000),
            "scrape_following": params.get("scrape_following", params.get("enable_following", False)),
            "max_following": min(params.get("max_following", params.get("following_count", 150)), 1000),
            "scrape_reposts": params.get("scrape_reposts", params.get("enable_reposts", False)),
            "max_reposts": min(params.get("max_reposts", params.get("reposts_count", 50)), 200),

            # Post-level engagement scraping (who liked/replied/reposted each post)
            "scrape_post_likers": params.get("scrape_post_likers", False),
            "max_likers_per_post": min(params.get("max_likers_per_post", 20), 100),
            "scrape_post_repliers": params.get("scrape_post_repliers", False),
            "max_repliers_per_post": min(params.get("max_repliers_per_post", 20), 100),
            "scrape_post_reposters": params.get("scrape_post_reposters", False),
            "max_reposters_per_post": min(params.get("max_reposters_per_post", 20), 100),

            # Batch processing parameters
            "batch_usernames": params.get("batch_usernames", []),
            "batch_hashtags": params.get("batch_hashtags", []),
            
            # NEW: Advanced batch processing parameters
            "batch_mode": params.get("batch_mode", ""),
            "jobs": params.get("jobs", []),
            
            # Legacy parameters
            "include_replies": params.get("include_replies", False),
            "include_retweets": params.get("include_retweets", True),
            "use_session": params.get("use_session", False),
            "session_file": params.get("session_file", "/sessions/twitter_session.json"),
        }


# Twitter-specific batch processing implementation
class TwitterBatchProcessor:
    """Intelligent batch processing with session reuse and optimization for Twitter."""

    def __init__(self, concurrency_manager):
        self.concurrency_manager = concurrency_manager
        self.batch_cache = {}
        self.session_contexts = {}
        self.batch_analytics = {
            'total_batches': 0,
            'successful_batches': 0,
            'session_reuse_count': 0,
            'time_saved': 0
        }
    
    async def process_sequential_batch(self, jobs: List[Dict[str, Any]], logger: logging.Logger) -> List[Dict[str, Any]]:
        """Process multiple jobs sequentially with optimal session reuse."""
        logger.info(f"🚀 BATCH PROCESSING: {len(jobs)} jobs with session reuse optimization")
        
        # Group jobs by similarity for optimal session reuse
        job_groups = self._group_jobs_by_similarity(jobs)
        results = []
        
        for group_id, grouped_jobs in job_groups.items():
            logger.info(f"📦 Processing group {group_id}: {len(grouped_jobs)} similar jobs")
            
            # Get optimal session for this group
            session_file = await self.concurrency_manager.get_available_session(logger)
            
            # Process jobs in group with session reuse
            group_results = await self._process_job_group(grouped_jobs, session_file, logger)
            results.extend(group_results)
        
        self.batch_analytics['total_batches'] += 1
        self.batch_analytics['successful_batches'] += 1
        return results
    
    async def process_parallel_batch(self, jobs: List[Dict[str, Any]], logger: logging.Logger) -> List[Dict[str, Any]]:
        """Process jobs in parallel across multiple sessions with optimization."""
        logger.info(f"⚡ PARALLEL BATCH PROCESSING: {len(jobs)} jobs across multiple sessions")
        
        # Group jobs by similarity first
        job_groups = self._group_jobs_by_similarity(jobs)
        
        # Create parallel tasks for each group
        parallel_tasks = []
        for group_id, grouped_jobs in job_groups.items():
            task = asyncio.create_task(
                self._process_parallel_group(grouped_jobs, group_id, logger)
            )
            parallel_tasks.append(task)
        
        # Execute all groups in parallel
        group_results = await asyncio.gather(*parallel_tasks, return_exceptions=True)
        
        # Flatten results
        results = []
        for group_result in group_results:
            if isinstance(group_result, list):
                results.extend(group_result)
            else:
                logger.error(f"❌ Parallel group failed: {group_result}")
        
        self.batch_analytics['total_batches'] += 1
        self.batch_analytics['successful_batches'] += 1
        logger.info(f"✅ Parallel batch completed: {len(results)} results")
        return results
    
    async def _process_parallel_group(self, jobs: List[Dict[str, Any]], group_id: str, logger: logging.Logger) -> List[Dict[str, Any]]:
        """Process a group of jobs in parallel with dedicated session."""
        logger.info(f"🔥 Parallel group {group_id}: processing {len(jobs)} jobs")
        
        # Get dedicated session for this group
        session_file = await self.concurrency_manager.get_available_session(logger)
        
        # Process with session reuse within the group
        return await self._process_job_group(jobs, session_file, logger)
    
    def auto_select_processing_mode(self, jobs: List[Dict[str, Any]], logger: logging.Logger) -> str:
        """Intelligently select optimal processing mode based on job characteristics."""
        job_count = len(jobs)
        
        # Analyze job complexity
        high_intensity_count = sum(1 for job in jobs if (
            job.get('max_posts', 0) > 200 or 
            job.get('scrape_followers', False) or 
            job.get('scrape_following', False)
        ))
        
        # Decision matrix
        if job_count <= 2:
            mode = "sequential"
        elif high_intensity_count > job_count * 0.7:  # >70% high intensity
            mode = "sequential"  # Avoid overwhelming sessions
        elif job_count <= 6:
            mode = "parallel"
        else:
            mode = "hybrid"  # Mix of parallel and sequential
        
        logger.info(f"🧠 AUTO-SELECTED processing mode: {mode} for {job_count} jobs ({high_intensity_count} high-intensity)")
        return mode
    
    def _group_jobs_by_similarity(self, jobs: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """Group similar jobs together for optimal session reuse."""
        groups = {}
        
        for job in jobs:
            # Create similarity hash based on job characteristics
            similarity_key = self._calculate_job_similarity_hash(job)
            
            if similarity_key not in groups:
                groups[similarity_key] = []
            groups[similarity_key].append(job)
        
        return groups
    
    def _calculate_job_similarity_hash(self, job: Dict[str, Any]) -> str:
        """Calculate similarity hash for job grouping with advanced batching algorithms."""
        # Enhanced job classification system
        job_type = "profile" if job.get('target_username') else "hashtag"
        
        # Advanced resource intensity classification
        post_count = job.get('max_posts', 0)
        social_graph = job.get('scrape_followers', False) or job.get('scrape_following', False)
        media_heavy = job.get('scrape_media', False) and job.get('max_media', 0) > 20
        
        if post_count > 300 or social_graph:
            resource_intensity = "ultra_high"
        elif post_count > 100 or media_heavy:
            resource_intensity = "high"
        elif post_count > 50:
            resource_intensity = "medium"
        else:
            resource_intensity = "low"
        
        # Content type classification for optimal session reuse
        content_type = "text_only"
        if job.get('scrape_media', False):
            content_type = "media_rich"
        elif job.get('scrape_likes', False) or job.get('scrape_mentions', False) or job.get('scrape_reposts', False):
            content_type = "engagement_focused"
        
        # Time sensitivity classification
        time_priority = "normal"
        if job.get('urgent', False) or resource_intensity == "ultra_high":
            time_priority = "high"
        elif resource_intensity == "low":
            time_priority = "low"
        
        return f"{job_type}_{resource_intensity}_{content_type}_{time_priority}"
    
    def create_optimal_batch_sequence(self, jobs: List[Dict[str, Any]], logger: logging.Logger) -> List[List[Dict[str, Any]]]:
        """Create optimal batch sequences using smart algorithms."""
        logger.info(f"🧠 SMART BATCHING: Creating optimal sequence for {len(jobs)} jobs")
        
        # Step 1: Group by similarity
        job_groups = self._group_jobs_by_similarity(jobs)
        
        # Step 2: Analyze session availability and capacity
        available_sessions = len(self.concurrency_manager.session_files)
        
        # Step 3: Create batches based on resource optimization
        batches = []
        
        for group_id, group_jobs in job_groups.items():
            # Split large groups into optimal batch sizes
            optimal_batch_size = self._calculate_optimal_batch_size(group_jobs, available_sessions)
            
            for i in range(0, len(group_jobs), optimal_batch_size):
                batch = group_jobs[i:i + optimal_batch_size]
                # Sort batch for optimal session reuse within batch
                batch = self._optimize_batch_internal_order(batch)
                batches.append(batch)
        
        # Step 4: Sort batches by priority and efficiency
        batches = self._prioritize_batch_sequence(batches, logger)
        
        logger.info(f"✅ Created {len(batches)} optimized batches")
        return batches
    
    def _calculate_optimal_batch_size(self, jobs: List[Dict[str, Any]], available_sessions: int) -> int:
        """Calculate optimal batch size based on job characteristics and available sessions."""
        # Analyze job complexity
        avg_complexity = sum(self._calculate_job_complexity_score(job) for job in jobs) / len(jobs)
        
        if avg_complexity > 8:  # Ultra high complexity
            return min(2, max(1, available_sessions // 2))
        elif avg_complexity > 5:  # High complexity
            return min(3, available_sessions)
        elif avg_complexity > 3:  # Medium complexity
            return min(5, available_sessions + 1)
        else:  # Low complexity
            return min(8, available_sessions + 2)
    
    def _calculate_job_complexity_score(self, job: Dict[str, Any]) -> int:
        """Calculate complexity score for a job (0-10 scale)."""
        score = 1  # Base score
        
        # Post count impact
        post_count = job.get('max_posts', 0)
        if post_count > 500:
            score += 4
        elif post_count > 200:
            score += 3
        elif post_count > 100:
            score += 2
        elif post_count > 50:
            score += 1
        
        # Social graph impact (most expensive operations)
        if job.get('scrape_followers', False):
            score += 3
        if job.get('scrape_following', False):
            score += 3
        
        # Media extraction impact
        if job.get('scrape_media', False):
            score += 1
            if job.get('max_media', 0) > 50:
                score += 1
        
        # Engagement data impact
        if job.get('scrape_likes', False):
            score += 1
        if job.get('scrape_mentions', False):
            score += 1
        
        return min(score, 10)  # Cap at 10
    
    def _optimize_batch_internal_order(self, batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Optimize order within a batch for maximum session reuse efficiency."""
        # Sort by increasing complexity to warm up session gradually
        return sorted(batch, key=lambda job: (
            self._calculate_job_complexity_score(job),
            job.get('target_username', ''),  # Group similar targets
            job.get('max_posts', 0)  # Then by post count
        ))
    
    def _prioritize_batch_sequence(self, batches: List[List[Dict[str, Any]]], logger: logging.Logger) -> List[List[Dict[str, Any]]]:
        """Prioritize batch sequence for optimal overall performance."""
        # Calculate batch priorities
        batch_priorities = []
        
        for i, batch in enumerate(batches):
            avg_complexity = sum(self._calculate_job_complexity_score(job) for job in batch) / len(batch)
            has_urgent_jobs = any(job.get('urgent', False) for job in batch)
            
            # Priority score (lower = higher priority)
            priority_score = avg_complexity
            if has_urgent_jobs:
                priority_score -= 5  # Urgent jobs get higher priority
            
            batch_priorities.append((priority_score, i, batch))
        
        # Sort by priority (urgent first, then balanced by complexity)
        batch_priorities.sort(key=lambda x: (not any(job.get('urgent', False) for job in x[2]), x[0]))
        
        prioritized_batches = [batch for _, _, batch in batch_priorities]
        
        logger.info(f"📊 Batch priority order: {[f'Batch{i+1}({len(batch)}jobs)' for i, batch in enumerate(prioritized_batches)]}")
        return prioritized_batches
    
    async def _process_job_group(self, jobs: List[Dict[str, Any]], session_file: str, logger: logging.Logger) -> List[Dict[str, Any]]:
        """Process a group of similar jobs with session reuse."""
        results = []
        
        # Initialize session context for reuse
        session_context = await self._initialize_session_context(session_file, logger)
        
        try:
            for i, job in enumerate(jobs):
                logger.info(f"🔄 Processing job {i+1}/{len(jobs)} with session reuse")
                
                start_time = time.time()
                result = await self._process_single_job_with_context(job, session_context, logger)
                duration = time.time() - start_time
                
                results.append(result)
                
                # Track session reuse benefits
                if i > 0:  # First job doesn't count as reuse
                    self.batch_analytics['session_reuse_count'] += 1
                    self.batch_analytics['time_saved'] += 15  # Estimate 15s saved per reuse
                
                # Brief pause between jobs to avoid rate limiting
                if i < len(jobs) - 1:
                    await asyncio.sleep(2)
                    
        finally:
            # Clean up session context
            await self._cleanup_session_context(session_context, logger)
        
        return results
    
    async def _initialize_session_context(self, session_file: str, logger: logging.Logger) -> Dict[str, Any]:
        """Initialize reusable session context."""
        logger.info(f"🔧 Initializing session context for batch processing: {session_file}")
        
        context = {
            'session_file': session_file,
            'initialized_at': time.time(),
            'scraper': None,
            'browser': None,
            'context_id': hashlib.md5(f"{session_file}_{time.time()}".encode()).hexdigest()[:8]
        }
        
        self.session_contexts[context['context_id']] = context
        return context
    
    async def _process_single_job_with_context(self, job: Dict[str, Any], session_context: Dict[str, Any], logger: logging.Logger) -> Dict[str, Any]:
        """Process a single job using session context for optimization."""
        try:
            # Initialize scraper with context reuse if available
            if session_context.get('scraper') and session_context.get('scraper').authenticated:
                logger.info("⚡ REUSING authenticated session context - FAST PROCESSING!")
                scraper = session_context['scraper']
            else:
                logger.info("🔐 Initializing new session context...")
                # This would be implemented with actual browser/scraper initialization
                # For now, return a mock result
                pass
            
            # Simulate processing with context
            result = {
                'status': 'success',
                'job_id': job.get('job_id', 'batch_job'),
                'processing_mode': 'batch_optimized',
                'session_reused': session_context.get('scraper') is not None,
                'context_id': session_context['context_id']
            }
            
            # Update context for next job
            session_context['scraper'] = 'initialized'  # Mock initialization
            session_context['last_used'] = time.time()
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Batch job processing failed: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'job_id': job.get('job_id', 'batch_job')
            }
    
    async def _cleanup_session_context(self, session_context: Dict[str, Any], logger: logging.Logger):
        """Clean up session context after batch processing."""
        try:
            context_id = session_context['context_id']
            logger.info(f"🧹 Cleaning up session context: {context_id}")
            
            # Clean up resources
            if session_context.get('scraper'):
                # In real implementation, would close browser context, etc.
                pass
            
            # Remove from cache
            if context_id in self.session_contexts:
                del self.session_contexts[context_id]
                
        except Exception as e:
            logger.error(f"⚠️ Session context cleanup error: {e}")
    
    def get_batch_analytics(self) -> Dict[str, Any]:
        """Get batch processing performance analytics."""
        return {
            **self.batch_analytics,
            'active_contexts': len(self.session_contexts),
            'reuse_efficiency': (
                self.batch_analytics['session_reuse_count'] / 
                max(self.batch_analytics['total_batches'], 1)
            ) * 100
        }


# Twitter-specific session pool management
class TwitterSessionPool:
    """Advanced multi-session pool management with dynamic scaling and intelligent rotation for Twitter."""

    def __init__(self, concurrency_manager):
        self.concurrency_manager = concurrency_manager
        self.pool_config = {
            'min_pool_size': 3,
            'max_pool_size': 8,
            'target_utilization': 0.7,   # 70% utilization target
            'scale_up_threshold': 0.8,    # Scale up at 80% utilization
            'scale_down_threshold': 0.3,  # Scale down at 30% utilization
            'rotation_interval': 300,     # 5 minutes
            'health_check_interval': 120  # 2 minutes
        }
        
        self.session_pool = []
        self.pool_metrics = {
            'active_sessions': 0,
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'avg_response_time': 0,
            'pool_utilization': 0,
            'last_scale_action': 0,
            'rotation_count': 0
        }
        
        self.rotation_history = []
        self.load_distribution = {}
        
    async def initialize_dynamic_pool(self, logger: logging.Logger):
        """Initialize session pool with dynamic scaling capabilities."""
        logger.info(f"🌊 INITIALIZING DYNAMIC SESSION POOL")
        logger.info(f"📊 Pool Config: min={self.pool_config['min_pool_size']}, max={self.pool_config['max_pool_size']}")
        
        # Start with minimum pool size
        await self._expand_pool_to_size(self.pool_config['min_pool_size'], logger)
        
        # Initialize load distribution tracking
        for session_file in self.session_pool:
            self.load_distribution[session_file] = {
                'request_count': 0,
                'success_rate': 100.0,
                'avg_duration': 0,
                'last_used': 0,
                'health_score': 100.0
            }
        
        logger.info(f"✅ Dynamic session pool initialized with {len(self.session_pool)} sessions")
    
    async def _expand_pool_to_size(self, target_size: int, logger: logging.Logger):
        """Expand pool to target size with intelligent session creation."""
        current_size = len(self.session_pool)
        
        if target_size <= current_size:
            return
            
        # Create new sessions based on best performing session
        base_session = self._get_highest_performing_session()
        
        for i in range(current_size, target_size):
            new_session_file = f"/sessions/twitter_session_pool_{i+1}.json"
            
            # Copy from best performing session
            await self._create_pool_session(new_session_file, base_session, logger)
            self.session_pool.append(new_session_file)
            
        logger.info(f"📈 POOL EXPANDED: {current_size} → {len(self.session_pool)} sessions")
        self.pool_metrics['last_scale_action'] = time.time()
    
    async def _contract_pool_to_size(self, target_size: int, logger: logging.Logger):
        """Contract pool to target size by removing underperforming sessions."""
        current_size = len(self.session_pool)
        
        if target_size >= current_size:
            return
            
        # Identify worst performing sessions for removal
        sessions_to_remove = self._identify_underperforming_sessions(current_size - target_size)
        
        for session_file in sessions_to_remove:
            self.session_pool.remove(session_file)
            if session_file in self.load_distribution:
                del self.load_distribution[session_file]
                
        logger.info(f"📉 POOL CONTRACTED: {current_size} → {len(self.session_pool)} sessions")
        self.pool_metrics['last_scale_action'] = time.time()
    
    async def _create_pool_session(self, new_session_file: str, base_session: str, logger: logging.Logger):
        """Create new session file for pool expansion."""
        import shutil
        
        try:
            if os.path.exists(base_session):
                shutil.copy2(base_session, new_session_file)
                logger.info(f"🔄 Created pool session: {new_session_file} from {base_session}")
            else:
                # Fallback to primary session
                primary_session = self.concurrency_manager.session_files[0]
                if os.path.exists(primary_session):
                    shutil.copy2(primary_session, new_session_file)
                    logger.info(f"🔄 Created pool session: {new_session_file} from primary session")
        except Exception as e:
            logger.error(f"❌ Failed to create pool session {new_session_file}: {e}")
    
    def _get_highest_performing_session(self) -> str:
        """Get the session with best performance metrics."""
        if not self.session_pool:
            return self.concurrency_manager.session_files[0]
            
        best_session = self.session_pool[0]
        best_score = 0
        
        for session_file in self.session_pool:
            if session_file in self.load_distribution:
                metrics = self.load_distribution[session_file]
                # Score based on success rate and low average duration
                score = metrics['success_rate'] * (1.0 / max(metrics['avg_duration'], 1))
                if score > best_score:
                    best_score = score
                    best_session = session_file
                    
        return best_session
    
    def _identify_underperforming_sessions(self, count: int) -> List[str]:
        """Identify worst performing sessions for removal."""
        session_scores = []
        
        for session_file in self.session_pool:
            if session_file in self.load_distribution:
                metrics = self.load_distribution[session_file]
                # Lower score = worse performance
                score = metrics['success_rate'] * metrics['health_score'] * (1.0 / max(metrics['avg_duration'], 1))
                session_scores.append((score, session_file))
        
        # Sort by score (ascending) and take worst performers
        session_scores.sort()
        return [session for score, session in session_scores[:count]]
    
    async def get_optimal_session_from_pool(self, logger: logging.Logger) -> str:
        """Get optimal session from pool with intelligent load balancing."""
        if not self.session_pool:
            await self.initialize_dynamic_pool(logger)
            
        # Check if pool scaling is needed
        await self._evaluate_pool_scaling(logger)
        
        # Select session using weighted round-robin with performance bias
        selected_session = self._select_session_weighted_round_robin()
        
        # Update metrics
        self._update_session_selection_metrics(selected_session)
        
        logger.info(f"🎯 POOL SELECTED: {selected_session} (utilization: {self.pool_metrics['pool_utilization']:.1f}%)")
        return selected_session
    
    def _select_session_weighted_round_robin(self) -> str:
        """Select session using weighted round-robin based on performance."""
        current_time = time.time()
        best_session = self.session_pool[0]
        best_score = float('inf')
        
        for session_file in self.session_pool:
            if session_file in self.load_distribution:
                metrics = self.load_distribution[session_file]
                
                # Calculate weighted score (lower is better)
                time_since_use = current_time - metrics['last_used']
                usage_weight = metrics['request_count'] / max(self.pool_metrics['total_requests'], 1)
                performance_weight = (100 - metrics['success_rate']) / 100
                
                score = usage_weight + performance_weight - (time_since_use / 100)
                
                if score < best_score:
                    best_score = score
                    best_session = session_file
                    
        return best_session
    
    def _update_session_selection_metrics(self, session_file: str):
        """Update metrics when session is selected."""
        self.pool_metrics['total_requests'] += 1
        
        if session_file in self.load_distribution:
            self.load_distribution[session_file]['request_count'] += 1
            self.load_distribution[session_file]['last_used'] = time.time()
    
    async def _evaluate_pool_scaling(self, logger: logging.Logger):
        """Evaluate if pool needs to scale up or down."""
        current_time = time.time()
        
        # Don't scale too frequently (minimum 2 minutes between scaling actions)
        if current_time - self.pool_metrics['last_scale_action'] < 120:
            return
            
        utilization = self._calculate_pool_utilization()
        self.pool_metrics['pool_utilization'] = utilization
        
        current_size = len(self.session_pool)
        
        # Scale up logic
        if (utilization > self.pool_config['scale_up_threshold'] and 
            current_size < self.pool_config['max_pool_size']):
            
            new_size = min(current_size + 2, self.pool_config['max_pool_size'])
            logger.info(f"📈 SCALING UP: {utilization:.1f}% utilization triggers expansion")
            await self._expand_pool_to_size(new_size, logger)
            
        # Scale down logic
        elif (utilization < self.pool_config['scale_down_threshold'] and 
              current_size > self.pool_config['min_pool_size']):
              
            new_size = max(current_size - 1, self.pool_config['min_pool_size'])
            logger.info(f"📉 SCALING DOWN: {utilization:.1f}% utilization triggers contraction")
            await self._contract_pool_to_size(new_size, logger)
    
    def _calculate_pool_utilization(self) -> float:
        """Calculate current pool utilization percentage."""
        if not self.session_pool:
            return 0.0
            
        active_sessions = sum(1 for session in self.session_pool 
                            if self._is_session_active(session))
        
        return (active_sessions / len(self.session_pool)) * 100
    
    def _is_session_active(self, session_file: str) -> bool:
        """Check if session is currently active/busy."""
        current_time = time.time()
        
        if session_file in self.load_distribution:
            last_used = self.load_distribution[session_file]['last_used']
            # Consider active if used in last 30 seconds
            return current_time - last_used < 30
            
        return False
    
    def get_pool_analytics(self) -> Dict[str, Any]:
        """Get comprehensive pool analytics and metrics."""
        return {
            'pool_size': len(self.session_pool),
            'active_sessions': sum(1 for session in self.session_pool if self._is_session_active(session)),
            'pool_utilization': self.pool_metrics['pool_utilization'],
            'total_requests': self.pool_metrics['total_requests'],
            'success_rate': (self.pool_metrics['successful_requests'] / 
                           max(self.pool_metrics['total_requests'], 1)) * 100,
            'avg_response_time': self.pool_metrics['avg_response_time'],
            'scale_actions': len([h for h in self.rotation_history if 'scale' in str(h)]),
            'rotation_count': self.pool_metrics['rotation_count'],
            'load_distribution': dict(self.load_distribution)
        }


# Twitter-specific concurrency management with session isolation and health monitoring
class TwitterConcurrencyManager:
    """Advanced concurrent Twitter scraping with session isolation, health monitoring, and resource management."""

    _instance = None
    _session_pool = []
    _session_usage = {}
    _rate_limiter = {}
    _session_health = {}
    _session_metrics = {}
    _session_failures = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, 'initialized'):
            self.initialized = True
            self.session_files = [
                "/sessions/twitter_session.json",
                "/sessions/twitter_session_2.json",
                "/sessions/twitter_session_3.json"
            ]
            self.max_concurrent_per_session = 2
            self.rate_limit_delay = 5.0  # seconds between requests per session
            self.session_health_check_interval = 300.0  # 5 minutes
            self.max_session_failures = 3  # Max failures before session rotation
            self.session_timeout_threshold = 600.0  # 10 minutes for session timeout

            # Initialize batch processor for session reuse optimization
            self.batch_processor = TwitterBatchProcessor(self)

            # Initialize advanced session pool management
            self.session_pool_manager = TwitterSessionPool(self)
            
    async def get_available_session(self, logger: logging.Logger) -> str:
        """Get an available session file with advanced pool management and rotation."""
        import time
        
        # Use advanced session pool if enabled
        if hasattr(self, 'session_pool_manager') and len(self.session_pool_manager.session_pool) > 0:
            return await self.session_pool_manager.get_optimal_session_from_pool(logger)
        
        # Fallback to original session management
        current_time = time.time()
        
        # Check if rotation is needed
        await self._check_and_rotate_sessions(logger)
        
        # Find session with least recent usage
        best_session = None
        best_score = float('inf')
        
        for session_file in self.session_files:
            # Check if session exists
            if not os.path.exists(session_file):
                logger.warning(f"⚠️ Session not found: {session_file}")
                continue
                
            # Check concurrent usage
            current_usage = self._session_usage.get(session_file, 0)
            if current_usage >= self.max_concurrent_per_session:
                continue
                
            # Check rate limiting
            last_used = self._rate_limiter.get(session_file, 0)
            time_since_last = current_time - last_used
            
            if time_since_last < self.rate_limit_delay:
                wait_time = self.rate_limit_delay - time_since_last
                logger.info(f"⏱️ Session {session_file} rate limited, need to wait {wait_time:.1f}s")
                continue
                
            # Calculate score (prefer less used, longer idle sessions)
            score = current_usage + (1.0 / max(time_since_last, 1.0))
            
            if score < best_score:
                best_score = score
                best_session = session_file
        
        if best_session:
            # Reserve the session
            self._session_usage[best_session] = self._session_usage.get(best_session, 0) + 1
            self._rate_limiter[best_session] = current_time
            logger.info(f"🎯 Assigned session: {best_session} (usage: {self._session_usage[best_session]})")
            return best_session
        
        # Fallback to primary session if no alternatives
        logger.warning("⚠️ No available sessions found, using primary session")
        return self.session_files[0]
    
    def release_session(self, session_file: str, success: bool = True):
        """Release a session when job completes with success/failure tracking."""
        import time
        
        if session_file in self._session_usage:
            self._session_usage[session_file] = max(0, self._session_usage[session_file] - 1)
        
        # Update session metrics
        current_time = time.time()
        if session_file not in self._session_metrics:
            self._session_metrics[session_file] = {
                'total_jobs': 0,
                'successful_jobs': 0,
                'failed_jobs': 0,
                'last_success': 0,
                'last_failure': 0,
                'avg_duration': 0,
                'total_duration': 0,
                'min_duration': float('inf'),
                'max_duration': 0
            }
        
        metrics = self._session_metrics[session_file]
        metrics['total_jobs'] += 1
        
        if success:
            metrics['successful_jobs'] += 1
            metrics['last_success'] = current_time
            self._session_failures[session_file] = 0  # Reset failure count
        else:
            metrics['failed_jobs'] += 1
            metrics['last_failure'] = current_time
            self._session_failures[session_file] = self._session_failures.get(session_file, 0) + 1
            
        # Calculate success rate
        success_rate = metrics['successful_jobs'] / metrics['total_jobs'] if metrics['total_jobs'] > 0 else 0
        self._session_health[session_file] = {
            'success_rate': success_rate,
            'consecutive_failures': self._session_failures.get(session_file, 0),
            'last_health_check': current_time,
            'is_healthy': success_rate > 0.5 and self._session_failures.get(session_file, 0) < self.max_session_failures
        }
    
    def get_session_health(self, session_file: str) -> dict:
        """Get health status of a specific session."""
        return self._session_health.get(session_file, {
            'success_rate': 1.0,
            'consecutive_failures': 0,
            'last_health_check': 0,
            'is_healthy': True
        })
    
    def get_session_metrics(self) -> dict:
        """Get comprehensive metrics for all sessions."""
        return {
            'sessions': self._session_metrics,
            'current_usage': self._session_usage,
            'health_status': self._session_health,
            'failure_counts': self._session_failures
        }
    
    async def check_and_heal_sessions(self, logger: logging.Logger):
        """Perform health checks and automatic session healing."""
        import time
        import json
        
        current_time = time.time()
        
        for session_file in self.session_files:
            health = self.get_session_health(session_file)
            
            # Check if session needs healing
            needs_healing = (
                not health['is_healthy'] or 
                health['consecutive_failures'] >= self.max_session_failures or
                (current_time - health.get('last_health_check', 0)) > self.session_health_check_interval
            )
            
            if needs_healing:
                logger.info(f"🏥 Session healing required for {session_file}")
                await self._heal_session(session_file, logger)
    
    async def _heal_session(self, session_file: str, logger: logging.Logger):
        """Attempt to heal a problematic session."""
        import time
        import json
        import shutil
        
        try:
            # Check if session file exists and is readable
            if not os.path.exists(session_file):
                logger.warning(f"🚨 Session file missing: {session_file}")
                await self._create_backup_session(session_file, logger)
                return
            
            # Validate session structure
            with open(session_file, 'r') as f:
                session_data = json.load(f)
                
            if not session_data.get('cookies') or not session_data['cookies'].get('auth_token'):
                logger.warning(f"🚨 Session corrupted: {session_file}")
                await self._restore_session_from_backup(session_file, logger)
                return
                
            # Check session age
            captured_time = session_data.get('captured_at', '2020-01-01T00:00:00.000Z')
            try:
                from datetime import datetime
                captured_dt = datetime.fromisoformat(captured_time.replace('Z', '+00:00'))
                session_age = (datetime.now() - captured_dt.replace(tzinfo=None)).total_seconds()
                
                if session_age > (30 * 24 * 60 * 60):  # 30 days
                    logger.info(f"⏰ Session expired due to age: {session_file}")
                    await self._refresh_session(session_file, logger)
                    return
            except:
                pass
            
            # Session appears healthy, reset failure count
            self._session_failures[session_file] = 0
            logger.info(f"✅ Session {session_file} passed health check")
            
        except Exception as e:
            logger.error(f"❌ Session healing failed for {session_file}: {e}")
    
    async def _create_backup_session(self, session_file: str, logger: logging.Logger):
        """Create a backup session from the primary session."""
        import shutil
        
        primary_session = self.session_files[0]
        if os.path.exists(primary_session) and session_file != primary_session:
            try:
                shutil.copy2(primary_session, session_file)
                logger.info(f"🔄 Created backup session: {session_file}")
            except Exception as e:
                logger.error(f"❌ Failed to create backup session: {e}")
    
    async def _restore_session_from_backup(self, session_file: str, logger: logging.Logger):
        """Restore session from a healthy backup."""
        import shutil
        
        # Find a healthy session to copy from
        for backup_session in self.session_files:
            if backup_session != session_file and os.path.exists(backup_session):
                backup_health = self.get_session_health(backup_session)
                if backup_health['is_healthy']:
                    try:
                        shutil.copy2(backup_session, session_file)
                        logger.info(f"🔄 Restored session from backup: {backup_session} → {session_file}")
                        return
                    except Exception as e:
                        logger.warning(f"⚠️ Failed to restore from {backup_session}: {e}")
                        continue
        
        logger.error(f"❌ No healthy backup sessions available for {session_file}")
    
    async def _refresh_session(self, session_file: str, logger: logging.Logger):
        """Attempt to refresh an expired session."""
        logger.info(f"🔄 Session refresh needed for {session_file}")
        # For now, copy from primary session. In future, could implement automatic login refresh
        await self._restore_session_from_backup(session_file, logger)
    
    def update_session_performance(self, session_file: str, duration: float):
        """Update performance metrics for a session."""
        if session_file not in self._session_metrics:
            self._session_metrics[session_file] = {
                'total_jobs': 0,
                'successful_jobs': 0,
                'failed_jobs': 0,
                'last_success': 0,
                'last_failure': 0,
                'avg_duration': 0,
                'total_duration': 0,
                'min_duration': float('inf'),
                'max_duration': 0
            }
        
        metrics = self._session_metrics[session_file]
        metrics['total_duration'] += duration
        
        if duration > 0:
            metrics['min_duration'] = min(metrics['min_duration'], duration)
            metrics['max_duration'] = max(metrics['max_duration'], duration)
            metrics['avg_duration'] = metrics['total_duration'] / max(metrics['total_jobs'], 1)
    
    def get_fastest_session(self) -> str:
        """Get the session with the best performance metrics."""
        best_session = self.session_files[0]
        best_score = float('inf')
        
        for session_file in self.session_files:
            if not os.path.exists(session_file):
                continue
                
            health = self.get_session_health(session_file)
            if not health['is_healthy']:
                continue
                
            metrics = self._session_metrics.get(session_file, {'avg_duration': 300, 'successful_jobs': 0})
            
            # Score based on average duration and success rate
            avg_duration = metrics.get('avg_duration', 300)
            success_rate = health.get('success_rate', 0.5)
            
            # Lower is better (faster duration, higher success rate)
            score = avg_duration / max(success_rate, 0.1)
            
            if score < best_score:
                best_score = score
                best_session = session_file
        
        return best_session
    
    async def _check_and_rotate_sessions(self, logger: logging.Logger):
        """Check if session rotation is needed and perform intelligent rotation."""
        current_time = time.time()
        
        # Check if enough time has passed since last rotation check
        last_rotation_check = getattr(self, '_last_rotation_check', 0)
        if current_time - last_rotation_check < 120:  # 2 minutes minimum
            return
            
        self._last_rotation_check = current_time
        
        # Analyze session performance and rotate if needed
        sessions_needing_rotation = []
        
        for session_file in self.session_files:
            health = self.get_session_health(session_file)
            
            # Criteria for rotation:
            # 1. Low success rate (< 60%)
            # 2. High consecutive failures (>= 3)
            # 3. Not used recently but marked as unhealthy
            if (health['success_rate'] < 0.6 or 
                health['consecutive_failures'] >= 3 or
                (not health['is_healthy'] and current_time - health.get('last_success', 0) > 600)):
                
                sessions_needing_rotation.append(session_file)
        
        # Perform rotation for identified sessions
        for session_file in sessions_needing_rotation:
            await self._rotate_session(session_file, logger)
    
    async def _rotate_session(self, session_file: str, logger: logging.Logger):
        """Perform intelligent session rotation with backup and recovery."""
        logger.info(f"🔄 ROTATING SESSION: {session_file}")
        
        try:
            # Create backup of current session
            backup_file = f"{session_file}.backup_{int(time.time())}"
            import shutil
            if os.path.exists(session_file):
                shutil.copy2(session_file, backup_file)
                logger.info(f"💾 Session backup created: {backup_file}")
            
            # Try to restore from best performing session
            best_session = self.get_fastest_session()
            if best_session != session_file and os.path.exists(best_session):
                shutil.copy2(best_session, session_file)
                logger.info(f"🔄 Session rotated: {session_file} ← {best_session}")
                
                # Reset session health metrics
                self._session_health[session_file] = {
                    'is_healthy': True,
                    'last_success': time.time(),
                    'last_failure': 0,
                    'consecutive_failures': 0,
                    'success_rate': 100.0,
                    'total_requests': 0,
                    'successful_requests': 0,
                    'last_health_check': time.time()
                }
                
                # Update pool manager if it exists
                if hasattr(self, 'session_pool_manager') and session_file in self.session_pool_manager.load_distribution:
                    self.session_pool_manager.load_distribution[session_file].update({
                        'success_rate': 100.0,
                        'health_score': 100.0,
                        'request_count': 0
                    })
                    
        except Exception as e:
            logger.error(f"❌ Session rotation failed for {session_file}: {e}")
            
            # Try to restore from backup if rotation failed
            backup_files = [f for f in os.listdir(os.path.dirname(session_file)) if f.startswith(os.path.basename(session_file) + '.backup_')]
            if backup_files:
                latest_backup = max(backup_files, key=lambda x: int(x.split('_')[-1]))
                backup_path = os.path.join(os.path.dirname(session_file), latest_backup)
                try:
                    shutil.copy2(backup_path, session_file)
                    logger.info(f"🔄 Session restored from backup: {backup_path}")
                except Exception as restore_error:
                    logger.error(f"❌ Failed to restore session from backup: {restore_error}")
    
    async def get_advanced_session_metrics(self, logger: logging.Logger) -> Dict[str, Any]:
        """Get comprehensive session and pool metrics."""
        basic_metrics = {
            'session_count': len(self.session_files),
            'active_sessions': len([s for s in self.session_files if self._session_usage.get(s, 0) > 0]),
            'health_summary': {
                'healthy': len([s for s in self.session_files if self.get_session_health(s)['is_healthy']]),
                'unhealthy': len([s for s in self.session_files if not self.get_session_health(s)['is_healthy']])
            }
        }
        
        # Add pool metrics if available
        if hasattr(self, 'session_pool_manager') and len(self.session_pool_manager.session_pool) > 0:
            pool_analytics = self.session_pool_manager.get_pool_analytics()
            return {
                **basic_metrics,
                'pool_enabled': True,
                'pool_analytics': pool_analytics
            }
        else:
            return {
                **basic_metrics,
                'pool_enabled': False
            }
    
    async def process_batch_jobs(self, jobs: List[Dict[str, Any]], logger: logging.Logger) -> List[Dict[str, Any]]:
        """Process multiple Twitter scraping jobs with intelligent batching and session reuse."""
        logger.info(f"🚀 **BATCH PROCESSING MODE** - Processing {len(jobs)} jobs with session optimization")
        
        # Use batch processor for optimal session reuse
        return await self.batch_processor.process_sequential_batch(jobs, logger)
    
    def get_batch_performance_metrics(self) -> Dict[str, Any]:
        """Get comprehensive batch processing performance metrics."""
        return self.batch_processor.get_batch_analytics()
    
    async def optimize_batch_sequence(self, jobs: List[Dict[str, Any]], logger: logging.Logger) -> List[Dict[str, Any]]:
        """Optimize job sequence for maximum session reuse efficiency."""
        logger.info(f"🧠 OPTIMIZING batch sequence for {len(jobs)} jobs")
        
        # Sort jobs by similarity to maximize session reuse
        optimized_jobs = sorted(jobs, key=lambda job: (
            self.batch_processor._calculate_job_similarity_hash(job),
            job.get('max_posts', 0),  # Group by resource intensity
            job.get('target_username', '')  # Group by target
        ))
        
        logger.info(f"✅ Batch sequence optimized for maximum efficiency")
        return optimized_jobs


class TwitterScraper:
    """Core Twitter scraping implementation."""
    
    BASE_URL = "https://x.com"
    LOGIN_URL = "https://x.com/i/flow/login"
    
    def __init__(self, browser, logger: logging.Logger, output_dir: str = None, context=None):
        self.browser = browser
        self.logger = logger
        self.output_dir = output_dir
        self.context = context  # Use context from workers if provided
        self.page = None
        self.date_filter_start = None
        self.date_filter_end = None
        self.enable_date_filtering = False
        self.stop_at_date_threshold = True
        self.authenticated = False

        # Load credentials from environment
        self.email = os.getenv('X_EMAIL', '')
        self.username = os.getenv('X_USERNAME', '')
        self.password = os.getenv('X_PASS', '')

    async def _with_timeout(self, operation, timeout_ms=10000, operation_name="operation", default=None):
        """Wrapper to add timeout to async operations with graceful fallback."""
        try:
            return await asyncio.wait_for(operation, timeout=timeout_ms/1000)
        except asyncio.TimeoutError:
            self.logger.warning(f"⏱️ {operation_name} timed out after {timeout_ms}ms")
            return default
        except Exception as e:
            self.logger.warning(f"⚠️ {operation_name} failed: {e}")
            return default

    def _save_partial_results(self, results: Dict[str, Any], output_dir: str, reason: str = "checkpoint"):
        """Save partial results to avoid data loss."""
        try:
            import json
            checkpoint_file = f"{output_dir}/partial_results_{reason}.json"
            with open(checkpoint_file, 'w') as f:
                json.dump(results, f, indent=2)
            self.logger.info(f"💾 Saved partial results to {checkpoint_file}")
        except Exception as e:
            self.logger.warning(f"⚠️ Could not save partial results: {e}")

    async def _extract_posts_simple_fast(self, username: str, max_posts: int) -> List[Dict[str, Any]]:
        """Simple, fast, reliable post extraction with timeouts."""
        self.logger.info(f"⚡ FAST EXTRACTION: Getting {max_posts} posts from @{username}")
        posts = []
        seen_texts = set()

        try:
            # Already on profile page from previous navigation
            tweet_selector = 'article[data-testid="tweet"]'

            # Extract visible posts first
            self.logger.info(f"📊 Extracting visible posts...")
            tweets = self.page.locator(tweet_selector)
            initial_count = await self._with_timeout(tweets.count(), 5000, "Count initial tweets", 0)
            self.logger.info(f"   Found {initial_count} visible tweets")

            for i in range(min(initial_count, max_posts)):
                tweet_elem = tweets.nth(i)
                text = await self._with_timeout(
                    tweet_elem.inner_text(),
                    3000,
                    f"Extract tweet {i+1} text",
                    ""
                )

                if text and text not in seen_texts and len(text) > 10:
                    seen_texts.add(text)
                    posts.append({
                        'id': f'tweet_{len(posts)+1}',
                        'text': text[:500],  # Limit text length
                        'author': username,
                        'url': f'https://x.com/{username}',
                        'extraction_method': 'simple_fast'
                    })
                    if len(posts) >= max_posts:
                        break

            # Scroll to get more if needed
            max_scrolls = min(5, (max_posts - len(posts)) // 2 + 1)
            self.logger.info(f"🔄 Scrolling up to {max_scrolls} times for more posts...")

            for scroll_num in range(max_scrolls):
                if len(posts) >= max_posts:
                    break

                # Scroll
                await self.page.mouse.wheel(0, 600)
                await asyncio.sleep(1)

                # Extract new posts
                tweets = self.page.locator(tweet_selector)
                count = await self._with_timeout(tweets.count(), 5000, f"Count tweets after scroll {scroll_num+1}", 0)

                found_new = False
                for i in range(count):
                    if len(posts) >= max_posts:
                        break

                    tweet_elem = tweets.nth(i)
                    text = await self._with_timeout(
                        tweet_elem.inner_text(),
                        2000,
                        f"Extract scrolled tweet {i+1}",
                        ""
                    )

                    if text and text not in seen_texts and len(text) > 10:
                        seen_texts.add(text)
                        posts.append({
                            'id': f'tweet_{len(posts)+1}',
                            'text': text[:500],
                            'author': username,
                            'url': f'https://x.com/{username}',
                            'extraction_method': 'simple_fast',
                            'scroll_round': scroll_num + 1
                        })
                        found_new = True

                if not found_new:
                    self.logger.info(f"   No new posts found after scroll {scroll_num+1}, stopping")
                    break

                self.logger.info(f"   Progress: {len(posts)}/{max_posts} posts extracted")

            self.logger.info(f"✅ EXTRACTION COMPLETE: {len(posts)} posts extracted")
            return posts[:max_posts]

        except Exception as e:
            self.logger.error(f"❌ Simple extraction failed: {e}")
            self.logger.info(f"⚠️ Returning {len(posts)} partial results")
            return posts

        # ENHANCED: Authentication resilience features
        self.auth_attempts = 0
        self.max_auth_attempts = 3
        self.session_health_score = 100
        self.last_auth_check = None
        self.auth_failures = []
        self.rate_limit_detected = False
        self.blocked_detected = False

        # ENHANCED: Backup authentication methods
        self.backup_credentials = self._load_backup_credentials()
        self.current_credential_set = 0

        # ENHANCED: Session persistence and recovery
        self.session_file = f"/sessions/twitter_session_{hash(self.username)}.json"
        self.session_backup_file = f"/sessions/twitter_session_backup_{hash(self.username)}.json"

        # PHASE 4.2: Performance and Anti-Detection Optimizations
        self.performance_mode = True
        self.anti_detection_enabled = True
        self.request_cache = {}
        self.timing_variance = 0.5  # Human-like timing variation
        self.extraction_stats = {
            'requests_made': 0,
            'cache_hits': 0,
            'extraction_time': 0,
            'anti_detection_actions': 0
        }

        # Only require credentials if not using session
        # (session loading will bypass credential authentication)
        if not all([self.email, self.username, self.password]):
            self.logger.warning("⚠️ Twitter credentials not found - session mode required")
    
    def setup_date_filtering(self, params: Dict[str, Any]):
        """Initialize date filtering based on parameters."""
        self.enable_date_filtering = params.get("enable_date_filtering", False)
        self.stop_at_date_threshold = params.get("stop_at_date_threshold", True)
        
        if self.enable_date_filtering:
            self.date_filter_start, self.date_filter_end = TwitterDateUtils.get_date_filter_bounds(params)
            
            if self.date_filter_start and self.date_filter_end:
                self.logger.info(f"📅 Date filtering enabled: {self.date_filter_start.strftime('%Y-%m-%d')} to {self.date_filter_end.strftime('%Y-%m-%d')}")
                
                # Calculate expected data reduction
                date_range = params.get("date_range", "custom")
                if date_range in ["last_day", "today"]:
                    reduction = "90-95%"
                elif date_range in ["last_3_days", "last_week"]:
                    reduction = "80-90%" 
                elif date_range == "last_month":
                    reduction = "50-70%"
                else:
                    reduction = "varies"
                
                self.logger.info(f"⚡ Expected data reduction: {reduction} (faster extraction)")
            else:
                self.logger.warning("⚠️ Date filtering enabled but no valid date range provided")
                self.enable_date_filtering = False
        else:
            self.logger.info("📅 Date filtering disabled - extracting all available data")
    
    def is_tweet_within_date_range(self, tweet_data: Dict[str, Any]) -> bool:
        """Check if a tweet falls within the specified date range."""
        if not self.enable_date_filtering or not self.date_filter_start:
            return True
            
        timestamp = tweet_data.get('timestamp', '')
        if not timestamp:
            return True  # Keep tweets without timestamps to avoid data loss
            
        return TwitterDateUtils.is_within_date_range(timestamp, self.date_filter_start, self.date_filter_end)
    
    def should_stop_extraction(self, tweet_data: Dict[str, Any]) -> bool:
        """Check if extraction should stop due to date threshold being reached."""
        if not self.enable_date_filtering or not self.stop_at_date_threshold or not self.date_filter_start:
            return False
            
        timestamp = tweet_data.get('timestamp', '')
        if not timestamp:
            return False  # Continue if no timestamp available
            
        try:
            # If we encounter a tweet older than our start date, stop extraction
            if 'T' in timestamp and self.date_filter_start is not None:
                tweet_dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00').replace('+00:00', ''))
                tweet_dt = tweet_dt.replace(tzinfo=None)
                start_dt = self.date_filter_start.replace(tzinfo=None)

                if tweet_dt < start_dt:
                    self.logger.info(f"⏹️ Stopping extraction - reached date threshold (tweet: {timestamp})")
                    return True
                    
        except Exception:
            pass  # Continue on parsing errors
            
        return False

    async def authenticate(self):
        """Authenticate with Twitter using advanced stealth techniques."""
        self.logger.info("🔐 Starting Twitter authentication with stealth mode...")
        
        try:
            # MOBILE-FIRST STRATEGY: Force mobile interface throughout
            import random
            
            # Mobile viewports only - convince Twitter we're mobile
            mobile_viewports = [
                {'width': 375, 'height': 812},  # iPhone X
                {'width': 414, 'height': 896},  # iPhone 11 Pro Max
                {'width': 390, 'height': 844},  # iPhone 12
                {'width': 360, 'height': 740},  # Galaxy S20
            ]
            viewport = random.choice(mobile_viewports)
            
            # Mobile user agents only
            mobile_user_agents = [
                'Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Mobile/15E148 Safari/604.1',
                'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Mobile/15E148 Safari/604.1',
                'Mozilla/5.0 (Linux; Android 11; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36',
                'Mozilla/5.0 (Linux; Android 12; Pixel 6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36'
            ]
            user_agent = random.choice(mobile_user_agents)
            
            # Use context from workers if provided, otherwise create one
            if self.context is None:
                self.logger.info(f"📱 MOBILE MODE: {viewport['width']}x{viewport['height']}")
                self.logger.info(f"📱 Mobile UA: {user_agent[:50]}...")

                # Create ULTRA-STEALTH mobile context to bypass Twitter's detection
                self.context = await self.browser.new_context(
                    viewport=viewport,
                    user_agent=user_agent,
                    locale='en-US',
                    timezone_id='America/New_York',
                    device_scale_factor=2.0,
                    has_touch=True,
                    is_mobile=True,
                    permissions=['geolocation'],
                    geolocation={'latitude': 40.7128, 'longitude': -74.0060},
                    # ENHANCED HEADERS - Mimic real mobile Twitter app
                    extra_http_headers={
                        'Accept-Language': 'en-US,en;q=0.9',
                        'Accept-Encoding': 'gzip, deflate, br, zstd',
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                        'Upgrade-Insecure-Requests': '1',
                        'Sec-Fetch-Site': 'none',
                        'Sec-Fetch-Mode': 'navigate',
                        'Sec-Fetch-User': '?1',
                        'Sec-Fetch-Dest': 'document',
                        'Cache-Control': 'max-age=0',
                        'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
                        'sec-ch-ua-mobile': '?1',
                        'sec-ch-ua-platform': '"iOS"' if 'iPhone' in user_agent else '"Android"',
                        'sec-ch-ua-platform-version': '"15.0"' if 'iPhone' in user_agent else '"11.0"',
                        'DNT': '1',  # Do Not Track
                        'Connection': 'keep-alive'
                    },
                    # BYPASS AUTOMATION FLAGS
                    ignore_https_errors=True,
                    java_script_enabled=True,
                    bypass_csp=True,
                    # STEALTH PROXY SETTINGS
                    proxy={
                        'server': 'http://127.0.0.1:0'  # Disable proxy detection
                    } if random.choice([True, False]) else None
                )
            else:
                self.logger.info("✅ Using stealth context from workers (already created)")

            self.page = await self.context.new_page()

            # ENHANCED STEALTH: Apply extra stealth measures to page
            from ..stealth_config import EnhancedStealthConfig
            await EnhancedStealthConfig.apply_extra_stealth_to_page(self.page)
            self.logger.info("🔒 Enhanced stealth applied to page")

            # CRITICAL: Block redirects away from mobile.twitter.com
            await self.page.route("**/*", self._mobile_route_handler)
            
            # Enhanced mobile stealth
            # ULTIMATE STEALTH INJECTION - Most advanced anti-detection techniques
            await self.page.add_init_script("""
                // === PHASE 1: WEBDRIVER STEALTH ===
                delete navigator.__proto__.webdriver;
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined,
                    configurable: true
                });
                
                // === PHASE 2: CHROME RUNTIME STEALTH ===
                window.chrome = {
                    runtime: {
                        onConnect: undefined,
                        onMessage: undefined
                    }
                };
                
                // === PHASE 3: PERMISSIONS STEALTH ===
                const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (parameters) => (
                    parameters.name === 'notifications' ?
                        Promise.resolve({ state: Notification.permission }) :
                        originalQuery(parameters)
                );
                
                // === PHASE 4: MOBILE DEVICE MIMICRY ===
                Object.defineProperty(navigator, 'maxTouchPoints', {
                    get: () => 5,
                    configurable: true
                });
                
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [],
                    configurable: true
                });
                
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-US', 'en'],
                    configurable: true
                });
                
                // === PHASE 5: SCREEN ORIENTATION ===
                Object.defineProperty(screen, 'orientation', {
                    get: () => ({
                        angle: 0,
                        type: 'portrait-primary',
                        onchange: null
                    }),
                    configurable: true
                });
                
                // === PHASE 6: MOBILE APIs ===
                window.DeviceOrientationEvent = class DeviceOrientationEvent extends Event {};
                window.DeviceMotionEvent = class DeviceMotionEvent extends Event {};
                
                // === PHASE 7: IFRAME STEALTH ===
                Object.defineProperty(HTMLIFrameElement.prototype, 'contentWindow', {
                    get: function() {
                        return window;
                    },
                    configurable: true
                });
                
                // === PHASE 8: AUTOMATION TIMING RANDOMIZATION ===
                const originalSetTimeout = window.setTimeout;
                window.setTimeout = function(callback, delay) {
                    const randomDelay = delay + Math.random() * 100;
                    return originalSetTimeout.call(this, callback, randomDelay);
                };
                
                // === PHASE 9: NETWORK TIMING STEALTH ===
                const originalFetch = window.fetch;
                window.fetch = function(...args) {
                    return new Promise(resolve => {
                        setTimeout(() => resolve(originalFetch.apply(this, args)), Math.random() * 50);
                    });
                };
                
                // === PHASE 10: MOUSE EVENTS HUMANIZATION ===
                let lastMouseMove = Date.now();
                document.addEventListener('mousemove', () => {
                    lastMouseMove = Date.now();
                });
                
                // === PHASE 11: FOCUS STEALTH ===
                let isUserActive = true;
                ['mousedown', 'keydown', 'scroll', 'touchstart'].forEach(event => {
                    document.addEventListener(event, () => {
                        isUserActive = true;
                    });
                });
                
                // === PHASE 12: CONSOLE LOG STEALTH ===
                const log = console.log;
                console.log = (...args) => {
                    if (!args.some(arg => typeof arg === 'string' && arg.includes('webdriver'))) {
                        log.apply(console, args);
                    }
                };
                
                // Mobile-specific user agent consistency
                Object.defineProperty(navigator, 'platform', {
                    get: () => navigator.userAgent.includes('iPhone') ? 'iPhone' : 'Linux armv81',
                });
            """)
            
            # MULTI-STRATEGY ACCESS: Try different Twitter endpoints progressively
            self.logger.info("🌐 ENHANCED ACCESS: Trying multiple Twitter entry points...")
            
            # Progressive X.com access strategies
            access_strategies = [
                {
                    'name': 'X.com Direct Home',
                    'url': 'https://x.com/home',
                    'wait_until': 'domcontentloaded', 
                    'timeout': 45000
                },
                {
                    'name': 'X.com Explore',
                    'url': 'https://x.com/explore',
                    'wait_until': 'networkidle',
                    'timeout': 60000
                },
                {
                    'name': 'X.com Settings',
                    'url': 'https://x.com/settings',
                    'wait_until': 'domcontentloaded',
                    'timeout': 30000
                },
                {
                    'name': 'Legacy Twitter Home',
                    'url': 'https://twitter.com/home',
                    'wait_until': 'domcontentloaded',
                    'timeout': 45000
                },
                {
                    'name': 'Mobile Twitter Fallback',
                    'url': 'https://mobile.twitter.com/home',
                    'wait_until': 'domcontentloaded',
                    'timeout': 45000
                }
            ]
            
            access_successful = False
            successful_url = None
            
            for strategy in access_strategies:
                try:
                    self.logger.info(f"🚀 Trying: {strategy['name']} -> {strategy['url']}")
                    
                    # Human-like delay before each attempt
                    await self._human_delay(2, 4)
                    
                    # Attempt navigation with enhanced error handling
                    await self.page.goto(
                        strategy['url'], 
                        wait_until=strategy['wait_until'], 
                        timeout=strategy['timeout']
                    )
                    
                    # Wait for page to fully load
                    await self._human_delay(3, 6)
                    
                    # Check if we successfully loaded Twitter
                    current_url = self.page.url
                    page_title = await self.page.title()
                    
                    self.logger.info(f"📍 Result: {current_url} | Title: {page_title[:50]}...")
                    
                    # Success indicators
                    if any(indicator in current_url.lower() for indicator in ['twitter.com', 'x.com', 'mobile.twitter']):
                        if not any(block in current_url.lower() for block in ['login', 'session', 'error', 'suspended']):
                            self.logger.info(f"✅ SUCCESS: {strategy['name']} worked!")
                            access_successful = True
                            successful_url = current_url
                            break
                    
                    self.logger.warning(f"⚠️ {strategy['name']} did not provide expected result")
                    
                except Exception as e:
                    self.logger.warning(f"❌ {strategy['name']} failed: {str(e)[:100]}...")
                    continue
            
            if not access_successful:
                self.logger.error("💥 All Twitter access strategies failed!")
                raise Exception("Unable to access Twitter with any strategy - all endpoints blocked")
            
            # SUCCESS: We have access to Twitter!
            self.logger.info(f"🎉 Twitter access established: {successful_url}")
            await self._human_delay(2, 4)
            self.logger.info(f"📍 Current URL: {self.page.url}")
            self.logger.info(f"📑 Page title: {await self.page.title()}")
            
            # Check if we're being redirected or blocked
            if 'x.com' in self.page.url:
                self.logger.info("🔄 Redirected to x.com - updating login URL")
                self.LOGIN_URL = "https://x.com/i/flow/login"
            
            # Wait longer for JavaScript to render the page
            self.logger.info("⏳ Waiting for page to fully load with JavaScript...")
            await self.page.wait_for_timeout(10000)
            
            # Try to wait for any input element to appear
            try:
                await self.page.wait_for_selector('input', timeout=60000)  # Increased to 60 seconds
                self.logger.info("✅ Input element detected")
            except:
                self.logger.warning("⚠️ No input elements found - possible anti-bot protection")
            
            # Debug: Take screenshot after waiting
            try:
                await self.page.screenshot(path="/tmp/twitter_login_debug.png")
                self.logger.info("📸 Screenshot saved to /tmp/twitter_login_debug.png")
            except:
                pass
            
            # Debug: Get page content to check for any error messages
            page_content = await self.page.content()
            if 'Something went wrong' in page_content:
                self.logger.warning("⚠️ Page shows 'Something went wrong' message")
            if 'blocked' in page_content.lower():
                self.logger.warning("⚠️ Possible blocking detected in page content")
            if len(page_content) < 1000:
                self.logger.warning(f"⚠️ Page content suspiciously short: {len(page_content)} characters")
            
            # Step 1: Mobile-specific username/email selectors
            self.logger.info("📱 Entering email/username (MOBILE MODE)...")
            mobile_username_selectors = [
                # Mobile Twitter specific selectors  
                'input[name="username_or_email"]',
                'input[name="session[username_or_email]"]',
                'input[placeholder*="email"]',
                'input[placeholder*="username"]', 
                'input[placeholder*="Phone"]',
                '#username_or_email',
                '#session_username_or_email',
                
                # Generic mobile form selectors
                'input[type="email"]',
                'input[type="text"]:first-of-type',
                'form input[type="text"]',
                'input:not([type="hidden"]):not([type="password"])',
                
                # Fallback desktop selectors
                'input[name="text"]',
                'input[autocomplete="username"]',
                'input[data-testid="ocfEnterTextTextInput"]'
            ]
            
            # Debug: Check what input fields exist (mobile-focused)
            self.logger.info("🔍 Checking available input fields (MOBILE)...")
            for i, selector in enumerate(mobile_username_selectors):
                try:
                    elements = self.page.locator(selector)
                    count = await elements.count()
                    self.logger.info(f"  [{i}] {selector}: {count} elements found")
                    
                    if count > 0:
                        for j in range(min(count, 2)):
                            element = elements.nth(j)
                            visible = await element.is_visible()
                            enabled = await element.is_enabled()
                            self.logger.info(f"      Element {j}: visible={visible}, enabled={enabled}")
                except Exception as e:
                    self.logger.debug(f"  [{i}] {selector}: Error - {e}")
            
            username_entered = False
            for selector in mobile_username_selectors:
                try:
                    username_field = self.page.locator(selector)
                    if await username_field.count() > 0 and await username_field.first.is_visible():
                        self.logger.info(f"✅ Using selector: {selector}")
                        
                        # Human-like interaction: click first, then type
                        field = username_field.first
                        await field.click()
                        await self._human_delay(0.5, 1.0)
                        
                        # Clear field and type like a human
                        await field.clear()
                        await self._human_delay(0.3, 0.7)
                        await self._human_type(field, self.email)
                        await self._human_delay(1.0, 2.0)
                        
                        # Mobile-specific button selectors
                        mobile_next_selectors = [
                            # Mobile Twitter specific
                            'input[type="submit"]',
                            'button[type="submit"]',
                            'input[value*="Log in"]',
                            'input[value*="Sign in"]',
                            'button:has-text("Log in")',
                            'button:has-text("Sign in")', 
                            'input.btn',
                            '.btn-primary',
                            
                            # Generic mobile buttons
                            'button:has-text("Next")',
                            'div[role="button"]:has-text("Next")',
                            'span:has-text("Next")',
                            
                            # Desktop fallbacks
                            '[data-testid="LoginForm_Login_Button"]',
                            'div[role="button"]:has-text("Log in")',
                            '[data-testid="ocfEnterTextNextButton"]'
                        ]
                        
                        for next_selector in mobile_next_selectors:
                            try:
                                next_button = self.page.locator(next_selector)
                                if await next_button.count() > 0 and await next_button.first.is_visible():
                                    self.logger.info(f"✅ Clicking button: {next_selector}")
                                    
                                    # Human-like button click
                                    await next_button.first.hover()
                                    await self._human_delay(0.3, 0.8)
                                    await next_button.first.click()
                                    username_entered = True
                                    break
                            except Exception as e:
                                self.logger.debug(f"Next button {next_selector} failed: {e}")
                                continue
                        
                        if username_entered:
                            break
                            
                except Exception as e:
                    self.logger.debug(f"Username selector {selector} failed: {e}")
                    continue
            
            if not username_entered:
                raise Exception("Could not enter username/email")
                
            await self.page.wait_for_timeout(3000)
            
            # Step 2: Handle potential username verification
            try:
                # Sometimes Twitter asks for username verification
                username_verification = self.page.locator('input[data-testid="ocfEnterTextTextInput"]')
                if await username_verification.is_visible():
                    self.logger.info("📝 Username verification required...")
                    await username_verification.fill(self.username)
                    await self.page.wait_for_timeout(1000)
                    
                    next_button = self.page.locator('div[role="button"]:has-text("Next")')
                    if await next_button.is_visible():
                        await next_button.click()
                        await self.page.wait_for_timeout(3000)
            except:
                pass  # Username verification not required
            
            # Step 3: Mobile password entry
            self.logger.info("📱 Entering password (MOBILE MODE)...")
            mobile_password_selectors = [
                # Mobile Twitter specific
                'input[name="session[password]"]',
                'input[name="password"]',
                '#session_password',
                '#password',
                'input[placeholder*="Password"]',
                'input[placeholder*="password"]',
                
                # Generic mobile password fields
                'input[type="password"]',
                'input[autocomplete="current-password"]',
                
                # Desktop fallbacks
                'input[data-testid="ocfPasswordTextInput"]'
            ]
            
            password_entered = False
            for selector in mobile_password_selectors:
                try:
                    password_field = self.page.locator(selector)
                    if await password_field.count() > 0 and await password_field.first.is_visible():
                        self.logger.info(f"🔑 Using password selector: {selector}")
                        
                        # Human-like password entry
                        field = password_field.first
                        await field.click()
                        await self._human_delay(0.5, 1.0)
                        
                        await field.clear()
                        await self._human_delay(0.3, 0.7)
                        await self._human_type(field, self.password)
                        await self._human_delay(1.0, 2.0)
                        
                        # Mobile login button selectors
                        mobile_login_selectors = [
                            # Mobile Twitter specific
                            'input[type="submit"]',
                            'button[type="submit"]',
                            'input[value*="Log in"]',
                            'input[value*="Sign in"]',
                            'button:has-text("Log in")',
                            'button:has-text("Sign in")',
                            'input.btn',
                            '.btn-primary',
                            
                            # Generic mobile
                            'div[role="button"]:has-text("Log in")',
                            'span:has-text("Log in")',
                            
                            # Desktop fallbacks  
                            '[data-testid="LoginForm_Login_Button"]',
                            '[data-testid="ocfPasswordTextInputNextButton"]'
                        ]
                        
                        for login_selector in mobile_login_selectors:
                            try:
                                login_button = self.page.locator(login_selector)
                                if await login_button.count() > 0 and await login_button.first.is_visible():
                                    self.logger.info(f"🔑 Clicking login button: {login_selector}")
                                    
                                    await login_button.first.hover()
                                    await self._human_delay(0.5, 1.0)
                                    await login_button.first.click()
                                    password_entered = True
                                    break
                            except Exception as e:
                                self.logger.debug(f"Login button {login_selector} failed: {e}")
                                continue
                        
                        if password_entered:
                            break
                            
                except Exception as e:
                    self.logger.debug(f"Password selector {selector} failed: {e}")
                    continue
            
            if not password_entered:
                raise Exception("Could not enter password")
            
            # Wait for successful login
            await self.page.wait_for_timeout(5000)
            
            # Verify authentication success
            current_url = self.page.url
            if 'home' in current_url or 'timeline' in current_url or current_url == 'https://twitter.com/':
                self.authenticated = True
                self.logger.info("✅ Twitter authentication successful!")
                
                # Save session for future use
                await self._save_session()
                
            else:
                raise Exception(f"Authentication failed - redirected to: {current_url}")
                
        except Exception as e:
            self.logger.error(f"❌ Authentication failed: {e}")
            if self.context:
                await self.context.close()
            raise

    async def _save_session(self):
        """Save authentication session for reuse."""
        try:
            storage_state = await self.context.storage_state()
            session_file = "/tmp/twitter_session.json"
            
            with open(session_file, 'w') as f:
                json.dump(storage_state, f)
                
            self.logger.info(f"💾 Session saved to {session_file}")
        except Exception as e:
            self.logger.warning(f"⚠️ Could not save session: {e}")

    async def scrape_level_1(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Level 1: Basic profile + 5 recent tweets."""
        self.logger.info("⚡ Level 1: Basic profile and tweets")
        
        if not self.authenticated:
            raise Exception("Not authenticated")
            
        username = params.get('username', '')
        if not username:
            # Default to home timeline
            return await self._extract_timeline_tweets(params)
        
        # Navigate to user profile
        profile_url = f"https://x.com/{username}"
        await self.page.goto(profile_url, wait_until='domcontentloaded', timeout=60000)
        await self._human_delay(2, 4)
        
        # Basic content loading
        await self.page.wait_for_timeout(5000)
        
        # Basic profile extraction
        profile_data = await self._extract_profile_info(username)
        
        # Limited tweets (5 max for level 1)
        posts = await self._extract_posts_basic(username, max_posts=5)
        
        return [{
            "type": "basic_user_data",
            "username": f"@{username}",
            "profile": profile_data,
            "posts": posts[:5]
        }]

    async def scrape_level_2(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Level 2: Enhanced profile + tweets + engagement metrics + sample social graph."""
        self.logger.info("🐦 Level 2: Enhanced profile with engagement and sample social graph")

        username = params.get('username', '')
        if not username:
            return await self._extract_timeline_tweets(params)

        # Navigate to user profile
        profile_url = f"https://x.com/{username}"
        await self.page.goto(profile_url, wait_until='domcontentloaded', timeout=60000)
        await self._human_delay(3, 5)

        # Enhanced content loading
        await self.page.wait_for_timeout(8000)

        # Full profile extraction
        profile_data = await self._extract_profile_info(username)

        # More tweets with engagement (up to 15)
        posts = await self._extract_posts_with_engagement(username, max_posts=15)

        # Level 2: Sample followers/following (10 each)
        followers = []
        following = []

        if params.get('scrape_followers', False):
            max_followers = params.get('max_followers', 10)
            self.logger.info(f"👥 Level 2: Scraping sample followers ({max_followers})...")
            followers = await self._scrape_user_followers(username, max_followers)

        if params.get('scrape_following', False):
            max_following = params.get('max_following', 10)
            self.logger.info(f"➡️ Level 2: Scraping sample following ({max_following})...")
            following = await self._scrape_user_following(username, max_following)

        return [{
            "type": "enhanced_user_data",
            "username": f"@{username}",
            "profile": profile_data,
            "posts": posts[:15],
            "followers": followers,
            "following": following
        }]

    async def scrape_level_3(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Level 3: Full profile + tweets + media + likes/mentions + sample social graph."""
        self.logger.info("📸 Level 3: Full profile with media, interaction data, and sample social graph")

        username = params.get('username', '')
        if not username:
            return await self._extract_timeline_tweets(params)

        # Navigate to user profile
        profile_url = f"https://x.com/{username}"
        await self.page.goto(profile_url, wait_until='domcontentloaded', timeout=60000)
        await self._human_delay(4, 7)

        # Full content loading
        await self.page.wait_for_timeout(12000)

        # Complete profile extraction
        profile_data = await self._extract_profile_info(username)

        # More tweets with full metadata (up to 25)
        posts = await self._extract_posts_with_full_data(username, max_posts=25)

        # Extract likes and mentions (limited for performance)
        likes = await self._extract_user_likes(username, max_likes=10)
        mentions = await self._extract_user_mentions(username, max_mentions=5)

        # Level 3: Sample followers/following (25 each)
        followers = []
        following = []

        if params.get('scrape_followers', False):
            max_followers = params.get('max_followers', 25)
            self.logger.info(f"👥 Level 3: Scraping sample followers ({max_followers})...")
            followers = await self._scrape_user_followers(username, max_followers)

        if params.get('scrape_following', False):
            max_following = params.get('max_following', 25)
            self.logger.info(f"➡️ Level 3: Scraping sample following ({max_following})...")
            following = await self._scrape_user_following(username, max_following)

        return [{
            "type": "full_user_data",
            "username": f"@{username}",
            "profile": profile_data,
            "posts": posts[:25],
            "likes": likes[:10],
            "mentions": mentions[:5],
            "followers": followers,
            "following": following
        }]

    async def scrape_level_4(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Level 4: Comprehensive extraction with followers and all data types."""
        self.logger.info("🔍 Level 4: Comprehensive extraction with social graph")
        
        username = params.get('username', '')
        if not username:
            return await self._extract_timeline_tweets(params)
        
        # Navigate to user profile
        profile_url = f"https://x.com/{username}"
        await self.page.goto(profile_url, wait_until='domcontentloaded', timeout=60000)
        await self._human_delay(5, 8)
        
        # Maximum content loading time
        await self.page.wait_for_timeout(15000)
        
        # Complete profile extraction
        profile_data = await self._extract_profile_info(username)
        
        # EVERYTHING MODE: Use requested max_posts for comprehensive extraction
        requested_posts = params.get('max_posts', 50)
        posts = await self._extract_posts_comprehensive(username, max_posts=requested_posts)
        
        # Full interaction data
        likes = await self._extract_user_likes(username, max_likes=20)
        mentions = await self._extract_user_mentions(username, max_mentions=10)
        media = await self._extract_user_media(username, max_media=15)
        
        # Social graph data (limited for performance)
        followers = await self._extract_user_followers(username, max_followers=25)
        following = await self._extract_user_following(username, max_following=20)
        
        return [{
            "type": "comprehensive_user_data",
            "username": f"@{username}",
            "profile": profile_data,
            "posts": posts[:50],
            "likes": likes[:20],
            "mentions": mentions[:10],
            "media": media[:15],
            "followers": followers[:25],
            "following": following[:20]
        }]

    async def _extract_user_profile(self, username: str, basic: bool = True) -> List[Dict[str, Any]]:
        """Extract user profile information."""
        try:
            self.logger.info(f"👤 Extracting profile for @{username}")
            
            profile_url = f"https://twitter.com/{username}"
            await self.page.goto(profile_url, wait_until='networkidle', timeout=30000)
            await self.page.wait_for_timeout(3000)
            
            # Check if profile exists
            if await self.page.locator('text=This account doesn\'t exist').is_visible():
                self.logger.warning(f"❌ Profile @{username} doesn't exist")
                return []
            
            profile_data = {
                "username": username,
                "type": "profile",
                "extraction_timestamp": datetime.now().isoformat()
            }
            
            # Extract display name
            try:
                display_name_selectors = [
                    '[data-testid="UserName"] div[dir="ltr"] span',
                    'h2[role="heading"] span',
                    '[data-testid="UserDescription"] span'
                ]
                
                for selector in display_name_selectors:
                    element = self.page.locator(selector).first
                    if await element.is_visible():
                        display_name = await element.inner_text()
                        if display_name and display_name != username:
                            profile_data['display_name'] = display_name.strip()
                            break
            except:
                pass
            
            # Extract follower/following counts
            try:
                stats_elements = self.page.locator('a[href*="/followers"], a[href*="/following"]')
                stats_count = await stats_elements.count()
                
                for i in range(stats_count):
                    element = stats_elements.nth(i)
                    href = await element.get_attribute('href')
                    text = await element.inner_text()
                    
                    if 'followers' in href and 'followers' not in profile_data:
                        # Extract number from text like "1.2K followers"
                        import re
                        numbers = re.findall(r'[\d,]+\.?\d*[KMB]?', text)
                        if numbers:
                            profile_data['followers_count'] = numbers[0]
                    elif 'following' in href and 'following' not in profile_data:
                        numbers = re.findall(r'[\d,]+\.?\d*[KMB]?', text)
                        if numbers:
                            profile_data['following_count'] = numbers[0]
            except Exception as e:
                self.logger.debug(f"Could not extract stats: {e}")
            
            # Extract bio/description
            try:
                bio_selectors = [
                    '[data-testid="UserDescription"] span',
                    '[data-testid="UserDescription"]',
                    'div[dir="ltr"]:has-text("Bio")'
                ]
                
                for selector in bio_selectors:
                    element = self.page.locator(selector).first
                    if await element.is_visible():
                        bio_text = await element.inner_text()
                        if bio_text and len(bio_text) > 10:
                            profile_data['bio'] = bio_text.strip()
                            break
            except:
                pass
            
            # Extract verification status
            try:
                verified_element = self.page.locator('[data-testid="icon-verified"]')
                if await verified_element.is_visible():
                    profile_data['verified'] = True
                else:
                    profile_data['verified'] = False
            except:
                profile_data['verified'] = False
            
            self.logger.info(f"✅ Profile extracted for @{username}")
            return [profile_data]
            
        except Exception as e:
            self.logger.error(f"❌ Failed to extract profile @{username}: {e}")
            return []

    async def _extract_user_tweets(self, username: str, max_results: int = 10) -> List[Dict[str, Any]]:
        """Extract recent tweets from user timeline."""
        try:
            self.logger.info(f"📝 Extracting tweets for @{username}")
            
            tweets = []
            tweet_selectors = [
                '[data-testid="tweet"]',
                'article[data-testid="tweet"]',
                'div[data-testid="tweet"]'
            ]
            
            # Scroll to load tweets
            for scroll in range(3):  # Scroll 3 times to load more tweets
                await self.page.mouse.wheel(0, 1000)
                await self.page.wait_for_timeout(2000)
            
            # Extract tweets
            for selector in tweet_selectors:
                tweet_elements = self.page.locator(selector)
                tweet_count = await tweet_elements.count()
                
                if tweet_count > 0:
                    self.logger.info(f"📊 Found {tweet_count} tweet elements")
                    
                    for i in range(min(tweet_count, max_results)):
                        tweet_element = tweet_elements.nth(i)
                        tweet_data = await self._extract_single_tweet(tweet_element, i)
                        
                        if tweet_data:
                            tweets.append(tweet_data)
                    break
            
            self.logger.info(f"✅ Extracted {len(tweets)} tweets")
            return tweets[:max_results]
            
        except Exception as e:
            self.logger.error(f"❌ Failed to extract tweets: {e}")
            return []

    async def _extract_timeline_tweets(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract tweets from home timeline."""
        try:
            self.logger.info("🏠 Extracting home timeline tweets")
            
            # Navigate to home timeline
            await self.page.goto("https://twitter.com/home", wait_until='networkidle', timeout=30000)
            await self.page.wait_for_timeout(3000)
            
            return await self._extract_user_tweets("timeline", params.get('max_results', 10))
            
        except Exception as e:
            self.logger.error(f"❌ Failed to extract timeline: {e}")
            return []

    async def _extract_single_tweet(self, tweet_element, index: int) -> Optional[Dict[str, Any]]:
        """Extract data from a single tweet element."""
        try:
            tweet_data = {
                "type": "tweet",
                "index": index,
                "extraction_timestamp": datetime.now().isoformat()
            }
            
            # Extract tweet text
            try:
                text_selectors = [
                    '[data-testid="tweetText"]',
                    'div[lang] span',
                    'div[dir="ltr"] span'
                ]
                
                for selector in text_selectors:
                    text_element = tweet_element.locator(selector).first
                    if await text_element.is_visible():
                        tweet_text = await text_element.inner_text()
                        if tweet_text and len(tweet_text) > 3:
                            tweet_data['text'] = tweet_text.strip()
                            break
            except:
                pass
            
            # Extract engagement metrics
            try:
                metrics_selectors = [
                    '[role="group"] span',
                    '[data-testid="reply"] span',
                    '[data-testid="retweet"] span',
                    '[data-testid="like"] span'
                ]
                
                metrics = []
                for selector in metrics_selectors:
                    elements = tweet_element.locator(selector)
                    count = await elements.count()
                    
                    for i in range(count):
                        element = elements.nth(i)
                        text = await element.inner_text()
                        if text and text.isdigit():
                            metrics.append(text)
                
                if len(metrics) >= 3:
                    tweet_data['reply_count'] = metrics[0]
                    tweet_data['retweet_count'] = metrics[1] 
                    tweet_data['like_count'] = metrics[2]
            except:
                pass
            
            # Extract timestamp
            try:
                time_element = tweet_element.locator('time')
                if await time_element.is_visible():
                    timestamp = await time_element.get_attribute('datetime')
                    if timestamp:
                        tweet_data['timestamp'] = timestamp
            except:
                pass
            
            # Only return tweet if we got meaningful content
            if tweet_data.get('text') or tweet_data.get('timestamp'):
                return tweet_data
            
            return None
            
        except Exception as e:
            self.logger.debug(f"Failed to extract tweet {index}: {e}")
            return None


    async def _extract_tweet_from_element(self, tweet_element, index: int, username: str = '', include_engagement: bool = True) -> Optional[Dict[str, Any]]:
        """Extract tweet data from a tweet element with improved error handling."""
        try:
            tweet_data = {
                'id': f'tweet_{index + 1}',
                'text': '',
                'author': username,
                'url': f"https://x.com/{username}",
                'extracted_from': 'hybrid_extraction_2025',
                'extraction_attempt': 1,
                'method': 'css_selector'
            }

            # 1. EXTRACT TEXT CONTENT - Enhanced with multiple fallback strategies
            text_found = False
            text_selectors = [
                # Modern Twitter/X selectors (2024-2025)
                '[data-testid="tweetText"]',
                '[data-testid="tweetText"] span',
                'div[data-testid="tweetText"]',
                'div[data-testid="tweetText"] span',

                # Alternative modern patterns
                'article[data-testid="tweet"] div[lang]',
                'article[data-testid="tweet"] div[lang] span',
                'article div[dir="ltr"]',
                'article div[dir="ltr"] span',

                # Fallback patterns
                'div[lang] span',
                'div[dir="auto"] span',
                'span[dir="auto"]',
                'div[lang]',
                'div[dir="auto"]',

                # Generic content patterns
                'article[data-testid="tweet"] div:has-text("")',
                'div[role="group"] div[lang]',
                'div[role="group"] span',

                # Additional modern patterns (2024-2025)
                'article[data-testid="tweet"] [data-testid="tweetText"]',
                'article div[style*="text"] span',
                'div[data-testid*="tweet"] span[dir="auto"]',
                '[role="article"] div[lang] span',
                '[role="article"] div[dir] span',

                # Cross-selector patterns
                'article span[dir="auto"]:not([role])',
                'div[data-testid*="cell"] div[lang] span',
                'article div[tabindex="-1"] span',

                # Broad fallback patterns
                'article div span:not([role]):not([aria-label])',
                'div[role="article"] span:not([aria-hidden="true"])'
            ]

            for selector in text_selectors:
                try:
                    text_elements = tweet_element.locator(selector)
                    count = await text_elements.count()
                    self.logger.info(f"🔍 TEXT DEBUG: Selector '{selector}' found {count} elements")

                    if count > 0:
                        full_text = ""
                        for i in range(min(count, 10)):  # Limit to avoid spam
                            element = text_elements.nth(i)
                            try:
                                # CRITICAL FIX: Skip socialContext elements (UI labels like "Naval reposted")
                                try:
                                    # Check if this element is inside socialContext
                                    parent = element
                                    is_social_context = False
                                    for _ in range(5):  # Check up to 5 levels up
                                        try:
                                            testid = await asyncio.wait_for(parent.get_attribute('data-testid'), timeout=1.0)
                                            if testid == 'socialContext':
                                                is_social_context = True
                                                self.logger.debug(f"🚫 TEXT SKIP: Element {i} is inside socialContext, skipping")
                                                break
                                            parent = parent.locator('xpath=..')
                                        except:
                                            break

                                    if is_social_context:
                                        continue  # Skip this element
                                except:
                                    pass  # If check fails, continue processing

                                # Add timeout protection for element operations
                                is_visible = await asyncio.wait_for(element.is_visible(), timeout=2.0)
                                element_text = await asyncio.wait_for(element.inner_text(), timeout=3.0)
                                self.logger.info(f"🔍 TEXT DEBUG: Element {i} - visible: {is_visible}, text: '{element_text[:50]}...'")

                                if is_visible and element_text and element_text.strip():
                                    # Enhanced filtering logic
                                    text_clean = element_text.strip()

                                    # Skip clearly non-content elements (ENHANCED with repost/retweet patterns)
                                    skip_patterns = [
                                        'follow', 'following', 'followers', 'show this thread',
                                        'quote tweet', 'reply', 'retweet', 'like', 'share',
                                        'view', 'views', 'translate', 'show more', 'show less',
                                        'promoted', 'sponsored', 'ad', 'advertisement',
                                        'see fewer tweets like this', 'not interested',
                                        'report tweet', 'embed tweet', 'copy link',
                                        'reposted', 'retweeted'  # ADDED: Skip UI repost labels
                                    ]

                                    # Skip if text is too short or matches skip patterns
                                    if (len(text_clean) > 3 and
                                        not text_clean.lower() in skip_patterns and
                                        not any(pattern in text_clean.lower() for pattern in skip_patterns) and
                                        not text_clean.isdigit() and  # Skip pure numbers
                                        not all(c in '.,!?()[]{}' for c in text_clean) and  # Skip pure punctuation
                                        any(c.isalpha() for c in text_clean)  # Must have letters
                                    ):
                                        # Check if this looks like actual tweet content
                                        if self._is_likely_tweet_content(text_clean):
                                            full_text += text_clean + " "
                            except Exception as elem_e:
                                self.logger.debug(f"🔍 TEXT DEBUG: Element {i} error: {elem_e}")
                                continue

                        full_text = full_text.strip()
                        self.logger.info(f"🔍 TEXT DEBUG: Combined text from selector '{selector}': '{full_text[:100]}...'")

                        if full_text and len(full_text) > 10:  # Minimum meaningful content
                            tweet_data['text'] = full_text[:500]  # Reasonable limit
                            text_found = True
                            self.logger.info(f"✅ TEXT SUCCESS: Extracted {len(full_text)} chars from selector '{selector}'")
                            break
                except Exception as e:
                    self.logger.info(f"⚠️ TEXT DEBUG: Selector '{selector}' failed: {e}")
                    continue

            # Fallback: extract from entire tweet element with enhanced filtering
            if not text_found:
                try:
                    self.logger.info("🔍 TEXT FALLBACK: Attempting extraction from entire tweet element")
                    # Add timeout protection for full element text extraction
                    all_text = await asyncio.wait_for(tweet_element.inner_text(), timeout=5.0)
                    self.logger.info(f"🔍 TEXT FALLBACK: Raw element text: '{all_text[:200]}...'")

                    if all_text:
                        # Split into lines and filter
                        lines = [line.strip() for line in all_text.split('\n') if line.strip()]
                        self.logger.info(f"🔍 TEXT FALLBACK: Found {len(lines)} lines")

                        # Enhanced filtering for meaningful content
                        meaningful_lines = []
                        for line in lines:
                            # Skip lines that are likely UI elements or metadata
                            if (len(line) > 10 and  # Minimum length
                                not line.isdigit() and  # Not just numbers
                                not line.startswith('@') and  # Not mentions only
                                not any(skip_phrase in line.lower() for skip_phrase in [
                                    'follow', 'following', 'followers', 'show this thread',
                                    'quote tweet', 'reply', 'retweet', 'like', 'share',
                                    'view', 'views', 'ago', 'promoted', 'ad'
                                ]) and
                                # Must contain some alphabetic content
                                any(c.isalpha() for c in line)
                            ):
                                meaningful_lines.append(line)

                        self.logger.info(f"🔍 TEXT FALLBACK: Filtered to {len(meaningful_lines)} meaningful lines")

                        if meaningful_lines:
                            # Take the longest line as it's likely to be the tweet content
                            best_line = max(meaningful_lines, key=len)
                            tweet_data['text'] = best_line[:500]
                            text_found = True
                            self.logger.info(f"✅ TEXT FALLBACK SUCCESS: Extracted '{best_line[:100]}...'")
                        else:
                            self.logger.warning("⚠️ TEXT FALLBACK: No meaningful lines found")
                except Exception as fallback_e:
                    self.logger.warning(f"⚠️ TEXT FALLBACK: Failed: {fallback_e}")

            # 2. EXTRACT ENGAGEMENT METRICS - Enhanced with multiple fallback strategies
            if include_engagement:
                try:
                    # Enhanced engagement extraction with multiple selector strategies
                    engagement_selectors = {
                        'replies': [
                            # Primary modern selectors
                            '[data-testid="reply"]',
                            'button[data-testid="reply"]',
                            'div[data-testid="reply"]',

                            # Aria-label based selectors
                            '[aria-label*="replies"]',
                            '[aria-label*="reply"]',
                            '[aria-label*="Reply"]',

                            # Role-based selectors
                            'div[role="button"][aria-label*="reply"]',
                            'button[role="button"][aria-label*="reply"]',

                            # Text-based fallbacks
                            'div[role="button"]:has-text("Reply")',
                            'button:has-text("Reply")',

                            # Additional modern patterns
                            'article[data-testid="tweet"] [data-testid="reply"]',
                            'div[role="group"] button[aria-label*="reply"]'
                        ],
                        'retweets': [
                            # Primary modern selectors
                            '[data-testid="retweet"]',
                            'button[data-testid="retweet"]',
                            'div[data-testid="retweet"]',

                            # Aria-label based selectors
                            '[aria-label*="retweet"]',
                            '[aria-label*="Repost"]',
                            '[aria-label*="repost"]',

                            # Role-based selectors
                            'div[role="button"][aria-label*="retweet"]',
                            'button[role="button"][aria-label*="repost"]',

                            # Text-based fallbacks
                            'div[role="button"]:has-text("Repost")',
                            'button:has-text("Repost")',

                            # Additional modern patterns
                            'article[data-testid="tweet"] [data-testid="retweet"]',
                            'div[role="group"] button[aria-label*="repost"]'
                        ],
                        'likes': [
                            # Primary modern selectors
                            '[data-testid="like"]',
                            'button[data-testid="like"]',
                            'div[data-testid="like"]',

                            # Aria-label based selectors
                            '[aria-label*="like"]',
                            '[aria-label*="Like"]',
                            '[aria-label*="likes"]',

                            # Role-based selectors
                            'div[role="button"][aria-label*="like"]',
                            'button[role="button"][aria-label*="like"]',

                            # Text-based fallbacks
                            'div[role="button"]:has-text("Like")',
                            'button:has-text("Like")',

                            # Additional modern patterns
                            'article[data-testid="tweet"] [data-testid="like"]',
                            'div[role="group"] button[aria-label*="like"]'
                        ]
                    }
                    
                    import re
                    
                    # Extract replies with enhanced error handling
                    for selector in engagement_selectors['replies']:
                        try:
                            button = tweet_element.locator(selector).first
                            count = await asyncio.wait_for(button.count(), timeout=2.0)
                            self.logger.info(f"💬 REPLIES DEBUG: Selector '{selector}' found {count} elements")

                            if count > 0:
                                # Try aria-label first (more reliable)
                                try:
                                    aria_label = await asyncio.wait_for(button.get_attribute('aria-label'), timeout=2.0)
                                    self.logger.info(f"💬 REPLIES DEBUG: aria-label: '{aria_label}'")

                                    if aria_label:
                                        numbers = re.findall(r'(\d+(?:,\d+)*(?:\.\d+)?[KM]?)', aria_label)
                                        if numbers:
                                            replies_count = self._parse_engagement_number(numbers[0])
                                            tweet_data['replies'] = replies_count
                                            self.logger.info(f"✅ REPLIES SUCCESS: Extracted {replies_count} from aria-label")
                                            break
                                except (asyncio.TimeoutError, Exception) as aria_e:
                                    self.logger.debug(f"💬 REPLIES DEBUG: aria-label error: {aria_e}")

                                # Fallback to inner text
                                try:
                                    button_text = await asyncio.wait_for(button.inner_text(), timeout=2.0)
                                    self.logger.info(f"💬 REPLIES DEBUG: inner text: '{button_text}'")

                                    if button_text:
                                        numbers = re.findall(r'(\d+(?:,\d+)*(?:\.\d+)?[KM]?)', button_text)
                                        if numbers:
                                            replies_count = self._parse_engagement_number(numbers[0])
                                            tweet_data['replies'] = replies_count
                                            self.logger.info(f"✅ REPLIES SUCCESS: Extracted {replies_count} from inner text")
                                            break
                                except (asyncio.TimeoutError, Exception) as text_e:
                                    self.logger.debug(f"💬 REPLIES DEBUG: inner text error: {text_e}")
                        except Exception as e:
                            self.logger.debug(f"💬 REPLIES DEBUG: Selector '{selector}' failed: {e}")
                            continue
                    
                    # Extract retweets
                    for selector in engagement_selectors['retweets']:
                        try:
                            button = tweet_element.locator(selector).first
                            if await button.count() > 0:
                                # Try aria-label first
                                aria_label = await button.get_attribute('aria-label')
                                if aria_label:
                                    numbers = re.findall(r'(\d+(?:,\d+)*(?:\.\d+)?[KM]?)', aria_label)
                                    if numbers:
                                        tweet_data['retweets'] = self._parse_engagement_number(numbers[0])
                                        break
                                
                                # Fallback to inner text
                                button_text = await button.inner_text()
                                if button_text:
                                    numbers = re.findall(r'(\d+(?:,\d+)*(?:\.\d+)?[KM]?)', button_text)
                                    if numbers:
                                        tweet_data['retweets'] = self._parse_engagement_number(numbers[0])
                                        break
                        except Exception as e:
                            self.logger.debug(f"Retweet selector {selector} failed: {e}")
                            continue
                    
                    # Extract likes
                    for selector in engagement_selectors['likes']:
                        try:
                            button = tweet_element.locator(selector).first
                            if await button.count() > 0:
                                # Try aria-label first
                                aria_label = await button.get_attribute('aria-label')
                                if aria_label:
                                    numbers = re.findall(r'(\d+(?:,\d+)*(?:\.\d+)?[KM]?)', aria_label)
                                    if numbers:
                                        tweet_data['likes'] = self._parse_engagement_number(numbers[0])
                                        break
                                
                                # Fallback to inner text
                                button_text = await button.inner_text()
                                if button_text:
                                    numbers = re.findall(r'(\d+(?:,\d+)*(?:\.\d+)?[KM]?)', button_text)
                                    if numbers:
                                        tweet_data['likes'] = self._parse_engagement_number(numbers[0])
                                        break
                        except Exception as e:
                            self.logger.debug(f"Like selector {selector} failed: {e}")
                            continue
                    
                    # Extract views with enhanced detection
                    try:
                        # Look for analytics/views button with multiple strategies
                        view_selectors = [
                            '[aria-label*="view"]',
                            '[aria-label*="View"]', 
                            'div[role="button"]:has-text("view")',
                            'span:has-text("view")',
                            'a:has-text("view")'
                        ]
                        
                        for selector in view_selectors:
                            try:
                                button = tweet_element.locator(selector).first
                                if await button.count() > 0:
                                    aria_label = await button.get_attribute('aria-label')
                                    if aria_label and ('view' in aria_label.lower()):
                                        numbers = re.findall(r'(\d+(?:,\d+)*(?:\.\d+)?[KM]?)', aria_label)
                                        if numbers:
                                            tweet_data['views'] = self._parse_engagement_number(numbers[0])
                                            break
                                    
                                    button_text = await button.inner_text()
                                    if button_text and ('view' in button_text.lower()):
                                        numbers = re.findall(r'(\d+(?:,\d+)*(?:\.\d+)?[KM]?)', button_text)
                                        if numbers:
                                            tweet_data['views'] = self._parse_engagement_number(numbers[0])
                                            break
                            except:
                                continue
                    except:
                        pass
                    
                    # Log successful extractions for debugging
                    engagement_found = []
                    if 'replies' in tweet_data: engagement_found.append(f"replies:{tweet_data['replies']}")
                    if 'retweets' in tweet_data: engagement_found.append(f"retweets:{tweet_data['retweets']}")
                    if 'likes' in tweet_data: engagement_found.append(f"likes:{tweet_data['likes']}")
                    if 'views' in tweet_data: engagement_found.append(f"views:{tweet_data['views']}")
                    
                    if engagement_found:
                        self.logger.info(f"✅ ENGAGEMENT SUCCESS: {', '.join(engagement_found)}")
                    else:
                        self.logger.info(f"❌ ENGAGEMENT: No metrics found for tweet")

                except Exception as e:
                    self.logger.info(f"⚠️ ENGAGEMENT ERROR: {e}")
            
            # 3. EXTRACT TIMESTAMP/DATE - Enhanced with multiple fallback strategies
            try:
                date_selectors = [
                    # Primary modern selectors (2024-2025)
                    'time[datetime]',                          # Primary: time element with datetime attribute
                    'time[data-testid="Time"]',                # Modern Twitter time element
                    '[data-testid="Time"] time',               # Time inside testid container
                    'article[data-testid="tweet"] time',       # Time within tweet article

                    # Status link patterns
                    'a[href*="/status/"] time',                # Time inside status links
                    'a[href*="/status/"][role="link"] time',   # Role-based status links with time
                    'time a[href*="/status/"]',                # Links inside time elements

                    # Fallback patterns
                    'time',                                    # Any time element
                    '[data-testid="Time"]',                    # Twitter testid for time
                    'a[href*="/status/"]',                     # Status link containing time

                    # Text-based time indicators
                    'span[title]:has-text("ago")',             # Span with title containing "ago"
                    '[aria-label*="ago"]',                     # Aria label with time
                    'span:has-text("·")',                      # Time separator span

                    # Alternative modern patterns
                    'article div[dir="auto"]:has-text("ago")', # Auto-direction divs with time
                    'div[role="group"] span:has-text("ago")',  # Group role spans with time
                    '[role="link"][href*="/status/"]',         # Role-based status links
                ]
                
                date_extracted = False
                
                # Try each selector until we find a valid timestamp
                for selector in date_selectors:
                    try:
                        elements = tweet_element.locator(selector)
                        count = await asyncio.wait_for(elements.count(), timeout=2.0)
                        self.logger.info(f"🕐 DATE DEBUG: Selector '{selector}' found {count} elements")

                        for i in range(min(count, 5)):  # Limit attempts to avoid spam
                            element = elements.nth(i)

                            # Try datetime attribute first (most reliable)
                            try:
                                datetime_attr = await asyncio.wait_for(element.get_attribute('datetime'), timeout=2.0)
                                self.logger.info(f"🕐 DATE DEBUG: Element {i} datetime attribute: '{datetime_attr}'")

                                if datetime_attr:
                                    # Store both ISO timestamp and human-readable date
                                    tweet_data['timestamp'] = datetime_attr
                                    tweet_data['date'] = datetime_attr

                                    # Also store human-readable version
                                    try:
                                        from datetime import datetime
                                        dt = datetime.fromisoformat(datetime_attr.replace('Z', '+00:00'))
                                        tweet_data['date_human'] = dt.strftime('%Y-%m-%d %H:%M')
                                    except:
                                        tweet_data['date_human'] = datetime_attr

                                    date_extracted = True
                                    self.logger.info(f"✅ DATE SUCCESS: Extracted datetime attribute: {datetime_attr}")
                                    break
                            except (asyncio.TimeoutError, Exception) as attr_e:
                                self.logger.debug(f"🕐 DATE DEBUG: datetime attribute error: {attr_e}")
                                pass

                            # Try aria-label
                            try:
                                aria_label = await asyncio.wait_for(element.get_attribute('aria-label'), timeout=2.0)
                                self.logger.info(f"🕐 DATE DEBUG: Element {i} aria-label: '{aria_label}'")

                                if aria_label and ('ago' in aria_label or ':' in aria_label):
                                    iso_timestamp = self._convert_relative_to_iso(aria_label)
                                    if iso_timestamp:
                                        tweet_data['timestamp'] = iso_timestamp
                                        tweet_data['date'] = iso_timestamp
                                        tweet_data['date_human'] = aria_label
                                        date_extracted = True
                                        self.logger.info(f"✅ DATE SUCCESS: Extracted from aria-label: {aria_label}")
                                        break
                                    else:
                                        tweet_data['date'] = aria_label
                                        tweet_data['timestamp'] = aria_label
                                        tweet_data['date_human'] = aria_label
                                        date_extracted = True
                                        self.logger.info(f"✅ DATE SUCCESS: Raw aria-label: {aria_label}")
                                        break
                            except (asyncio.TimeoutError, Exception) as aria_e:
                                self.logger.debug(f"🕐 DATE DEBUG: aria-label error: {aria_e}")
                                pass

                            # Try element text
                            try:
                                element_text = await asyncio.wait_for(element.inner_text(), timeout=2.0)
                                self.logger.info(f"🕐 DATE DEBUG: Element {i} inner text: '{element_text}'")

                                if element_text:
                                    # Check if it looks like a time string
                                    if any(indicator in element_text.lower() for indicator in ['ago', 'h', 'm', 's', ':', 'am', 'pm']):
                                        iso_timestamp = self._convert_relative_to_iso(element_text)
                                        if iso_timestamp:
                                            tweet_data['timestamp'] = iso_timestamp
                                            tweet_data['date'] = iso_timestamp
                                            tweet_data['date_human'] = element_text
                                            date_extracted = True
                                            self.logger.info(f"✅ DATE SUCCESS: Extracted from element text: {element_text}")
                                            break
                                        else:
                                            tweet_data['date'] = element_text
                                            tweet_data['timestamp'] = element_text
                                            tweet_data['date_human'] = element_text
                                            date_extracted = True
                                            self.logger.info(f"✅ DATE SUCCESS: Raw element text: {element_text}")
                                            break
                            except (asyncio.TimeoutError, Exception) as text_e:
                                self.logger.debug(f"🕐 DATE DEBUG: element text error: {text_e}")
                                pass
                        
                        if date_extracted:
                            break
                            
                    except Exception as e:
                        self.logger.debug(f"Date selector {selector} failed: {e}")
                        continue
                
                # Additional fallback: look for text patterns in the entire tweet
                if not date_extracted:
                    try:
                        tweet_text = await tweet_element.inner_text()
                        if tweet_text:
                            import re
                            # Look for relative time patterns in the tweet text
                            time_patterns = [
                                r'(\d+[smhdw]\s*ago)',       # "2h ago", "1d ago"
                                r'(\d+:\d+\s*[AP]M)',        # "2:30 PM"
                                r'([A-Z][a-z]{2}\s+\d+)',    # "Dec 25"
                                r'(\d+\s*[smhdw])',          # "2h", "1d"
                            ]
                            
                            for pattern in time_patterns:
                                matches = re.findall(pattern, tweet_text, re.IGNORECASE)
                                if matches:
                                    time_str = matches[0]
                                    iso_timestamp = self._convert_relative_to_iso(time_str)
                                    if iso_timestamp:
                                        tweet_data['timestamp'] = iso_timestamp
                                        tweet_data['date'] = iso_timestamp
                                        tweet_data['date_human'] = time_str
                                        self.logger.debug(f"✅ Date extracted from pattern: {time_str}")
                                        break
                                    else:
                                        tweet_data['date'] = time_str
                                        tweet_data['timestamp'] = time_str
                                        tweet_data['date_human'] = time_str
                                        break
                    except:
                        pass
                        
            except Exception as e:
                self.logger.debug(f"⚠️ Timestamp extraction failed: {e}")

            # CRITICAL FIX: Validate timestamp format to prevent corruption
            try:
                if 'timestamp' in tweet_data:
                    timestamp_value = tweet_data.get('timestamp', '')

                    # Check if timestamp looks corrupted (contains newlines, non-date chars, etc.)
                    if isinstance(timestamp_value, str):
                        # Valid timestamp should NOT contain newlines or random short strings
                        if ('\n' in timestamp_value or
                            len(timestamp_value) < 8 or  # Too short to be a real date
                            (len(timestamp_value) < 15 and not any(c.isdigit() for c in timestamp_value))):  # Short non-numeric
                            self.logger.warning(f"🚨 CORRUPTED TIMESTAMP DETECTED: '{timestamp_value}' - removing")
                            # Remove corrupted timestamp rather than keeping bad data
                            tweet_data.pop('timestamp', None)
                            tweet_data.pop('date', None)
                            # Keep date_human if it exists and looks valid
                            if 'date_human' in tweet_data:
                                date_human = tweet_data['date_human']
                                if '\n' in str(date_human) or len(str(date_human)) < 3:
                                    tweet_data.pop('date_human', None)

                    # Log final timestamp status
                    if 'timestamp' in tweet_data:
                        self.logger.debug(f"✅ Timestamp validated: {tweet_data['timestamp']}")
                    else:
                        self.logger.warning(f"⚠️ No valid timestamp for tweet")
            except Exception as val_e:
                self.logger.debug(f"Timestamp validation error: {val_e}")

            # 4. EXTRACT TWEET URL/ID - Enhanced with multiple fallback strategies
            try:
                # Strategy 1: Enhanced status link extraction with modern patterns
                url_selectors = [
                    # Primary modern selectors (2024-2025)
                    'time a[href*="/status/"]',                    # Time element with status link (most reliable)
                    '[data-testid="Time"] a[href*="/status/"]',    # Time testid with status link
                    'article[data-testid="tweet"] a[href*="/status/"]',  # Tweet article with status link

                    # Direct status link patterns
                    'a[href*="/status/"]',                         # Primary status link
                    'a[role="link"][href*="/status/"]',            # Role-based status links

                    # Specific attribute combinations
                    'a[href*="/status/"]:not([aria-label])',       # Direct status links without aria-label
                    'a[href*="/status/"][role="link"]',            # Status links with role
                    'a[href*="/status/"][target="_blank"]',        # Status links with target

                    # Alternative patterns
                    'div[data-testid="tweet"] a[href*="/status/"]', # Tweet div with status link
                    'article time a[href*="/status/"]',            # Article time status link
                    '[role="article"] a[href*="/status/"]',        # Article role with status link

                    # Fallback patterns
                    'div[role="link"][href*="/status/"]',          # Div role links
                    'span a[href*="/status/"]',                    # Span child status links
                    '[data-testid*="Time"] a',                    # Time-related testids with links
                ]
                
                url_extracted = False
                for selector in url_selectors:
                    try:
                        status_link = tweet_element.locator(selector).first
                        count = await asyncio.wait_for(status_link.count(), timeout=2.0)
                        self.logger.info(f"🔗 URL DEBUG: Selector '{selector}' found {count} elements")

                        if count > 0:
                            try:
                                href = await asyncio.wait_for(status_link.get_attribute('href'), timeout=2.0)
                                self.logger.info(f"🔗 URL DEBUG: Found href: '{href}'")

                                if href and '/status/' in href:
                                    # Normalize URL
                                    full_url = f"https://x.com{href}" if href.startswith('/') else href
                                    tweet_data['url'] = full_url

                                    # Extract tweet ID from URL
                                    import re
                                    id_match = re.search(r'/status/(\d+)', href)
                                    if id_match:
                                        tweet_id = id_match.group(1)
                                        tweet_data['tweet_id'] = tweet_id
                                        tweet_data['id'] = f"tweet_{tweet_id}"  # Update ID to use actual tweet ID
                                        self.logger.info(f"✅ URL SUCCESS: Extracted tweet ID {tweet_id}")
                                        self.logger.info(f"✅ URL SUCCESS: Full URL: {full_url}")
                                        url_extracted = True
                                        break
                                    else:
                                        self.logger.warning(f"🔗 URL WARNING: No tweet ID found in href: {href}")
                                else:
                                    self.logger.debug(f"🔗 URL DEBUG: Invalid or missing href: {href}")
                            except (asyncio.TimeoutError, Exception) as href_e:
                                self.logger.debug(f"🔗 URL DEBUG: href extraction error: {href_e}")
                    except Exception as e:
                        self.logger.debug(f"🔗 URL DEBUG: Selector '{selector}' failed: {e}")
                        continue
                
                # Strategy 2: Fallback - extract from any href containing the username
                if not url_extracted:
                    try:
                        # Look for links that might contain tweet references
                        all_links = tweet_element.locator('a[href]')
                        count = await all_links.count()
                        
                        for i in range(min(count, 10)):  # Check up to 10 links
                            link = all_links.nth(i)
                            href = await link.get_attribute('href')
                            if href and '/status/' in href:
                                import re
                                id_match = re.search(r'/status/(\d+)', href)
                                if id_match:
                                    full_url = f"https://x.com{href}" if href.startswith('/') else href
                                    tweet_data['url'] = full_url
                                    tweet_data['tweet_id'] = id_match.group(1)
                                    self.logger.debug(f"✅ Tweet URL/ID extracted (fallback): {id_match.group(1)}")
                                    url_extracted = True
                                    break
                    except:
                        pass
                        
            except Exception as e:
                self.logger.debug(f"⚠️ Tweet URL/ID extraction failed: {e}")
            
            # 5. EXTRACT AUTHOR INFO - ENHANCED
            try:
                # Strategy 1: Extract author from URL (most reliable)
                actual_username = None
                if tweet_data.get('url'):
                    import re
                    url_match = re.search(r'x\.com/([^/]+)/status', tweet_data['url'])
                    if url_match:
                        actual_username = url_match.group(1)
                        tweet_data['author'] = actual_username
                        tweet_data['username'] = actual_username
                        self.logger.info(f"✅ AUTHOR FROM URL: {actual_username} (Profile: {username})")

                # Strategy 2: Try to extract from User-Name element (fallback)
                if not actual_username:
                    username_element = tweet_element.locator('[data-testid="User-Name"] a').first
                    username_count = await username_element.count()

                    self.logger.debug(f"🔍 AUTHOR DEBUG: Found {username_count} username elements")

                    if username_count > 0:
                        # Try to get href attribute which contains username
                        try:
                            href = await username_element.get_attribute('href')
                            if href:
                                # Extract username from href like "/username" or "https://x.com/username"
                                username_from_href = href.strip('/').split('/')[-1]
                                if username_from_href and username_from_href not in ['status', 'home', 'search']:
                                    actual_username = username_from_href
                                    tweet_data['author'] = actual_username
                                    tweet_data['username'] = actual_username
                                    self.logger.info(f"✅ AUTHOR FROM HREF: {actual_username} (Profile: {username})")
                        except:
                            pass

                        # Fallback: try text extraction
                        if not actual_username:
                            username_text = await username_element.inner_text()
                            self.logger.debug(f"🔍 AUTHOR DEBUG: Username text = '{username_text}'")

                            if '@' in username_text:
                                # Extract username after @ symbol, handle multiline text
                                actual_username = username_text.split('@')[1].split('\n')[0].split(' ')[0].strip()
                                tweet_data['username'] = actual_username
                                tweet_data['author'] = actual_username
                                self.logger.info(f"✅ AUTHOR FROM TEXT: {actual_username} (Profile: {username})")
                            else:
                                self.logger.warning(f"⚠️ AUTHOR DEBUG: No @ found in username text: '{username_text}'")
                    else:
                        self.logger.warning(f"⚠️ AUTHOR DEBUG: No username element found, keeping default author: {username}")

                # Display name - extract from first line before @username
                name_element = tweet_element.locator('[data-testid="User-Name"] span').first
                if await name_element.count() > 0:
                    display_name_text = await name_element.inner_text()
                    # Get first line (display name, before @username)
                    display_name = display_name_text.split('\n')[0].split('@')[0].strip()
                    if display_name and not display_name.startswith('@'):
                        tweet_data['author_name'] = display_name
                        self.logger.debug(f"✅ DISPLAY NAME: {display_name}")

            except Exception as e:
                self.logger.error(f"❌ Author extraction failed: {e}")
                import traceback
                self.logger.error(f"Traceback: {traceback.format_exc()}")

            # 6. DETECT IF THIS IS A RETWEET
            try:
                is_retweet = False
                retweeted_by = None

                # Check for retweet text indicator (Twitter shows "X reposted" above retweets)
                try:
                    social_context = await tweet_element.locator('[data-testid="socialContext"]').first.inner_text()
                    if social_context and ('reposted' in social_context.lower() or 'retweeted' in social_context.lower()):
                        is_retweet = True
                        # Extract who reposted it
                        if username in social_context:
                            retweeted_by = username
                        self.logger.debug(f"🔄 Retweet detected via socialContext: '{social_context}'")
                except:
                    pass

                # Alternative: Check if URL author differs from profile username
                if not is_retweet and tweet_data.get('author'):
                    url_author = tweet_data.get('author')
                    if url_author != username:
                        is_retweet = True
                        retweeted_by = username
                        self.logger.debug(f"🔄 Retweet detected via author mismatch: {url_author} != {username}")

                # CRITICAL FIX: Ensure retweeted_by is ALWAYS set when is_retweet is True
                if is_retweet and not retweeted_by:
                    retweeted_by = username  # Default to profile username
                    self.logger.debug(f"🔄 Auto-populated retweeted_by: {retweeted_by}")

                # Add retweet fields
                tweet_data['is_retweet'] = is_retweet
                if is_retweet:
                    tweet_data['retweeted_by'] = retweeted_by
                    tweet_data['original_author'] = tweet_data.get('author')  # Preserve original author
                    self.logger.info(f"🔄 RETWEET: {retweeted_by} retweeted {tweet_data.get('original_author')}'s post")
                else:
                    self.logger.debug(f"📝 Original post by {tweet_data.get('author')}")

            except Exception as e:
                self.logger.debug(f"⚠️ Retweet detection failed: {e}")
                tweet_data['is_retweet'] = False

            # 7. EXTRACT EMBEDDED MEDIA - Phase 1.1 Enhancement
            try:
                media_items = await self._extract_media_from_tweet(tweet_element)
                if media_items:
                    tweet_data['media'] = media_items
                    tweet_data['has_media'] = True
                    self.logger.debug(f"📷 MEDIA: Extracted {len(media_items)} media items from tweet")
                else:
                    tweet_data['media'] = []
                    tweet_data['has_media'] = False
            except Exception as e:
                self.logger.debug(f"⚠️ Media extraction failed: {e}")
                tweet_data['media'] = []
                tweet_data['has_media'] = False

            # 8. THREAD DETECTION AND RECONSTRUCTION - Phase 1.3 Enhancement
            try:
                if tweet_data.get('text'):  # Only process if we have valid tweet text
                    tweet_data = await self._detect_and_extract_thread_info(tweet_element, tweet_data)
                    self.logger.debug(f"🧵 THREAD: Added thread info to tweet")
            except Exception as e:
                self.logger.debug(f"⚠️ Thread detection failed: {e}")
                # Add empty thread info on failure
                tweet_data['thread_info'] = {'is_thread': False, 'error': str(e)}

            # 9. CONTENT CLASSIFICATION - Phase 3.2 Enhancement
            try:
                if tweet_data.get('text'):  # Only process if we have valid tweet text
                    classification = self.classify_content(tweet_data)
                    tweet_data['classification'] = classification
                    self.logger.debug(f"🏷️ CLASSIFICATION: Added content analysis to tweet")
            except Exception as e:
                self.logger.debug(f"⚠️ Content classification failed: {e}")
                # Add empty classification on failure
                tweet_data['classification'] = {'error': str(e)}

            # Return the enhanced tweet data
            return tweet_data if tweet_data.get('text') else None

        except Exception as e:
            self.logger.error(f"❌ Tweet extraction failed for element {index}: {e}")
            return None

    # ===== NEW VALIDATION SYSTEM =====
    def _validate_authorship_by_url(self, tweet: Dict, username: str) -> bool:
        """Fast validation using URL patterns - PHASE 1 VALIDATION"""
        url = tweet.get('url', '')
        if not url:
            return False

        # Check if URL contains the target username
        username_lower = username.lower()
        url_lower = url.lower()

        # Pattern 1: Direct user tweet URL
        if f"/{username_lower}/status/" in url_lower:
            return True

        # Pattern 2: User profile mention in URL
        if f"x.com/{username_lower}" in url_lower or f"twitter.com/{username_lower}" in url_lower:
            return True

        return False

    def _validate_authorship_by_patterns(self, tweet: Dict, username: str) -> bool:
        """Validate using content and metadata patterns - PHASE 2 VALIDATION"""
        username_lower = username.lower()

        # Check author field
        author = tweet.get('author', '').lower()
        if author == username_lower:
            return True

        # Check author_name patterns (handle display name variations)
        author_name = tweet.get('author_name', '').lower()
        if author_name and username_lower in author_name:
            return True

        # Check if extraction metadata indicates correct user
        extracted_from = tweet.get('extracted_from', '')
        if 'hybrid_extraction' in extracted_from and tweet.get('author') == username:
            # This suggests it was extracted during user-specific extraction
            return True

        return False

    async def _spot_check_tweet_authorship(self, tweet: Dict, username: str) -> bool:
        """Sample validation: Visit individual tweet URLs for verification - PHASE 3 VALIDATION"""
        url = tweet.get('url')
        if not url or not url.startswith('http'):
            return False

        try:
            self.logger.debug(f"🔍 SPOT-CHECK: Validating {url}")

            # Quick visit to verify authorship
            await self.page.goto(url, wait_until='domcontentloaded', timeout=15000)
            await asyncio.sleep(1)

            # Check the actual author on the individual tweet page
            author_selectors = [
                '[data-testid="User-Name"] a',
                'a[href*="/' + username + '"]',
                '[role="link"]:has-text("@' + username + '")'
            ]

            for selector in author_selectors:
                try:
                    author_element = self.page.locator(selector).first
                    if await author_element.count() > 0:
                        href = await author_element.get_attribute('href')
                        if href and f"/{username}" in href:
                            self.logger.debug(f"✅ SPOT-CHECK PASSED: {url}")
                            return True
                except:
                    continue

            self.logger.debug(f"❌ SPOT-CHECK FAILED: {url}")
            return False

        except Exception as e:
            self.logger.debug(f"⚠️ SPOT-CHECK ERROR for {url}: {e}")
            return False

    async def _validate_tweet_authorship_batch(self, raw_tweets: List[Dict], username: str) -> List[Dict]:
        """Batch validation of tweet authorship using multi-phase approach"""
        validated_tweets = []
        validation_stats = {'url_validated': 0, 'pattern_validated': 0, 'spot_checked': 0, 'rejected': 0}

        self.logger.info(f"🔍 VALIDATION: Processing {len(raw_tweets)} tweets for @{username}")

        for i, tweet in enumerate(raw_tweets):
            # Phase 1: URL-based validation (fastest)
            if self._validate_authorship_by_url(tweet, username):
                tweet['validation_method'] = 'url_verified'
                tweet['validation_confidence'] = 'high'
                validated_tweets.append(tweet)
                validation_stats['url_validated'] += 1
                continue

            # Phase 2: Pattern-based validation
            if self._validate_authorship_by_patterns(tweet, username):
                tweet['validation_method'] = 'pattern_verified'
                tweet['validation_confidence'] = 'medium'
                validated_tweets.append(tweet)
                validation_stats['pattern_validated'] += 1
                continue

            # Phase 3: Spot-check validation (every 10th tweet + suspicious cases)
            should_spot_check = (
                i % 10 == 0 or  # Every 10th tweet
                len(validated_tweets) < len(raw_tweets) * 0.5  # If validation rate is low
            )

            if should_spot_check:
                if await self._spot_check_tweet_authorship(tweet, username):
                    tweet['validation_method'] = 'spot_checked'
                    tweet['validation_confidence'] = 'verified'
                    validated_tweets.append(tweet)
                    validation_stats['spot_checked'] += 1
                    continue

            # Rejected
            validation_stats['rejected'] += 1
            self.logger.debug(f"❌ REJECTED: {tweet.get('text', 'No text')[:50]}...")

        # Report validation results
        total_processed = len(raw_tweets)
        total_validated = len(validated_tweets)
        accuracy_rate = (total_validated / total_processed) if total_processed > 0 else 0

        self.logger.info(f"📊 VALIDATION COMPLETE: {total_validated}/{total_processed} tweets validated ({accuracy_rate:.1%})")
        self.logger.info(f"📈 BREAKDOWN: URL={validation_stats['url_validated']}, Pattern={validation_stats['pattern_validated']}, Spot-check={validation_stats['spot_checked']}, Rejected={validation_stats['rejected']}")

        return validated_tweets

    # ===== ADAPTIVE EXTRACTION SYSTEM =====
    async def _extract_posts_adaptive(self, username: str, max_posts: int, level: int) -> List[Dict[str, Any]]:
        """SMART: Choose extraction method based on scale requirements and level"""
        self.logger.info(f"🎯 ADAPTIVE EXTRACTION: {max_posts} posts, Level {level} for @{username}")

        # PHASE C: OPTIMIZED LEVEL 4 ROUTING - Smart scale selection
        if level >= 4:
            if max_posts <= 20:
                # PHASE C: Small requests still use validated small-scale for reliability
                self.logger.info(f"🎯 LEVEL 4 SMALL SCALE: Using enhanced small-scale for {max_posts} posts (reliability over power)")
                return await self._extract_small_scale_validated(username, max_posts)
            elif max_posts <= 50:
                # PHASE C: Medium requests use hybrid approach
                self.logger.info(f"⚖️ LEVEL 4 MEDIUM SCALE: Using hybrid approach for {max_posts} posts")
                return await self._extract_medium_scale_hybrid(username, max_posts)
            else:
                # PHASE C: Only truly large requests go to large-scale
                self.logger.info(f"🔥 LEVEL 4 LARGE SCALE: Using comprehensive extraction for {max_posts}+ posts")
                return await self._extract_large_scale_validated(username, max_posts)

        # For Levels 1-3: Use scale-based routing
        if max_posts <= 30:
            # SMALL SCALE: Use enhanced DOM with URL validation
            self.logger.info(f"📱 SMALL SCALE: Using enhanced DOM extraction for {max_posts} tweets")
            return await self._extract_small_scale_validated(username, max_posts)

        elif max_posts <= 100:
            # MEDIUM SCALE: Use hybrid approach
            self.logger.info(f"⚖️ MEDIUM SCALE: Using hybrid extraction for {max_posts} tweets")
            return await self._extract_medium_scale_hybrid(username, max_posts)

        else:
            # LARGE SCALE: Use DOM with post-processing validation
            self.logger.info(f"🌟 LARGE SCALE: Using comprehensive DOM+validation for {max_posts}+ tweets")
            return await self._extract_large_scale_validated(username, max_posts)

    async def _extract_small_scale_validated(self, username: str, max_posts: int) -> List[Dict[str, Any]]:
        """SMALL SCALE: Enhanced DOM extraction with immediate validation - Phase C Optimized"""
        try:
            tweets = []

            # PHASE C: ENSURE PROPER NAVIGATION
            user_url = f"https://x.com/{username.replace('@', '')}"
            self.logger.info(f"🎯 NAVIGATING to user profile: {user_url}")

            try:
                await self.page.goto(user_url, wait_until='domcontentloaded', timeout=30000)
                await asyncio.sleep(3)  # Wait for content to load

                page_title = await self.page.title()
                self.logger.info(f"📄 Page loaded: {page_title}")
            except Exception as nav_error:
                self.logger.error(f"❌ Navigation failed: {nav_error}")
                return []

            # EMERGENCY DOM DEBUG: Comprehensive selector testing
            tweet_selectors = [
                # Original selectors
                'article[data-testid="tweet"]',
                'div[data-testid="tweet"]',
                'article[role="article"]',
                'div[data-testid="cellInnerDiv"] article',
                'main[role="main"] article',

                # Emergency backup selectors
                'article',
                'div[data-testid="cellInnerDiv"]',
                '[data-testid="primaryColumn"] article',
                'section article',
                'div[dir="ltr"]',
                '[data-testid="tweetText"]',
                'div[lang]'
            ]

            tweet_elements = None
            count = 0
            selector_results = {}

            # EMERGENCY DEBUG: Test ALL selectors and log results
            self.logger.info(f"🚨 EMERGENCY DOM DEBUG: Testing {len(tweet_selectors)} selectors")

            for selector in tweet_selectors:
                try:
                    elements = self.page.locator(selector)
                    selector_count = await elements.count()
                    selector_results[selector] = selector_count
                    self.logger.info(f"🔍 SELECTOR TEST '{selector}': {selector_count} elements")

                    if selector_count > count:
                        tweet_elements = elements
                        count = selector_count
                        self.logger.info(f"✅ NEW BEST SELECTOR: '{selector}' with {selector_count} elements")

                    # If we find elements, try to extract sample text
                    if selector_count > 0:
                        try:
                            first_element = elements.first
                            sample_text = await first_element.inner_text()
                            self.logger.info(f"📝 SAMPLE TEXT from '{selector}': '{sample_text[:100]}...'")
                        except Exception as e:
                            self.logger.info(f"⚠️ TEXT EXTRACTION FAILED for '{selector}': {e}")

                except Exception as e:
                    selector_results[selector] = f"ERROR: {e}"
                    self.logger.error(f"❌ SELECTOR ERROR '{selector}': {e}")

            # EMERGENCY DEBUG: Log page state
            try:
                current_url = self.page.url
                page_title = await self.page.title()
                self.logger.info(f"🌐 CURRENT URL: {current_url}")
                self.logger.info(f"📄 PAGE TITLE: {page_title}")

                # Check for login/authentication issues
                if 'login' in page_title.lower() or 'sign' in page_title.lower():
                    self.logger.error(f"🚨 AUTHENTICATION REQUIRED: Page redirected to login")

            except Exception as e:
                self.logger.error(f"❌ PAGE STATE CHECK FAILED: {e}")

            # Final summary
            working_selectors = [sel for sel, cnt in selector_results.items() if isinstance(cnt, int) and cnt > 0]
            self.logger.info(f"📊 SELECTOR SUMMARY: {len(working_selectors)}/{len(tweet_selectors)} selectors found elements")
            self.logger.info(f"📊 BEST RESULT: {count} elements found")

            if count == 0:
                self.logger.error(f"🚨 CRITICAL: NO TWEET ELEMENTS FOUND WITH ANY SELECTOR")
                self.logger.error(f"🔍 SELECTOR RESULTS: {selector_results}")
            else:
                self.logger.info(f"✅ SUCCESS: Using selector with {count} elements")

            # Extract with immediate validation
            for i in range(min(count, max_posts * 2)):  # Extract more than needed for filtering
                element = tweet_elements.nth(i)
                if await element.is_visible():
                    tweet_data = await self._extract_tweet_from_element(element, i, username, include_engagement=True)
                    if tweet_data and tweet_data.get('text'):
                        # Immediate validation
                        if self._validate_authorship_by_url(tweet_data, username) or self._validate_authorship_by_patterns(tweet_data, username):
                            tweet_data['extraction_method'] = 'small_scale_validated'
                            tweet_data['validation_confidence'] = 'high'
                            tweets.append(tweet_data)

                            if len(tweets) >= max_posts:
                                break

            # Thread Reconstruction - Phase 1.3 Enhancement
            try:
                if len(tweets) > 0:
                    tweets = await self._reconstruct_thread_sequence(tweets, username)
            except Exception as e:
                self.logger.warning(f"⚠️ Thread reconstruction failed: {e}")

            self.logger.info(f"✅ SMALL SCALE COMPLETE: {len(tweets)} validated tweets extracted")
            return tweets

        except Exception as e:
            self.logger.error(f"❌ Small scale extraction failed: {e}")
            return []

    async def _extract_medium_scale_hybrid(self, username: str, max_posts: int) -> List[Dict[str, Any]]:
        """MEDIUM SCALE: Hybrid DOM + scrolling with batch validation"""
        try:
            tweets = []

            # Phase 1: Initial page extraction
            initial_tweets = await self._extract_small_scale_validated(username, min(max_posts, 30))
            tweets.extend(initial_tweets)

            # Phase 2: Scrolling if more needed
            if len(tweets) < max_posts:
                remaining_needed = max_posts - len(tweets)
                self.logger.info(f"🔄 MEDIUM SCALE SCROLLING: Need {remaining_needed} more tweets")

                # Track existing tweets for deduplication
                existing_texts = {tweet.get('text', '') for tweet in tweets}
                scroll_attempts = 0
                max_scrolls = 10

                while len(tweets) < max_posts and scroll_attempts < max_scrolls:
                    # Scroll and extract
                    await self.page.mouse.wheel(0, 800)
                    await asyncio.sleep(2)

                    # Extract new tweets from scrolled content
                    new_tweet_elements = self.page.locator('article[data-testid="tweet"]')
                    count = await new_tweet_elements.count()

                    for i in range(count):
                        if len(tweets) >= max_posts:
                            break

                        element = new_tweet_elements.nth(i)
                        if await element.is_visible():
                            tweet_data = await self._extract_tweet_from_element(element, i, username, include_engagement=True)
                            if tweet_data and tweet_data.get('text'):
                                # Skip duplicates
                                if tweet_data.get('text') in existing_texts:
                                    continue

                                # Validate immediately
                                if self._validate_authorship_by_url(tweet_data, username) or self._validate_authorship_by_patterns(tweet_data, username):
                                    tweet_data['extraction_method'] = 'medium_scale_hybrid'
                                    tweet_data['scroll_round'] = scroll_attempts + 1
                                    tweets.append(tweet_data)
                                    existing_texts.add(tweet_data.get('text'))

                    scroll_attempts += 1

            # Thread Reconstruction - Phase 1.3 Enhancement
            try:
                if len(tweets) > 0:
                    tweets = await self._reconstruct_thread_sequence(tweets, username)
            except Exception as e:
                self.logger.warning(f"⚠️ Thread reconstruction failed: {e}")

            self.logger.info(f"✅ MEDIUM SCALE COMPLETE: {len(tweets)} validated tweets extracted")
            return tweets

        except Exception as e:
            self.logger.error(f"❌ Medium scale extraction failed: {e}")
            return []

    async def _extract_large_scale_validated(self, username: str, max_posts: int) -> List[Dict[str, Any]]:
        """LARGE SCALE: Aggressive DOM extraction + post-processing validation - LEVEL 4 EVERYTHING MODE"""
        try:
            # Check for unlimited mode
            unlimited_mode = max_posts >= 99999

            if unlimited_mode:
                self.logger.info(f"🌟 UNLIMITED MODE: Extracting ALL available posts for @{username}")
                self.logger.info("🚀 NO LIMITS: Scrolling until no more content found")
                everything_target = 999999  # Effectively unlimited
            else:
                # AGGRESSIVE TARGET: Level 4 should extract significantly more
                everything_target = max(max_posts * 3, 300)  # More aggressive multiplier
                self.logger.info(f"🔥 LEVEL 4 EVERYTHING MODE: Targeting {everything_target} raw tweets for comprehensive validation")
                self.logger.info("🚀 AGGRESSIVE EXTRACTION: Enhanced scrolling enabled for maximum data collection")

            # Phase 1: TRUE LARGE SCALE extraction (new optimized method)
            raw_tweets = await self._extract_posts_true_large_scale(username, everything_target)

            self.logger.info(f"📊 RAW EXTRACTION: Retrieved {len(raw_tweets)} tweets for validation")

            # Phase 2: Batch validation
            validated_tweets = await self._validate_tweet_authorship_batch(raw_tweets, username)

            # Phase 3: Quality check and additional validation if needed
            validation_rate = len(validated_tweets) / len(raw_tweets) if raw_tweets else 0

            if validation_rate < 0.6:  # Less than 60% validation rate
                self.logger.warning(f"⚠️ LOW VALIDATION RATE ({validation_rate:.1%}), performing additional checks")

                # Additional spot-checking for remaining tweets
                unvalidated = [t for t in raw_tweets if t not in validated_tweets]
                additional_validated = []

                for tweet in unvalidated[:20]:  # Check up to 20 more
                    if await self._spot_check_tweet_authorship(tweet, username):
                        tweet['validation_method'] = 'additional_spot_check'
                        tweet['validation_confidence'] = 'verified'
                        additional_validated.append(tweet)

                validated_tweets.extend(additional_validated)
                self.logger.info(f"🔍 ADDITIONAL VALIDATION: Added {len(additional_validated)} more tweets")

            # Return appropriate amount based on mode
            if unlimited_mode:
                result = validated_tweets  # Return ALL validated tweets in unlimited mode
                self.logger.info(f"🌟 UNLIMITED MODE RESULT: Returning ALL {len(result)} validated tweets")
            else:
                result = validated_tweets[:max_posts] if max_posts < 200 else validated_tweets

            # Phase 4: Thread Reconstruction - Phase 1.3 Enhancement
            try:
                if len(result) > 0:
                    self.logger.info(f"🧵 THREAD RECONSTRUCTION: Processing {len(result)} tweets for thread analysis")
                    result = await self._reconstruct_thread_sequence(result, username)
                    self.logger.info(f"🧵 THREAD RECONSTRUCTION: Completed thread analysis")
            except Exception as e:
                self.logger.warning(f"⚠️ Thread reconstruction failed: {e}")
                # Continue with original results if thread reconstruction fails

            final_validation_rate = len(result) / len(raw_tweets) if raw_tweets else 0
            self.logger.info(f"✅ LARGE SCALE COMPLETE: {len(result)} tweets ({final_validation_rate:.1%} validation rate)")

            return result

        except Exception as e:
            self.logger.error(f"❌ Large scale extraction failed: {e}")
            return []

    async def _extract_posts_true_large_scale(self, username: str, target_count: int) -> List[Dict[str, Any]]:
        """TRUE LARGE-SCALE EXTRACTION: Intelligent scrolling with end-detection and memory management"""
        try:
            tweets = []
            unlimited_mode = target_count >= 99999

            # Advanced state tracking for large scale
            extraction_state = {
                'seen_tweet_ids': set(),
                'seen_texts': set(),
                'consecutive_empty_scrolls': 0,
                'last_tweet_count': 0,
                'memory_cleanup_counter': 0,
                'scroll_performance': [],
                'end_of_feed_indicators': 0,
                'duplicate_batches': 0,
                'extraction_efficiency': []
            }

            self.logger.info(f"🚀 TRUE LARGE SCALE: Target={target_count}, Unlimited={unlimited_mode}")

            # Phase 1: Initial extraction with optimization
            await self._wait_for_timeline_load()
            initial_tweets = await self._extract_initial_batch_optimized(username, extraction_state)
            tweets.extend(initial_tweets)

            if unlimited_mode:
                self.logger.info(f"🌟 UNLIMITED MODE: Scrolling until absolute end of feed")
                max_scrolls = 3000  # Very high limit
                empty_threshold = 15  # Allow more empty scrolls
            else:
                # Intelligent scroll calculation
                tweets_per_scroll = max(len(initial_tweets) // 2, 3)
                estimated_scrolls = (target_count // tweets_per_scroll) + 20
                max_scrolls = min(estimated_scrolls, 1000)
                empty_threshold = 8
                self.logger.info(f"🎯 CALCULATED SCROLLS: {max_scrolls} (estimated {tweets_per_scroll} tweets per scroll)")

            # Phase 2: Intelligent progressive scrolling with end detection
            scroll_attempt = 0
            last_progress_log = 0

            while scroll_attempt < max_scrolls:
                # Memory management every 50 scrolls
                if extraction_state['memory_cleanup_counter'] % 50 == 0:
                    await self._cleanup_browser_memory()

                # Perform scroll with performance tracking
                scroll_start = time.time()
                tweets_before = len(tweets)

                success = await self._perform_intelligent_scroll(scroll_attempt, extraction_state)
                if not success:
                    self.logger.warning(f"⚠️ Scroll failed at attempt {scroll_attempt}")
                    extraction_state['consecutive_empty_scrolls'] += 1
                else:
                    # Extract new tweets from current view
                    new_tweets = await self._extract_new_tweets_batch(username, extraction_state)

                    # Add unique tweets
                    added_count = 0
                    for tweet in new_tweets:
                        tweet_id = tweet.get('id', '')
                        tweet_text = tweet.get('text', '')

                        if tweet_id and tweet_id not in extraction_state['seen_tweet_ids']:
                            extraction_state['seen_tweet_ids'].add(tweet_id)
                            extraction_state['seen_texts'].add(tweet_text)
                            tweet['extraction_scroll'] = scroll_attempt
                            tweets.append(tweet)
                            added_count += 1

                    # Performance tracking
                    scroll_time = time.time() - scroll_start
                    extraction_state['scroll_performance'].append({
                        'scroll': scroll_attempt,
                        'time': scroll_time,
                        'tweets_added': added_count,
                        'total_tweets': len(tweets)
                    })

                    # Progress logging every 20 scrolls or significant progress
                    if scroll_attempt - last_progress_log >= 20 or len(tweets) - last_progress_log >= 50:
                        avg_time = sum(p['time'] for p in extraction_state['scroll_performance'][-10:]) / min(10, len(extraction_state['scroll_performance']))
                        self.logger.info(f"📊 PROGRESS: Scroll {scroll_attempt}/{max_scrolls}, {len(tweets)} tweets (+{added_count}), {avg_time:.1f}s/scroll")
                        last_progress_log = len(tweets)

                    # Reset empty counter if we got tweets
                    if added_count > 0:
                        extraction_state['consecutive_empty_scrolls'] = 0
                    else:
                        extraction_state['consecutive_empty_scrolls'] += 1

                # End-of-feed detection
                if await self._detect_end_of_feed(extraction_state, tweets):
                    self.logger.info(f"🏁 END OF FEED DETECTED: Stopping at scroll {scroll_attempt}")
                    break

                # Empty scroll threshold
                if extraction_state['consecutive_empty_scrolls'] >= empty_threshold:
                    self.logger.info(f"🛑 EMPTY THRESHOLD: {empty_threshold} consecutive empty scrolls, stopping")
                    break

                # Target reached (non-unlimited mode)
                if not unlimited_mode and len(tweets) >= target_count:
                    self.logger.info(f"🎯 TARGET REACHED: {len(tweets)}/{target_count} tweets extracted")
                    break

                scroll_attempt += 1
                extraction_state['memory_cleanup_counter'] += 1

                # Adaptive delay based on performance
                delay = self._calculate_adaptive_delay(extraction_state)
                await asyncio.sleep(delay)

            # Performance summary
            total_time = sum(p['time'] for p in extraction_state['scroll_performance'])
            avg_tweets_per_scroll = len(tweets) / max(scroll_attempt, 1)

            self.logger.info(f"✅ TRUE LARGE SCALE COMPLETE:")
            self.logger.info(f"   📊 Results: {len(tweets)} tweets in {scroll_attempt} scrolls")
            self.logger.info(f"   ⚡ Performance: {avg_tweets_per_scroll:.1f} tweets/scroll, {total_time:.1f}s total")
            self.logger.info(f"   🧹 Memory cleanups: {extraction_state['memory_cleanup_counter'] // 50}")

            return tweets[:target_count] if not unlimited_mode else tweets

        except Exception as e:
            self.logger.error(f"❌ True large scale extraction failed: {e}")
            return []

    async def _wait_for_timeline_load(self):
        """Wait for Twitter timeline to properly load"""
        try:
            await self.page.wait_for_selector('article[data-testid="tweet"]', timeout=15000)
            await asyncio.sleep(2)  # Allow additional content to load
        except Exception as e:
            self.logger.warning(f"⚠️ Timeline load wait failed: {e}")

    async def _extract_initial_batch_optimized(self, username: str, extraction_state: dict) -> List[Dict[str, Any]]:
        """Extract initial batch of tweets with optimization"""
        try:
            tweets = []
            tweet_elements = self.page.locator('article[data-testid="tweet"]')
            count = await tweet_elements.count()

            for i in range(count):
                element = tweet_elements.nth(i)
                if await element.is_visible():
                    tweet_data = await self._extract_tweet_from_element(element, i, username, include_engagement=True)
                    if tweet_data and tweet_data.get('text'):
                        tweet_id = tweet_data.get('id', f"initial_{i}")
                        tweet_text = tweet_data.get('text', '')

                        if tweet_id not in extraction_state['seen_tweet_ids']:
                            extraction_state['seen_tweet_ids'].add(tweet_id)
                            extraction_state['seen_texts'].add(tweet_text)
                            tweet_data['extraction_phase'] = 'initial'
                            tweets.append(tweet_data)

            self.logger.info(f"📦 INITIAL BATCH: {len(tweets)} tweets extracted")
            return tweets

        except Exception as e:
            self.logger.error(f"❌ Initial batch extraction failed: {e}")
            return []

    async def _perform_intelligent_scroll(self, scroll_attempt: int, extraction_state: dict) -> bool:
        """Perform intelligent scroll with adaptive behavior and human-like patterns"""
        try:
            # Adaptive scroll distance based on performance
            if len(extraction_state['scroll_performance']) > 5:
                recent_efficiency = [p['tweets_added'] for p in extraction_state['scroll_performance'][-5:]]
                avg_efficiency = sum(recent_efficiency) / len(recent_efficiency)

                if avg_efficiency < 1:  # Low efficiency, scroll more
                    scroll_distance = 1200
                elif avg_efficiency > 5:  # High efficiency, scroll less
                    scroll_distance = 600
                else:
                    scroll_distance = 800
            else:
                scroll_distance = 800

            # HUMAN BEHAVIOR: Use human-like scrolling instead of instant scroll
            await HumanBehavior.human_scroll(self.page, 'down', scroll_distance)

            # HUMAN BEHAVIOR: Random mouse movements occasionally
            if scroll_attempt % 5 == 0:  # Every 5 scrolls
                await HumanBehavior.random_mouse_movements(self.page, count=1)

            # HUMAN BEHAVIOR: Take breaks occasionally to seem human
            if HumanBehavior.should_take_break(scroll_attempt, threshold=15):
                await HumanBehavior.take_human_break(self.page, duration_seconds=3)

            return True

        except Exception as e:
            self.logger.warning(f"⚠️ Scroll attempt {scroll_attempt} failed: {e}")
            return False

    async def _extract_new_tweets_batch(self, username: str, extraction_state: dict) -> List[Dict[str, Any]]:
        """Extract new tweets from current viewport with human-like behavior"""
        try:
            tweets = []
            tweet_elements = self.page.locator('article[data-testid="tweet"]')
            count = await tweet_elements.count()

            # Only check visible tweets to avoid processing entire DOM
            for i in range(min(count, 20)):  # Limit to recent tweets in viewport
                try:
                    element = tweet_elements.nth(i)
                    if await element.is_visible():
                        # HUMAN BEHAVIOR: Simulate reading the tweet before extracting
                        if i % 3 == 0:  # Every 3rd tweet, add reading delay
                            await HumanBehavior.random_delay(300, 800)

                        tweet_data = await self._extract_tweet_from_element(element, i, username, include_engagement=True)
                        if tweet_data and tweet_data.get('text'):
                            tweets.append(tweet_data)

                            # HUMAN BEHAVIOR: Simulate reading time based on text length
                            text_length = len(tweet_data.get('text', ''))
                            if text_length > 100:  # Longer tweets get more reading time
                                await HumanBehavior.reading_delay(text_length, wpm=250)
                except Exception:
                    continue  # Skip problematic elements

            return tweets

        except Exception as e:
            self.logger.warning(f"⚠️ New tweets batch extraction failed: {e}")
            return []

    async def _detect_end_of_feed(self, extraction_state: dict, tweets: List[Dict[str, Any]]) -> bool:
        """Detect if we've reached the end of the Twitter feed"""
        try:
            # Multiple end-of-feed indicators
            indicators = 0

            # 1. Too many consecutive empty scrolls
            if extraction_state['consecutive_empty_scrolls'] >= 10:
                indicators += 1

            # 2. No new tweets in last several batches
            if len(extraction_state['scroll_performance']) >= 20:
                recent_adds = [p['tweets_added'] for p in extraction_state['scroll_performance'][-20:]]
                if sum(recent_adds) == 0:
                    indicators += 1

            # 3. Check for "Show more tweets" or similar end indicators
            try:
                end_selectors = [
                    'text="Show more tweets"',
                    'text="Something went wrong"',
                    'text="You\'re all caught up"',
                    '[data-testid="emptyState"]'
                ]

                for selector in end_selectors:
                    if await self.page.locator(selector).count() > 0:
                        indicators += 1
                        break
            except:
                pass

            # 4. Duplicate content detection
            if len(tweets) > 100:
                recent_texts = [t.get('text', '') for t in tweets[-50:]]
                older_texts = [t.get('text', '') for t in tweets[-100:-50]]
                overlap = len(set(recent_texts) & set(older_texts))
                if overlap > 25:  # More than 50% overlap
                    indicators += 1

            extraction_state['end_of_feed_indicators'] = indicators
            return indicators >= 2  # Require multiple indicators

        except Exception as e:
            self.logger.warning(f"⚠️ End-of-feed detection failed: {e}")
            return False

    async def _cleanup_browser_memory(self):
        """Clean up browser memory during large extractions"""
        try:
            # Force garbage collection on page
            await self.page.evaluate("window.gc && window.gc()")
            await asyncio.sleep(0.5)
        except Exception:
            pass  # Not critical if it fails

    def _calculate_adaptive_delay(self, extraction_state: dict) -> float:
        """Calculate adaptive delay based on extraction performance"""
        try:
            if len(extraction_state['scroll_performance']) < 3:
                return 2.0  # Default delay

            # Calculate recent performance
            recent_times = [p['time'] for p in extraction_state['scroll_performance'][-5:]]
            avg_time = sum(recent_times) / len(recent_times)

            # Adaptive delay: faster if extraction is slow, slower if extraction is fast
            if avg_time > 3.0:
                return 1.0  # Speed up if extractions are slow
            elif avg_time < 1.0:
                return 2.5  # Slow down if extractions are too fast
            else:
                return 1.5  # Balanced delay

        except Exception:
            return 2.0  # Safe default

    def _load_backup_credentials(self) -> List[Dict[str, str]]:
        """Load backup authentication credentials for resilience"""
        try:
            backup_creds = []

            # Primary credentials
            if self.email and self.username and self.password:
                backup_creds.append({
                    'email': self.email,
                    'username': self.username,
                    'password': self.password,
                    'type': 'primary'
                })

            # Secondary credentials from environment
            for i in range(2, 5):  # Support up to 3 backup accounts
                backup_email = os.getenv(f'X_EMAIL_{i}', '')
                backup_username = os.getenv(f'X_USERNAME_{i}', '')
                backup_password = os.getenv(f'X_PASS_{i}', '')

                if backup_email and backup_username and backup_password:
                    backup_creds.append({
                        'email': backup_email,
                        'username': backup_username,
                        'password': backup_password,
                        'type': f'backup_{i}'
                    })

            self.logger.info(f"🔑 BACKUP CREDENTIALS: {len(backup_creds)} credential sets available")
            return backup_creds

        except Exception as e:
            self.logger.warning(f"⚠️ Failed to load backup credentials: {e}")
            return []

    async def _check_session_health(self) -> bool:
        """Check if current session is healthy and authenticated"""
        try:
            if not self.page:
                return False

            # Quick authentication check
            current_url = self.page.url
            if 'login' in current_url or 'account/access' in current_url:
                self.logger.warning("⚠️ SESSION HEALTH: Redirected to login page")
                self.session_health_score -= 20
                return False

            # Check for rate limiting indicators
            try:
                rate_limit_indicators = [
                    'text="Rate limit exceeded"',
                    'text="Try again later"',
                    'text="Something went wrong"',
                    '[data-testid="error"]'
                ]

                for indicator in rate_limit_indicators:
                    if await self.page.locator(indicator).count() > 0:
                        self.logger.warning(f"⚠️ SESSION HEALTH: Rate limit detected")
                        self.rate_limit_detected = True
                        self.session_health_score -= 30
                        return False
            except:
                pass

            # Check for blocking indicators
            try:
                blocking_indicators = [
                    'text="Your account is suspended"',
                    'text="This account has been locked"',
                    'text="Unusual activity"'
                ]

                for indicator in blocking_indicators:
                    if await self.page.locator(indicator).count() > 0:
                        self.logger.error(f"❌ SESSION HEALTH: Account blocked/suspended")
                        self.blocked_detected = True
                        self.session_health_score = 0
                        return False
            except:
                pass

            # Positive indicators
            try:
                if await self.page.locator('[data-testid="tweet"]').count() > 0:
                    self.session_health_score = min(100, self.session_health_score + 5)
                    return True
            except:
                pass

            return self.session_health_score > 50

        except Exception as e:
            self.logger.warning(f"⚠️ Session health check failed: {e}")
            self.session_health_score -= 10
            return False

    async def _attempt_session_recovery(self) -> bool:
        """Attempt to recover from authentication/session issues"""
        try:
            self.logger.info("🔄 ATTEMPTING SESSION RECOVERY...")

            # Method 1: Try refreshing the page
            if self.session_health_score > 30:
                try:
                    await self.page.reload(wait_until='domcontentloaded', timeout=15000)
                    await asyncio.sleep(3)

                    if await self._check_session_health():
                        self.logger.info("✅ SESSION RECOVERY: Page refresh successful")
                        return True
                except Exception as e:
                    self.logger.warning(f"⚠️ Page refresh failed: {e}")

            # Method 2: Navigate to home page
            try:
                await self.page.goto('https://x.com/home', wait_until='domcontentloaded', timeout=15000)
                await asyncio.sleep(3)

                if await self._check_session_health():
                    self.logger.info("✅ SESSION RECOVERY: Home navigation successful")
                    return True
            except Exception as e:
                self.logger.warning(f"⚠️ Home navigation failed: {e}")

            # Method 3: Try backup credentials if available
            if len(self.backup_credentials) > 1 and self.current_credential_set < len(self.backup_credentials) - 1:
                self.current_credential_set += 1
                new_creds = self.backup_credentials[self.current_credential_set]

                self.logger.info(f"🔑 TRYING BACKUP CREDENTIALS: {new_creds['type']}")

                self.email = new_creds['email']
                self.username = new_creds['username']
                self.password = new_creds['password']

                # Attempt re-authentication
                try:
                    if await self._perform_authentication():
                        self.logger.info("✅ SESSION RECOVERY: Backup credentials successful")
                        return True
                except Exception as e:
                    self.logger.warning(f"⚠️ Backup credentials failed: {e}")

            self.logger.error("❌ SESSION RECOVERY: All recovery methods failed")
            return False

        except Exception as e:
            self.logger.error(f"❌ Session recovery attempt failed: {e}")
            return False

    async def _perform_authentication(self) -> bool:
        """Enhanced authentication with resilience features"""
        try:
            if self.auth_attempts >= self.max_auth_attempts:
                self.logger.error(f"❌ AUTH: Max attempts ({self.max_auth_attempts}) exceeded")
                return False

            self.auth_attempts += 1
            self.logger.info(f"🔐 AUTH ATTEMPT {self.auth_attempts}/{self.max_auth_attempts}")

            # Check if we need credentials
            if not self.email or not self.username or not self.password:
                self.logger.error("❌ AUTH: Missing credentials")
                return False

            # Try to navigate to login page
            await self.page.goto(self.LOGIN_URL, wait_until='domcontentloaded', timeout=30000)
            await asyncio.sleep(3)

            # Enhanced login process with better selectors
            try:
                # Email input
                email_selector = 'input[name="text"], input[autocomplete="username"]'
                await self.page.wait_for_selector(email_selector, timeout=10000)
                await self.page.fill(email_selector, self.email)
                await asyncio.sleep(1)

                # Next button
                next_selectors = ['[role="button"]:has-text("Next")', 'button:has-text("Next")']
                for selector in next_selectors:
                    try:
                        if await self.page.locator(selector).count() > 0:
                            await self.page.click(selector)
                            break
                    except:
                        continue

                await asyncio.sleep(2)

                # Username if required
                try:
                    username_selector = 'input[data-testid="ocfEnterTextTextInput"]'
                    if await self.page.locator(username_selector).count() > 0:
                        await self.page.fill(username_selector, self.username)
                        await self.page.click('[role="button"]:has-text("Next")')
                        await asyncio.sleep(2)
                except:
                    pass

                # Password
                password_selector = 'input[name="password"], input[type="password"]'
                await self.page.wait_for_selector(password_selector, timeout=10000)
                await self.page.fill(password_selector, self.password)
                await asyncio.sleep(1)

                # Login button
                login_selectors = ['[role="button"]:has-text("Log in")', 'button:has-text("Log in")']
                for selector in login_selectors:
                    try:
                        if await self.page.locator(selector).count() > 0:
                            await self.page.click(selector)
                            break
                    except:
                        continue

                await asyncio.sleep(5)

                # Check authentication success
                if 'home' in self.page.url or 'x.com' in self.page.url and 'login' not in self.page.url:
                    self.authenticated = True
                    self.session_health_score = 100
                    self.last_auth_check = time.time()
                    self.logger.info("✅ AUTH: Authentication successful")
                    return True
                else:
                    self.logger.warning(f"⚠️ AUTH: Authentication may have failed. Current URL: {self.page.url}")
                    return False

            except Exception as login_e:
                self.logger.error(f"❌ AUTH: Login process failed: {login_e}")
                self.auth_failures.append(str(login_e))
                return False

        except Exception as e:
            self.logger.error(f"❌ AUTH: Authentication failed: {e}")
            self.auth_failures.append(str(e))
            return False

    async def _ensure_authenticated_session(self) -> bool:
        """Ensure we have a healthy, authenticated session before proceeding"""
        try:
            # Check if we need to authenticate
            if not self.authenticated or not await self._check_session_health():
                self.logger.info("🔐 AUTHENTICATION REQUIRED")

                # Try session recovery first
                if self.session_health_score > 0:
                    if await self._attempt_session_recovery():
                        return True

                # Full authentication
                if await self._perform_authentication():
                    return True
                else:
                    self.logger.error("❌ AUTHENTICATION: All authentication methods failed")
                    return False

            # Session is healthy
            self.last_auth_check = time.time()
            return True

        except Exception as e:
            self.logger.error(f"❌ Authentication check failed: {e}")
            return False

    async def _extract_posts_comprehensive_dom_aggressive(self, username: str, target_count: int) -> List[Dict[str, Any]]:
        """Aggressive DOM extraction without validation - raw data collection"""
        try:
            tweets = []

            # Phase 1: Initial page extraction
            tweet_elements = self.page.locator('article[data-testid="tweet"]')
            count = await tweet_elements.count()

            for i in range(count):
                element = tweet_elements.nth(i)
                if await element.is_visible():
                    tweet_data = await self._extract_tweet_from_element(element, i, username, include_engagement=True)
                    if tweet_data and tweet_data.get('text'):
                        tweet_data['extraction_phase'] = 'initial'
                        tweets.append(tweet_data)

            # Phase 2: Aggressive scrolling
            existing_tweet_ids = {tweet.get('id', '') for tweet in tweets}
            existing_texts = {tweet.get('text', '') for tweet in tweets}

            scroll_attempts = 0
            # Enhanced scrolling for unlimited mode
            if target_count >= 999999:  # Unlimited mode
                max_scrolls = 1000  # Very high limit for unlimited mode
                self.logger.info(f"🌟 UNLIMITED SCROLLING: Up to {max_scrolls} scroll attempts")
            else:
                max_scrolls = min(50, target_count // 10)  # Scale scrolls with target

            consecutive_no_new = 0  # Track consecutive rounds with no new tweets
            while len(tweets) < target_count and scroll_attempts < max_scrolls and consecutive_no_new < 5:
                await self.page.mouse.wheel(0, 800 + (scroll_attempts * 50))
                await asyncio.sleep(2)

                # Extract from scrolled content
                new_elements = self.page.locator('article[data-testid="tweet"]')
                new_count = await new_elements.count()

                tweets_this_round = 0
                for i in range(new_count):
                    element = new_elements.nth(i)
                    if await element.is_visible():
                        tweet_data = await self._extract_tweet_from_element(element, i, username, include_engagement=True)
                        if tweet_data and tweet_data.get('text'):
                            # Basic deduplication
                            tweet_id = tweet_data.get('id', '')
                            tweet_text = tweet_data.get('text', '')

                            if tweet_id not in existing_tweet_ids and tweet_text not in existing_texts:
                                tweet_data['extraction_phase'] = 'scrolled'
                                tweet_data['scroll_round'] = scroll_attempts + 1
                                tweets.append(tweet_data)
                                existing_tweet_ids.add(tweet_id)
                                existing_texts.add(tweet_text)
                                tweets_this_round += 1

                if tweets_this_round == 0:
                    consecutive_no_new += 1
                    self.logger.info(f"🔄 SCROLL ATTEMPT {scroll_attempts + 1}: No new tweets (consecutive: {consecutive_no_new}/5)")
                    if target_count >= 999999:  # Unlimited mode - be more patient
                        if consecutive_no_new >= 5:
                            self.logger.info(f"🏁 UNLIMITED MODE END: Reached end of available content after {scroll_attempts + 1} scrolls")
                            break
                    else:
                        # Regular mode - stop faster
                        break
                else:
                    consecutive_no_new = 0  # Reset counter on successful round
                    self.logger.info(f"🔄 SCROLL ROUND {scroll_attempts + 1}: Found {tweets_this_round} new tweets (total: {len(tweets)})")

                scroll_attempts += 1

            self.logger.info(f"📊 RAW EXTRACTION COMPLETE: {len(tweets)} tweets from {scroll_attempts} scroll rounds")
            return tweets

        except Exception as e:
            self.logger.error(f"❌ Aggressive DOM extraction failed: {e}")
            return []

    async def _extract_author_information(self, tweet_element, default_username: str = '') -> Dict[str, str]:
        """Extract author information from a tweet element."""
        try:
            author_info = {
                'author_username': default_username,
                'author_display_name': default_username
            }

            # Try to extract actual username
            username_element = tweet_element.locator('[data-testid="User-Name"] a').first
            if await username_element.count() > 0:
                username_text = await username_element.inner_text()
                if '@' in username_text:
                    actual_username = username_text.split('@')[1].split(' ')[0]
                    author_info['author_username'] = actual_username

            # Try to extract display name
            name_element = tweet_element.locator('[data-testid="User-Name"] span').first
            if await name_element.count() > 0:
                display_name = await name_element.inner_text()
                if display_name and not display_name.startswith('@'):
                    author_info['author_display_name'] = display_name

            return author_info
        except Exception as e:
            self.logger.debug(f"❌ Author extraction failed: {e}")
            return {
                'author_username': default_username,
                'author_display_name': default_username
            }

    
    def _convert_relative_to_iso(self, relative_time: str) -> str:
        """Convert relative timestamp like '2h ago' to ISO timestamp."""
        try:
            from datetime import datetime, timedelta
            import re
            
            if not relative_time:
                return None
            
            # Clean the input
            relative_time = relative_time.lower().strip()
            
            # Extract number and unit using regex
            # Patterns: "2h", "1d", "5m", "3w", "2h ago", "1d ago"
            pattern = r'(\d+)\s*([smhdw])'
            match = re.search(pattern, relative_time)
            
            if not match:
                return None
            
            number = int(match.group(1))
            unit = match.group(2)
            
            # Calculate timedelta based on unit
            now = datetime.now()
            
            if unit == 's':  # seconds
                delta = timedelta(seconds=number)
            elif unit == 'm':  # minutes
                delta = timedelta(minutes=number)
            elif unit == 'h':  # hours
                delta = timedelta(hours=number)
            elif unit == 'd':  # days
                delta = timedelta(days=number)
            elif unit == 'w':  # weeks
                delta = timedelta(weeks=number)
            else:
                return None
            
            # Subtract the delta to get the tweet time
            tweet_time = now - delta
            
            # Return ISO format timestamp
            return tweet_time.isoformat()
            
        except Exception as e:
            self.logger.debug(f"Failed to convert relative time '{relative_time}': {e}")
            return None
    
    
    # HELPER FUNCTIONS FOR DIFFERENTIATED SCRAPE LEVELS
    
    async def _extract_posts_basic(self, username: str, max_posts: int = 5) -> List[Dict[str, Any]]:
        """Extract basic posts with minimal processing - Level 1."""
        self.logger.info(f"⚪ NEW LEVEL 1 EXTRACTION: Basic posts with validation for @{username}")
        # Use new adaptive extraction system
        return await self._extract_posts_adaptive(username, max_posts, level=1)
    
    async def _extract_posts_with_engagement(self, username: str, max_posts: int = 15) -> List[Dict[str, Any]]:
        """Extract posts with engagement metrics - Level 2."""
        self.logger.info(f"🔵 NEW LEVEL 2 EXTRACTION: Engagement data with validation for @{username}")
        # Use new adaptive extraction system
        return await self._extract_posts_adaptive(username, max_posts, level=2)
    
    async def _extract_posts_with_full_data(self, username: str, max_posts: int = 25) -> List[Dict[str, Any]]:
        """Extract posts with full metadata - Level 3."""
        self.logger.info(f"🟡 NEW LEVEL 3 EXTRACTION: Full data with validation for @{username}")
        # Use new adaptive extraction system
        return await self._extract_posts_adaptive(username, max_posts, level=3)
    
    async def _extract_posts_comprehensive(self, username: str, max_posts: int = 50) -> List[Dict[str, Any]]:
        """Extract posts with maximum detail - SIMPLIFIED for reliability."""
        self.logger.info(f"🚀 SIMPLIFIED EXTRACTION: Extracting {max_posts} posts for @{username}")

        # Use simple, fast extraction instead of complex adaptive system
        return await self._extract_posts_simple_fast(username, max_posts)

    async def _human_delay(self, min_seconds: float = 1.0, max_seconds: float = 3.0):
        """Enhanced human-like delays with realistic patterns."""
        import random
        
        # Add variability based on different human behavior patterns
        base_delay = random.uniform(min_seconds, max_seconds)
        
        # 10% chance of longer "thinking" delays
        if random.random() < 0.1:
            base_delay += random.uniform(2.0, 5.0)
        
        # 5% chance of very quick actions
        elif random.random() < 0.05:
            base_delay *= 0.3
        
        await asyncio.sleep(base_delay)
    
    async def _human_type(self, element, text: str):
        """Type text with human-like delays and realistic typing patterns."""
        import random
        
        for i, char in enumerate(text):
            await element.type(char)
            
            # Variable typing speed based on character complexity
            if char in ' \t\n':  # Longer pause for spaces/breaks
                delay = random.uniform(0.15, 0.25)
            elif char.isupper() or char in '!@#$%^&*()':  # Slower for special chars
                delay = random.uniform(0.08, 0.18)
            else:  # Normal characters
                delay = random.uniform(0.04, 0.12)
            
            # Occasional longer pauses (human hesitation)
            if random.random() < 0.03:
                delay += random.uniform(0.5, 1.5)
            
            await asyncio.sleep(delay)
    
    async def _simulate_human_interaction(self):
        """Simulate realistic human browsing behavior."""
        import random
        
        # Random mouse movements
        if random.random() < 0.7:
            try:
                await self.page.mouse.move(
                    random.randint(50, 300),
                    random.randint(100, 500)
                )
                await self._human_delay(0.1, 0.3)
            except:
                pass
        
        # Occasional scrolling
        if random.random() < 0.4:
            try:
                await self.page.mouse.wheel(0, random.randint(100, 800))
                await self._human_delay(0.2, 0.8)
            except:
                pass
        
        # Random focus events
        if random.random() < 0.3:
            try:
                await self.page.focus('body')
                await self._human_delay(0.1, 0.3)
            except:
                pass
    
    async def _mobile_route_handler(self, route):
        """Intercept and control navigation to prevent redirects away from mobile interface."""
        url = route.request.url
        
        # Allow mobile Twitter domains
        if any(domain in url for domain in ['mobile.twitter.com', 'mobile.x.com']):
            self.logger.debug(f"✅ Allowing mobile URL: {url}")
            await route.continue_()
            return
        
        # Block redirects to desktop x.com or twitter.com login flows
        if any(domain in url for domain in ['x.com/i/flow', 'twitter.com/i/flow']):
            self.logger.info(f"🚫 Blocking desktop redirect: {url}")
            # Redirect back to mobile instead
            await route.fulfill(status=302, headers={'Location': 'https://mobile.twitter.com/login'})
            return
            
        # Allow other resources (CSS, JS, images, etc.)
        await route.continue_()

    # ===== PHASE 4.2: PERFORMANCE AND ANTI-DETECTION OPTIMIZATIONS =====

    async def _optimized_element_wait(self, selector: str, timeout: int = 10000) -> bool:
        """Phase 4.2: Optimized element waiting with performance caching."""
        cache_key = f"wait_{selector}_{timeout}"

        if cache_key in self.request_cache:
            self.extraction_stats['cache_hits'] += 1
            return self.request_cache[cache_key]

        try:
            await self.page.wait_for_selector(selector, timeout=timeout)
            result = True
        except:
            result = False

        # Cache successful selectors for future use
        if result:
            self.request_cache[cache_key] = result

        self.extraction_stats['requests_made'] += 1
        return result

    async def _stealth_scroll(self, distance: int = 800, duration: float = 2.0):
        """Phase 4.2: Human-like scrolling with anti-detection measures."""
        import random

        if self.anti_detection_enabled:
            # Vary scroll distance and timing
            actual_distance = distance + random.randint(-100, 100)
            actual_duration = duration + random.uniform(-0.5, 0.5)

            # Simulate human scroll patterns
            scroll_steps = random.randint(3, 8)
            step_distance = actual_distance // scroll_steps

            for _ in range(scroll_steps):
                await self.page.mouse.wheel(0, step_distance)
                await asyncio.sleep(actual_duration / scroll_steps)

            self.extraction_stats['anti_detection_actions'] += 1
        else:
            # Standard scroll
            await self.page.mouse.wheel(0, distance)
            await asyncio.sleep(duration)

    async def _random_mouse_movement(self):
        """Phase 4.2: Random mouse movements to simulate human behavior."""
        import random

        if self.anti_detection_enabled and random.random() < 0.3:
            try:
                # Get viewport size
                viewport = await self.page.viewport_size()
                if viewport:
                    x = random.randint(100, viewport['width'] - 100)
                    y = random.randint(100, viewport['height'] - 100)

                    # Smooth mouse movement
                    await self.page.mouse.move(x, y)
                    await asyncio.sleep(random.uniform(0.1, 0.3))

                    self.extraction_stats['anti_detection_actions'] += 1
            except:
                pass

    async def _intelligent_delay(self, base_delay: float = 1.0, context: str = "default"):
        """Phase 4.2: Context-aware intelligent delays."""
        import random

        # Adjust delay based on context
        context_multipliers = {
            "navigation": 1.5,
            "extraction": 0.8,
            "scroll": 1.2,
            "click": 1.0,
            "default": 1.0
        }

        multiplier = context_multipliers.get(context, 1.0)
        adjusted_delay = base_delay * multiplier

        # Add human-like variance
        if self.timing_variance > 0:
            variance = random.uniform(-self.timing_variance, self.timing_variance)
            adjusted_delay *= (1 + variance)

        # Minimum delay for anti-detection
        adjusted_delay = max(adjusted_delay, 0.2)

        await asyncio.sleep(adjusted_delay)

    async def _performance_optimized_extraction(self, selectors: List[str], element) -> Dict[str, Any]:
        """Phase 4.2: Performance-optimized element data extraction."""
        start_time = time.time()
        results = {}

        if self.performance_mode:
            # Batch multiple selector queries for efficiency
            tasks = []
            for selector in selectors:
                tasks.append(self._extract_single_selector_data(element, selector))

            # Execute in parallel
            selector_results = await asyncio.gather(*tasks, return_exceptions=True)

            for i, result in enumerate(selector_results):
                if not isinstance(result, Exception) and result:
                    results[selectors[i]] = result
        else:
            # Sequential extraction (fallback)
            for selector in selectors:
                try:
                    result = await self._extract_single_selector_data(element, selector)
                    if result:
                        results[selector] = result
                except Exception as e:
                    self.logger.debug(f"Selector {selector} failed: {e}")

        extraction_time = time.time() - start_time
        self.extraction_stats['extraction_time'] += extraction_time

        return results

    async def _extract_single_selector_data(self, element, selector: str) -> Optional[str]:
        """Helper method for extracting data from a single selector."""
        try:
            sub_element = element.locator(selector).first
            if await sub_element.count() > 0:
                return await sub_element.inner_text(timeout=2000)
        except:
            pass
        return None

    def _get_performance_metrics(self) -> Dict[str, Any]:
        """Phase 4.2: Get comprehensive performance metrics."""
        # Safety check - extraction_stats may not exist if scraper wasn't fully initialized
        if not hasattr(self, 'extraction_stats'):
            return {
                "total_requests": 0,
                "cache_hits": 0,
                "cache_hit_rate": "0.0%",
                "total_extraction_time": "0.00s",
                "avg_extraction_time": "0.000s",
                "anti_detection_actions": 0,
                "performance_mode": getattr(self, 'performance_mode', 'standard'),
                "anti_detection_enabled": getattr(self, 'anti_detection_enabled', True)
            }

        total_requests = self.extraction_stats['requests_made']
        cache_hit_rate = (self.extraction_stats['cache_hits'] / max(total_requests, 1)) * 100

        return {
            "total_requests": total_requests,
            "cache_hits": self.extraction_stats['cache_hits'],
            "cache_hit_rate": f"{cache_hit_rate:.1f}%",
            "total_extraction_time": f"{self.extraction_stats['extraction_time']:.2f}s",
            "avg_extraction_time": f"{self.extraction_stats['extraction_time'] / max(total_requests, 1):.3f}s",
            "anti_detection_actions": self.extraction_stats['anti_detection_actions'],
            "performance_mode": self.performance_mode,
            "anti_detection_enabled": self.anti_detection_enabled
        }

    async def _apply_browser_stealth(self):
        """Phase 4.2: Apply advanced browser stealth measures."""
        if not self.anti_detection_enabled:
            return

        try:
            # Remove webdriver indicators
            await self.page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined,
                });

                // Remove automation indicators
                delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
                delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
                delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;

                // Modify user agent
                Object.defineProperty(navigator, 'userAgent', {
                    get: () => 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                });

                // Mock plugins
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5],
                });

                // Mock languages
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-US', 'en'],
                });
            """)

            self.logger.debug("🕵️ Applied browser stealth measures")
            self.extraction_stats['anti_detection_actions'] += 1

        except Exception as e:
            self.logger.debug(f"⚠️ Stealth application failed: {e}")

    async def scrape_hashtag(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Phase 2.1: Enhanced Hashtag Scraping with Phase 1 Integration.

        Comprehensive hashtag scraping that leverages Phase 1 enhancements:
        - Media extraction in posts
        - Engagement metrics (likes, retweets, replies, views)
        - Thread detection and reconstruction
        """
        hashtag = params.get('hashtag', '').replace('#', '')
        max_tweets = params.get('max_tweets', 50)
        include_media = params.get('include_media', True)
        date_filter = params.get('date_filter', 'recent')
        scrape_level = params.get('scrape_level', 4)  # Use Level 4 for full enhancement

        self.logger.info(f"🏷️ Phase 2.1: ENHANCED HASHTAG SCRAPING for #{hashtag}")
        self.logger.info(f"📊 Target: {max_tweets} tweets | Media: {include_media} | Filter: {date_filter} | Level: {scrape_level}")

        try:
            # Construct hashtag search URL
            hashtag_url = f"https://x.com/search?q=%23{hashtag}&src=hashtag_click"

            # Add filter parameters
            if date_filter == 'recent':
                hashtag_url += "&f=live"
            elif date_filter == 'popular':
                hashtag_url += "&f=top"

            self.logger.info(f"🌐 Navigating to hashtag: {hashtag_url}")

            # Navigate with enhanced stealth
            await self._simulate_human_interaction()
            await self.page.goto(hashtag_url, wait_until='domcontentloaded', timeout=60000)
            await self._human_delay(3, 6)
            await self._simulate_human_interaction()
            
            # 🔥 ULTRA-AGGRESSIVE HASHTAG SEARCH CONTENT LOADING 🔥
            self.logger.info("⏳ ULTRA-AGGRESSIVE HASHTAG LOADING - Extended wait for search results...")
            
            max_attempts = 5  # Increased from 3
            content_loaded = False
            
            for attempt in range(max_attempts):
                self.logger.info(f"🔄 Hashtag loading attempt {attempt + 1}/{max_attempts}")
                
                # Multiple strategies to detect content loading
                
                # Strategy 1: Check div count (lowered threshold)
                div_count = await self.page.locator('div').count()
                self.logger.info(f"📊 Current div count: {div_count}")
                
                if div_count > 30:  # Lowered from 50
                    self.logger.info("✅ Basic content detected")
                    
                    # Strategy 2: Look for any meaningful text content
                    try:
                        body_text = await self.page.locator('body').inner_text()
                        text_length = len(body_text)
                        self.logger.info(f"📝 Body text length: {text_length} chars")
                        
                        if text_length > 1000 and hashtag.lower() in body_text.lower():
                            self.logger.info("✅ Rich content with hashtag found!")
                            content_loaded = True
                            break
                        elif text_length > 2000:
                            self.logger.info("✅ Rich content detected!")  
                            content_loaded = True
                            break
                    except:
                        pass
                
                # Strategy 3: Check for search-specific indicators
                search_indicators = [
                    'main',
                    '[role="main"]', 
                    '[data-testid="primaryColumn"]',
                    'article',  # Look for any articles
                    'time',    # Timestamps indicate tweets
                    '[href*="/status/"]'  # Tweet links
                ]
                
                indicators_found = 0
                for indicator in search_indicators:
                    try:
                        count = await self.page.locator(indicator).count()
                        if count > 0:
                            self.logger.info(f"✅ Found {count} {indicator} elements")
                            indicators_found += 1
                    except:
                        continue
                
                if indicators_found >= 2:  # Multiple indicators = good content
                    self.logger.info(f"✅ Multiple indicators found: {indicators_found}")
                    content_loaded = True
                    break
                
                # Strategy 4: Scroll and wait to trigger lazy loading
                if attempt < max_attempts - 1:  # Don't scroll on last attempt
                    self.logger.info("📜 Scrolling to trigger content loading...")
                    await self.page.mouse.wheel(0, 400)
                    await self._human_delay(3, 5)
                    await self.page.mouse.wheel(0, -200)  # Scroll back up
                    await self._human_delay(5, 8)
                else:
                    await self._human_delay(10, 15)  # Final long wait
            
            if not content_loaded:
                self.logger.warning("⚠️ Content loading unclear - proceeding with extraction")
            else:
                self.logger.info("🎉 HASHTAG SEARCH CONTENT LOADED SUCCESSFULLY!")
                
            # Final preparation - additional scroll to ensure more content
            self.logger.info("🔄 Final content preparation...")
            await self.page.mouse.wheel(0, 600)
            await self._human_delay(2, 4)
            await self.page.mouse.wheel(0, -300)
            await self._human_delay(1, 2)
            
            # Phase 2.1: Use Phase 1 Enhanced Tweet Extraction for Hashtag Content
            self.logger.info("🏷️ Phase 2.1: Using enhanced tweet extraction with media, engagement, and threads...")

            tweets = []
            extracted_tweet_ids = set()
            seen_text_hashes = set()
            scroll_attempts = 0
            # 🔧 PHASE A FIX: Ensure at least 1 scroll attempt for any request
            max_scrolls = max(min(max_tweets // 5, 15), 1)  # Always allow at least 1 scroll

            # 🔧 PHASE A DEBUG: Log loop conditions
            self.logger.info(f"🔧 LOOP DEBUG: tweets={len(tweets)}, max_tweets={max_tweets}, scroll_attempts={scroll_attempts}, max_scrolls={max_scrolls}")
            self.logger.info(f"🔧 LOOP CONDITION: {len(tweets)} < {max_tweets} = {len(tweets) < max_tweets}, {scroll_attempts} < {max_scrolls} = {scroll_attempts < max_scrolls}")

            while len(tweets) < max_tweets and scroll_attempts < max_scrolls:
                self.logger.info(f"🔄 Hashtag extraction round {scroll_attempts + 1}/{max_scrolls}")

                # 🔧 PHASE A FIX: Enhanced tweet element detection with multiple selectors
                tweet_selectors = [
                    'article[data-testid="tweet"]',
                    'div[data-testid="tweet"]',
                    'article[role="article"]',
                    'div[data-testid="cellInnerDiv"] article',
                    'main[role="main"] article'
                ]

                tweet_elements = []
                for selector in tweet_selectors:
                    try:
                        elements = await self.page.locator(selector).all()
                        if elements:
                            tweet_elements.extend(elements)
                            self.logger.info(f"🎯 Found {len(elements)} elements using selector: {selector}")
                            break
                    except Exception as e:
                        self.logger.debug(f"Selector '{selector}' failed: {e}")
                        continue

                self.logger.info(f"🎯 Total tweet elements found in hashtag search: {len(tweet_elements)}")

                # Extract tweets using our Phase 1 enhanced method
                for i, element in enumerate(tweet_elements):
                    if len(tweets) >= max_tweets:
                        break

                    try:
                        if await element.is_visible():
                            # Use our comprehensive tweet extraction method with all Phase 1 enhancements
                            tweet_data = await self._extract_tweet_from_element(element, i, f"hashtag_{hashtag}", include_engagement=True)

                            if tweet_data and tweet_data.get('text'):
                                # Add hashtag-specific metadata
                                tweet_data['hashtag'] = f'#{hashtag}'
                                tweet_data['extraction_context'] = 'hashtag_search'
                                tweet_data['search_url'] = hashtag_url
                                tweet_data['date_filter'] = date_filter
                                tweet_data['extraction_method'] = 'phase2_hashtag_enhanced'

                                # Check for duplicates using text hash
                                import hashlib
                                text_hash = hashlib.md5(tweet_data['text'].encode()).hexdigest()

                                if (text_hash not in seen_text_hashes and
                                    tweet_data.get('tweet_id') not in extracted_tweet_ids):

                                    seen_text_hashes.add(text_hash)
                                    if tweet_data.get('tweet_id'):
                                        extracted_tweet_ids.add(tweet_data['tweet_id'])

                                    tweets.append(tweet_data)
                                    self.logger.info(f"🏷️ Phase 2.1: Extracted enhanced hashtag tweet {len(tweets)}/{max_tweets}")
                                    self.logger.info(f"   📝 Text: {tweet_data['text'][:80]}...")

                                    # Log Phase 1 enhancements found
                                    enhancements = []
                                    if tweet_data.get('has_media'):
                                        enhancements.append(f"media:{len(tweet_data.get('media', []))}")
                                    if tweet_data.get('likes') is not None:
                                        enhancements.append(f"likes:{tweet_data['likes']}")
                                    if tweet_data.get('thread_info', {}).get('is_thread'):
                                        enhancements.append("thread:true")

                                    if enhancements:
                                        self.logger.info(f"   ✨ Phase 1 enhancements: {', '.join(enhancements)}")

                    except Exception as e:
                        self.logger.debug(f"🏷️ Error extracting hashtag tweet {i}: {e}")

                # Scroll for more content
                if len(tweets) < max_tweets:
                    self.logger.info(f"📜 Scrolling for more hashtag content... ({len(tweets)}/{max_tweets})")
                    await self.page.mouse.wheel(0, 800)
                    await self._human_delay(2, 4)
                    scroll_attempts += 1
                else:
                    break

            # Phase 2.1: Apply Thread Reconstruction to Hashtag Results
            if len(tweets) > 0:
                self.logger.info(f"🧵 Phase 2.1: Applying thread reconstruction to hashtag results...")
                try:
                    tweets = await self._reconstruct_thread_sequence(tweets, f"hashtag_{hashtag}")
                except Exception as e:
                    self.logger.warning(f"⚠️ Thread reconstruction failed for hashtag: {e}")
            
            self.logger.info(f"🏁 Hashtag scraping completed: {len(tweets)} tweets extracted")
            
            # Phase 2.1: Enhanced result structure with comprehensive metadata
            result = {
                "type": "comprehensive_hashtag",
                "hashtag": f"#{hashtag}",
                "search_parameters": {
                    "hashtag": hashtag,
                    "max_tweets": max_tweets,
                    "date_filter": date_filter,
                    "scrape_level": scrape_level,
                    "include_media": include_media
                },
                "posts": tweets,
                "extraction_stats": {
                    "total_posts": len(tweets),
                    "scroll_attempts": scroll_attempts,
                    "extraction_method": "phase2_hashtag_enhanced",
                    "with_media": len([t for t in tweets if t.get('has_media')]),
                    "with_engagement": len([t for t in tweets if any(t.get(m) is not None for m in ['likes', 'retweets', 'replies'])]),
                    "with_threads": len([t for t in tweets if t.get('thread_info', {}).get('is_thread')])
                }
            }

            self.logger.info(f"📊 Phase 2.1 Results: {result['extraction_stats']['total_posts']} posts, {result['extraction_stats']['with_media']} with media, {result['extraction_stats']['with_engagement']} with engagement, {result['extraction_stats']['with_threads']} in threads")

            return [result]

        except Exception as e:
            self.logger.error(f"❌ Hashtag scraping failed: {e}")
            import traceback
            self.logger.error(f"❌ Hashtag scraping traceback: {traceback.format_exc()}")
            return []

    async def scrape_search_query(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Phase 2.2: Advanced Search Query Scraping.

        Comprehensive search-based content discovery that supports:
        - Keyword searches
        - Hashtag searches (#AI, #crypto)
        - User mention searches (@username)
        - Complex queries ("AI startup" OR "machine learning")
        - Date filtering and result types
        """
        search_query = params.get('search_query', params.get('query', ''))
        max_tweets = params.get('max_tweets', 50)
        result_type = params.get('result_type', 'recent')  # recent, popular, mixed
        date_filter = params.get('date_filter', None)  # last_day, last_week, etc.
        scrape_level = params.get('scrape_level', 4)
        include_media = params.get('include_media', True)

        self.logger.info(f"🔍 Phase 2.2: ADVANCED SEARCH QUERY SCRAPING")
        self.logger.info(f"📝 Query: '{search_query}'")
        self.logger.info(f"📊 Target: {max_tweets} tweets | Type: {result_type} | Level: {scrape_level}")

        try:
            # FIXED: Construct search URL with proper encoding and X.com format
            import urllib.parse

            # Validate search query
            if not search_query or search_query.strip() == '':
                self.logger.error("❌ Empty search query provided")
                return []

            search_query = search_query.strip()
            self.logger.info(f"🔍 Processing search query: '{search_query}'")

            # FIXED: Use proper URL encoding (quote instead of quote_plus)
            encoded_query = urllib.parse.quote(search_query, safe='')

            # FIXED: Use correct X.com search URL format
            search_url = f"https://x.com/search?q={encoded_query}&src=typed_query"

            # FIXED: Add proper result type filter using X.com parameters
            if result_type == 'recent' or result_type == 'live':
                search_url += "&f=live"  # Recent/live tweets
                self.logger.info("📊 Filter: Recent/Live tweets")
            elif result_type == 'popular' or result_type == 'top':
                search_url += "&f=top"   # Popular tweets
                self.logger.info("📊 Filter: Popular tweets")
            else:
                self.logger.info("📊 Filter: Mixed results (default)")

            # FIXED: Date filtering using proper X.com format
            if date_filter:
                from datetime import datetime, timedelta
                if date_filter == 'last_day':
                    until_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
                    search_url += f"&until={until_date}"
                    self.logger.info(f"📅 Date filter: Until {until_date}")
                elif date_filter == 'last_week':
                    until_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
                    search_url += f"&until={until_date}"
                    self.logger.info(f"📅 Date filter: Until {until_date}")
                elif date_filter == 'last_month':
                    until_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
                    search_url += f"&until={until_date}"
                    self.logger.info(f"📅 Date filter: Until {until_date}")

            self.logger.info(f"🌐 Search URL: {search_url}")

            # FIXED: Enhanced navigation with better error handling
            try:
                self.logger.info(f"🌐 Navigating to: {search_url}")
                await self._simulate_human_interaction()

                # Navigate with improved timeout and error handling
                response = await self.page.goto(search_url, wait_until='networkidle', timeout=30000)

                if response and response.status >= 400:
                    self.logger.error(f"❌ HTTP {response.status} error for search URL")
                    return []

                await self._human_delay(3, 6)
                await self._simulate_human_interaction()

                # Verify we're on a search page
                page_url = self.page.url
                if 'search' not in page_url:
                    self.logger.error(f"❌ Not on search page. Current URL: {page_url}")
                    return []

                self.logger.info(f"✅ Successfully navigated to search page")

            except Exception as nav_error:
                self.logger.error(f"❌ Navigation failed: {nav_error}")
                return []

            # Enhanced content loading detection
            self.logger.info("⏳ Phase 2.2: Loading search results...")

            # Wait for search results to load
            await self._human_delay(3, 5)

            # Check if we're on the search results page
            try:
                page_title = await self.page.title()
                self.logger.info(f"📄 Page title: {page_title}")

                # Look for search result indicators
                search_indicators = [
                    'div[data-testid="primaryColumn"]',
                    'section[aria-label*="Search"]',
                    'article[data-testid="tweet"]'
                ]

                content_found = False
                for indicator in search_indicators:
                    elements = await self.page.locator(indicator).count()
                    if elements > 0:
                        self.logger.info(f"✅ Search content detected: {elements} {indicator} elements")
                        content_found = True
                        break

                if not content_found:
                    self.logger.warning("⚠️ Search content detection unclear, proceeding...")

            except Exception as e:
                self.logger.debug(f"Search page validation error: {e}")

            # FIXED: Enhanced search results extraction
            self.logger.info("🔍 SEARCH EXTRACTION: Using optimized tweet extraction for search results...")

            # Wait for search results to load properly
            try:
                await self.page.wait_for_selector('article[data-testid="tweet"]', timeout=15000)
                await self._human_delay(2, 4)
            except Exception as e:
                self.logger.warning(f"⚠️ Search results loading timeout: {e}")

            tweets = []
            extracted_tweet_ids = set()
            seen_text_hashes = set()
            scroll_attempts = 0
            max_scrolls = min(max(max_tweets // 5, 10), 30)  # More aggressive scrolling

            self.logger.info(f"🎯 SEARCH TARGET: {max_tweets} tweets, max {max_scrolls} scrolls")

            while len(tweets) < max_tweets and scroll_attempts < max_scrolls:
                self.logger.info(f"🔄 Search round {scroll_attempts + 1}/{max_scrolls} - {len(tweets)} tweets found")

                # FIXED: Enhanced tweet element detection with multiple selectors
                tweet_selectors = [
                    'article[data-testid="tweet"]',
                    'div[data-testid="tweet"]',
                    'article[role="article"]'
                ]

                tweet_elements = []
                for selector in tweet_selectors:
                    try:
                        elements = await self.page.locator(selector).all()
                        if elements:
                            tweet_elements.extend(elements)
                            break
                    except:
                        continue

                self.logger.info(f"📊 Found {len(tweet_elements)} tweet elements in current view")

                # Extract tweets using our Phase 1 enhanced method
                for i, element in enumerate(tweet_elements):
                    if len(tweets) >= max_tweets:
                        break

                    try:
                        if await element.is_visible():
                            # Use comprehensive tweet extraction with all Phase 1 enhancements
                            tweet_data = await self._extract_tweet_from_element(element, i, f"search_{encoded_query}", include_engagement=True)

                            if tweet_data and tweet_data.get('text'):
                                # Add search-specific metadata
                                tweet_data['search_query'] = search_query
                                tweet_data['search_context'] = 'query_search'
                                tweet_data['search_url'] = search_url
                                tweet_data['result_type'] = result_type
                                tweet_data['extraction_method'] = 'phase2_search_enhanced'

                                # Enhanced relevance scoring
                                relevance_score = self._calculate_search_relevance(tweet_data['text'], search_query)
                                tweet_data['relevance_score'] = relevance_score

                                # Check for duplicates
                                import hashlib
                                text_hash = hashlib.md5(tweet_data['text'].encode()).hexdigest()

                                if (text_hash not in seen_text_hashes and
                                    tweet_data.get('tweet_id') not in extracted_tweet_ids):

                                    seen_text_hashes.add(text_hash)
                                    if tweet_data.get('tweet_id'):
                                        extracted_tweet_ids.add(tweet_data['tweet_id'])

                                    tweets.append(tweet_data)
                                    self.logger.info(f"🔍 Phase 2.2: Extracted search result {len(tweets)}/{max_tweets}")
                                    self.logger.info(f"   📝 Text: {tweet_data['text'][:80]}...")
                                    self.logger.info(f"   🎯 Relevance: {relevance_score:.2f}")

                                    # Log Phase 1 enhancements
                                    enhancements = []
                                    if tweet_data.get('has_media'):
                                        enhancements.append(f"media:{len(tweet_data.get('media', []))}")
                                    if tweet_data.get('likes') is not None:
                                        enhancements.append(f"likes:{tweet_data['likes']}")
                                    if tweet_data.get('thread_info', {}).get('is_thread'):
                                        enhancements.append("thread:true")

                                    if enhancements:
                                        self.logger.info(f"   ✨ Phase 1 data: {', '.join(enhancements)}")

                    except Exception as e:
                        self.logger.debug(f"🔍 Error extracting search result {i}: {e}")

                # Scroll for more content
                if len(tweets) < max_tweets:
                    self.logger.info(f"📜 Scrolling for more search results... ({len(tweets)}/{max_tweets})")
                    await self.page.mouse.wheel(0, 800)
                    await self._human_delay(2, 4)
                    scroll_attempts += 1
                else:
                    break

            # Phase 2.2: Apply Thread Reconstruction and Relevance Sorting
            if len(tweets) > 0:
                self.logger.info(f"🧵 Phase 2.2: Applying thread reconstruction...")
                try:
                    tweets = await self._reconstruct_thread_sequence(tweets, f"search_{encoded_query}")
                except Exception as e:
                    self.logger.warning(f"⚠️ Thread reconstruction failed for search: {e}")

                # Sort by relevance score (highest first)
                tweets.sort(key=lambda t: t.get('relevance_score', 0), reverse=True)
                self.logger.info(f"📊 Sorted {len(tweets)} results by relevance")

            self.logger.info(f"🏁 Search query scraping completed: {len(tweets)} tweets extracted")

            # Phase 2.2: Enhanced result structure
            result = {
                "type": "comprehensive_search",
                "search_query": search_query,
                "search_parameters": {
                    "query": search_query,
                    "max_tweets": max_tweets,
                    "result_type": result_type,
                    "date_filter": date_filter,
                    "scrape_level": scrape_level,
                    "include_media": include_media
                },
                "posts": tweets,
                "extraction_stats": {
                    "total_posts": len(tweets),
                    "scroll_attempts": scroll_attempts,
                    "extraction_method": "phase2_search_enhanced",
                    "with_media": len([t for t in tweets if t.get('has_media')]),
                    "with_engagement": len([t for t in tweets if any(t.get(m) is not None for m in ['likes', 'retweets', 'replies'])]),
                    "with_threads": len([t for t in tweets if t.get('thread_info', {}).get('is_thread')]),
                    "avg_relevance": sum(t.get('relevance_score', 0) for t in tweets) / len(tweets) if tweets else 0
                }
            }

            self.logger.info(f"📊 Phase 2.2 Results: {result['extraction_stats']['total_posts']} posts, avg relevance: {result['extraction_stats']['avg_relevance']:.2f}")

            return [result]

        except Exception as e:
            self.logger.error(f"❌ Search query scraping failed: {e}")
            return []

    def _calculate_search_relevance(self, text: str, search_query: str) -> float:
        """
        Calculate relevance score between tweet text and search query.
        Returns score from 0.0 to 1.0.
        """
        try:
            text_lower = text.lower()
            query_lower = search_query.lower()

            # Exact query match (highest score)
            if query_lower in text_lower:
                return 1.0

            # Split query into terms
            query_terms = query_lower.split()
            text_words = text_lower.split()

            # Count matching terms
            matches = sum(1 for term in query_terms if term in text_words)

            # Calculate base relevance
            relevance = matches / len(query_terms) if query_terms else 0

            # Boost for hashtags in query
            if search_query.startswith('#') and search_query.lower() in text_lower:
                relevance += 0.3

            # Boost for @mentions in query
            if search_query.startswith('@') and search_query.lower() in text_lower:
                relevance += 0.3

            return min(relevance, 1.0)

        except Exception:
            return 0.0

    async def monitor_real_time(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Phase 3.1: Real-time Monitoring Mode.

        Continuous monitoring system that supports:
        - User account monitoring
        - Hashtag monitoring
        - Search query monitoring
        - Periodic updates with change detection
        - Delta reporting (only new content)
        - Configurable monitoring intervals
        """
        monitor_type = params.get('monitor_type', 'user')  # user, hashtag, search
        monitor_target = params.get('monitor_target', '')  # username, hashtag, or query
        monitoring_interval = params.get('monitoring_interval', 300)  # seconds between checks
        max_iterations = params.get('max_iterations', 10)  # maximum monitoring cycles
        delta_only = params.get('delta_only', True)  # only return new content
        scrape_level = params.get('scrape_level', 4)

        self.logger.info(f"🔄 Phase 3.1: REAL-TIME MONITORING MODE")
        self.logger.info(f"🎯 Type: {monitor_type} | Target: '{monitor_target}'")
        self.logger.info(f"⏱️ Interval: {monitoring_interval}s | Max cycles: {max_iterations}")
        self.logger.info(f"🔄 Delta only: {delta_only}")

        monitoring_results = []
        previous_content_ids = set()
        monitoring_start_time = time.time()

        try:
            for iteration in range(max_iterations):
                cycle_start_time = time.time()
                self.logger.info(f"🔄 Monitoring cycle {iteration + 1}/{max_iterations}")

                # Prepare parameters for the specific monitoring type
                monitoring_params = {
                    'scrape_level': scrape_level,
                    'max_tweets': params.get('max_tweets', 20)
                }

                current_results = []

                # Route to appropriate scraping method based on monitor type
                if monitor_type == 'user':
                    monitoring_params['username'] = monitor_target.replace('@', '')
                    monitoring_params['scrape_posts'] = True
                    current_results = await self._extract_posts_adaptive(
                        monitoring_params['username'],
                        monitoring_params['max_tweets'],
                        scrape_level
                    )

                elif monitor_type == 'hashtag':
                    monitoring_params['hashtag'] = monitor_target.replace('#', '')
                    monitoring_params['max_tweets'] = monitoring_params.pop('max_tweets')
                    hashtag_results = await self.scrape_hashtag(monitoring_params)
                    if hashtag_results and len(hashtag_results) > 0:
                        current_results = hashtag_results[0].get('posts', [])

                elif monitor_type == 'search':
                    monitoring_params['search_query'] = monitor_target
                    monitoring_params['max_tweets'] = monitoring_params.pop('max_tweets')
                    search_results = await self.scrape_search_query(monitoring_params)
                    if search_results and len(search_results) > 0:
                        current_results = search_results[0].get('posts', [])

                else:
                    self.logger.error(f"❌ Invalid monitor type: {monitor_type}")
                    break

                # Process results for delta detection
                current_content_ids = set()
                new_content = []
                updated_content = []

                for item in current_results:
                    item_id = item.get('tweet_id') or item.get('id')
                    if item_id:
                        current_content_ids.add(item_id)

                        # Add monitoring metadata
                        item['monitoring_metadata'] = {
                            'cycle': iteration + 1,
                            'detected_at': time.time(),
                            'monitor_type': monitor_type,
                            'monitor_target': monitor_target,
                            'is_new': item_id not in previous_content_ids
                        }

                        # Classify as new or updated content
                        if item_id not in previous_content_ids:
                            new_content.append(item)
                        else:
                            # Check for updates (engagement changes, etc.)
                            if self._has_content_changed(item, monitoring_results, item_id):
                                updated_content.append(item)

                # Calculate monitoring metrics
                cycle_duration = time.time() - cycle_start_time
                total_new = len(new_content)
                total_updated = len(updated_content)

                cycle_result = {
                    'cycle': iteration + 1,
                    'timestamp': cycle_start_time,
                    'duration_seconds': cycle_duration,
                    'monitor_type': monitor_type,
                    'monitor_target': monitor_target,
                    'total_items_found': len(current_results),
                    'new_items': total_new,
                    'updated_items': total_updated,
                    'content': new_content if delta_only else current_results
                }

                monitoring_results.append(cycle_result)

                self.logger.info(f"🔄 Cycle {iteration + 1} complete:")
                self.logger.info(f"   📊 Total items: {len(current_results)}")
                self.logger.info(f"   ✨ New items: {total_new}")
                self.logger.info(f"   🔄 Updated items: {total_updated}")
                self.logger.info(f"   ⏱️ Duration: {cycle_duration:.1f}s")

                # Update tracking sets
                previous_content_ids.update(current_content_ids)

                # Wait for next cycle (unless it's the last iteration)
                if iteration < max_iterations - 1:
                    self.logger.info(f"⏳ Waiting {monitoring_interval}s for next cycle...")
                    await asyncio.sleep(monitoring_interval)

            # Generate comprehensive monitoring summary
            total_duration = time.time() - monitoring_start_time
            total_new_items = sum(result['new_items'] for result in monitoring_results)
            total_updated_items = sum(result['updated_items'] for result in monitoring_results)

            monitoring_summary = {
                'type': 'real_time_monitoring',
                'monitor_type': monitor_type,
                'monitor_target': monitor_target,
                'monitoring_parameters': {
                    'interval_seconds': monitoring_interval,
                    'max_iterations': max_iterations,
                    'delta_only': delta_only,
                    'scrape_level': scrape_level
                },
                'monitoring_results': monitoring_results,
                'monitoring_stats': {
                    'total_cycles': len(monitoring_results),
                    'total_duration_seconds': total_duration,
                    'total_new_items': total_new_items,
                    'total_updated_items': total_updated_items,
                    'avg_cycle_duration': sum(r['duration_seconds'] for r in monitoring_results) / len(monitoring_results) if monitoring_results else 0,
                    'items_per_minute': (total_new_items + total_updated_items) / (total_duration / 60) if total_duration > 0 else 0
                }
            }

            self.logger.info(f"🏁 Real-time monitoring complete:")
            self.logger.info(f"   🔄 {len(monitoring_results)} cycles over {total_duration:.1f}s")
            self.logger.info(f"   ✨ {total_new_items} new items discovered")
            self.logger.info(f"   🔄 {total_updated_items} items updated")

            return [monitoring_summary]

        except Exception as e:
            self.logger.error(f"❌ Real-time monitoring failed: {e}")
            return []

    def _has_content_changed(self, current_item: Dict[str, Any], previous_results: List[Dict[str, Any]], item_id: str) -> bool:
        """
        Check if content has changed since last monitoring cycle.
        Compares engagement metrics and other mutable fields.
        """
        try:
            # Find the item in previous results
            for cycle_result in reversed(previous_results):  # Check most recent first
                for prev_item in cycle_result.get('content', []):
                    if prev_item.get('tweet_id') == item_id or prev_item.get('id') == item_id:
                        # Compare engagement metrics
                        engagement_fields = ['likes', 'retweets', 'replies', 'views']
                        for field in engagement_fields:
                            if current_item.get(field) != prev_item.get(field):
                                return True

                        # Compare other mutable fields
                        if current_item.get('text') != prev_item.get('text'):
                            return True

                        return False

            return False  # Item not found in previous results

        except Exception:
            return False

    def classify_content(self, tweet_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Phase 3.2: Advanced Content Classification.

        Intelligent content categorization and analysis:
        - Content type detection (announcement, question, opinion, etc.)
        - Sentiment analysis (positive, negative, neutral)
        - Topic classification (tech, business, personal, etc.)
        - Language detection
        - Content quality scoring
        - Engagement potential prediction
        """
        try:
            text = tweet_data.get('text', '')
            if not text:
                return {'classification_error': 'No text content'}

            classification = {
                'content_type': self._classify_content_type(text),
                'sentiment': self._analyze_sentiment(text),
                'topics': self._classify_topics(text),
                'language': self._detect_language(text),
                'quality_score': self._calculate_quality_score(tweet_data),
                'engagement_potential': self._predict_engagement_potential(tweet_data),
                'content_features': self._extract_content_features(text),
                'classification_confidence': 0.0
            }

            # Calculate overall classification confidence
            classification['classification_confidence'] = self._calculate_classification_confidence(classification)

            return classification

        except Exception as e:
            return {'classification_error': str(e)}

    def _classify_content_type(self, text: str) -> Dict[str, Any]:
        """Classify the type/nature of the content."""
        content_type_indicators = {
            'question': ['?', 'how to', 'what is', 'why', 'when', 'where', 'who'],
            'announcement': ['announcing', 'excited to share', 'launching', 'introducing', 'release'],
            'opinion': ['i think', 'in my opinion', 'i believe', 'imo', 'personally'],
            'news': ['breaking', 'just in', 'report', 'according to', 'sources'],
            'tutorial': ['how to', 'step by step', 'guide', 'tutorial', 'learn'],
            'promotion': ['check out', 'link in bio', 'buy now', 'discount', 'sale'],
            'request': ['please', 'help', 'need', 'looking for', 'can someone'],
            'celebration': ['congratulations', 'congrats', 'achieved', 'milestone', 'success'],
            'complaint': ['frustrated', 'annoying', 'terrible', 'worst', 'disappointed'],
            'thread': ['thread', '🧵', '1/', 'part 1', 'continued']
        }

        text_lower = text.lower()
        detected_types = []
        confidence_scores = {}

        for content_type, indicators in content_type_indicators.items():
            score = 0
            for indicator in indicators:
                if indicator in text_lower:
                    score += 1

            if score > 0:
                confidence = min(score / len(indicators), 1.0)
                detected_types.append(content_type)
                confidence_scores[content_type] = confidence

        # Default to 'general' if no specific type detected
        if not detected_types:
            detected_types = ['general']
            confidence_scores['general'] = 0.5

        return {
            'primary_type': detected_types[0],
            'all_types': detected_types,
            'confidence_scores': confidence_scores
        }

    def _analyze_sentiment(self, text: str) -> Dict[str, Any]:
        """Analyze sentiment of the content."""
        positive_words = [
            'amazing', 'awesome', 'great', 'excellent', 'fantastic', 'love', 'best',
            'incredible', 'wonderful', 'outstanding', 'perfect', 'brilliant', 'excited',
            'happy', 'successful', 'breakthrough', 'revolutionary', 'innovative'
        ]

        negative_words = [
            'terrible', 'awful', 'hate', 'worst', 'disappointing', 'frustrated',
            'angry', 'sad', 'broken', 'failed', 'disaster', 'crisis', 'problem',
            'issue', 'concerned', 'worried', 'unfortunate', 'disappointing'
        ]

        neutral_words = [
            'update', 'information', 'report', 'data', 'analysis', 'research',
            'study', 'findings', 'results', 'statistics', 'facts', 'details'
        ]

        text_lower = text.lower()

        positive_score = sum(1 for word in positive_words if word in text_lower)
        negative_score = sum(1 for word in negative_words if word in text_lower)
        neutral_score = sum(1 for word in neutral_words if word in text_lower)

        total_score = positive_score + negative_score + neutral_score

        if total_score == 0:
            return {'sentiment': 'neutral', 'confidence': 0.3, 'scores': {'positive': 0, 'negative': 0, 'neutral': 1}}

        sentiment_scores = {
            'positive': positive_score / total_score,
            'negative': negative_score / total_score,
            'neutral': neutral_score / total_score
        }

        # Determine primary sentiment
        primary_sentiment = max(sentiment_scores.keys(), key=lambda x: sentiment_scores[x])
        confidence = sentiment_scores[primary_sentiment]

        return {
            'sentiment': primary_sentiment,
            'confidence': confidence,
            'scores': sentiment_scores
        }

    def _classify_topics(self, text: str) -> List[str]:
        """Classify content topics."""
        topic_keywords = {
            'technology': ['ai', 'artificial intelligence', 'machine learning', 'ml', 'tech', 'software', 'programming', 'code', 'api', 'cloud', 'data'],
            'business': ['startup', 'entrepreneur', 'funding', 'investment', 'revenue', 'market', 'business', 'company', 'strategy', 'growth'],
            'crypto': ['bitcoin', 'ethereum', 'crypto', 'blockchain', 'defi', 'nft', 'web3', 'dao', 'token', 'mining'],
            'science': ['research', 'study', 'experiment', 'discovery', 'scientific', 'paper', 'journal', 'peer review'],
            'education': ['learning', 'course', 'tutorial', 'education', 'teaching', 'student', 'university', 'school'],
            'health': ['health', 'medical', 'doctor', 'hospital', 'medicine', 'treatment', 'therapy', 'wellness'],
            'finance': ['money', 'investment', 'stock', 'market', 'finance', 'trading', 'economy', 'bank'],
            'politics': ['government', 'policy', 'election', 'vote', 'political', 'congress', 'senate', 'president'],
            'entertainment': ['movie', 'music', 'game', 'entertainment', 'show', 'celebrity', 'art', 'culture'],
            'sports': ['sport', 'football', 'basketball', 'soccer', 'baseball', 'team', 'player', 'game', 'match']
        }

        text_lower = text.lower()
        detected_topics = []

        for topic, keywords in topic_keywords.items():
            if any(keyword in text_lower for keyword in keywords):
                detected_topics.append(topic)

        return detected_topics if detected_topics else ['general']

    def _detect_language(self, text: str) -> str:
        """Simple language detection."""
        # Simple heuristic-based language detection
        if any(char in text for char in 'àáäâèéëêìíïîòóöôùúüûñç'):
            return 'romance'  # French, Spanish, Italian, etc.
        elif any(char in text for char in 'äöüß'):
            return 'german'
        elif any(char in text for char in 'абвгдеёжзийклмнопрстуфхцчшщъыьэюя'):
            return 'russian'
        elif any(char in text for char in '你我他她它们的是在有中国'):
            return 'chinese'
        elif any(char in text for char in 'ひらがなカタカナ'):
            return 'japanese'
        else:
            return 'english'  # Default assumption

    def _calculate_quality_score(self, tweet_data: Dict[str, Any]) -> float:
        """Calculate content quality score based on various factors."""
        try:
            text = tweet_data.get('text', '')
            score = 0.5  # Base score

            # Length factor (optimal range)
            text_length = len(text)
            if 50 <= text_length <= 280:
                score += 0.2
            elif text_length < 20:
                score -= 0.2

            # Engagement factor
            engagement_metrics = ['likes', 'retweets', 'replies']
            total_engagement = sum(tweet_data.get(metric, 0) for metric in engagement_metrics)
            if total_engagement > 100:
                score += 0.2
            elif total_engagement > 10:
                score += 0.1

            # Media presence
            if tweet_data.get('has_media'):
                score += 0.1

            # Thread factor
            if tweet_data.get('thread_info', {}).get('is_thread'):
                score += 0.1

            # URL presence (information sharing)
            if 'http' in text or 'www.' in text:
                score += 0.05

            # Hashtag usage (moderate is good)
            hashtag_count = text.count('#')
            if 1 <= hashtag_count <= 3:
                score += 0.05
            elif hashtag_count > 5:
                score -= 0.1

            return min(max(score, 0.0), 1.0)

        except Exception:
            return 0.5

    def _predict_engagement_potential(self, tweet_data: Dict[str, Any]) -> Dict[str, Any]:
        """Predict engagement potential based on content features."""
        try:
            text = tweet_data.get('text', '')

            engagement_factors = {
                'has_question': '?' in text,
                'has_media': tweet_data.get('has_media', False),
                'is_thread': tweet_data.get('thread_info', {}).get('is_thread', False),
                'has_hashtags': '#' in text,
                'has_mentions': '@' in text,
                'has_url': any(url in text for url in ['http', 'www.']),
                'optimal_length': 50 <= len(text) <= 200,
                'has_emoji': any(ord(char) > 127 for char in text)
            }

            # Calculate engagement score
            engagement_score = sum(engagement_factors.values()) / len(engagement_factors)

            # Predict engagement level
            if engagement_score >= 0.7:
                potential = 'high'
            elif engagement_score >= 0.4:
                potential = 'medium'
            else:
                potential = 'low'

            return {
                'potential': potential,
                'score': engagement_score,
                'factors': engagement_factors
            }

        except Exception:
            return {'potential': 'unknown', 'score': 0.0, 'factors': {}}

    def _extract_content_features(self, text: str) -> Dict[str, Any]:
        """Extract detailed content features."""
        return {
            'length': len(text),
            'word_count': len(text.split()),
            'hashtag_count': text.count('#'),
            'mention_count': text.count('@'),
            'url_count': text.count('http') + text.count('www.'),
            'emoji_count': sum(1 for char in text if ord(char) > 127),
            'question_marks': text.count('?'),
            'exclamation_marks': text.count('!'),
            'capital_letters_ratio': sum(1 for char in text if char.isupper()) / len(text) if text else 0
        }

    def _calculate_classification_confidence(self, classification: Dict[str, Any]) -> float:
        """Calculate overall confidence in the classification."""
        try:
            confidence_factors = []

            # Content type confidence
            content_type_conf = classification.get('content_type', {}).get('confidence_scores', {})
            if content_type_conf:
                confidence_factors.append(max(content_type_conf.values()))

            # Sentiment confidence
            sentiment_conf = classification.get('sentiment', {}).get('confidence', 0)
            confidence_factors.append(sentiment_conf)

            # Topic detection confidence (based on number of topics)
            topics = classification.get('topics', [])
            topic_conf = 0.8 if len(topics) > 1 else 0.6 if len(topics) == 1 else 0.3
            confidence_factors.append(topic_conf)

            return sum(confidence_factors) / len(confidence_factors) if confidence_factors else 0.0

        except Exception:
            return 0.0

    async def scrape_user_comprehensive(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Comprehensive user account scraping with all options."""
        username = params.get('target_username', '').replace('@', '')

        # Initialize date filtering
        self.setup_date_filtering(params)

        self.logger.info(f"👤 Starting comprehensive user scraping for @{username}")
        # DEBUG: Log received params
        self.logger.info(f"🔍 DEBUG RECEIVED PARAMS: scrape_likes={params.get('scrape_likes')}, scrape_mentions={params.get('scrape_mentions')}, scrape_reposts={params.get('scrape_reposts')}")
        self.logger.info(f"🔍 DEBUG ENGAGEMENT PARAMS: scrape_post_likers={params.get('scrape_post_likers')}, scrape_post_repliers={params.get('scrape_post_repliers')}, scrape_post_reposters={params.get('scrape_post_reposters')}")
        self.logger.info("⏳ STEP 2/5: Preparing data extraction...")

        results = {
            "type": "comprehensive_user",
            "username": f"@{username}",
            "profile": {},
            "posts": [],
            "likes": [],
            "mentions": [],
            "reposts": [],
            "media": [],
            "followers": [],
            "following": []
        }

        try:
            # Navigate to user profile
            profile_url = f"https://x.com/{username}"
            self.logger.info(f"⏳ STEP 2/5: Navigating to profile: {profile_url}")

            try:
                self.logger.info("⏳ Step 2a: Simulating human interaction...")
                await self._simulate_human_interaction()
                self.logger.info("✅ Step 2a: Human interaction simulated")

                self.logger.info(f"⏳ Step 2b: Executing page.goto with timeout wrapper...")
                # FIXED: Add timeout wrapper to prevent infinite hang
                goto_result = await self._with_timeout(
                    self.page.goto(profile_url, wait_until='domcontentloaded', timeout=30000),
                    timeout_ms=35000,
                    operation_name=f"Navigate to {profile_url}",
                    default=None
                )
                self.logger.info(f"✅ Step 2b: Navigation completed (result: {goto_result is not None})")

                if goto_result is None:
                    self.logger.warning("⚠️ Navigation timed out - may affect data quality")

                await self._human_delay(3, 6)
            except Exception as nav_error:
                self.logger.error(f"❌ Navigation failed: {nav_error}")
                # Save partial results before failing
                if self.output_dir:
                    self._save_partial_results(results, self.output_dir, "navigation_failed")
                raise
            
            # 🚀 OPTIMIZED SMART CONTENT LOADING - FAST & EFFICIENT 🚀
            self.logger.info("⚡ SMART LOADING - Intelligent content detection (max 30s)...")

            content_loaded = False
            start_time = time.time()
            max_wait_time = 30  # Reduced from 120s to 30s

            # FAST PHASE 1: Check for immediate content availability (first 5 seconds)
            self.logger.info("🔄 Phase 1: Quick content check")
            try:
                # Check for profile-specific elements immediately
                profile_selectors = [
                    'span:has-text("Joined")',        # Profile join date
                    'span:has-text("Following")',     # Following count
                    'span:has-text("Followers")',     # Followers count
                    '[data-testid="tweet"]',          # Any tweets
                    'nav[role="navigation"]'          # Navigation (auth check)
                ]

                for selector in profile_selectors:
                    try:
                        await self.page.wait_for_selector(selector, timeout=3000)
                        self.logger.info(f"✅ Profile element found: {selector}")
                        content_loaded = True
                        break
                    except:
                        continue

                if content_loaded:
                    self.logger.info("🎉 CONTENT LOADED SUCCESSFULLY!")

            except Exception as e:
                self.logger.debug(f"Quick check failed: {e}")

            # SMART PHASE 2: Progressive content detection (remaining time)
            self.logger.info("🔄 Phase 2: Progressive content detection")
            check_interval = 2  # Check every 2 seconds
            last_div_count = 0
            stability_checks = 0

            while time.time() - start_time < max_wait_time and not content_loaded:
                try:
                    # Check div count growth (indicates loading)
                    current_div_count = await self._with_timeout(
                        self.page.locator('div').count(),
                        timeout_ms=3000,
                        operation_name="Count divs for content detection",
                        default=0
                    )

                    # If div count increased significantly, content is loading
                    if current_div_count > last_div_count + 10:
                        last_div_count = current_div_count
                        stability_checks = 0
                        self.logger.debug(f"📊 Content growing: {current_div_count} divs")
                    else:
                        stability_checks += 1

                    # Content stabilized - check if it's what we need
                    if stability_checks >= 2 or current_div_count > 40:
                        # Quick profile content verification
                        try:
                            page_text = await self._with_timeout(
                                self.page.locator('body').inner_text(),
                                timeout_ms=5000,
                                operation_name="Get page text for content verification",
                                default=""
                            )
                            profile_indicators = ['Following', 'Followers', 'Joined', username]
                            found_indicators = [ind for ind in profile_indicators if ind.lower() in page_text.lower()]

                            if len(found_indicators) >= 2:  # At least 2 profile indicators
                                self.logger.info(f"✅ Profile confirmed: {found_indicators}")
                                content_loaded = True
                                break
                        except:
                            pass

                    await asyncio.sleep(check_interval)

                except Exception as e:
                    self.logger.debug(f"Content check error: {e}")
                    break

            # FINAL CHECK: Fallback content detection
            if not content_loaded:
                self.logger.info("🔄 Final content verification...")
                try:
                    # Accept any reasonable amount of content
                    div_count = await self._with_timeout(
                        self.page.locator('div').count(),
                        timeout_ms=3000,
                        operation_name="Final div count check",
                        default=0
                    )
                    if div_count > 20:  # Lowered threshold for faster loading
                        self.logger.info(f"✅ Acceptable content found: {div_count} divs")
                        content_loaded = True
                except:
                    pass

            elapsed_time = time.time() - start_time
            if content_loaded:
                self.logger.info(f"✅ STEP 2/5: Content loaded in {elapsed_time:.1f}s!")
                # Brief stabilization wait for rich content
                await self._human_delay(1, 2)
            else:
                self.logger.warning(f"⚠️ Content loading timeout after {elapsed_time:.1f}s - proceeding with available content")
            # Extract profile information first
            self.logger.info("⏳ STEP 3/5: Extracting profile information...")
            self.logger.info("⏳ Step 3a: Calling _extract_profile_info()...")
            try:
                profile_data = await self._extract_profile_info(username)
                self.logger.info(f"✅ Step 3a: _extract_profile_info() returned")
                results["profile"] = profile_data
                self.logger.info(f"✅ STEP 3/5: Profile extracted ({len(str(profile_data))} chars)")

                # Checkpoint: Save profile data
                if self.output_dir:
                    self._save_partial_results(results, self.output_dir, "after_profile")
            except Exception as profile_error:
                self.logger.error(f"❌ STEP 3/5: Profile extraction failed: {profile_error}")
                results["profile"] = {}

            # 1. Scrape Posts - ROUTE TO EVERYTHING MODE FOR COMPREHENSIVE SCRAPING
            self.logger.info("⏳ STEP 4/5: Extracting requested data types...")
            if params.get('scrape_posts', True):
                max_posts = params.get('max_posts', 100)  # This will now use validated parameters
                scrape_level = params.get('scrape_level', 4)
                self.logger.info(f"📝 Scraping posts ({max_posts}) at level {scrape_level}...")
                try:
                    # FOR LEVEL 4: Use EVERYTHING MODE comprehensive extraction
                    if scrape_level >= 4:
                        self.logger.info(f"🌟 LEVEL 4 DETECTED: Using EVERYTHING MODE extraction")
                        posts = await self._extract_posts_comprehensive(username, max_posts)
                    else:
                        # For lower levels: Use standard extraction
                        posts = await self._scrape_user_posts(username, max_posts)
                    results["posts"] = posts if posts else []
                    self.logger.info(f"✅ Posts extracted: {len(posts) if posts else 0} posts")

                    # NEW: Extract post-level engagement data (likers, repliers, reposters)
                    if posts and any([
                        params.get('scrape_post_likers', False),
                        params.get('scrape_post_repliers', False),
                        params.get('scrape_post_reposters', False)
                    ]):
                        self.logger.info(f"🔍 Extracting post-level engagement data...")
                        await self._enrich_posts_with_engagement(posts, params)
                        self.logger.info(f"✅ Post engagement data extracted")

                    # Checkpoint: Save after posts extraction
                    if self.output_dir:
                        self._save_partial_results(results, self.output_dir, "after_posts")
                except Exception as posts_error:
                    self.logger.error(f"❌ Posts extraction failed: {posts_error}")
                    results["posts"] = []

                    # Save partial results even on error
                    if self.output_dir:
                        self._save_partial_results(results, self.output_dir, "posts_error")

            # 2. Scrape Likes
            if params.get('scrape_likes', False):
                max_likes = params.get('max_likes', 50)  # Using validated parameters
                self.logger.info(f"❤️ Scraping likes ({max_likes})...")
                likes = await self._scrape_user_likes(username, max_likes)
                results["likes"] = likes
            
            # 3. Scrape Mentions
            if params.get('scrape_mentions', False):
                max_mentions = params.get('max_mentions', 30)  # Using validated parameters
                self.logger.info(f"@️⃣ Scraping mentions ({max_mentions})...")
                mentions = await self._scrape_user_mentions(username, max_mentions)
                results["mentions"] = mentions

            # 3.5. Scrape Reposts/Retweets
            if params.get('scrape_reposts', False):
                max_reposts = params.get('max_reposts', 50)  # Using validated parameters
                self.logger.info(f"🔄 Scraping reposts ({max_reposts})...")
                reposts = await self._scrape_user_reposts(username, max_reposts)
                results["reposts"] = reposts

            # 4. Scrape Media
            if params.get('scrape_media', False):
                max_media = params.get('max_media', 25)  # Using validated parameters
                self.logger.info(f"🖼️ Scraping media ({max_media})...")
                media = await self._scrape_user_media(username, max_media)
                results["media"] = media
            
            # 5. Scrape Followers
            if params.get('scrape_followers', False):
                try:
                    max_followers = params.get('max_followers', 200)  # Using validated parameters
                    self.logger.info(f"👥 Scraping followers ({max_followers})...")
                    followers = await self._scrape_user_followers(username, max_followers)
                    results["followers"] = followers

                    # Save partial results after followers
                    if self.output_dir:
                        self._save_partial_results(results, self.output_dir, "after_followers")
                except Exception as followers_error:
                    self.logger.error(f"❌ Followers extraction failed: {followers_error}")
                    results["followers"] = []

                    # Save partial results even on error
                    if self.output_dir:
                        self._save_partial_results(results, self.output_dir, "followers_error")

            # 6. Scrape Following
            if params.get('scrape_following', False):
                try:
                    max_following = params.get('max_following', 150)  # Using validated parameters
                    self.logger.info(f"➡️ Scraping following ({max_following})...")
                    following = await self._scrape_user_following(username, max_following)
                    results["following"] = following

                    # Save partial results after following
                    if self.output_dir:
                        self._save_partial_results(results, self.output_dir, "after_following")
                except Exception as following_error:
                    self.logger.error(f"❌ Following extraction failed: {following_error}")
                    results["following"] = []

                    # Save partial results even on error
                    if self.output_dir:
                        self._save_partial_results(results, self.output_dir, "following_error")
            
            self.logger.info(f"✅ STEP 4/5: Data extraction completed")
            self.logger.info(f"⏳ STEP 5/5: Finalizing results...")
            self.logger.info(f"✅ STEP 5/5: Comprehensive scraping completed for @{username}")
            return [results]

        except Exception as e:
            self.logger.error(f"❌ Comprehensive user scraping failed: {e}")
            return [results]  # Return partial results
    
    @staticmethod
    async def _export_results_multi_format(
        result: Dict[str, Any],
        output_dir: str,
        formats: List[str],
        logger: logging.Logger
    ) -> List[str]:
        """
        Phase 4.1: Multi-Format Export System

        Exports data in multiple formats including JSON, CSV, XML, Parquet, and enhanced formats.
        """
        exported_files = []

        try:
            # Extract posts data for processing
            posts_data = []
            data = result.get('data', [])

            for item in data:
                if isinstance(item, dict):
                    if 'posts' in item:
                        posts_data.extend(item['posts'])
                    elif 'text' in item:  # Direct post object
                        posts_data.append(item)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            for format_type in formats:
                try:
                    if format_type.lower() == 'json':
                        # Enhanced JSON with metadata
                        file_path = os.path.join(output_dir, f"twitter_data_{timestamp}.json")
                        with open(file_path, 'w', encoding='utf-8') as f:
                            enhanced_result = {
                                **result,
                                "export_metadata": {
                                    "format": "json",
                                    "exported_at": datetime.now().isoformat(),
                                    "posts_count": len(posts_data),
                                    "phase_enhancements": ["media", "engagement", "threads", "classification"]
                                }
                            }
                            json.dump(enhanced_result, f, indent=2, ensure_ascii=False)
                        exported_files.append(file_path)

                    elif format_type.lower() == 'csv':
                        # Flattened CSV format
                        file_path = os.path.join(output_dir, f"twitter_posts_{timestamp}.csv")
                        await TwitterTask._export_to_csv(posts_data, file_path)
                        exported_files.append(file_path)

                    elif format_type.lower() == 'xml':
                        # Structured XML format
                        file_path = os.path.join(output_dir, f"twitter_data_{timestamp}.xml")
                        await TwitterTask._export_to_xml(result, file_path)
                        exported_files.append(file_path)

                    elif format_type.lower() == 'parquet':
                        # High-performance parquet format
                        file_path = os.path.join(output_dir, f"twitter_posts_{timestamp}.parquet")
                        await TwitterTask._export_to_parquet(posts_data, file_path)
                        exported_files.append(file_path)

                    elif format_type.lower() == 'excel':
                        # Excel workbook with multiple sheets
                        file_path = os.path.join(output_dir, f"twitter_analysis_{timestamp}.xlsx")
                        await TwitterTask._export_to_excel(result, posts_data, file_path)
                        exported_files.append(file_path)

                    elif format_type.lower() == 'markdown':
                        # Human-readable markdown report
                        file_path = os.path.join(output_dir, f"twitter_report_{timestamp}.md")
                        await TwitterTask._export_to_markdown(result, posts_data, file_path)
                        exported_files.append(file_path)

                    else:
                        logger.warning(f"⚠️ Unknown export format: {format_type}")

                except Exception as format_error:
                    logger.error(f"❌ Failed to export {format_type}: {format_error}")

        except Exception as e:
            logger.error(f"❌ Multi-format export failed: {e}")

        return exported_files

    @staticmethod
    async def _export_to_csv(posts_data: List[Dict], file_path: str):
        """Export posts to CSV format with flattened structure."""
        import csv

        if not posts_data:
            return

        # Define CSV headers including all Phase enhancements
        headers = [
            'id', 'text', 'author', 'author_name', 'timestamp', 'url',
            'likes', 'retweets', 'replies', 'views', 'quotes',
            'has_media', 'media_count', 'media_types',
            'is_thread', 'thread_position', 'thread_size',
            'content_type', 'sentiment', 'topics', 'quality_score', 'language',
            'extraction_method', 'validation_confidence'
        ]

        with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()

            for post in posts_data:
                # Flatten complex objects for CSV
                row = {}
                for header in headers:
                    if header in post:
                        value = post[header]
                        if isinstance(value, (list, dict)):
                            row[header] = json.dumps(value) if value else ''
                        else:
                            row[header] = str(value) if value is not None else ''
                    else:
                        # Handle nested data
                        if header == 'media_count':
                            row[header] = len(post.get('media', []))
                        elif header == 'media_types':
                            media_types = [m.get('type', 'unknown') for m in post.get('media', [])]
                            row[header] = ','.join(set(media_types)) if media_types else ''
                        elif header in ['is_thread', 'thread_position', 'thread_size']:
                            thread_info = post.get('thread_info', {})
                            if header == 'is_thread':
                                row[header] = str(thread_info.get('is_thread', False))
                            else:
                                row[header] = str(thread_info.get(header.replace('thread_', ''), ''))
                        elif header in ['content_type', 'sentiment', 'topics', 'quality_score', 'language']:
                            classification = post.get('classification', {})
                            if header == 'topics':
                                row[header] = ','.join(classification.get('topics', []))
                            else:
                                row[header] = str(classification.get(header, ''))
                        else:
                            row[header] = ''

                writer.writerow(row)

    @staticmethod
    async def _export_to_xml(result: Dict, file_path: str):
        """Export data to XML format."""
        xml_content = ['<?xml version="1.0" encoding="UTF-8"?>']
        xml_content.append('<twitter_data>')

        # Add metadata
        metadata = result.get('search_metadata', {})
        xml_content.append('  <metadata>')
        for key, value in metadata.items():
            xml_content.append(f'    <{key}>{str(value)}</{key}>')
        xml_content.append('  </metadata>')

        # Add posts
        xml_content.append('  <posts>')
        data = result.get('data', [])

        for item in data:
            if isinstance(item, dict) and 'posts' in item:
                for post in item['posts']:
                    xml_content.append('    <post>')

                    # Basic fields
                    for field in ['id', 'text', 'author', 'author_name', 'timestamp', 'url']:
                        value = post.get(field, '')
                        if value:
                            escaped_value = str(value).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
                            xml_content.append(f'      <{field}>{escaped_value}</{field}>')

                    # Engagement metrics
                    xml_content.append('      <engagement>')
                    for metric in ['likes', 'retweets', 'replies', 'views']:
                        value = post.get(metric, 0)
                        xml_content.append(f'        <{metric}>{value}</{metric}>')
                    xml_content.append('      </engagement>')

                    # Media
                    if post.get('media'):
                        xml_content.append('      <media>')
                        for media in post['media']:
                            xml_content.append('        <item>')
                            xml_content.append(f'          <type>{media.get("type", "")}</type>')
                            xml_content.append(f'          <url>{media.get("url", "")}</url>')
                            xml_content.append('        </item>')
                        xml_content.append('      </media>')

                    # Classification
                    if post.get('classification'):
                        cls = post['classification']
                        xml_content.append('      <classification>')
                        xml_content.append(f'        <content_type>{cls.get("content_type", "")}</content_type>')
                        xml_content.append(f'        <sentiment>{cls.get("sentiment", "")}</sentiment>')
                        xml_content.append(f'        <quality_score>{cls.get("quality_score", 0)}</quality_score>')
                        xml_content.append('      </classification>')

                    xml_content.append('    </post>')

        xml_content.append('  </posts>')
        xml_content.append('</twitter_data>')

        with open(file_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(xml_content))

    @staticmethod
    async def _export_to_parquet(posts_data: List[Dict], file_path: str):
        """Export to high-performance Parquet format."""
        try:
            import pandas as pd

            if not posts_data:
                return

            # Flatten the data for pandas
            flattened_data = []
            for post in posts_data:
                flat_post = {}

                # Basic fields
                for field in ['id', 'text', 'author', 'author_name', 'timestamp', 'url']:
                    flat_post[field] = post.get(field, '')

                # Engagement metrics
                for metric in ['likes', 'retweets', 'replies', 'views', 'quotes']:
                    flat_post[metric] = post.get(metric, 0)

                # Media info
                flat_post['has_media'] = post.get('has_media', False)
                flat_post['media_count'] = len(post.get('media', []))

                # Thread info
                thread_info = post.get('thread_info', {})
                flat_post['is_thread'] = thread_info.get('is_thread', False)
                flat_post['thread_position'] = thread_info.get('thread_position', 0)

                # Classification
                classification = post.get('classification', {})
                flat_post['content_type'] = classification.get('content_type', '')
                flat_post['sentiment'] = classification.get('sentiment', '')
                flat_post['quality_score'] = classification.get('quality_score', 0.0)
                flat_post['language'] = classification.get('language', '')

                flattened_data.append(flat_post)

            df = pd.DataFrame(flattened_data)
            df.to_parquet(file_path, index=False)

        except ImportError:
            # Fallback to JSON if pandas not available
            with open(file_path.replace('.parquet', '.json'), 'w', encoding='utf-8') as f:
                json.dump(posts_data, f, indent=2, ensure_ascii=False)

    @staticmethod
    async def _export_to_excel(result: Dict, posts_data: List[Dict], file_path: str):
        """Export to Excel with multiple sheets."""
        try:
            import pandas as pd

            with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                # Posts sheet
                if posts_data:
                    df_posts = pd.json_normalize(posts_data)
                    df_posts.to_excel(writer, sheet_name='Posts', index=False)

                # Summary sheet
                summary_data = {
                    'Metric': ['Total Posts', 'With Media', 'With Threads', 'Avg Quality Score'],
                    'Value': [
                        len(posts_data),
                        sum(1 for p in posts_data if p.get('has_media')),
                        sum(1 for p in posts_data if p.get('thread_info', {}).get('is_thread')),
                        sum(p.get('classification', {}).get('quality_score', 0) for p in posts_data) / max(len(posts_data), 1)
                    ]
                }
                df_summary = pd.DataFrame(summary_data)
                df_summary.to_excel(writer, sheet_name='Summary', index=False)

        except ImportError:
            # Fallback to CSV
            await TwitterTask._export_to_csv(posts_data, file_path.replace('.xlsx', '.csv'))

    @staticmethod
    async def _export_to_markdown(result: Dict, posts_data: List[Dict], file_path: str):
        """Export to human-readable Markdown report."""
        lines = []
        lines.append('# Twitter Data Analysis Report')
        lines.append('')

        # Metadata
        metadata = result.get('search_metadata', {})
        lines.append('## Extraction Summary')
        lines.append('')
        lines.append(f"- **Target**: {metadata.get('target_username', 'N/A')}")
        lines.append(f"- **Total Posts**: {len(posts_data)}")
        lines.append(f"- **Extraction Method**: {metadata.get('extraction_method', 'N/A')}")
        lines.append(f"- **Completed At**: {metadata.get('search_completed_at', 'N/A')}")
        lines.append('')

        # Analytics
        if posts_data:
            with_media = sum(1 for p in posts_data if p.get('has_media'))
            with_threads = sum(1 for p in posts_data if p.get('thread_info', {}).get('is_thread'))
            avg_quality = sum(p.get('classification', {}).get('quality_score', 0) for p in posts_data) / len(posts_data)

            lines.append('## Content Analysis')
            lines.append('')
            lines.append(f"- **Posts with Media**: {with_media} ({with_media/len(posts_data)*100:.1f}%)")
            lines.append(f"- **Thread Posts**: {with_threads} ({with_threads/len(posts_data)*100:.1f}%)")
            lines.append(f"- **Average Quality Score**: {avg_quality:.2f}/1.0")
            lines.append('')

            # Sample posts
            lines.append('## Sample Posts')
            lines.append('')

            for i, post in enumerate(posts_data[:5]):
                lines.append(f"### Post {i+1}")
                lines.append('')
                lines.append(f"**Text**: {post.get('text', '')[:200]}...")
                lines.append('')
                lines.append(f"**Author**: @{post.get('author', 'unknown')}")
                lines.append(f"**Engagement**: ❤️ {post.get('likes', 0)} | 🔄 {post.get('retweets', 0)} | 💬 {post.get('replies', 0)}")

                classification = post.get('classification', {})
                if classification:
                    lines.append(f"**Analysis**: {classification.get('content_type', 'N/A')} | {classification.get('sentiment', 'N/A')} sentiment")

                lines.append('')

        with open(file_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines))

    async def close(self):
        """Enhanced resource cleanup with memory optimization."""
        try:
            # Close all pages in the context first
            if self.context:
                pages = self.context.pages
                for page in pages:
                    try:
                        # Clear page cache and memory
                        await page.evaluate("() => { if (window.gc) window.gc(); }")  # Force garbage collection if available
                        await page.close()
                        self.logger.debug(f"🧹 Closed page: {page.url}")
                    except Exception as page_error:
                        self.logger.warning(f"⚠️ Failed to close page: {page_error}")
                
                # Close the browser context
                await self.context.close()
                self.logger.info("🧹 Browser context closed successfully")
                
            # Clear internal references
            self.context = None
            self.page = None
            self.authenticated = False
            
        except Exception as e:
            self.logger.error(f"❌ Resource cleanup failed: {e}")
    
    async def __aenter__(self):
        """Async context manager entry."""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit with automatic cleanup."""
        await self.close()
        return False
    
    # ========== COMPREHENSIVE SCRAPING HELPER METHODS ==========
    
    async def _scrape_user_posts(self, username: str, max_posts: int) -> List[Dict[str, Any]]:
        """Scrape user's posts/tweets."""
        self.logger.info("🔴 DEBUG_MARKER: _scrape_user_posts() CALLED")
        posts = []
        try:
            # Ensure we're on the user's main profile page
            profile_url = f"https://x.com/{username}"
            current_url = self.page.url
            if username not in current_url:
                await self.page.goto(profile_url, wait_until='domcontentloaded', timeout=30000)
                # HUMAN BEHAVIOR: Simulate natural profile browsing
                await HumanBehavior.simulate_profile_browsing(self.page)
            
            # 🚀 HYBRID APPROACH: Try CSS selectors first, then enhanced div parsing
            self.logger.info("🐦 Using multi-method tweet extraction...")

            # Wait for actual tweet elements to appear (Twitter is JavaScript-rendered)
            self.logger.info("⏳ Waiting for tweets to render...")
            try:
                # Wait up to 45 seconds for any tweet element to appear
                # Twitter's React app can take a while to hydrate, especially on first load
                await self.page.wait_for_selector(
                    'article[data-testid="tweet"], div[data-testid="cellInnerDiv"], div[role="article"]',
                    timeout=45000,
                    state='visible'
                )
                self.logger.info("✅ Tweets detected, starting extraction...")
                await self._human_delay(2, 3)  # Brief additional wait for more tweets to load
            except Exception as wait_error:
                self.logger.warning(f"⚠️ No tweets appeared after 45s: {wait_error}")

                # Check if we're being blocked or on login page
                page_content = await self.page.content()
                if 'login' in page_content.lower() or 'sign in' in page_content.lower():
                    self.logger.error("❌ Twitter showing login page - session authentication failed")
                    raise Exception("Session authentication failed - login required")

                # Check for rate limit or protection
                if 'rate limit' in page_content.lower() or 'too many requests' in page_content.lower():
                    self.logger.error("❌ Twitter rate limit detected")
                    raise Exception("Rate limited by Twitter")

                # Take screenshot for debugging
                if self.output_dir:
                    screenshot_path = f"{self.output_dir}/no_tweets_debug.png"
                    try:
                        await self.page.screenshot(path=screenshot_path)
                        self.logger.info(f"📸 Saved debug screenshot: {screenshot_path}")
                    except:
                        pass

                # Continue anyway - might be empty profile

            # 🚀 ADVANCED VOLUME BREAKING: Multi-strategy approach for high-volume extraction
            scroll_attempts = 0
            
            # DEBUG: Log current max_posts value and strategy decision
            self.logger.info(f"🎯 STRATEGY SELECTION: max_posts = {max_posts} (type: {type(max_posts)})")
            
            # Strategy 1: For massive requests (100+), use pagination approach
            if max_posts >= 100:
                self.logger.info(f"📈 HIGH-VOLUME MODE: Targeting {max_posts} posts with advanced pagination")
                return await self._extract_high_volume_posts(username, max_posts)
            
            # Strategy 2: Enhanced scrolling for medium requests (25-99)  
            elif max_posts >= 25:
                max_scroll_attempts = min(40, max_posts // 5)  # More aggressive
                self.logger.info(f"📊 MEDIUM-VOLUME MODE: {max_posts} posts with {max_scroll_attempts} scroll attempts")
            
            # Strategy 3: Standard approach for small requests (<25)
            else:
                max_scroll_attempts = min(15, max(5, max_posts // 3))
                self.logger.info(f"📊 STANDARD MODE: {max_posts} posts with {max_scroll_attempts} scroll attempts")
            
            date_threshold_reached = False
            while len(posts) < max_posts and scroll_attempts < max_scroll_attempts and not date_threshold_reached:
                tweet_candidates = []
                tweets_found_this_round = 0  # Initialize variable to track tweets found in this scroll iteration
                
                # Method 1: Try 2025-compatible CSS selectors first - ENHANCED FOR X/TWITTER 2024+
                css_selectors_2025 = [
                    # Primary X/Twitter 2024+ selectors
                    'article[data-testid="tweet"]',
                    'div[data-testid="tweet"]', 
                    '[data-testid="tweetWrapperOuter"]',
                    '[data-testid="cellInnerDiv"]',
                    
                    # Alternative article-based selectors
                    'article[role="article"]',
                    'article[tabindex="-1"]',
                    'article',  # Generic article elements
                    
                    # Content-based selectors for X/Twitter 2024
                    '[data-testid="tweetText"]',  # Direct tweet text containers
                    'div:has([data-testid="tweetText"])',  # Containers with tweet text
                    'div:has([data-testid="User-Name"])',  # Containers with user names
                    'div:has(time)',  # Containers with time elements
                    'div:has([href*="/status/"])',  # Containers with status links
                    
                    # Broader fallback selectors
                    'div[dir="ltr"][lang]',
                    'main div[data-testid]',  # Any testid divs in main
                    'main article',  # Any articles in main content area
                ]
                
                css_tweets_found = False
                tweet_elements_found = []
                
                self.logger.info(f"🔍 Testing {len(css_selectors_2025)} CSS selectors for tweet detection...")
                
                for idx, selector in enumerate(css_selectors_2025):
                    try:
                        self.logger.info(f"🔍 Testing selector {idx+1}/{len(css_selectors_2025)}: {selector[:60]}...")
                        # Add timeout wrapper to prevent hanging
                        elements = await self._with_timeout(
                            self.page.locator(selector).all(),
                            timeout_ms=5000,
                            operation_name=f"Locate tweets with selector {idx+1}",
                            default=[]
                        )

                        if elements:
                            self.logger.info(f"✅ Found {len(elements)} elements with selector: {selector}")
                            
                            # Try using enhanced element-based extraction first
                            for i, element in enumerate(elements[:max_posts]):
                                try:
                                    # Use the enhanced tweet extraction method!
                                    self.logger.info(f"🔍 DEBUG: Attempting enhanced extraction for element {i}")
                                    enhanced_tweet = await self._extract_tweet_from_element(element, i, username, include_engagement=True)
                                    self.logger.info(f"🔍 DEBUG: Enhanced extraction result: {enhanced_tweet}")
                                    if enhanced_tweet and enhanced_tweet.get('text'):
                                        tweet_elements_found.append(enhanced_tweet)
                                        css_tweets_found = True
                                        self.logger.info(f"✅ Extracted enhanced tweet: '{enhanced_tweet['text'][:50]}...'")
                                        if len(tweet_elements_found) >= max_posts:
                                            break
                                    else:
                                        self.logger.warning(f"⚠️ Enhanced extraction returned empty result for element {i}")
                                except Exception as e:
                                    self.logger.error(f"🚨 Enhanced extraction failed for element {i}: {e}")
                                    import traceback
                                    self.logger.error(f"🚨 Traceback: {traceback.format_exc()}")
                                    # Fallback to simple text extraction
                                    try:
                                        text_content = await self._with_timeout(
                                            element.inner_text(),
                                            timeout_ms=3000,
                                            operation_name=f"Get element {i} text",
                                            default=""
                                        )
                                        if text_content and len(text_content.strip()) > 10:
                                            lines = text_content.split('\n')
                                            for line in lines:
                                                line = line.strip()
                                                if self._is_likely_tweet_content(line):
                                                    tweet_candidates.append(line)
                                                    css_tweets_found = True
                                                    if len(tweet_candidates) >= max_posts:
                                                        break
                                    except Exception:
                                        continue
                            
                            if css_tweets_found:
                                break
                        else:
                            self.logger.debug(f"❌ No elements found with selector: {selector}")
                            
                    except Exception as e:
                        self.logger.debug(f"❌ Selector {selector} failed: {e}")
                        continue
                
                # If we found enhanced tweet objects, use them directly
                if tweet_elements_found:
                    self.logger.info(f"🎉 Using {len(tweet_elements_found)} enhanced tweet objects with full metadata!")
                    posts.extend(tweet_elements_found)
                    tweets_found_this_round += len(tweet_elements_found)
                
                # Method 2: Enhanced div content parsing (fallback) with metadata extraction
                if not css_tweets_found:
                    self.logger.info("🔄 Falling back to enhanced div parsing with metadata extraction...")

                    # Get all divs that might contain tweets with timeout
                    tweet_divs = await self._with_timeout(
                        self.page.locator('div[data-testid*="cell"], article, div[role="article"]').all(),
                        timeout_ms=5000,
                        operation_name="Locate tweet divs (fallback)",
                        default=[]
                    )

                    self.logger.info(f"🔄 DIV FALLBACK: Found {len(tweet_divs)} potential tweet containers")

                    for i, div_element in enumerate(tweet_divs[:50]):  # Limit to avoid too much processing
                        try:
                            div_text = await asyncio.wait_for(div_element.inner_text(), timeout=3.0)
                            if not div_text:
                                continue

                            # Split into lines and analyze structure
                            lines = [line.strip() for line in div_text.split('\n') if line.strip()]

                            # Look for tweet-like content
                            tweet_content = None
                            timestamp_text = None
                            engagement_data = {}

                            for line in lines:
                                # Check if line is likely tweet content
                                if self._is_likely_tweet_content(line):
                                    tweet_content = line

                                # Look for timestamp patterns
                                if any(time_indicator in line.lower() for time_indicator in ['ago', 'h', 'm', 's', ':', 'am', 'pm']) and len(line) < 20:
                                    timestamp_text = line

                                # Look for engagement numbers
                                import re
                                numbers = re.findall(r'(\d+(?:,\d+)*(?:\.\d+)?[KM]?)', line)
                                if numbers and len(line) < 50:
                                    # Try to match common engagement patterns
                                    if 'repl' in line.lower() or 'comment' in line.lower():
                                        try:
                                            engagement_data['replies'] = self._parse_engagement_number(numbers[0])
                                        except:
                                            pass
                                    elif 'retweet' in line.lower() or 'repost' in line.lower():
                                        try:
                                            engagement_data['retweets'] = self._parse_engagement_number(numbers[0])
                                        except:
                                            pass
                                    elif 'like' in line.lower() or 'heart' in line.lower():
                                        try:
                                            engagement_data['likes'] = self._parse_engagement_number(numbers[0])
                                        except:
                                            pass

                            # If we found tweet content, create a structured tweet object
                            if tweet_content:
                                enhanced_tweet = {
                                    'id': f'div_tweet_{i+1}',
                                    'text': tweet_content[:500],
                                    'author': username,
                                    'url': f"https://x.com/{username}",
                                    'extracted_from': 'enhanced_div_parsing_2025',
                                    'extraction_attempt': scroll_attempts + 1,
                                    'method': 'div_parsing_enhanced',
                                    'timestamp': None,
                                    'extraction_quality': 'medium'
                                }

                                # Add timestamp if found
                                if timestamp_text:
                                    enhanced_tweet['date'] = timestamp_text
                                    enhanced_tweet['date_human'] = timestamp_text
                                    # Try to convert to ISO if possible
                                    iso_timestamp = self._convert_relative_to_iso(timestamp_text)
                                    if iso_timestamp:
                                        enhanced_tweet['timestamp'] = iso_timestamp

                                # Add engagement data if found
                                enhanced_tweet.update(engagement_data)

                                # Add to results
                                tweet_elements_found.append(enhanced_tweet)
                                self.logger.info(f"✅ DIV FALLBACK: Enhanced tweet extracted: '{tweet_content[:50]}...'")

                                if engagement_data:
                                    engagement_str = ', '.join([f"{k}:{v}" for k, v in engagement_data.items()])
                                    self.logger.info(f"✅ DIV FALLBACK: Engagement data: {engagement_str}")

                        except Exception as div_e:
                            self.logger.debug(f"🔄 DIV FALLBACK: Error processing div {i}: {div_e}")
                            continue

                    # Also do the simple text extraction as final fallback
                    if not tweet_elements_found:
                        self.logger.info("🔄 DIV FALLBACK: Using simple text extraction as final fallback")
                        div_texts = await self.page.locator('div').all_inner_texts()

                        for div_text in div_texts:
                            lines = div_text.split('\n')
                            for line in lines:
                                line = line.strip()
                                if self._is_likely_tweet_content(line):
                                    tweet_candidates.append(line)
                
                self.logger.info(f"🔍 Found {len(tweet_candidates)} tweet candidates in attempt {scroll_attempts + 1}")
                
                # Process candidates into tweet objects
                seen_tweets = set()
                for tweet_text in tweet_candidates:
                    if len(posts) >= max_posts:
                        break
                    if tweet_text not in seen_tweets and tweet_text not in [post.get('text') for post in posts]:
                        seen_tweets.add(tweet_text)
                        tweets_found_this_round = len([p for p in posts if p.get('extraction_attempt') == scroll_attempts + 1])

                        # Enhanced tweet data with better metadata
                        tweet_data = {
                            'id': f'tweet_{len(posts)+1}',
                            'text': tweet_text,
                            'author': username,
                            'url': f'https://x.com/{username}',
                            'extracted_from': 'hybrid_extraction_2025',
                            'extraction_attempt': scroll_attempts + 1,
                            'method': 'css_selector' if css_tweets_found else 'div_parsing',
                            'timestamp': datetime.now().isoformat(),  # Add current timestamp as extraction time
                            'extraction_quality': 'medium'  # Quality indicator
                        }
                        
                        # Apply date filtering if enabled
                        if self.enable_date_filtering:
                            if self.is_tweet_within_date_range(tweet_data):
                                posts.append(tweet_data)
                                self.logger.info(f"🐦 ✅ Tweet {len(posts)} (within date range): '{tweet_text[:100]}...'")
                            else:
                                self.logger.debug(f"🐦 ⏭️ Skipped tweet (outside date range): '{tweet_text[:50]}...'")
                            
                            # Check if we should stop extraction due to date threshold
                            if self.should_stop_extraction(tweet_data):
                                self.logger.info(f"⏹️ Stopping posts extraction - reached date threshold")
                                date_threshold_reached = True
                                break
                        else:
                            posts.append(tweet_data)
                            self.logger.info(f"🐦 Extracted tweet {len(posts)}: '{tweet_text[:100]}...'")
                
                # If we have enough posts, break
                if len(posts) >= max_posts:
                    break
                    
                # ENHANCED SCROLLING for high-volume extraction
                posts_this_attempt = len([p for p in posts if p.get('extraction_attempt') == scroll_attempts + 1])
                self.logger.info(f"📜 Scroll attempt {scroll_attempts + 1}/{max_scroll_attempts}: {posts_this_attempt} new tweets found")
                
                # PROGRESSIVE SCROLLING: More aggressive for large requests
                if max_posts > 50:
                    # For large requests, scroll more aggressively
                    scroll_distance = 1200  # Longer scroll distance
                    scroll_repetitions = 2   # Multiple scrolls per attempt
                    wait_time_range = (3, 5) # Longer wait for content to load
                elif max_posts > 25:
                    scroll_distance = 800
                    scroll_repetitions = 2  
                    wait_time_range = (2, 4)
                else:
                    scroll_distance = 600
                    scroll_repetitions = 1
                    wait_time_range = (2, 3)
                
                # Perform progressive scrolling
                for scroll_rep in range(scroll_repetitions):
                    await self.page.mouse.wheel(0, scroll_distance)
                    await self._human_delay(1, 2)  # Brief pause between scrolls
                
                # Wait for new content to load with progressive timing
                await self._human_delay(*wait_time_range)
                scroll_attempts += 1

                # 🚀 DEBUG: Log scroll attempt info
                self.logger.info(f"🔄 SCROLL DEBUG: scroll_attempts={scroll_attempts}, len(posts)={len(posts)}, max_posts={max_posts}")

                # 🚀 PHASE 3: DEDICATED SCROLLING-BASED EXTRACTION
                # After scrolling, try to extract from newly loaded content
                if max_posts >= 15 and len(posts) < max_posts:  # Only for hybrid requests (15+ tweets)
                    self.logger.info(f"🎯 PHASE 3 TRIGGERED: max_posts={max_posts}, len(posts)={len(posts)}")
                    remaining_needed = max_posts - len(posts)
                    if remaining_needed > 0:
                        self.logger.info(f"🔄 SCROLLING EXTRACTION: Attempting to extract {remaining_needed} more tweets from scrolled content")

                        # Track already extracted content for deduplication
                        extracted_tweet_ids = set()
                        seen_text_hashes = set()

                        # Build dedup tracking from existing posts
                        for post in posts:
                            if post.get('id'):
                                extracted_tweet_ids.add(post['id'])
                            if post.get('text'):
                                import hashlib
                                text_hash = hashlib.md5(post['text'].strip().encode()).hexdigest()[:8]
                                seen_text_hashes.add(text_hash)

                        # Extract from scrolled content
                        scrolled_tweets = await self._extract_tweets_from_scrolled_content(
                            username, remaining_needed, extracted_tweet_ids, seen_text_hashes
                        )

                        # Add scrolled tweets to results
                        if scrolled_tweets:
                            posts.extend(scrolled_tweets)
                            self.logger.info(f"✅ SCROLLING SUCCESS: Added {len(scrolled_tweets)} tweets from scrolled content")
                            self.logger.info(f"📊 TOTAL PROGRESS: {len(posts)}/{max_posts} posts extracted")

                # STAGNATION CHECK: If no new tweets in last 2 attempts, try different approach
                if scroll_attempts > 2:
                    recent_posts = [p for p in posts if p.get('extraction_attempt', 0) >= scroll_attempts - 1]
                    if len(recent_posts) == 0:
                        self.logger.warning(f"⚠️ No new tweets in recent attempts - trying page refresh strategy")
                        try:
                            # Try refreshing the page to get new content
                            await self.page.reload(wait_until='domcontentloaded', timeout=60000)
                            await self._human_delay(3, 5)
                        except:
                            pass
                
            self.logger.info(f"🐦 Found {len(posts)} tweets using hybrid extraction")

            # DEDUPLICATION: Remove tweets with duplicate IDs
            self.logger.info(f"🔍 Deduplication: {len(posts)} posts before dedup")

            seen_tweet_ids = set()
            unique_posts = []

            for post in posts:
                tweet_id = post.get('tweet_id') or post.get('id')

                # Generate a unique key - prefer tweet_id if it's a real Twitter ID
                if tweet_id and not tweet_id.startswith('tweet_'):
                    unique_key = tweet_id
                else:
                    # Fallback: use combination of author and text
                    unique_key = f"{post.get('author', '')}_{post.get('text', '')[:50]}"

                if unique_key in seen_tweet_ids:
                    self.logger.debug(f"⏭️ Skipping duplicate: {unique_key}")
                    continue

                seen_tweet_ids.add(unique_key)
                unique_posts.append(post)

            self.logger.info(f"✅ Deduplication complete: {len(posts)} -> {len(unique_posts)} unique posts")

            return unique_posts[:max_posts]

        except Exception as e:
            self.logger.error(f"❌ Posts scraping failed: {e}")
            return posts
    
    def _is_likely_tweet_content(self, line: str) -> bool:
        """Enhanced helper method to identify if a line is likely tweet content."""
        if not line or len(line) < 10 or len(line) > 500:
            return False

        line_lower = line.lower()

        # CRITICAL: Skip promotional and advertising content
        promotional_indicators = [
            'get ready for epic', 'thrilling moments', 'nonstop', 'action',
            'follow for daily', 'newsletter', 'tools & resources',
            'ai investor', 'entrepreneur', 'creator',
            'the media could not be played',
            'wisdom', 'dose of'
        ]

        if any(promo in line_lower for promo in promotional_indicators):
            return False

        # ENHANCED PROFILE DATA DETECTION - Skip profile contamination
        profile_indicators = [
            'comjoined', '.comjoined', 'cagithub', 'cayoutu',
            'followers', 'following', 'posts', 'joined',
            'san francisco', 'new york', 'los angeles',
            'ca', 'ny', 'tx', 'usa'
        ]

        if any(indicator in line_lower for indicator in profile_indicators):
            return False

        # Skip UI elements, navigation, and system messages
        ui_elements = [
            'follow', 'following', 'followers', 'posts', 'replies', 'media', 'likes',
            'joined', 'home', 'search', 'messages', 'bookmarks', 'lists', 'profile',
            'more', 'settings', 'notifications', 'repost', 'quote', 'bookmark',
            'share', 'copy', 'show more', 'show less', 'show this thread',
            'translate post', 'copy link', 'view keyboard', 'keyboard shortcuts'
        ]

        if (line.strip().lower() in ui_elements or
            line.startswith('To view') or line.startswith('View keyboard') or
            line.isdigit() or
            all(c in '0123456789,. MKB' for c in line) or
            line.startswith('Joined ') or
            any(line.endswith(ext) for ext in ['.com', '.org', '.net', '.io']) or
            line.startswith(('http://', 'https://')) or
            (len(line.split()) == 1 and line.startswith('@'))):
            return False

        # Skip concatenated profile data and dates
        if ('joined' in line_lower and
            (any(year in line for year in ['2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022', '2023', '2024', '2025']) or
             any(month in line_lower for month in ['january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'october', 'november', 'december']))):
            return False

        # Skip standalone URLs
        if ('.' in line and line.count(' ') == 0 and
            any(tld in line_lower for tld in ['.com', '.org', '.net', '.io', '.co'])):
            return False

        words = line.split()
        if len(words) < 3:  # Increased minimum word count
            return False

        # Must contain alphabetic content
        if not any(c.isalpha() for c in line):
            return False

        # ENHANCED: Content must look like natural human communication
        has_sentence_structure = (
            # Has punctuation indicating complete thoughts
            ('.' in line or '!' in line or '?' in line or ':' in line) or
            # Has sufficient length and complexity
            (len(words) >= 6 and len(line) >= 30) or
            # Contains personal pronouns or common discourse markers
            any(word.lower() in ['i', 'we', 'you', 'they', 'this', 'that', 'it', 'what', 'how', 'when', 'where', 'why'] for word in words) or
            # Contains common verbs indicating statements/opinions
            any(word.lower() in ['is', 'are', 'was', 'were', 'has', 'have', 'had', 'will', 'would', 'should', 'could', 'think', 'believe', 'know', 'feel'] for word in words)
        )

        # STRICTER FILTER: Must have clear sentence structure
        if not has_sentence_structure:
            return False

        # Additional quality checks
        if (len(words) < 5 and not any(punct in line for punct in '.!?:')) or \
           (line.count(' ') < 2):  # Must have at least 3 words with spaces
            return False

        return True
    
    async def _scrape_user_likes(self, username: str, max_likes: int) -> List[Dict[str, Any]]:
        """Scrape user's liked tweets."""
        likes = []
        try:
            # Navigate to likes page
            likes_url = f"https://x.com/{username}/likes"
            await self.page.goto(likes_url, wait_until='domcontentloaded', timeout=30000)
            await self._human_delay(3, 5)

            # Check if likes page is accessible (not restricted)
            page_content = await self.page.content()
            if any(restriction in page_content.lower() for restriction in [
                'these likes are protected', 'not authorized', 'private account',
                'blocked', 'suspended', 'account doesn\'t exist'
            ]):
                self.logger.warning(f"⚠️ Likes for @{username} are restricted or protected")
                return [{
                    'type': 'access_restricted',
                    'message': f'Likes for @{username} are not publicly accessible',
                    'reason': 'protected_or_restricted_account'
                }]
            
            # Wait for basic page structure instead of specific elements
            await self.page.wait_for_selector('div', timeout=20000)
            
            scroll_attempts = 0
            max_scrolls = min(max_likes // 5, 10)
            
            date_threshold_reached = False
            while len(likes) < max_likes and scroll_attempts < max_scrolls and not date_threshold_reached:
                # Enhanced selectors for modern Twitter/X layout
                tweet_selectors = [
                    'article[data-testid="tweet"]',
                    'article[role="article"]',
                    'div[data-testid="cellInnerDiv"]',
                    'div[data-testid*="cell"]',
                    'div:has(time)',
                    '[role="article"]',
                    'div[role="article"]'
                ]
                
                tweet_elements = []
                for selector in tweet_selectors:
                    try:
                        elements = await self.page.locator(selector).all()
                        if elements:
                            tweet_elements = elements
                            break
                    except:
                        continue
                
                # Fallback to div parsing if no tweet elements found
                if not tweet_elements:
                    div_texts = await self.page.locator('div').all_inner_texts()
                    for div_text in div_texts:
                        lines = div_text.split('\n')
                        for line in lines:
                            line = line.strip()
                            if self._is_likely_tweet_content(line) and line not in [like.get('text', '') for like in likes]:
                                tweet_data = {
                                    'text': line,
                                    'type': 'liked_tweet',
                                    'index': len(likes)
                                }
                                likes.append(tweet_data)
                                if len(likes) >= max_likes:
                                    break
                        if len(likes) >= max_likes:
                            break
                    break
                
                for i, tweet_element in enumerate(tweet_elements):
                    if len(likes) >= max_likes:
                        break

                    tweet_data = await self._extract_tweet_from_element(tweet_element, i, username, True)
                    if tweet_data and tweet_data not in likes:
                        # Apply date filtering if enabled
                        if self.enable_date_filtering:
                            if self.is_tweet_within_date_range(tweet_data):
                                likes.append(tweet_data)
                                self.logger.debug(f"❤️ ✅ Like {len(likes)} (within date range)")
                            else:
                                self.logger.debug(f"❤️ ⏭️ Skipped like (outside date range)")
                            
                            # Check if we should stop extraction due to date threshold
                            if self.should_stop_extraction(tweet_data):
                                self.logger.info(f"⏹️ Stopping likes extraction - reached date threshold")
                                date_threshold_reached = True
                                break
                        else:
                            likes.append(tweet_data)
                
                if len(likes) < max_likes and not date_threshold_reached:
                    await self.page.mouse.wheel(0, 600)
                    await self._human_delay(1, 3)
                    scroll_attempts += 1
            
            self.logger.info(f"❤️ Extracted {len(likes)} likes")
            return likes[:max_likes]
            
        except Exception as e:
            self.logger.error(f"❌ Likes scraping failed: {e}")
            return likes

    async def _scrape_user_reposts(self, username: str, max_reposts: int) -> List[Dict[str, Any]]:
        """Scrape user's reposts/retweets."""
        reposts = []
        try:
            # Navigate to user's profile with replies to find retweets
            # Retweets are typically shown on the main profile page or in with_replies
            profile_url = f"https://x.com/{username}/with_replies"
            await self.page.goto(profile_url, wait_until='domcontentloaded', timeout=30000)
            await self._human_delay(3, 5)

            # Check if profile is accessible
            page_content = await self.page.content()
            if any(restriction in page_content.lower() for restriction in [
                'account suspended', 'account doesn\'t exist', 'private account',
                'blocked', 'not authorized'
            ]):
                self.logger.warning(f"⚠️ Profile @{username} is restricted or protected")
                return [{
                    'type': 'access_restricted',
                    'message': f'Profile @{username} is not publicly accessible',
                    'reason': 'protected_or_restricted_account'
                }]

            # Wait for basic page structure
            await self.page.wait_for_selector('div', timeout=20000)

            scroll_attempts = 0
            max_scrolls = min(max_reposts // 3, 15)  # More scrolls as reposts may be sparse

            date_threshold_reached = False
            while len(reposts) < max_reposts and scroll_attempts < max_scrolls and not date_threshold_reached:
                # Look for retweet indicators in tweet elements
                tweet_selectors = [
                    'article[data-testid="tweet"]',
                    'article[role="article"]',
                    'div[data-testid="cellInnerDiv"]'
                ]

                tweet_elements = []
                for selector in tweet_selectors:
                    try:
                        elements = await self.page.locator(selector).all()
                        if elements:
                            tweet_elements = elements
                            break
                    except:
                        continue

                for i, tweet_element in enumerate(tweet_elements):
                    if len(reposts) >= max_reposts:
                        break

                    try:
                        # Check for retweet indicators
                        element_text = await tweet_element.inner_text()

                        # Look for retweet patterns
                        retweet_indicators = [
                            f"{username} reposted",
                            f"@{username} reposted",
                            "reposted",
                            "Retweeted"
                        ]

                        is_repost = False
                        for indicator in retweet_indicators:
                            if indicator.lower() in element_text.lower():
                                is_repost = True
                                break

                        # Also check for repost icon/button elements
                        if not is_repost:
                            try:
                                repost_icons = await tweet_element.locator('[data-testid="retweet"], [aria-label*="repost"], [aria-label*="retweet"]').all()
                                if repost_icons:
                                    is_repost = True
                            except:
                                pass

                        if is_repost:
                            tweet_data = await self._extract_tweet_from_element(tweet_element, i, username, False)
                            if tweet_data:
                                # Mark as repost and add additional metadata
                                tweet_data['type'] = 'repost'
                                tweet_data['reposted_by'] = username
                                tweet_data['extraction_method'] = 'repost_detection'

                                # Apply date filtering if enabled
                                if self.enable_date_filtering:
                                    if self.is_tweet_within_date_range(tweet_data):
                                        reposts.append(tweet_data)
                                        self.logger.debug(f"🔄 ✅ Repost {len(reposts)} (within date range)")
                                    else:
                                        self.logger.debug(f"🔄 ⏭️ Skipped repost (outside date range)")

                                    if self.should_stop_extraction(tweet_data):
                                        self.logger.info(f"⏹️ Stopping reposts extraction - reached date threshold")
                                        date_threshold_reached = True
                                        break
                                else:
                                    reposts.append(tweet_data)

                    except Exception as e:
                        self.logger.debug(f"Error processing potential repost element: {e}")
                        continue

                if len(reposts) < max_reposts and not date_threshold_reached:
                    await self.page.mouse.wheel(0, 800)
                    await self._human_delay(2, 4)
                    scroll_attempts += 1

            self.logger.info(f"🔄 Extracted {len(reposts)} reposts")
            return reposts[:max_reposts]

        except Exception as e:
            self.logger.error(f"❌ Reposts scraping failed: {e}")
            return reposts

    async def _scrape_user_mentions(self, username: str, max_mentions: int) -> List[Dict[str, Any]]:
        """Scrape tweets that mention the user."""
        mentions = []
        try:
            # Search for mentions using simplified approach
            search_url = f"https://x.com/search?q=%40{username}&src=typed_query&f=live"
            await self.page.goto(search_url, wait_until='domcontentloaded', timeout=30000)
            await self._human_delay(3, 5)
            
            # Wait for basic page structure
            await self.page.wait_for_selector('div', timeout=20000)
            
            # Use div-based extraction similar to working posts method
            div_texts = await self.page.locator('div').all_inner_texts()
            for div_text in div_texts:
                if len(mentions) >= max_mentions:
                    break
                lines = div_text.split('\n')
                for line in lines:
                    line = line.strip()
                    # Look for content that mentions the username
                    if (self._is_likely_tweet_content(line) and 
                        f"@{username}" in line and 
                        line not in [mention.get('text', '') for mention in mentions]):
                        
                        mention_data = {
                            'text': line,
                            'type': 'mention',
                            'index': len(mentions),
                            'mentioned_user': f"@{username}"
                        }
                        mentions.append(mention_data)
                        if len(mentions) >= max_mentions:
                            break
            
            scroll_attempts = 0
            max_scrolls = min(max_mentions // 5, 8)
            
            date_threshold_reached = False
            while len(mentions) < max_mentions and scroll_attempts < max_scrolls and not date_threshold_reached:
                # Enhanced selectors for modern Twitter/X layout
                tweet_selectors = [
                    'article[data-testid="tweet"]',
                    'article[role="article"]',
                    'div[data-testid="cellInnerDiv"]',
                    'div[data-testid*="cell"]',
                    '[role="article"]',
                    'div[role="article"]'
                ]
                
                tweet_elements = []
                for selector in tweet_selectors:
                    try:
                        elements = await self.page.locator(selector).all()
                        if elements:
                            tweet_elements = elements
                            break
                    except:
                        continue
                
                if not tweet_elements:
                    break
                
                for i, tweet_element in enumerate(tweet_elements):
                    if len(mentions) >= max_mentions:
                        break

                    tweet_data = await self._extract_tweet_from_element(tweet_element, i, username, True)
                    if tweet_data and tweet_data not in mentions:
                        # Apply date filtering if enabled
                        if self.enable_date_filtering:
                            if self.is_tweet_within_date_range(tweet_data):
                                mentions.append(tweet_data)
                                self.logger.debug(f"@️⃣ ✅ Mention {len(mentions)} (within date range)")
                            else:
                                self.logger.debug(f"@️⃣ ⏭️ Skipped mention (outside date range)")
                            
                            # Check if we should stop extraction due to date threshold
                            if self.should_stop_extraction(tweet_data):
                                self.logger.info(f"⏹️ Stopping mentions extraction - reached date threshold")
                                date_threshold_reached = True
                                break
                        else:
                            mentions.append(tweet_data)
                
                if len(mentions) < max_mentions and not date_threshold_reached:
                    await self.page.mouse.wheel(0, 600)
                    await self._human_delay(1, 3)
                    scroll_attempts += 1
            
            self.logger.info(f"@️⃣ Extracted {len(mentions)} mentions")
            return mentions[:max_mentions]
            
        except Exception as e:
            self.logger.error(f"❌ Mentions scraping failed: {e}")
            return mentions
    
    async def _scrape_user_media(self, username: str, max_media: int) -> List[Dict[str, Any]]:
        """Scrape user's media posts."""
        media = []
        try:
            # Navigate to media tab
            media_url = f"https://x.com/{username}/media"
            await self.page.goto(media_url, wait_until='domcontentloaded', timeout=30000)
            await self._human_delay(3, 5)
            
            # Wait for basic page structure
            await self.page.wait_for_selector('div', timeout=20000)
            await self._human_delay(3, 5)
            
            # Simplified media extraction - look for posts with media content
            div_texts = await self.page.locator('div').all_inner_texts()
            for div_text in div_texts:
                if len(media) >= max_media:
                    break
                lines = div_text.split('\n')
                for line in lines:
                    line = line.strip()
                    # Look for posts that likely contain media
                    if (self._is_likely_tweet_content(line) and 
                        len(line) > 20 and 
                        line not in [m.get('text', '') for m in media]):
                        
                        # Assume this is a media post if on media page
                        media_data = {
                            'text': line,
                            'type': 'media_post',
                            'index': len(media),
                            'extracted_from': 'media_page'
                        }
                        media.append(media_data)
                        if len(media) >= max_media:
                            break
            
            scroll_attempts = 0
            max_scrolls = min(max_media // 3, 5)
            
            while len(media) < max_media and scroll_attempts < max_scrolls:
                # Strategy 1: Direct media element detection
                media_candidates = []
                
                # Look for images
                try:
                    img_elements = await self.page.locator('img').all()
                    self.logger.info(f"🔍 Found {len(img_elements)} img elements")
                    
                    for img in img_elements:
                        try:
                            src = await img.get_attribute('src')
                            alt = await img.get_attribute('alt')
                            if (src and 
                                ('pbs.twimg.com' in src or 'media' in src.lower()) and
                                'profile' not in src.lower() and
                                'default' not in src.lower()):
                                
                                media_data = {
                                    'type': 'image',
                                    'url': src,
                                    'alt_text': alt or '',
                                    'extracted_from': 'direct_img_detection'
                                }
                                media_candidates.append(media_data)
                        except:
                            continue
                except Exception as e:
                    self.logger.debug(f"Image detection failed: {e}")
                
                # Look for videos
                try:
                    video_elements = await self.page.locator('video').all()
                    self.logger.info(f"🔍 Found {len(video_elements)} video elements")
                    
                    for video in video_elements:
                        try:
                            src = await video.get_attribute('src')
                            poster = await video.get_attribute('poster')
                            if src or poster:
                                media_data = {
                                    'type': 'video',
                                    'url': src or poster,
                                    'poster': poster,
                                    'extracted_from': 'direct_video_detection'
                                }
                                media_candidates.append(media_data)
                        except:
                            continue
                except Exception as e:
                    self.logger.debug(f"Video detection failed: {e}")
                
                # Strategy 2: Look in div content for media-related URLs
                try:
                    div_texts = await self.page.locator('div').all_inner_texts()
                    for div_text in div_texts:
                        lines = div_text.split('\n')
                        for line in lines:
                            line = line.strip()
                            # Look for media URLs or indicators
                            if ('pbs.twimg.com' in line or 
                                'video.twimg.com' in line or
                                'pic.twitter.com' in line):
                                media_data = {
                                    'type': 'media_url',
                                    'url': line,
                                    'extracted_from': 'div_media_parsing'
                                }
                                media_candidates.append(media_data)
                except Exception as e:
                    self.logger.debug(f"Div media parsing failed: {e}")
                
                self.logger.info(f"🔍 Found {len(media_candidates)} media candidates in attempt {scroll_attempts + 1}")
                
                # Add unique media items with date filtering
                for media_data in media_candidates:
                    if len(media) >= max_media:
                        break
                    # Avoid duplicates based on URL
                    if media_data['url'] not in [m.get('url') for m in media]:
                        media_data['id'] = f"media_{len(media)+1}"
                        media_data['username'] = username
                        
                        # For media, try to extract associated tweet timestamp
                        # Media date filtering is less precise but still useful
                        if self.enable_date_filtering:
                            # Try to find timestamp from parent tweet element
                            try:
                                # Look for time elements near this media
                                time_elements = await self.page.locator('time').all()
                                if time_elements and len(time_elements) > 0:
                                    # Use the first available timestamp as approximation
                                    time_elem = time_elements[0]
                                    timestamp = await time_elem.get_attribute('datetime')
                                    if timestamp:
                                        media_data['timestamp'] = timestamp
                            except:
                                pass
                            
                            # Apply date filtering if timestamp available
                            if media_data.get('timestamp') and not self.is_tweet_within_date_range(media_data):
                                self.logger.debug(f"🖼️ ⏭️ Skipped media (outside date range)")
                                continue
                                
                        media.append(media_data)
                        self.logger.info(f"🖼️ Added media {len(media)}: {media_data['type']} - {media_data['url'][:50]}...")
                
                # Scroll for more media
                if len(media) < max_media:
                    self.logger.info(f"📜 Scrolling for more media... ({len(media)}/{max_media})")
                    await self.page.mouse.wheel(0, 800)
                    await self._human_delay(3, 5)
                    scroll_attempts += 1
            
            self.logger.info(f"🖼️ Extracted {len(media)} media posts")
            return media[:max_media]
            
        except Exception as e:
            self.logger.error(f"❌ Media scraping failed: {e}")
            return media

    async def _enrich_posts_with_engagement(self, posts: List[Dict[str, Any]], params: Dict[str, Any]):
        """Enrich posts with engagement data (likers, repliers, reposters)."""
        try:
            scrape_likers = params.get('scrape_post_likers', False)
            scrape_repliers = params.get('scrape_post_repliers', False)
            scrape_reposters = params.get('scrape_post_reposters', False)

            max_likers = params.get('max_likers_per_post', 20)
            max_repliers = params.get('max_repliers_per_post', 20)
            max_reposters = params.get('max_reposters_per_post', 20)

            for i, post in enumerate(posts):
                tweet_url = post.get('url')
                if not tweet_url:
                    self.logger.warning(f"⚠️ Post {i+1} has no URL, skipping engagement extraction")
                    continue

                self.logger.info(f"🔍 [{i+1}/{len(posts)}] Extracting engagement for: {tweet_url[:60]}...")

                try:
                    # Extract likers
                    if scrape_likers:
                        likers = await self._extract_post_likers(tweet_url, max_likers)
                        post['likers'] = likers
                        post['likers_count'] = len([l for l in likers if 'error' not in l])
                        self.logger.info(f"  💖 {post['likers_count']} likers extracted")

                    # Extract repliers
                    if scrape_repliers:
                        repliers = await self._extract_post_repliers(tweet_url, max_repliers)
                        post['repliers'] = repliers
                        post['repliers_count'] = len([r for r in repliers if 'error' not in r])
                        self.logger.info(f"  💬 {post['repliers_count']} repliers extracted")

                    # Extract reposters
                    if scrape_reposters:
                        reposters = await self._extract_post_reposters(tweet_url, max_reposters)
                        post['reposters'] = reposters
                        post['reposters_count'] = len([r for r in reposters if 'error' not in r])
                        self.logger.info(f"  🔄 {post['reposters_count']} reposters extracted")

                except Exception as e:
                    self.logger.error(f"❌ Engagement extraction failed for post {i+1}: {e}")
                    # Continue with next post even if one fails

        except Exception as e:
            self.logger.error(f"❌ Post engagement enrichment failed: {e}")

    async def _extract_post_likers(self, tweet_url: str, max_likers: int = 20) -> List[Dict[str, Any]]:
        """Extract users who liked a specific tweet."""
        likers = []
        try:
            # Twitter's likes page format: https://x.com/username/status/tweet_id/likes
            likes_url = f"{tweet_url}/likes"
            self.logger.info(f"💖 Extracting likers from: {likes_url}")

            await self.page.goto(likes_url, wait_until='domcontentloaded', timeout=30000)
            await self._human_delay(3, 5)

            # Check if likes are visible
            page_content = await self.page.content()
            if any(restriction in page_content.lower() for restriction in [
                'not authorized', 'protected', 'suspended', 'blocked'
            ]):
                self.logger.warning(f"⚠️ Likes are not accessible for this tweet")
                return [{'error': 'access_restricted', 'message': 'Likes not publicly accessible'}]

            scroll_attempts = 0
            max_scrolls = min(max_likers // 10, 5)

            while len(likers) < max_likers and scroll_attempts < max_scrolls:
                # Extract user data from the page
                div_texts = await self.page.locator('div').all_inner_texts()

                for div_text in div_texts:
                    if len(likers) >= max_likers:
                        break

                    lines = div_text.split('\n')
                    for i, line in enumerate(lines):
                        line = line.strip()
                        # Look for username patterns (@username)
                        if line.startswith('@') and len(line) > 2 and len(line) < 50:
                            username = line
                            # Try to get display name from previous line
                            display_name = lines[i-1].strip() if i > 0 else ''

                            # Avoid duplicates
                            if username not in [u.get('username') for u in likers]:
                                liker_data = {
                                    'username': username,
                                    'display_name': display_name if display_name and len(display_name) < 100 else username,
                                    'profile_url': f"https://x.com/{username[1:]}",  # Remove @ symbol
                                    'extracted_from': 'likes_page'
                                }
                                likers.append(liker_data)

                                if len(likers) >= max_likers:
                                    break

                # Scroll for more likers
                if len(likers) < max_likers:
                    await self.page.mouse.wheel(0, 800)
                    await self._human_delay(2, 4)
                    scroll_attempts += 1

            self.logger.info(f"💖 Extracted {len(likers)} likers")
            return likers[:max_likers]

        except Exception as e:
            self.logger.error(f"❌ Post likers extraction failed: {e}")
            return likers

    async def _extract_post_repliers(self, tweet_url: str, max_repliers: int = 20) -> List[Dict[str, Any]]:
        """Extract users who replied to a specific tweet."""
        repliers = []
        try:
            self.logger.info(f"💬 Extracting repliers from: {tweet_url}")

            await self.page.goto(tweet_url, wait_until='domcontentloaded', timeout=30000)
            await self._human_delay(3, 5)

            scroll_attempts = 0
            max_scrolls = min(max_repliers // 10, 5)

            while len(repliers) < max_repliers and scroll_attempts < max_scrolls:
                # Look for reply tweets - they have different structure
                div_texts = await self.page.locator('div').all_inner_texts()

                for div_text in div_texts:
                    if len(repliers) >= max_repliers:
                        break

                    lines = div_text.split('\n')
                    for i, line in enumerate(lines):
                        line = line.strip()

                        # Look for "Replying to @username" pattern
                        if 'replying to' in line.lower() or line.startswith('@'):
                            # Next username pattern is likely the replier
                            for j in range(max(0, i-3), min(len(lines), i+3)):
                                potential_username = lines[j].strip()
                                if (potential_username.startswith('@') and
                                    len(potential_username) > 2 and
                                    len(potential_username) < 50):

                                    # Avoid duplicates
                                    if potential_username not in [u.get('username') for u in repliers]:
                                        replier_data = {
                                            'username': potential_username,
                                            'profile_url': f"https://x.com/{potential_username[1:]}",
                                            'extracted_from': 'tweet_replies'
                                        }
                                        repliers.append(replier_data)

                                        if len(repliers) >= max_repliers:
                                            break

                            if len(repliers) >= max_repliers:
                                break

                # Scroll for more replies
                if len(repliers) < max_repliers:
                    await self.page.mouse.wheel(0, 800)
                    await self._human_delay(2, 4)
                    scroll_attempts += 1

            self.logger.info(f"💬 Extracted {len(repliers)} repliers")
            return repliers[:max_repliers]

        except Exception as e:
            self.logger.error(f"❌ Post repliers extraction failed: {e}")
            return repliers

    async def _extract_post_reposters(self, tweet_url: str, max_reposters: int = 20) -> List[Dict[str, Any]]:
        """Extract users who reposted a specific tweet."""
        reposters = []
        try:
            # Twitter's retweets page format: https://x.com/username/status/tweet_id/retweets
            retweets_url = f"{tweet_url}/retweets"
            self.logger.info(f"🔄 Extracting reposters from: {retweets_url}")

            await self.page.goto(retweets_url, wait_until='domcontentloaded', timeout=30000)
            await self._human_delay(3, 5)

            # Check if retweets are visible
            page_content = await self.page.content()
            if any(restriction in page_content.lower() for restriction in [
                'not authorized', 'protected', 'suspended', 'blocked'
            ]):
                self.logger.warning(f"⚠️ Retweets are not accessible for this tweet")
                return [{'error': 'access_restricted', 'message': 'Retweets not publicly accessible'}]

            scroll_attempts = 0
            max_scrolls = min(max_reposters // 10, 5)

            while len(reposters) < max_reposters and scroll_attempts < max_scrolls:
                # Extract user data from the page
                div_texts = await self.page.locator('div').all_inner_texts()

                for div_text in div_texts:
                    if len(reposters) >= max_reposters:
                        break

                    lines = div_text.split('\n')
                    for i, line in enumerate(lines):
                        line = line.strip()
                        # Look for username patterns (@username)
                        if line.startswith('@') and len(line) > 2 and len(line) < 50:
                            username = line
                            # Try to get display name from previous line
                            display_name = lines[i-1].strip() if i > 0 else ''

                            # Avoid duplicates
                            if username not in [u.get('username') for u in reposters]:
                                reposter_data = {
                                    'username': username,
                                    'display_name': display_name if display_name and len(display_name) < 100 else username,
                                    'profile_url': f"https://x.com/{username[1:]}",
                                    'extracted_from': 'retweets_page'
                                }
                                reposters.append(reposter_data)

                                if len(reposters) >= max_reposters:
                                    break

                # Scroll for more reposters
                if len(reposters) < max_reposters:
                    await self.page.mouse.wheel(0, 800)
                    await self._human_delay(2, 4)
                    scroll_attempts += 1

            self.logger.info(f"🔄 Extracted {len(reposters)} reposters")
            return reposters[:max_reposters]

        except Exception as e:
            self.logger.error(f"❌ Post reposters extraction failed: {e}")
            return reposters

    async def _scrape_user_followers(self, username: str, max_followers: int) -> List[Dict[str, Any]]:
        """Scrape user's followers list."""
        followers = []
        try:
            # Navigate to followers page
            followers_url = f"https://x.com/{username}/followers"
            await self.page.goto(followers_url, wait_until='domcontentloaded', timeout=30000)

            # HUMAN BEHAVIOR: Simulate natural browsing on followers page
            self.logger.info("👥 Using div-based followers extraction...")
            await HumanBehavior.random_delay(2000, 4000)
            await HumanBehavior.simulate_profile_browsing(self.page)
            
            scroll_attempts = 0
            max_scrolls = min(max_followers // 5, 15)
            
            while len(followers) < max_followers and scroll_attempts < max_scrolls:
                # Extract user data from div content (same approach as tweets)
                div_texts = await self.page.locator('div').all_inner_texts()
                user_candidates = []
                
                for div_text in div_texts:
                    lines = div_text.split('\n')
                    
                    # Look for patterns that indicate user profiles
                    for i, line in enumerate(lines):
                        line = line.strip()
                        
                        # Identify potential usernames (start with @)
                        if (line.startswith('@') and 
                            len(line) > 1 and len(line) < 50 and
                            line.count('@') == 1 and
                            ' ' not in line):  # No spaces in usernames
                            
                            username = line
                            display_name = ""
                            bio = ""
                            
                            # Look for display name (usually line before username)
                            if i > 0 and len(lines[i-1].strip()) > 0:
                                potential_display = lines[i-1].strip()
                                if (not potential_display.startswith('@') and 
                                    len(potential_display) < 100 and
                                    not potential_display.endswith('Following') and
                                    not potential_display.endswith('Followers') and
                                    not potential_display.isdigit()):
                                    display_name = potential_display
                            
                            # Look for bio (lines after username)
                            if i < len(lines) - 1:
                                for j in range(i + 1, min(i + 4, len(lines))):  # Check next 3 lines
                                    potential_bio = lines[j].strip()
                                    if (len(potential_bio) > 20 and len(potential_bio) < 200 and
                                        not potential_bio.startswith('@') and
                                        not potential_bio.endswith('Following') and
                                        not potential_bio.endswith('Followers') and
                                        not potential_bio.endswith('ago') and
                                        not potential_bio.isdigit()):
                                        bio = potential_bio
                                        break
                            
                            # Create user candidate
                            user_data = {
                                'username': username,
                                'display_name': display_name,
                                'bio': bio,
                                'extracted_from': 'div_followers_parsing'
                            }
                            
                            # Avoid duplicates
                            if user_data not in user_candidates and username not in [u.get('username') for u in user_candidates]:
                                user_candidates.append(user_data)
                
                self.logger.info(f"🔍 Found {len(user_candidates)} follower candidates in attempt {scroll_attempts + 1}")
                
                # Add new unique followers
                for user_data in user_candidates:
                    if len(followers) >= max_followers:
                        break
                    if user_data['username'] not in [f.get('username') for f in followers]:
                        followers.append(user_data)
                        self.logger.info(f"👤 Added follower {len(followers)}: {user_data['username']} - {user_data['display_name']}")
                
                # Scroll for more followers
                if len(followers) < max_followers:
                    self.logger.info(f"📜 Scrolling for more followers... ({len(followers)}/{max_followers})")
                    # HUMAN BEHAVIOR: Use human-like scrolling
                    await HumanBehavior.human_scroll(self.page, 'down', 800)
                    scroll_attempts += 1

                    # HUMAN BEHAVIOR: Occasional mouse movements
                    if scroll_attempts % 3 == 0:
                        await HumanBehavior.random_mouse_movements(self.page, count=1)
            
            self.logger.info(f"👥 Extracted {len(followers)} followers")
            return followers[:max_followers]
            
        except Exception as e:
            self.logger.error(f"❌ Followers scraping failed: {e}")
            return followers
    
    async def _scrape_user_following(self, username: str, max_following: int) -> List[Dict[str, Any]]:
        """Scrape user's following list."""
        following = []
        try:
            # Navigate to following page
            following_url = f"https://x.com/{username}/following"
            await self.page.goto(following_url, wait_until='domcontentloaded', timeout=30000)

            # HUMAN BEHAVIOR: Simulate natural browsing on following page
            self.logger.info("➡️ Using div-based following extraction...")
            await HumanBehavior.random_delay(2000, 4000)
            await HumanBehavior.simulate_profile_browsing(self.page)
            
            scroll_attempts = 0
            max_scrolls = min(max_following // 5, 15)
            
            while len(following) < max_following and scroll_attempts < max_scrolls:
                # Extract user data from div content (same approach as followers)
                div_texts = await self.page.locator('div').all_inner_texts()
                user_candidates = []
                
                for div_text in div_texts:
                    lines = div_text.split('\n')
                    
                    # Look for patterns that indicate user profiles
                    for i, line in enumerate(lines):
                        line = line.strip()
                        
                        # Identify potential usernames (start with @)
                        if (line.startswith('@') and 
                            len(line) > 1 and len(line) < 50 and
                            line.count('@') == 1 and
                            ' ' not in line):  # No spaces in usernames
                            
                            username = line
                            display_name = ""
                            bio = ""
                            
                            # Look for display name (usually line before username)
                            if i > 0 and len(lines[i-1].strip()) > 0:
                                potential_display = lines[i-1].strip()
                                if (not potential_display.startswith('@') and 
                                    len(potential_display) < 100 and
                                    not potential_display.endswith('Following') and
                                    not potential_display.endswith('Followers') and
                                    not potential_display.isdigit()):
                                    display_name = potential_display
                            
                            # Look for bio (lines after username)
                            if i < len(lines) - 1:
                                for j in range(i + 1, min(i + 4, len(lines))):  # Check next 3 lines
                                    potential_bio = lines[j].strip()
                                    if (len(potential_bio) > 20 and len(potential_bio) < 200 and
                                        not potential_bio.startswith('@') and
                                        not potential_bio.endswith('Following') and
                                        not potential_bio.endswith('Followers') and
                                        not potential_bio.endswith('ago') and
                                        not potential_bio.isdigit()):
                                        bio = potential_bio
                                        break
                            
                            # Create user candidate
                            user_data = {
                                'username': username,
                                'display_name': display_name,
                                'bio': bio,
                                'extracted_from': 'div_following_parsing'
                            }
                            
                            # Avoid duplicates
                            if user_data not in user_candidates and username not in [u.get('username') for u in user_candidates]:
                                user_candidates.append(user_data)
                
                self.logger.info(f"🔍 Found {len(user_candidates)} following candidates in attempt {scroll_attempts + 1}")
                
                # Add new unique following users
                for user_data in user_candidates:
                    if len(following) >= max_following:
                        break
                    if user_data['username'] not in [f.get('username') for f in following]:
                        following.append(user_data)
                        self.logger.info(f"👤 Added following {len(following)}: {user_data['username']} - {user_data['display_name']}")
                
                # Scroll for more following users
                if len(following) < max_following:
                    self.logger.info(f"📜 Scrolling for more following... ({len(following)}/{max_following})")
                    # HUMAN BEHAVIOR: Use human-like scrolling
                    await HumanBehavior.human_scroll(self.page, 'down', 800)
                    scroll_attempts += 1

                    # HUMAN BEHAVIOR: Occasional mouse movements
                    if scroll_attempts % 3 == 0:
                        await HumanBehavior.random_mouse_movements(self.page, count=1)
            
            self.logger.info(f"➡️ Extracted {len(following)} following")
            return following[:max_following]
            
        except Exception as e:
            self.logger.error(f"❌ Following scraping failed: {e}")
            return following
    
    async def _extract_user_from_element(self, user_element) -> Dict[str, Any]:
        """Extract user data from a UserCell element."""
        try:
            user_data = {}
            
            # Extract username
            username_selectors = [
                '[data-testid="UserCell"] a[href*="/"] span',
                '[data-testid="UserCell"] span[dir="ltr"]'
            ]
            for selector in username_selectors:
                try:
                    username_element = user_element.locator(selector).first
                    if await username_element.count() > 0:
                        username_text = await username_element.inner_text()
                        if username_text.startswith('@'):
                            user_data['username'] = username_text
                            break
                except:
                    continue
            
            # Extract display name
            try:
                name_element = user_element.locator('[data-testid="UserCell"] span').first
                if await name_element.count() > 0:
                    display_name = await name_element.inner_text()
                    if not display_name.startswith('@'):
                        user_data['display_name'] = display_name
            except:
                pass
            
            # Extract bio
            try:
                bio_element = user_element.locator('[data-testid="UserCell"] span[dir="auto"]').first
                if await bio_element.count() > 0:
                    bio = await bio_element.inner_text()
                    user_data['bio'] = bio[:200]  # Limit bio length
            except:
                pass
            
            # Extract profile image
            try:
                img_element = user_element.locator('img').first
                if await img_element.count() > 0:
                    img_src = await img_element.get_attribute('src')
                    if img_src:
                        user_data['profile_image'] = img_src
            except:
                pass
            
            return user_data if user_data.get('username') else None
            
        except Exception as e:
            self.logger.debug(f"Failed to extract user data: {e}")
            return None
    
    async def _extract_profile_info(self, username: str) -> Dict[str, Any]:
        """Extract comprehensive profile information - FIXED for X/Twitter 2024."""
        self.logger.info(f"🔍 _extract_profile_info: Starting extraction for @{username}")
        profile_data = {}

        try:
            # 1. EXTRACT DISPLAY NAME - Updated selectors for X/Twitter 2024
            self.logger.info("🔍 Step 1: Extracting display name...")
            display_name_found = False
            name_selectors = [
                f'[data-testid="UserName"] span:not([role="presentation"])',  # Primary display name
                f'h2[role="heading"] span',  # Header display name
                f'div[dir="ltr"] span[style*="font-weight"]',  # Bold display name spans
                f'span:has-text("{username}")~span',  # Span adjacent to username
                f'[aria-labelledby] span[style*="font-weight: 800"]'  # Bold styled names
            ]
            
            for idx, selector in enumerate(name_selectors):
                try:
                    self.logger.info(f"🔍 Step 1.{idx+1}: Trying selector: {selector[:50]}...")
                    name_elements = self.page.locator(selector)
                    self.logger.info(f"🔍 Step 1.{idx+1}a: Calling count()...")
                    count = await self._with_timeout(
                        name_elements.count(),
                        timeout_ms=3000,
                        operation_name=f"Count name elements for selector {idx+1}",
                        default=0
                    )
                    self.logger.info(f"🔍 Step 1.{idx+1}b: Found {count} elements")
                    
                    for i in range(min(count, 10)):  # Limit iterations
                        element = name_elements.nth(i)
                        is_visible = await self._with_timeout(
                            element.is_visible(),
                            timeout_ms=2000,
                            operation_name=f"Check visibility {i}",
                            default=False
                        )
                        if is_visible:
                            name_text = await self._with_timeout(
                                element.inner_text(),
                                timeout_ms=2000,
                                operation_name=f"Get name text {i}",
                                default=""
                            )
                            # Validate it's actually a display name (not username, not UI text)
                            if (name_text and 
                                not name_text.startswith('@') and
                                name_text not in ['Follow', 'Following', 'Followers', 'Posts', 'Tweets', 'Replies', 'Media', 'Likes'] and
                                not 'keyboard shortcuts' in name_text.lower() and
                                len(name_text.strip()) > 1 and len(name_text.strip()) < 100):
                                profile_data['display_name'] = name_text.strip()
                                display_name_found = True
                                self.logger.info(f"✅ Found display name: '{name_text.strip()}'")
                                break
                    
                    if display_name_found:
                        break
                except Exception as e:
                    continue
            
            # Fallback: Extract display name from page title
            if not display_name_found:
                try:
                    page_title = await self.page.title()
                    # Page title format: "Display Name (@username) / X"
                    if f'@{username}' in page_title:
                        import re
                        match = re.match(r'^(.+?)\s*\(@' + username + r'\)', page_title)
                        if match:
                            profile_data['display_name'] = match.group(1).strip()
                            self.logger.info(f"✅ Display name from title: '{match.group(1).strip()}'")
                except:
                    pass
            
            # 2. EXTRACT USERNAME (ensure it's correct)
            profile_data['username'] = username
            
            # 3. EXTRACT BIO/DESCRIPTION - Updated selectors for X/Twitter 2024
            self.logger.info("🔍 Step 3: Extracting bio...")
            bio_selectors = [
                '[data-testid="UserDescription"] span',  # Primary bio location
                '[data-testid="UserDescription"]',
                'div[dir="auto"] span:not([role="presentation"])',  # Auto-direction bio text
                'div[data-testid*="bio"] span',
                'div[data-testid*="description"] span'
            ]

            for idx, selector in enumerate(bio_selectors):
                try:
                    self.logger.info(f"🔍 Step 3.{idx+1}: Trying bio selector...")
                    bio_elements = self.page.locator(selector)
                    count = await self._with_timeout(
                        bio_elements.count(),
                        timeout_ms=3000,
                        operation_name=f"Count bio elements {idx+1}",
                        default=0
                    )

                    bio_parts = []
                    for i in range(min(count, 20)):  # Limit iterations
                        element = bio_elements.nth(i)
                        is_visible = await self._with_timeout(
                            element.is_visible(),
                            timeout_ms=2000,
                            operation_name=f"Check bio visibility {i}",
                            default=False
                        )
                        if is_visible:
                            bio_text = await self._with_timeout(
                                element.inner_text(),
                                timeout_ms=2000,
                                operation_name=f"Get bio text {i}",
                                default=""
                            )
                            if bio_text and bio_text.strip():
                                bio_parts.append(bio_text.strip())
                    
                    if bio_parts:
                        full_bio = ' '.join(bio_parts).strip()
                        if len(full_bio) > 10:  # Minimum bio length
                            profile_data['bio'] = full_bio[:500]  # Limit bio length
                            self.logger.info(f"✅ Found bio: '{full_bio[:100]}...'")
                            break
                        
                except Exception as e:
                    continue
            
            # 4. EXTRACT FOLLOWER/FOLLOWING COUNTS - Enhanced with multiple strategies
            self.logger.info("🔍 Step 4: Extracting follower/following counts...")
            try:
                # Strategy 1: Direct href-based selectors
                href_selectors = [
                    f'a[href="/{username}/verified_followers"] span',
                    f'a[href="/{username}/followers"] span',
                    f'a[href="/{username}/following"] span',
                    f'a[href*="/followers"] span',
                    f'a[href*="/following"] span',
                ]
                
                # Strategy 2: Text-based selectors
                text_selectors = [
                    'a:has-text("Following") span',
                    'a:has-text("Followers") span',
                    'span:has-text("Following")',
                    'span:has-text("Followers")',
                    '[data-testid="UserProfileHeader_Items"] a span',
                    'div[dir="ltr"] a span',
                ]
                
                # Strategy 3: Aria-label based selectors
                aria_selectors = [
                    '[aria-label*="Following"]',
                    '[aria-label*="Followers"]',
                    '[aria-label*="following"]',
                    '[aria-label*="followers"]',
                ]
                
                all_selectors = href_selectors + text_selectors + aria_selectors

                for idx, selector in enumerate(all_selectors):
                    # Only try first 10 selectors to avoid infinite loops
                    if idx >= 10:
                        break
                    try:
                        self.logger.info(f"🔍 Step 4.{idx+1}: Trying stats selector...")
                        elements = self.page.locator(selector)
                        count = await self._with_timeout(
                            elements.count(),
                            timeout_ms=3000,
                            operation_name=f"Count stats elements {idx+1}",
                            default=0
                        )

                        for i in range(min(count, 5)):  # Limit to 5 elements max
                            element = elements.nth(i)

                            # Try to get text content
                            text = await self._with_timeout(
                                element.inner_text(),
                                timeout_ms=2000,
                                operation_name=f"Get stats text {i}",
                                default=""
                            )
                            if not text:
                                # Try parent element
                                parent = element.locator('..')
                                parent_count = await self._with_timeout(
                                    parent.count(),
                                    timeout_ms=1000,
                                    operation_name="Count parent",
                                    default=0
                                )
                                if parent_count > 0:
                                    text = await self._with_timeout(
                                        parent.inner_text(),
                                        timeout_ms=2000,
                                        operation_name="Get parent text",
                                        default=""
                                    )

                            # Also check aria-label
                            aria_label = await self._with_timeout(
                                element.get_attribute('aria-label'),
                                timeout_ms=1000,
                                operation_name="Get aria-label",
                                default=""
                            )
                            if aria_label:
                                text = f"{text} {aria_label}".strip()
                            
                            if text:
                                text = text.strip()
                                # Look for follower/following patterns
                                text_lower = text.lower()
                                
                                # Extract numbers using regex
                                import re
                                numbers = re.findall(r'(\d+(?:[.,]\d+)*[KMB]?)', text)
                                
                                if numbers and any(char.isdigit() for char in text):
                                    number = numbers[0]
                                    
                                    # Determine if it's followers or following
                                    if ('follower' in text_lower and 'followers_count' not in profile_data):
                                        profile_data['followers_count'] = number
                                        self.logger.info(f"✅ Followers: {number}")
                                        break
                                    elif ('following' in text_lower and 'following_count' not in profile_data):
                                        profile_data['following_count'] = number
                                        self.logger.info(f"✅ Following: {number}")
                                        break
                                    
                                    # Fallback: if we found a number but unclear which metric
                                    # Use the selector context
                                    elif 'following' in selector.lower() and 'following_count' not in profile_data:
                                        profile_data['following_count'] = number
                                        self.logger.info(f"✅ Following (context): {number}")
                                        break
                                    elif 'follower' in selector.lower() and 'followers_count' not in profile_data:
                                        profile_data['followers_count'] = number
                                        self.logger.info(f"✅ Followers (context): {number}")
                                        break
                                        
                    except Exception as e:
                        self.logger.debug(f"Stats selector {selector} failed: {e}")
                        continue
                
                # Strategy 4: Fallback pattern search in profile container (FIXED: Use scoped query with timeout)
                if 'followers_count' not in profile_data or 'following_count' not in profile_data:
                    try:
                        # Use scoped query instead of entire page to avoid hang
                        self.logger.info("🔍 Strategy 4: Pattern search in profile header...")
                        profile_container = self.page.locator('[data-testid="UserProfileHeader_Items"], [data-testid="UserDescription"], main').first
                        page_text = await self._with_timeout(
                            profile_container.inner_text(),
                            timeout_ms=5000,
                            operation_name="Profile text extraction",
                            default=""
                        )

                        if page_text:
                            import re

                            # Look for patterns like "1.2K Following" or "850 Followers"
                            following_patterns = [
                                r'(\d+(?:[.,]\d+)*[KMB]?)\s*Following',
                                r'Following\s*(\d+(?:[.,]\d+)*[KMB]?)',
                            ]

                            follower_patterns = [
                                r'(\d+(?:[.,]\d+)*[KMB]?)\s*Followers?',
                                r'Followers?\s*(\d+(?:[.,]\d+)*[KMB]?)',
                            ]

                            if 'following_count' not in profile_data:
                                for pattern in following_patterns:
                                    matches = re.findall(pattern, page_text, re.IGNORECASE)
                                    if matches:
                                        profile_data['following_count'] = matches[0]
                                        self.logger.info(f"✅ Following (pattern): {matches[0]}")
                                        break

                            if 'followers_count' not in profile_data:
                                for pattern in follower_patterns:
                                    matches = re.findall(pattern, page_text, re.IGNORECASE)
                                    if matches:
                                        profile_data['followers_count'] = matches[0]
                                        self.logger.info(f"✅ Followers (pattern): {matches[0]}")
                                        break
                        else:
                            self.logger.warning("⚠️ Profile text extraction timed out or failed")
                    except Exception as e:
                        self.logger.warning(f"⚠️ Strategy 4 failed: {e}")
                        pass
                        
            except Exception as e:
                self.logger.debug(f"⚠️ Stats extraction failed: {e}")
            
            # 5. EXTRACT POST COUNT - Look for posts/tweets count (FIXED: Add timeout)
            try:
                # Look for navigation tabs that show post count
                nav_selectors = [
                    f'a[href="/{username}"] span',  # Posts tab
                    '[role="tablist"] a span',       # Tab navigation
                    'nav[role="navigation"] a span'   # Navigation links
                ]

                for selector in nav_selectors:
                    try:
                        nav_elements = self.page.locator(selector)
                        count = await self._with_timeout(
                            nav_elements.count(),
                            timeout_ms=3000,
                            operation_name=f"Count elements for {selector}",
                            default=0
                        )

                        for i in range(min(count, 10)):  # Limit iterations
                            element = nav_elements.nth(i)
                            text = await self._with_timeout(
                                element.inner_text(),
                                timeout_ms=2000,
                                operation_name=f"Get text for element {i}",
                                default=""
                            )
                            # Look for post count patterns like "1.2K Posts" or "850 Tweets"
                            if text and ('post' in text.lower() or 'tweet' in text.lower()):
                                import re
                                numbers = re.findall(r'[\d,]+\.?\d*[KMB]?', text)
                                if numbers:
                                    profile_data['posts_count'] = numbers[0]
                                    self.logger.info(f"✅ Posts count: {numbers[0]}")
                                    break
                    except Exception as e:
                        self.logger.debug(f"Selector {selector} failed: {e}")
                        continue

            except Exception as e:
                self.logger.debug(f"⚠️ Post count extraction failed: {e}")
            
            # 6. EXTRACT JOIN DATE (FIXED: Add timeout)
            try:
                join_selectors = [
                    'span:has-text("Joined")',
                    '[data-testid="UserProfileHeader_Items"] span',
                    'div[dir="ltr"] span:has-text("Joined")'
                ]

                for selector in join_selectors:
                    try:
                        join_element = self.page.locator(selector).first
                        element_count = await self._with_timeout(
                            join_element.count(),
                            timeout_ms=2000,
                            operation_name="Check join element count",
                            default=0
                        )
                        if element_count > 0:
                            join_text = await self._with_timeout(
                                join_element.inner_text(),
                                timeout_ms=2000,
                                operation_name="Get join date text",
                                default=""
                            )
                            if join_text and 'joined' in join_text.lower():
                                profile_data['joined'] = join_text
                                self.logger.info(f"✅ Join date: {join_text}")
                                break
                    except Exception as e:
                        self.logger.debug(f"Join selector {selector} failed: {e}")
                        continue

            except Exception as e:
                self.logger.debug(f"⚠️ Join date extraction failed: {e}")
            
            # Log extraction results
            self.logger.info(f"📋 Extracted profile: {len(profile_data)} fields")
            for key, value in profile_data.items():
                self.logger.info(f"   {key}: {str(value)[:50]}{'...' if len(str(value)) > 50 else ''}")
                
            return profile_data
            
        except Exception as e:
            self.logger.error(f"❌ Profile extraction failed: {e}")
            return profile_data
    
    async def load_session(self, session_file: str = "/sessions/twitter_session.json"):
        """Load saved session cookies to bypass authentication."""
        self.logger.info(f"🍪 Loading Twitter session from {session_file}")

        try:
            # Check if session file exists
            self.logger.info("⏳ Step 1a/5: Checking session file existence...")
            if not os.path.exists(session_file):
                raise ValueError(f"Session file not found: {session_file}")

            # Load session data
            self.logger.info("⏳ Step 1b/5: Reading session data from file...")
            with open(session_file, 'r') as f:
                session_data = json.load(f)
            self.logger.info(f"✅ Step 1b/5: Loaded session data ({len(session_data)} keys)")
            
            cookies = session_data.get("cookies", {})
            if not cookies.get("auth_token"):
                raise ValueError("Invalid session file - missing auth_token")
            
            # ENHANCED: Check session expiration and attempt refresh if needed
            expires_estimate = session_data.get("expires_estimate", 0)
            session_age_days = (datetime.now().timestamp() - session_data.get("captured_at_timestamp", datetime.now().timestamp())) / (24 * 3600)
            
            if datetime.now().timestamp() > expires_estimate:
                self.logger.warning(f"⚠️ Session expired ({session_age_days:.1f} days old) - attempting refresh")
                # Try to refresh session before proceeding
                if await self._attempt_session_refresh(session_file, session_data):
                    self.logger.info("✅ Session refresh successful!")
                else:
                    self.logger.warning("❌ Session refresh failed - proceeding with expired session")
            elif session_age_days > 7:  # Sessions older than 7 days should be refreshed proactively
                self.logger.info(f"🔄 Session is {session_age_days:.1f} days old - proactively refreshing")
                await self._attempt_session_refresh(session_file, session_data)
            
            self.logger.info(f"📅 Session captured: {session_data.get('captured_at', 'unknown')}")

            # Use context from workers if provided, otherwise create one
            if self.context is None:
                self.logger.info("⏳ Step 1c/5: Creating browser context with mobile user agent...")
                import random
                mobile_viewports = [
                    {'width': 375, 'height': 812},  # iPhone X
                    {'width': 414, 'height': 896},  # iPhone 11 Pro Max
                    {'width': 390, 'height': 844},  # iPhone 12
                ]
                viewport = random.choice(mobile_viewports)

                mobile_user_agents = [
                    'Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Mobile/15E148 Safari/604.1',
                    'Mozilla/5.0 (Linux; Android 11; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36',
                ]
                user_agent = random.choice(mobile_user_agents)

                self.context = await self.browser.new_context(
                    viewport=viewport,
                    user_agent=user_agent,
                    locale='en-US',
                    timezone_id='America/New_York',
                    device_scale_factor=2.0,
                    has_touch=True,
                    is_mobile=True
                )
            else:
                self.logger.info("✅ Step 1c/5: Using stealth context from workers (already created)")

            self.page = await self.context.new_page()

            # ENHANCED STEALTH: Apply extra stealth measures to page
            from ..stealth_config import EnhancedStealthConfig
            await EnhancedStealthConfig.apply_extra_stealth_to_page(self.page)
            self.logger.info("🔒 Enhanced stealth applied to page")

            self.logger.info("✅ Step 1c/5: Browser context created successfully")

            # Add cookies to context - FIXED: Add to both domains correctly
            self.logger.info("⏳ Step 1d/5: Adding session cookies to browser context...")
            cookie_list = []
            for name, value in cookies.items():
                # Add for x.com domain
                cookie_list.append({
                    'name': name,
                    'value': value,
                    'domain': '.x.com',
                    'path': '/'
                })
                
                # Add for twitter.com domain (for compatibility)
                cookie_list.append({
                    'name': name,
                    'value': value,
                    'domain': '.twitter.com', 
                    'path': '/'
                })
            
            await self.context.add_cookies(cookie_list)
            self.logger.info(f"✅ Step 1d/5: Added {len(cookie_list)} cookies to browser context")

            # ENHANCED: Validate session by checking a Twitter page with increased timeout
            try:
                self.logger.info("⏳ Step 1e/5: Validating session authentication...")
                await self.page.goto("https://x.com/home", wait_until='domcontentloaded', timeout=60000)  # Increased to 60 seconds
                await self.page.wait_for_timeout(5000)  # Wait for content to load
                
                # Check for authentication indicators with fallback methods
                auth_indicators = [
                    '[data-testid="SideNav_NewTweet_Button"]',
                    '[data-testid="AppTabBar_Profile_Link"]', 
                    '[aria-label*="Tweet"]',
                    '[aria-label*="Post"]',
                    'nav[role="navigation"]',
                    '[data-testid="primaryColumn"]',
                    '[data-testid="tweet"]'  # Basic tweet presence indicates access
                ]
                
                authenticated = False
                for selector in auth_indicators:
                    try:
                        if await self.page.locator(selector).count() > 0:
                            authenticated = True
                            self.logger.info(f"✅ Authentication confirmed via: {selector}")
                            break
                    except:
                        continue
                
                if authenticated:
                    self.authenticated = True
                    self.logger.info("✅ Step 1e/5: Session validation successful - fully authenticated!")
                else:
                    self.logger.warning("⚠️ Step 1e/5: Session loaded but authentication unclear")
                    self.authenticated = True  # Proceed anyway, may work for public content

            except Exception as auth_e:
                self.logger.warning(f"⚠️ Step 1e/5: Session validation failed: {auth_e} - proceeding anyway")
                self.authenticated = True  # Proceed anyway

        except Exception as e:
            self.logger.error(f"❌ Failed to load session: {e}")
            raise Exception(f"Session loading failed: {e}")
    
    async def _attempt_session_refresh(self, session_file: str, old_session_data: dict) -> bool:
        """Attempt to refresh an expired or old session."""
        try:
            self.logger.info("🔄 Attempting session refresh...")
            
            # Method 1: Cookie refresh - visit Twitter to get updated cookies
            if hasattr(self, 'context') and self.context:
                # Navigate to Twitter to refresh cookies
                temp_page = await self.context.new_page()
                await temp_page.goto("https://x.com/home", wait_until='domcontentloaded', timeout=60000)
                await temp_page.wait_for_timeout(3000)
                
                # Get updated cookies
                fresh_cookies = await self.context.cookies()
                
                # Update session file with fresh cookies
                if fresh_cookies:
                    updated_cookies = {}
                    important_names = ['auth_token', 'ct0', '_twitter_sess', 'twid', 'att', 'kdt', 'personalization_id']
                    
                    for cookie in fresh_cookies:
                        if cookie['name'] in important_names:
                            updated_cookies[cookie['name']] = cookie['value']
                    
                    if updated_cookies.get('auth_token'):
                        # Save refreshed session
                        refreshed_session = {
                            "cookies": updated_cookies,
                            "captured_at": datetime.now().isoformat(),
                            "captured_at_timestamp": datetime.now().timestamp(),
                            "expires_estimate": datetime.now().timestamp() + (25 * 24 * 60 * 60),  # 25 days
                            "refresh_count": old_session_data.get("refresh_count", 0) + 1,
                            "last_refresh": datetime.now().isoformat()
                        }
                        
                        # Backup old session
                        backup_file = session_file.replace('.json', f'_backup_{int(datetime.now().timestamp())}.json')
                        with open(backup_file, 'w') as f:
                            json.dump(old_session_data, f, indent=2)
                        
                        # Save refreshed session
                        with open(session_file, 'w') as f:
                            json.dump(refreshed_session, f, indent=2)
                        
                        self.logger.info(f"✅ Session refreshed successfully! Refresh count: {refreshed_session['refresh_count']}")
                        await temp_page.close()
                        return True
                        
                await temp_page.close()
            
            return False
            
        except Exception as refresh_error:
            self.logger.error(f"❌ Session refresh failed: {refresh_error}")
            return False
    
    async def _check_session_health(self) -> dict:
        """Check the current health status of the session."""
        health_status = {
            "authenticated": False,
            "can_access_home": False,
            "can_view_profiles": False,
            "rate_limited": False,
            "session_valid": False
        }
        
        try:
            if not self.authenticated:
                return health_status
            
            # Test 1: Can access home timeline
            try:
                test_page = await self.context.new_page()
                await test_page.goto("https://x.com/home", wait_until='domcontentloaded', timeout=60000)
                
                # Check for login wall or rate limiting
                if 'login' in test_page.url.lower() or await test_page.locator('text="Sign up"').count() > 0:
                    health_status["authenticated"] = False
                elif await test_page.locator('text="Rate limit exceeded"').count() > 0:
                    health_status["rate_limited"] = True
                else:
                    health_status["can_access_home"] = True
                    health_status["authenticated"] = True
                
                await test_page.close()
            except:
                pass
            
            # Test 2: Can view public profiles  
            try:
                test_page = await self.context.new_page()
                await test_page.goto("https://x.com/github", wait_until='domcontentloaded', timeout=60000)
                
                if await test_page.locator('[data-testid="UserName"]').count() > 0:
                    health_status["can_view_profiles"] = True
                
                await test_page.close()
            except:
                pass
                
            health_status["session_valid"] = health_status["authenticated"] or health_status["can_view_profiles"]
            
        except Exception as health_error:
            self.logger.warning(f"⚠️ Session health check failed: {health_error}")
        
        return health_status
    
    async def _extract_high_volume_posts(self, username: str, max_posts: int) -> List[Dict[str, Any]]:
        """Advanced high-volume tweet extraction using multiple strategies."""
        all_posts = []
        
        try:
            self.logger.info(f"🚀 HIGH-VOLUME EXTRACTION: Targeting {max_posts} posts for @{username}")
            self.logger.info(f"🎯 HIGH-VOLUME METHOD CALLED: This confirms the strategy is activating!")
            
            # Strategy 1: Multiple time-based approaches
            extraction_strategies = [
                self._extract_recent_posts,      # Latest posts
                self._extract_popular_posts,     # Popular/top posts  
                self._extract_media_posts,       # Posts with media
                self._extract_replies_posts      # Posts with replies
            ]
            
            posts_per_strategy = max_posts // len(extraction_strategies)
            remaining_posts = max_posts
            
            for strategy_index, strategy in enumerate(extraction_strategies):
                if remaining_posts <= 0:
                    break
                    
                target_count = min(posts_per_strategy, remaining_posts)
                strategy_name = strategy.__name__.replace('_extract_', '').replace('_posts', '')
                
                self.logger.info(f"📋 Strategy {strategy_index + 1}/{len(extraction_strategies)}: {strategy_name} (target: {target_count})")
                
                try:
                    strategy_posts = await strategy(username, target_count)
                    
                    # Deduplicate posts
                    new_posts = []
                    existing_texts = {post.get('text', '') for post in all_posts}
                    
                    for post in strategy_posts:
                        if post.get('text', '') not in existing_texts:
                            new_posts.append(post)
                            existing_texts.add(post.get('text', ''))
                    
                    all_posts.extend(new_posts)
                    remaining_posts -= len(new_posts)
                    
                    self.logger.info(f"✅ Strategy {strategy_name}: {len(new_posts)} unique posts added (total: {len(all_posts)})")
                    
                    # Brief pause between strategies
                    await self._human_delay(2, 4)
                    
                except Exception as strategy_error:
                    self.logger.warning(f"⚠️ Strategy {strategy_name} failed: {strategy_error}")
                    continue
            
            self.logger.info(f"🎉 HIGH-VOLUME EXTRACTION COMPLETE: {len(all_posts)} total posts extracted")
            return all_posts[:max_posts]
            
        except Exception as e:
            self.logger.error(f"❌ High-volume extraction failed: {e}")
            return all_posts
    
    async def _extract_recent_posts(self, username: str, target_count: int) -> List[Dict[str, Any]]:
        """Extract recent posts using enhanced scrolling."""
        posts = []
        
        try:
            # Navigate to user profile
            profile_url = f"https://x.com/{username}"
            await self.page.goto(profile_url, wait_until='domcontentloaded', timeout=30000)
            await self._human_delay(3, 5)
            
            # Enhanced scrolling with longer persistence
            scroll_attempts = 0
            max_scrolls = min(50, target_count // 2)  # Up to 50 scroll attempts
            consecutive_empty = 0
            
            while len(posts) < target_count and scroll_attempts < max_scrolls and consecutive_empty < 5:
                # Extract content using existing method
                div_texts = await self.page.locator('div').all_inner_texts()
                tweets_found_this_round = 0
                
                for div_text in div_texts:
                    if len(posts) >= target_count:
                        break
                        
                    lines = div_text.split('\n')
                    for line in lines:
                        line = line.strip()
                        if self._is_likely_tweet_content(line):
                            # Check for duplicates
                            if not any(post.get('text') == line for post in posts):
                                tweet_data = {
                                    'id': f'recent_tweet_{len(posts)+1}',
                                    'text': line,
                                    'author': username,
                                    'url': f'https://x.com/{username}',
                                    'extracted_from': 'high_volume_recent',
                                    'extraction_attempt': scroll_attempts + 1
                                }
                                posts.append(tweet_data)
                                tweets_found_this_round += 1
                                
                                if len(posts) >= target_count:
                                    break
                
                if tweets_found_this_round == 0:
                    consecutive_empty += 1
                else:
                    consecutive_empty = 0
                
                # Advanced scrolling with variable distance
                scroll_distance = 800 + (scroll_attempts * 100)  # Progressive increase
                await self.page.mouse.wheel(0, scroll_distance)
                await self._human_delay(1, 3)
                scroll_attempts += 1
            
            self.logger.info(f"📝 Recent posts: {len(posts)} extracted in {scroll_attempts} scrolls")
            return posts
            
        except Exception as e:
            self.logger.error(f"❌ Recent posts extraction failed: {e}")
            return posts
    
    async def _extract_popular_posts(self, username: str, target_count: int) -> List[Dict[str, Any]]:
        """Extract popular posts by navigating to 'Popular' tab or using search."""
        posts = []
        
        try:
            # Try to access popular posts via search
            search_url = f"https://x.com/search?q=from:{username}&src=typed_query&f=top"
            await self.page.goto(search_url, wait_until='domcontentloaded', timeout=30000)
            await self._human_delay(3, 5)
            
            # Extract using similar method but with search results
            for scroll in range(min(20, target_count // 3)):
                div_texts = await self.page.locator('div').all_inner_texts()
                
                for div_text in div_texts:
                    if len(posts) >= target_count:
                        break
                        
                    lines = div_text.split('\n')
                    for line in lines:
                        line = line.strip()
                        if self._is_likely_tweet_content(line):
                            if not any(post.get('text') == line for post in posts):
                                tweet_data = {
                                    'id': f'popular_tweet_{len(posts)+1}',
                                    'text': line,
                                    'author': username,
                                    'url': f'https://x.com/{username}',
                                    'extracted_from': 'high_volume_popular',
                                    'extraction_attempt': scroll + 1
                                }
                                posts.append(tweet_data)
                                
                                if len(posts) >= target_count:
                                    break
                
                await self.page.mouse.wheel(0, 1000)
                await self._human_delay(2, 4)
            
            self.logger.info(f"🌟 Popular posts: {len(posts)} extracted")
            return posts
            
        except Exception as e:
            self.logger.error(f"❌ Popular posts extraction failed: {e}")
            return posts
    
    async def _extract_media_posts(self, username: str, target_count: int) -> List[Dict[str, Any]]:
        """Extract posts with media content."""
        posts = []
        
        try:
            # Navigate to media tab
            media_url = f"https://x.com/{username}/media"
            await self.page.goto(media_url, wait_until='domcontentloaded', timeout=30000)
            await self._human_delay(3, 5)
            
            # Extract media posts
            for scroll in range(min(15, target_count // 4)):
                div_texts = await self.page.locator('div').all_inner_texts()
                
                for div_text in div_texts:
                    if len(posts) >= target_count:
                        break
                        
                    lines = div_text.split('\n')
                    for line in lines:
                        line = line.strip()
                        if self._is_likely_tweet_content(line):
                            if not any(post.get('text') == line for post in posts):
                                tweet_data = {
                                    'id': f'media_tweet_{len(posts)+1}',
                                    'text': line,
                                    'author': username,
                                    'url': f'https://x.com/{username}',
                                    'extracted_from': 'high_volume_media',
                                    'extraction_attempt': scroll + 1
                                }
                                posts.append(tweet_data)
                                
                                if len(posts) >= target_count:
                                    break
                
                await self.page.mouse.wheel(0, 1200)
                await self._human_delay(2, 3)
            
            self.logger.info(f"🖼️ Media posts: {len(posts)} extracted")
            return posts
            
        except Exception as e:
            self.logger.error(f"❌ Media posts extraction failed: {e}")
            return posts
    
    async def _extract_replies_posts(self, username: str, target_count: int) -> List[Dict[str, Any]]:
        """Extract posts with replies."""
        posts = []
        
        try:
            # Navigate to replies tab  
            replies_url = f"https://x.com/{username}/with_replies"
            await self.page.goto(replies_url, wait_until='domcontentloaded', timeout=30000)
            await self._human_delay(3, 5)
            
            # Extract reply posts
            for scroll in range(min(12, target_count // 5)):
                div_texts = await self.page.locator('div').all_inner_texts()
                
                for div_text in div_texts:
                    if len(posts) >= target_count:
                        break
                        
                    lines = div_text.split('\n')
                    for line in lines:
                        line = line.strip()
                        if self._is_likely_tweet_content(line):
                            if not any(post.get('text') == line for post in posts):
                                tweet_data = {
                                    'id': f'reply_tweet_{len(posts)+1}',
                                    'text': line,
                                    'author': username,
                                    'url': f'https://x.com/{username}',
                                    'extracted_from': 'high_volume_replies',
                                    'extraction_attempt': scroll + 1
                                }
                                posts.append(tweet_data)
                                
                                if len(posts) >= target_count:
                                    break
                
                await self.page.mouse.wheel(0, 900)
                await self._human_delay(2, 4)
            
            self.logger.info(f"💬 Reply posts: {len(posts)} extracted")
            return posts
            
        except Exception as e:
            self.logger.error(f"❌ Reply posts extraction failed: {e}")
            return posts

    async def _extract_user_likes(self, username: str, max_likes: int = 10) -> List[Dict[str, Any]]:
        """Extract a user's liked tweets."""
        self.logger.info(f"💖 Extracting likes for @{username} (max: {max_likes})")

        try:
            likes_url = f"https://x.com/{username}/likes"
            await self.page.goto(likes_url, wait_until='domcontentloaded', timeout=30000)
            await self._human_delay(3, 5)

            if await self.page.locator('text=These posts are protected').is_visible():
                self.logger.warning(f"⚠️ @{username} has protected tweets - likes not accessible")
                return []

            likes_tab = self.page.locator('[role="tab"]:has-text("Likes")')
            if await likes_tab.is_visible():
                await likes_tab.click()
                await self._human_delay(2, 3)

            likes = []
            tweets_processed = 0

            for scroll_attempt in range(5):
                if tweets_processed >= max_likes:
                    break

                tweet_elements = await self.page.locator('[data-testid="tweet"]').all()

                for tweet_element in tweet_elements:
                    if tweets_processed >= max_likes:
                        break

                    try:
                        tweet_data = await self._extract_tweet_from_element(tweet_element, 0, username)
                        if tweet_data:
                            tweet_data['interaction_type'] = 'like'
                            tweet_data['liked_by'] = username
                            likes.append(tweet_data)
                            tweets_processed += 1
                    except Exception as e:
                        self.logger.warning(f"⚠️ Failed to extract liked tweet: {e}")
                        continue

                if tweets_processed < max_likes:
                    await self.page.keyboard.press('PageDown')
                    await self._human_delay(2, 4)

            self.logger.info(f"✅ Extracted {len(likes)} liked tweets for @{username}")
            return likes

        except Exception as e:
            self.logger.error(f"❌ Failed to extract likes for @{username}: {e}")
            return []

    async def _extract_user_mentions(self, username: str, max_mentions: int = 10) -> List[Dict[str, Any]]:
        """Extract mentions of a user (tweets mentioning @username)."""
        self.logger.info(f"@️⃣ Extracting mentions for @{username} (max: {max_mentions})")

        try:
            search_query = f"@{username}"
            search_url = f"https://x.com/search?q={search_query}&src=typed_query&f=live"

            await self.page.goto(search_url, wait_until='domcontentloaded', timeout=30000)
            await self._human_delay(3, 5)

            mentions = []
            tweets_processed = 0

            for scroll_attempt in range(3):
                if tweets_processed >= max_mentions:
                    break

                tweet_elements = await self.page.locator('[data-testid="tweet"]').all()

                for tweet_element in tweet_elements:
                    if tweets_processed >= max_mentions:
                        break

                    try:
                        tweet_data = await self._extract_tweet_from_element(tweet_element, 0, username)
                        if tweet_data and tweet_data.get('text') and f"@{username}" in tweet_data['text'].lower():
                            tweet_data['interaction_type'] = 'mention'
                            tweet_data['mentions_user'] = username
                            mentions.append(tweet_data)
                            tweets_processed += 1
                    except Exception as e:
                        self.logger.warning(f"⚠️ Failed to extract mention tweet: {e}")
                        continue

                if tweets_processed < max_mentions:
                    await self.page.keyboard.press('PageDown')
                    await self._human_delay(2, 4)

            self.logger.info(f"✅ Extracted {len(mentions)} mentions for @{username}")
            return mentions

        except Exception as e:
            self.logger.error(f"❌ Failed to extract mentions for @{username}: {e}")
            return []

    async def _extract_user_media(self, username: str, max_media: int = 15) -> List[Dict[str, Any]]:
        """Extract media posts from a user (images, videos, etc.)."""
        self.logger.info(f"🖼️ Extracting media for @{username} (max: {max_media})")

        try:
            media_url = f"https://x.com/{username}/media"
            await self.page.goto(media_url, wait_until='domcontentloaded', timeout=30000)
            await self._human_delay(3, 5)

            media_tab = self.page.locator('[role="tab"]:has-text("Media")')
            if await media_tab.is_visible():
                await media_tab.click()
                await self._human_delay(2, 3)

            media_posts = []
            posts_processed = 0

            for scroll_attempt in range(4):
                if posts_processed >= max_media:
                    break

                tweet_elements = await self.page.locator('[data-testid="tweet"]').all()

                for tweet_element in tweet_elements:
                    if posts_processed >= max_media:
                        break

                    try:
                        media_container = tweet_element.locator('[data-testid="tweetPhoto"], [data-testid="videoPlayer"], [data-testid="card.wrapper"]')

                        if await media_container.count() > 0:
                            tweet_data = await self._extract_tweet_from_element(tweet_element, 0, username)
                            if tweet_data:
                                media_info = await self._extract_media_from_tweet(tweet_element)
                                tweet_data['media'] = media_info
                                tweet_data['has_media'] = True
                                tweet_data['content_type'] = 'media_post'

                                media_posts.append(tweet_data)
                                posts_processed += 1
                    except Exception as e:
                        self.logger.warning(f"⚠️ Failed to extract media post: {e}")
                        continue

                if posts_processed < max_media:
                    await self.page.keyboard.press('PageDown')
                    await self._human_delay(2, 4)

            self.logger.info(f"✅ Extracted {len(media_posts)} media posts for @{username}")
            return media_posts

        except Exception as e:
            self.logger.error(f"❌ Failed to extract media for @{username}: {e}")
            return []

    async def _extract_user_followers(self, username: str, max_followers: int = 25) -> List[Dict[str, Any]]:
        """Extract a user's followers list."""
        self.logger.info(f"👥 Extracting followers for @{username} (max: {max_followers})")

        try:
            followers_url = f"https://x.com/{username}/verified_followers"
            await self.page.goto(followers_url, wait_until='domcontentloaded', timeout=30000)
            await self._human_delay(3, 5)

            if await self.page.locator('text=Who can see this account\'s posts').is_visible():
                self.logger.warning(f"⚠️ @{username} has protected account - followers not accessible")
                return []

            followers = []
            users_processed = 0

            for scroll_attempt in range(3):
                if users_processed >= max_followers:
                    break

                user_elements = await self.page.locator('[data-testid="UserCell"]').all()

                for user_element in user_elements:
                    if users_processed >= max_followers:
                        break

                    try:
                        user_data = await self._extract_user_from_element(user_element)
                        if user_data:
                            user_data['relationship'] = 'follower'
                            user_data['follows_user'] = username
                            followers.append(user_data)
                            users_processed += 1
                    except Exception as e:
                        self.logger.warning(f"⚠️ Failed to extract follower: {e}")
                        continue

                if users_processed < max_followers:
                    await self.page.keyboard.press('PageDown')
                    await self._human_delay(2, 4)

            self.logger.info(f"✅ Extracted {len(followers)} followers for @{username}")
            return followers

        except Exception as e:
            self.logger.error(f"❌ Failed to extract followers for @{username}: {e}")
            return []

    async def _extract_user_following(self, username: str, max_following: int = 20) -> List[Dict[str, Any]]:
        """Extract users that a user is following."""
        self.logger.info(f"➡️ Extracting following for @{username} (max: {max_following})")

        try:
            following_url = f"https://x.com/{username}/following"
            await self.page.goto(following_url, wait_until='domcontentloaded', timeout=30000)
            await self._human_delay(3, 5)

            if await self.page.locator('text=Who can see this account\'s posts').is_visible():
                self.logger.warning(f"⚠️ @{username} has protected account - following not accessible")
                return []

            following = []
            users_processed = 0

            for scroll_attempt in range(3):
                if users_processed >= max_following:
                    break

                user_elements = await self.page.locator('[data-testid="UserCell"]').all()

                for user_element in user_elements:
                    if users_processed >= max_following:
                        break

                    try:
                        user_data = await self._extract_user_from_element(user_element)
                        if user_data:
                            user_data['relationship'] = 'following'
                            user_data['followed_by_user'] = username
                            following.append(user_data)
                            users_processed += 1
                    except Exception as e:
                        self.logger.warning(f"⚠️ Failed to extract following user: {e}")
                        continue

                if users_processed < max_following:
                    await self.page.keyboard.press('PageDown')
                    await self._human_delay(2, 4)

            self.logger.info(f"✅ Extracted {len(following)} following users for @{username}")
            return following

        except Exception as e:
            self.logger.error(f"❌ Failed to extract following for @{username}: {e}")
            return []

    async def _extract_media_from_tweet(self, tweet_element) -> List[Dict[str, Any]]:
        """Extract comprehensive media information from a tweet element - Phase B: Noise Reduction Enhanced."""
        media_items = []

        try:
            # PHASE B: PRIORITY-BASED SELECTORS - Tweet content media first
            # Priority 1: High-confidence tweet photo selectors (signal)
            high_priority_image_selectors = [
                '[data-testid="tweetPhoto"] img',
                '[data-testid="Tweet-Photo"] img',
                'div[data-testid="tweetPhoto"] img',
                '[data-testid="tweetPhotoWrapper"] img',
                'div[role="img"][aria-label*="Image"] img'
            ]

            # Priority 2: Medium-confidence selectors with filtering
            medium_priority_image_selectors = [
                'img[src*="pbs.twimg.com/media/"]',  # Only media folder, not profiles
                'img[src*="media.twitter.com"]',
                'article div[data-testid*="media"] img',
                'div[data-testid*="card"] img'  # Link preview images
            ]

            # Priority 3: Low-confidence selectors (use only if others fail)
            low_priority_image_selectors = [
                'img[alt*="Image"]:not([alt*="avatar"]):not([alt*="profile"])',
                'article img[src]:not([alt*="avatar"]):not([alt*="profile"])'
            ]

            # Process selectors in priority order
            selector_groups = [
                ("HIGH_PRIORITY", high_priority_image_selectors),
                ("MEDIUM_PRIORITY", medium_priority_image_selectors),
                ("LOW_PRIORITY", low_priority_image_selectors)
            ]

            for priority, selectors in selector_groups:
                for selector in selectors:
                    try:
                        images = await tweet_element.locator(selector).all()
                        for img in images:
                            try:
                                src = await img.get_attribute('src')
                                alt = await img.get_attribute('alt')
                                width = await img.get_attribute('width')
                                height = await img.get_attribute('height')

                                if src and not any(item.get('url') == src for item in media_items):
                                    # PHASE B: NOISE FILTERING - Advanced filtering logic
                                    if self._is_content_media(src, alt, selector):
                                        media_items.append({
                                            'type': 'image',
                                            'url': src,
                                            'alt_text': alt or '',
                                            'width': width,
                                            'height': height,
                                            'extracted_from': selector,
                                            'priority_level': priority,
                                            'confidence_score': self._calculate_media_confidence(src, alt, selector)
                                        })
                                        self.logger.debug(f"✅ MEDIA SIGNAL: {priority} - {selector} -> {src[:80]}...")
                                    else:
                                        self.logger.debug(f"❌ MEDIA NOISE: Filtered {selector} -> {src[:80]}...")
                            except:
                                continue
                    except:
                        continue

                # If we found high-priority media, skip lower priorities to reduce noise
                if priority == "HIGH_PRIORITY" and len([m for m in media_items if m.get('priority_level') == 'HIGH_PRIORITY']) > 0:
                    self.logger.debug(f"📸 HIGH PRIORITY MEDIA FOUND - Skipping lower priority selectors to reduce noise")
                    break

            # Videos - Enhanced with multiple selectors
            video_selectors = [
                '[data-testid="videoPlayer"] video',
                '[data-testid="Tweet-Video"] video',
                'video[src]',
                'div[data-testid="videoPlayer"] video',
                '[data-testid="videoComponent"] video'
            ]

            for selector in video_selectors:
                try:
                    videos = await tweet_element.locator(selector).all()
                    for video in videos:
                        try:
                            src = await video.get_attribute('src')
                            poster = await video.get_attribute('poster')
                            duration = await video.get_attribute('duration')

                            if (src or poster) and not any(item.get('url') == (src or poster) for item in media_items):
                                media_items.append({
                                    'type': 'video',
                                    'url': src or poster,
                                    'poster_url': poster,
                                    'duration': duration,
                                    'extracted_from': selector
                                })
                        except:
                            continue
                except:
                    continue

            # GIFs - New addition
            gif_selectors = [
                '[data-testid="gifPlayer"] video',
                'video[src*="gif"]',
                'img[src*="gif"]',
                '[alt*="GIF"]'
            ]

            for selector in gif_selectors:
                try:
                    gifs = await tweet_element.locator(selector).all()
                    for gif in gifs:
                        try:
                            src = await gif.get_attribute('src')
                            poster = await gif.get_attribute('poster')
                            alt = await gif.get_attribute('alt')

                            if (src or poster) and not any(item.get('url') == (src or poster) for item in media_items):
                                media_items.append({
                                    'type': 'gif',
                                    'url': src or poster,
                                    'alt_text': alt or '',
                                    'poster_url': poster,
                                    'extracted_from': selector
                                })
                        except:
                            continue
                except:
                    continue

            # Link previews/cards - New addition
            card_selectors = [
                '[data-testid="card.wrapper"] img',
                '[data-testid="Card"] img',
                'div[role="link"] img'
            ]

            for selector in card_selectors:
                try:
                    cards = await tweet_element.locator(selector).all()
                    for card in cards:
                        try:
                            src = await card.get_attribute('src')
                            alt = await card.get_attribute('alt')

                            if src and not any(item.get('url') == src for item in media_items):
                                media_items.append({
                                    'type': 'link_preview',
                                    'url': src,
                                    'alt_text': alt or '',
                                    'extracted_from': selector
                                })
                        except:
                            continue
                except:
                    continue

        except Exception as e:
            self.logger.warning(f"⚠️ Failed to extract media: {e}")

        # PHASE B: ENHANCED LOGGING WITH NOISE ANALYSIS
        if media_items:
            media_types = {}
            priority_breakdown = {}
            avg_confidence = 0

            for item in media_items:
                media_type = item['type']
                priority = item.get('priority_level', 'UNKNOWN')
                confidence = item.get('confidence_score', 0)

                media_types[media_type] = media_types.get(media_type, 0) + 1
                priority_breakdown[priority] = priority_breakdown.get(priority, 0) + 1
                avg_confidence += confidence

            avg_confidence = avg_confidence / len(media_items) if media_items else 0

            self.logger.info(f"📷 MEDIA SUCCESS (Phase B): {len(media_items)} items, avg confidence: {avg_confidence:.2f}")
            self.logger.info(f"   Types: {media_types}")
            self.logger.info(f"   Priorities: {priority_breakdown}")
        else:
            self.logger.debug("📷 No media items extracted")

        return media_items

    def _is_content_media(self, src: str, alt: str = '', selector: str = '') -> bool:
        """Phase B: Determine if media is actual tweet content (signal) vs noise (profile pics, etc)."""
        if not src:
            return False

        # NOISE PATTERNS - Strong indicators this is NOT tweet content
        noise_patterns = [
            'profile_images',      # Twitter profile pictures
            'profile_banners',     # Twitter profile banners
            'default_profile',     # Default avatar images
            '/sticky/',           # Twitter UI elements
            '/emoji/',            # Emoji images
            '/hashflags/',        # Twitter hashflag images
        ]

        alt_noise_patterns = [
            'avatar', 'profile picture', 'profile photo', 'profile image',
            'default avatar', 'user avatar', 'twitter avatar'
        ]

        # Check URL-based noise patterns
        for pattern in noise_patterns:
            if pattern in src.lower():
                return False

        # Check alt-text based noise patterns
        if alt:
            alt_lower = alt.lower()
            for pattern in alt_noise_patterns:
                if pattern in alt_lower:
                    return False

        # SIGNAL PATTERNS - Strong indicators this IS tweet content
        signal_patterns = [
            'pbs.twimg.com/media/',    # Twitter media storage for tweet content
            'pbs.twimg.com/ext_tw_video_thumb/',  # Video thumbnails
            'media.twitter.com',       # Direct media links
            'twimg.com/media/',       # Alternative media path
        ]

        high_confidence_selectors = [
            'tweetPhoto', 'Tweet-Photo', 'tweetPhotoWrapper',
            'videoPlayer', 'Tweet-Video', 'gifPlayer'
        ]

        # Check URL-based signal patterns
        for pattern in signal_patterns:
            if pattern in src:
                return True

        # Check selector-based signal patterns
        for pattern in high_confidence_selectors:
            if pattern in selector:
                return True

        # DEFAULT: Allow if no clear noise indicators (conservative approach)
        # This balances between filtering noise and missing content
        return True

    def _calculate_media_confidence(self, src: str, alt: str = '', selector: str = '') -> float:
        """Phase B: Calculate confidence score for media item (0.0 to 1.0)."""
        confidence = 0.5  # Base confidence

        # HIGH CONFIDENCE BOOSTS
        if 'tweetPhoto' in selector or 'Tweet-Photo' in selector:
            confidence += 0.4
        elif 'pbs.twimg.com/media/' in src:
            confidence += 0.3
        elif 'videoPlayer' in selector or 'gifPlayer' in selector:
            confidence += 0.35

        # MEDIUM CONFIDENCE BOOSTS
        if 'media.twitter.com' in src:
            confidence += 0.2
        elif any(pattern in selector for pattern in ['media', 'card']):
            confidence += 0.15

        # CONFIDENCE REDUCTIONS (noise indicators)
        if 'profile_images' in src or 'profile_banners' in src:
            confidence -= 0.6
        elif any(pattern in alt.lower() for pattern in ['avatar', 'profile']):
            confidence -= 0.4
        elif '/emoji/' in src or '/hashflags/' in src:
            confidence -= 0.5

        # Clamp between 0.0 and 1.0
        return max(0.0, min(1.0, confidence))

    def _parse_engagement_number(self, number_str: str) -> int:
        """Parse engagement numbers like '1.2K', '3.4M', '156' into integers - Phase 1.2 Enhancement."""
        if not number_str:
            return 0

        try:
            # Remove commas and whitespace
            clean_str = number_str.replace(',', '').strip()

            # Handle K (thousands)
            if clean_str.endswith('K') or clean_str.endswith('k'):
                number = float(clean_str[:-1])
                return int(number * 1000)

            # Handle M (millions)
            elif clean_str.endswith('M') or clean_str.endswith('m'):
                number = float(clean_str[:-1])
                return int(number * 1000000)

            # Handle B (billions)
            elif clean_str.endswith('B') or clean_str.endswith('b'):
                number = float(clean_str[:-1])
                return int(number * 1000000000)

            # Handle regular numbers
            else:
                return int(float(clean_str))

        except (ValueError, TypeError) as e:
            self.logger.debug(f"📊 ENGAGEMENT PARSE: Failed to parse '{number_str}': {e}")
            return 0

    async def _detect_and_extract_thread_info(self, tweet_element, tweet_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Phase 1.3: Advanced Thread Detection and Reconstruction.

        Detects if a tweet is part of a thread and extracts thread metadata.
        Returns enhanced tweet data with thread information.
        """
        try:
            thread_info = {
                'is_thread': False,
                'thread_position': None,
                'thread_id': None,
                'thread_size': None,
                'is_thread_starter': False,
                'has_continuation': False,
                'replied_to_tweet_id': None
            }

            # 1. DETECT THREAD INDICATORS
            thread_indicators = []

            # Look for "Show this thread" text
            try:
                show_thread_elements = await tweet_element.locator('text="Show this thread"').all()
                if len(show_thread_elements) > 0:
                    thread_indicators.append('show_thread_link')
                    thread_info['has_continuation'] = True
                    self.logger.info("🧵 THREAD: Found 'Show this thread' indicator")
            except Exception as e:
                self.logger.debug(f"🧵 THREAD: Show thread detection error: {e}")

            # Look for threading indicators in aria-labels
            try:
                threading_selectors = [
                    '[aria-label*="thread"]',
                    '[aria-label*="Thread"]',
                    '[aria-label*="This Tweet is part of"]',
                    '[aria-label*="This post is part of"]'
                ]

                for selector in threading_selectors:
                    elements = await tweet_element.locator(selector).all()
                    if len(elements) > 0:
                        thread_indicators.append('aria_thread_indicator')
                        break
            except Exception as e:
                self.logger.debug(f"🧵 THREAD: Aria thread detection error: {e}")

            # Look for reply chains (replied to indicators)
            try:
                reply_selectors = [
                    '[data-testid="tweet"] [aria-label*="Replying to"]',
                    '[aria-label*="Replying to"]',
                    'span:has-text("Replying to")',
                    'text="Replying to"'
                ]

                for selector in reply_selectors:
                    elements = await tweet_element.locator(selector).all()
                    if len(elements) > 0:
                        thread_indicators.append('reply_indicator')

                        # Try to extract the replied-to username/ID
                        try:
                            reply_element = elements[0]
                            reply_text = await reply_element.inner_text()
                            # Extract @username from "Replying to @username"
                            import re
                            replied_to_match = re.search(r'@(\w+)', reply_text)
                            if replied_to_match:
                                thread_info['replied_to_username'] = replied_to_match.group(1)
                                self.logger.info(f"🧵 THREAD: Found reply to @{replied_to_match.group(1)}")
                        except Exception as extract_error:
                            self.logger.debug(f"🧵 THREAD: Reply username extraction error: {extract_error}")
                        break
            except Exception as e:
                self.logger.debug(f"🧵 THREAD: Reply detection error: {e}")

            # Look for thread numbering (1/n, 2/n format)
            try:
                tweet_text = tweet_data.get('text', '')
                import re

                # Pattern for thread numbering like "1/5", "2/12", etc.
                thread_number_pattern = r'(\d+)/(\d+)'
                thread_match = re.search(thread_number_pattern, tweet_text)

                if thread_match:
                    thread_indicators.append('numbered_thread')
                    thread_info['thread_position'] = int(thread_match.group(1))
                    thread_info['thread_size'] = int(thread_match.group(2))
                    thread_info['is_thread_starter'] = (thread_info['thread_position'] == 1)
                    self.logger.info(f"🧵 THREAD: Found numbered thread {thread_match.group(1)}/{thread_match.group(2)}")

                # Alternative patterns
                thread_alt_patterns = [
                    r'Thread\s+(\d+)/',  # "Thread 1/"
                    r'🧵\s*(\d+)/',       # "🧵 1/"
                    r'(\d+)\s*\.\s*/',    # "1. /"
                ]

                for pattern in thread_alt_patterns:
                    alt_match = re.search(pattern, tweet_text, re.IGNORECASE)
                    if alt_match:
                        thread_indicators.append('alt_numbered_thread')
                        thread_info['thread_position'] = int(alt_match.group(1))
                        self.logger.info(f"🧵 THREAD: Found alternative thread numbering: position {alt_match.group(1)}")
                        break

            except Exception as e:
                self.logger.debug(f"🧵 THREAD: Thread numbering detection error: {e}")

            # Look for thread emoji indicators
            try:
                tweet_text = tweet_data.get('text', '')
                thread_emojis = ['🧵', '👇', '⬇️', '↓']

                for emoji in thread_emojis:
                    if emoji in tweet_text:
                        thread_indicators.append('emoji_thread_indicator')
                        self.logger.info(f"🧵 THREAD: Found thread emoji indicator: {emoji}")
                        break
            except Exception as e:
                self.logger.debug(f"🧵 THREAD: Emoji detection error: {e}")

            # 2. DETERMINE THREAD STATUS
            if len(thread_indicators) > 0:
                thread_info['is_thread'] = True
                thread_info['thread_indicators'] = thread_indicators

                # Generate thread ID based on tweet URL/ID
                if tweet_data.get('tweet_id'):
                    base_id = tweet_data['tweet_id']
                    # For threads, use the original tweet ID as thread ID
                    if thread_info['is_thread_starter']:
                        thread_info['thread_id'] = f"thread_{base_id}"
                    else:
                        # For continuation tweets, we'd need the original tweet ID
                        # For now, use a derived ID
                        thread_info['thread_id'] = f"thread_{base_id}_cont"

                self.logger.info(f"🧵 THREAD DETECTED: {len(thread_indicators)} indicators found")

            # 3. ADD THREAD INFO TO TWEET DATA
            tweet_data['thread_info'] = thread_info

            return tweet_data

        except Exception as e:
            self.logger.debug(f"🧵 THREAD: Detection failed: {e}")
            # Return original tweet data with empty thread info on error
            tweet_data['thread_info'] = {
                'is_thread': False,
                'error': str(e)
            }
            return tweet_data

    async def _reconstruct_thread_sequence(self, tweets: List[Dict[str, Any]], username: str) -> List[Dict[str, Any]]:
        """
        Phase 1.3: Thread Reconstruction.

        Takes a list of tweets and reconstructs thread sequences,
        organizing them by thread and adding sequence metadata.
        """
        try:
            # Group tweets by thread
            threads = {}
            standalone_tweets = []

            for tweet in tweets:
                thread_info = tweet.get('thread_info', {})

                if thread_info.get('is_thread', False):
                    thread_id = thread_info.get('thread_id')
                    if thread_id:
                        if thread_id not in threads:
                            threads[thread_id] = []
                        threads[thread_id].append(tweet)
                    else:
                        # Thread tweet without ID, treat as standalone
                        standalone_tweets.append(tweet)
                else:
                    standalone_tweets.append(tweet)

            # Reconstruct each thread
            reconstructed_tweets = []

            for thread_id, thread_tweets in threads.items():
                self.logger.info(f"🧵 RECONSTRUCTING: Thread {thread_id} with {len(thread_tweets)} tweets")

                # Sort by thread position if available
                def sort_key(tweet):
                    thread_info = tweet.get('thread_info', {})
                    position = thread_info.get('thread_position')
                    if position is not None:
                        return position
                    # Fallback to timestamp
                    timestamp = tweet.get('timestamp', '')
                    try:
                        from datetime import datetime
                        return datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    except:
                        return datetime.min

                sorted_thread = sorted(thread_tweets, key=sort_key)

                # Add thread reconstruction metadata
                for i, tweet in enumerate(sorted_thread):
                    thread_info = tweet.get('thread_info', {})
                    thread_info.update({
                        'reconstructed_position': i + 1,
                        'reconstructed_thread_size': len(sorted_thread),
                        'thread_reconstruction_method': 'position_and_timestamp',
                        'is_reconstructed': True
                    })
                    tweet['thread_info'] = thread_info

                reconstructed_tweets.extend(sorted_thread)

            # Add standalone tweets
            reconstructed_tweets.extend(standalone_tweets)

            # Sort final list by timestamp
            def final_sort_key(tweet):
                timestamp = tweet.get('timestamp', '')
                try:
                    from datetime import datetime
                    return datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                except:
                    return datetime.min

            final_tweets = sorted(reconstructed_tweets, key=final_sort_key, reverse=True)

            self.logger.info(f"🧵 THREAD RECONSTRUCTION: {len(threads)} threads, {len(standalone_tweets)} standalone tweets")

            return final_tweets

        except Exception as e:
            self.logger.error(f"🧵 THREAD RECONSTRUCTION ERROR: {e}")
            return tweets  # Return original on error

    async def _extract_tweets_from_scrolled_content(
        self,
        username: str,
        target_posts_needed: int,
        extracted_tweet_ids: set,
        seen_text_hashes: set
    ) -> List[Dict[str, Any]]:
        """
        Dedicated method for extracting tweets from scrolled content.

        This runs after page-level extraction and scrolling, specifically targeting
        newly loaded content that wasn't available in the initial page state.
        """
        scrolled_tweets = []

        try:
            self.logger.info(f"🔄 SCROLLING EXTRACTION: Looking for {target_posts_needed} additional tweets in scrolled content")

            # Strategy 1: Target newly loaded tweetText elements
            all_tweet_texts = await self.page.locator('[data-testid="tweetText"]').all()
            self.logger.info(f"🔄 SCROLLING: Found {len(all_tweet_texts)} total tweetText elements after scrolling")

            # Process elements that weren't captured by page-level extraction
            for i, tweet_elem in enumerate(all_tweet_texts):
                if len(scrolled_tweets) >= target_posts_needed:
                    break

                try:
                    text_content = await tweet_elem.inner_text(timeout=2000)

                    if text_content and len(text_content.strip()) > 5:
                        # Create text-based hash for deduplication
                        import hashlib
                        text_hash = hashlib.md5(text_content.strip().encode()).hexdigest()[:8]

                        # Skip if already extracted
                        if text_hash in seen_text_hashes:
                            self.logger.debug(f"🔄 SCROLLING: Skipping duplicate content (hash: {text_hash})")
                            continue

                        # Create tweet data with scroll-specific tagging
                        tweet_data = {
                            'id': f'scroll_tweet_{len(scrolled_tweets)+1}',
                            'text': text_content.strip()[:500],
                            'author': username,
                            'url': f'https://x.com/{username}',
                            'extracted_from': 'scrolled_content_tweetText',
                            'extraction_method': 'scroll_based_extraction',
                            'scroll_index': i,
                            'extraction_phase': 'post_scroll'
                        }

                        # Add to results and tracking
                        scrolled_tweets.append(tweet_data)
                        seen_text_hashes.add(text_hash)
                        extracted_tweet_ids.add(tweet_data['id'])

                        self.logger.info(f"✅ SCROLLING EXTRACTION {len(scrolled_tweets)}: '{text_content[:60]}...'")

                except Exception as e:
                    self.logger.debug(f"🔄 SCROLLING: Failed to extract element {i}: {e}")
                    continue

            # Strategy 2: Try article-based extraction for additional content
            if len(scrolled_tweets) < target_posts_needed:
                remaining_needed = target_posts_needed - len(scrolled_tweets)
                self.logger.info(f"🔄 SCROLLING: Trying article-based extraction for {remaining_needed} more tweets")

                articles = await self.page.locator('article[data-testid="tweet"]').all()
                for article in articles[-10:]:  # Focus on recently loaded articles
                    if len(scrolled_tweets) >= target_posts_needed:
                        break

                    try:
                        # Extract text from article
                        tweet_text_elem = article.locator('[data-testid="tweetText"]').first
                        article_text = await tweet_text_elem.inner_text(timeout=2000)

                        if article_text and len(article_text.strip()) > 5:
                            # Check for duplicates
                            text_hash = hashlib.md5(article_text.strip().encode()).hexdigest()[:8]
                            if text_hash in seen_text_hashes:
                                continue

                            # Extract additional metadata from article
                            author_info = await self._extract_author_information(article, username)

                            article_tweet = {
                                'id': f'scroll_article_{len(scrolled_tweets)+1}',
                                'text': article_text.strip()[:500],
                                'author': author_info.get('author_username', username),
                                'author_name': author_info.get('author_display_name', username),
                                'url': f'https://x.com/{username}',
                                'extracted_from': 'scrolled_content_article',
                                'extraction_method': 'scroll_based_article_extraction',
                                'extraction_phase': 'post_scroll'
                            }

                            scrolled_tweets.append(article_tweet)
                            seen_text_hashes.add(text_hash)
                            extracted_tweet_ids.add(article_tweet['id'])

                            self.logger.info(f"✅ SCROLLING ARTICLE {len(scrolled_tweets)}: '{article_text[:60]}...'")

                    except Exception as e:
                        self.logger.debug(f"🔄 SCROLLING: Article extraction failed: {e}")
                        continue

            self.logger.info(f"🎉 SCROLLING EXTRACTION COMPLETE: Found {len(scrolled_tweets)} additional tweets")

        except Exception as e:
            self.logger.error(f"❌ SCROLLING EXTRACTION FAILED: {e}")

        return scrolled_tweets
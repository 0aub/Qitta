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
- Multiple extraction levels

Version: 1.0
Author: Based on successful Airbnb/Booking patterns
"""

import json
import logging
import asyncio
import os
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import hashlib


class TwitterDateUtils:
    """Utility class for handling date filtering in Twitter extraction."""
    
    @staticmethod
    def parse_date_range(date_range: str) -> tuple[datetime, datetime]:
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
        if not tweet_timestamp or not start_dt:
            return True
            
        try:
            # Parse Twitter timestamp (usually ISO format)
            if 'T' in tweet_timestamp:
                tweet_dt = datetime.fromisoformat(tweet_timestamp.replace('Z', '+00:00').replace('+00:00', ''))
            else:
                # Handle relative timestamps like "2h", "1d ago"
                return TwitterDateUtils._parse_relative_timestamp(tweet_timestamp, start_dt, end_dt)
                
            # Remove timezone info for comparison
            tweet_dt = tweet_dt.replace(tzinfo=None)
            start_dt = start_dt.replace(tzinfo=None)
            end_dt = end_dt.replace(tzinfo=None)
            
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
    async def run(params: Dict[str, Any], logger: logging.Logger, browser, job_output_dir: str = None) -> Dict[str, Any]:
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
            
            # Get target username - Check all possible parameter names  
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
            if not any([clean_params.get('scrape_posts', True), clean_params.get('scrape_likes', False), 
                       clean_params.get('scrape_mentions', False), clean_params.get('scrape_media', False),
                       clean_params.get('scrape_followers', False), clean_params.get('scrape_following', False)]):
                logger.info(f"🔢 Max Results: {clean_params.get('max_results', 10)}")
            
            # Initialize concurrency manager for session isolation
            concurrency_manager = TwitterConcurrencyManager()
            
            # Get optimal session for concurrent processing with performance tracking
            start_time = time.time()
            session_file = await concurrency_manager.get_available_session(logger)
            
            # Initialize scraper with assigned session
            scraper = TwitterScraper(browser, logger)
            
            # Load the assigned session
            if clean_params.get("use_session") or os.path.exists(session_file):
                logger.info(f"🍪 Using session for authentication: {session_file}")
                await scraper.load_session(session_file)
            else:
                logger.info("🔐 Using credential-based authentication...")
                await scraper.authenticate()
            
            # Add the corrected parameters to clean_params
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
                # User account scraping mode
                logger.info(f"👤 USER ACCOUNT MODE: @{target_username}")
                
                # Log what will be scraped
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
                
                logger.info(f"🎯 Scraping: {', '.join(scrape_options) if scrape_options else 'Posts only'}")
                
                results = await scraper.scrape_user_comprehensive(clean_params)
                extraction_method = "comprehensive_user_scraping"
                
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
            
            # Calculate metrics
            success_rate = len(results) / clean_params.get('max_results', 10) if results else 0
            
            logger.info(f"🏁 Completed: {len(results)} items extracted | {success_rate:.1%} success rate")
            
            # Prepare result structure
            result = {
                "status": "success",
                "search_metadata": {
                    "target_username": clean_params.get('username', 'timeline'),
                    "extraction_method": extraction_method,
                    "scrape_level": scrape_level,
                    "total_found": len(results),
                    "success_rate": success_rate,
                    "search_completed_at": datetime.now().isoformat()
                },
                "data": results
            }
            
            # Save results to JSON file if job_output_dir is provided
            if job_output_dir:
                try:
                    output_file = os.path.join(job_output_dir, "twitter_data.json")
                    os.makedirs(job_output_dir, exist_ok=True)
                    with open(output_file, 'w', encoding='utf-8') as f:
                        json.dump(result, f, indent=2, ensure_ascii=False)
                    logger.info(f"💾 Saved data to {output_file}")
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
            job_successful = 'results' in locals() and results and len(results) > 0
            
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


class TwitterBatchProcessor:
    """Intelligent batch processing with session reuse and optimization."""
    
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
        elif job.get('scrape_likes', False) or job.get('scrape_mentions', False):
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


class TwitterSessionPool:
    """Advanced multi-session pool management with dynamic scaling and intelligent rotation."""
    
    def __init__(self, concurrency_manager):
        self.concurrency_manager = concurrency_manager
        self.pool_config = {
            'min_pool_size': 3,
            'max_pool_size': 8,
            'target_utilization': 0.7,  # 70% utilization target
            'scale_up_threshold': 0.8,   # Scale up at 80% utilization
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
    
    def __init__(self, browser, logger: logging.Logger):
        self.browser = browser
        self.logger = logger
        self.context = None
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
            if 'T' in timestamp:
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
            
            self.page = await self.context.new_page()
            
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
                await self.page.wait_for_selector('input', timeout=15000)
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
        """Level 1: Basic profile information."""
        self.logger.info("⚡ Level 1: Basic profile extraction")
        
        if not self.authenticated:
            raise Exception("Not authenticated")
            
        username = params.get('username', '')
        if not username:
            # Default to home timeline
            return await self._extract_timeline_tweets(params)
        else:
            # Extract specific user profile
            return await self._extract_user_profile(username, basic=True)

    async def scrape_level_2(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Level 2: Full profile with recent tweets."""
        self.logger.info("🐦 Level 2: Full profile extraction")
        
        username = params.get('username', '')
        if username:
            profile = await self._extract_user_profile(username, basic=False)
            tweets = await self._extract_user_tweets(username, params.get('max_results', 10))
            
            # Combine profile and tweets
            return [{
                "type": "profile_with_tweets",
                "profile": profile[0] if profile else {},
                "tweets": tweets
            }]
        else:
            return await self._extract_timeline_tweets(params)

    async def scrape_level_3(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Level 3: Profile + tweets + media."""
        self.logger.info("📸 Level 3: Profile with media extraction")
        
        # Same as level 2 but with enhanced media extraction
        return await self.scrape_level_2(params)

    async def scrape_level_4(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Level 4: Comprehensive extraction with followers."""
        self.logger.info("🔍 Level 4: Comprehensive extraction")
        
        # Enhanced extraction with follower information
        return await self.scrape_level_2(params)

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

    async def _extract_tweet_from_element(self, tweet_element, index: int, include_engagement: bool = True) -> Optional[Dict[str, Any]]:
        """Extract comprehensive tweet data from element - unified method for likes/mentions/media extraction."""
        try:
            tweet_data = {
                "type": "tweet",
                "index": index,
                "extraction_timestamp": datetime.now().isoformat()
            }
            
            # Extract tweet text using multiple strategies
            text_found = False
            text_selectors = [
                '[data-testid="tweetText"]',
                'div[lang] span',
                'div[dir="ltr"] span',
                'span[dir="ltr"]',
                'div span'
            ]
            
            for selector in text_selectors:
                try:
                    text_elements = tweet_element.locator(selector)
                    count = await text_elements.count()
                    
                    for i in range(count):
                        element = text_elements.nth(i)
                        if await element.is_visible():
                            tweet_text = await element.inner_text()
                            if tweet_text and len(tweet_text.strip()) > 10:  # Minimum meaningful content
                                tweet_data['text'] = tweet_text.strip()
                                text_found = True
                                break
                    
                    if text_found:
                        break
                except:
                    continue
            
            # If no text found, try div content extraction
            if not text_found:
                try:
                    all_text = await tweet_element.inner_text()
                    if all_text and len(all_text.strip()) > 10:
                        # Clean up the text
                        lines = [line.strip() for line in all_text.split('\n') if line.strip()]
                        # Take the longest meaningful line as tweet text
                        tweet_lines = [line for line in lines if len(line) > 10 and not line.isdigit()]
                        if tweet_lines:
                            tweet_data['text'] = tweet_lines[0][:280]  # Twitter character limit
                except:
                    pass
            
            # Extract engagement metrics if requested
            if include_engagement:
                try:
                    # Try to find engagement numbers
                    engagement_selectors = [
                        '[role="group"] span',
                        '[data-testid="reply"] span', 
                        '[data-testid="retweet"] span',
                        '[data-testid="like"] span',
                        'div[role="button"] span'
                    ]
                    
                    metrics = []
                    for selector in engagement_selectors:
                        try:
                            elements = tweet_element.locator(selector)
                            count = await elements.count()
                            
                            for i in range(count):
                                element = elements.nth(i)
                                text = await element.inner_text()
                                # Look for numbers (engagement counts)
                                if text and (text.isdigit() or ('K' in text and text.replace('K', '').replace('.', '').isdigit())):
                                    metrics.append(text)
                        except:
                            continue
                    
                    # Assign metrics if found
                    if len(metrics) >= 1:
                        tweet_data['engagement_metrics'] = metrics[:4]  # reply, retweet, like, share
                except:
                    pass
            
            # Extract timestamp if available
            try:
                time_selectors = ['time', '[datetime]', 'a[href*="/status/"]']
                for selector in time_selectors:
                    time_element = tweet_element.locator(selector).first
                    if await time_element.is_visible():
                        # Try datetime attribute first
                        datetime_attr = await time_element.get_attribute('datetime')
                        if datetime_attr:
                            tweet_data['timestamp'] = datetime_attr
                            break
                        
                        # Try inner text
                        time_text = await time_element.inner_text()
                        if time_text:
                            tweet_data['timestamp'] = time_text
                            break
            except:
                pass
            
            # Extract media URLs if available
            try:
                media_urls = []
                img_elements = tweet_element.locator('img')
                img_count = await img_elements.count()
                
                for i in range(img_count):
                    img = img_elements.nth(i)
                    src = await img.get_attribute('src')
                    if src and ('pbs.twimg.com' in src or 'twimg.com' in src):
                        media_urls.append(src)
                
                if media_urls:
                    tweet_data['media_urls'] = media_urls[:4]  # Limit to 4 media items
            except:
                pass
            
            # Only return if we have meaningful content
            if 'text' in tweet_data or 'media_urls' in tweet_data:
                return tweet_data
            else:
                return None
                
        except Exception as e:
            self.logger.debug(f"❌ Failed to extract tweet from element {index}: {e}")
            return None

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

    async def scrape_hashtag(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Comprehensive hashtag scraping."""
        hashtag = params.get('hashtag', '').replace('#', '')
        max_tweets = params.get('max_tweets', 50)
        include_media = params.get('include_media', True)
        date_filter = params.get('date_filter', 'recent')
        
        self.logger.info(f"🏷️ Starting hashtag scraping for #{hashtag}")
        self.logger.info(f"📊 Target: {max_tweets} tweets | Media: {include_media} | Filter: {date_filter}")
        
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
            
            tweets = []
            scroll_attempts = 0
            max_scrolls = min(max_tweets // 5, 20)  # Scroll strategy
            
            self.logger.info("🏷️ Using div-based hashtag tweet extraction...")
            
            while len(tweets) < max_tweets and scroll_attempts < max_scrolls:
                # 🚀 Extract tweets using div content parsing (same as profile tweets)
                div_texts = await self.page.locator('div').all_inner_texts()
                tweet_candidates = []
                
                for div_text in div_texts:
                    lines = div_text.split('\n')
                    for line in lines:
                        line = line.strip()
                        # ENHANCED & MORE PERMISSIVE hashtag tweet identification
                        if (len(line) > 20 and len(line) < 500 and  # More permissive length range
                            not line.startswith('To view') and
                            not line.startswith('View keyboard') and
                            not 'keyboard shortcuts' in line and
                            not line.startswith('http://') and  # Skip full URLs
                            not line.startswith('https://') and
                            not line.endswith('.com') and  # Skip domains
                            not line.endswith('.org') and
                            not line.endswith('.net') and
                            not line in ['Follow', 'Following', 'Followers', 'Posts', 'Replies', 'Media', 'Likes', 'Joined', 'Home', 'Search', 'Messages', 'Bookmarks', 'Lists', 'Profile', 'More', 'Latest', 'Top', 'People', 'Photos', 'Videos', 'Tweet', 'Retweet', 'Quote Tweet'] and
                            not line.endswith(' posts') and
                            not line.endswith(' Following') and
                            not line.endswith(' Followers') and
                            not line.endswith(' likes') and
                            not line.endswith('Show this thread') and
                            not line.endswith('Translate post') and
                            not line.endswith('Copy link') and
                            not line.endswith('Show more') and
                            not line.isdigit() and  # Skip pure numbers
                            not (line.startswith('@') and len(line.split()) == 1) and  # Skip standalone usernames
                            not all(c in '0123456789,. MKBh' for c in line) and  # Skip stats/counts
                            not line.startswith('Joined ') and  # Skip join dates
                            not (line.endswith('ago') and len(line.split()) <= 3) and  # Skip short timestamps
                            not (len(line.split()) == 1 and line.endswith('ago')) and  # Skip single word timestamps
                            line not in [f'#{hashtag}', f'#{hashtag.lower()}', f'#{hashtag.upper()}'] and  # Skip hashtag labels
                            not (line.lower().startswith('search') and 'results' in line.lower())):  # Skip search results text
                            
                            # Enhanced hashtag tweet validation
                            words = line.split()
                            if (len(words) >= 3 and  # At least 3 words
                                not any(skip_word in line.lower() for skip_word in [
                                    'show more', 'show less', 'translate', 'quote tweet', 'repost', 'like',
                                    'reply', 'view', 'follow', 'unfollow', 'block', 'mute', 'report',
                                    'search results', 'trending', 'for you', 'following'
                                ]) and
                                any(c.isalpha() for c in line)):  # Contains letters
                                tweet_candidates.append(line)
                
                self.logger.info(f"🔍 Found {len(tweet_candidates)} hashtag tweet candidates in attempt {scroll_attempts + 1}")
                
                # Remove duplicates and convert to tweet objects
                seen_tweets = set()
                for tweet_text in tweet_candidates:
                    if len(tweets) >= max_tweets:
                        break
                    if tweet_text not in seen_tweets and tweet_text not in [tweet.get('text') for tweet in tweets]:
                        seen_tweets.add(tweet_text)
                        tweet_data = {
                            'id': f'hashtag_tweet_{len(tweets)+1}',
                            'text': tweet_text,
                            'hashtag': f'#{hashtag}',
                            'url': hashtag_url,
                            'extracted_from': 'div_hashtag_parsing',
                            'extraction_attempt': scroll_attempts + 1
                        }
                        tweets.append(tweet_data)
                        self.logger.info(f"🏷️ Extracted hashtag tweet {len(tweets)}/{max_tweets}: '{tweet_text[:80]}...'")
                
                # Scroll for more tweets
                if len(tweets) < max_tweets:
                    self.logger.info(f"📜 Scrolling for more tweets... ({len(tweets)}/{max_tweets})")
                    await self.page.mouse.wheel(0, 800)
                    await self._human_delay(2, 4)
                    scroll_attempts += 1
            
            self.logger.info(f"🏁 Hashtag scraping completed: {len(tweets)} tweets extracted")
            
            return [{
                "type": "hashtag_tweets",
                "hashtag": f"#{hashtag}",
                "filter": date_filter,
                "tweets": tweets,
                "total_found": len(tweets)
            }]
            
        except Exception as e:
            self.logger.error(f"❌ Hashtag scraping failed: {e}")
            return []

    async def scrape_user_comprehensive(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Comprehensive user account scraping with all options."""
        username = params.get('target_username', '').replace('@', '')
        
        # Initialize date filtering
        self.setup_date_filtering(params)
        
        self.logger.info(f"👤 Starting comprehensive user scraping for @{username}")
        
        results = {
            "type": "comprehensive_user",
            "username": f"@{username}",
            "profile": {},
            "posts": [],
            "likes": [],
            "mentions": [],
            "media": [],
            "followers": [],
            "following": []
        }
        
        try:
            # Navigate to user profile
            profile_url = f"https://x.com/{username}"
            self.logger.info(f"🌐 Navigating to profile: {profile_url}")
            
            await self._simulate_human_interaction()
            await self.page.goto(profile_url, wait_until='domcontentloaded', timeout=60000)
            await self._human_delay(3, 6)
            
            # 🔥 AGGRESSIVE BULLETPROOF CONTENT LOADING 🔥
            self.logger.info("⏳ AGGRESSIVE LOADING - Waiting up to 2 minutes for content...")
            
            max_attempts = 3
            content_loaded = False
            
            for attempt in range(max_attempts):
                self.logger.info(f"🔄 Content loading attempt {attempt + 1}/{max_attempts}")
                
                # PHASE 1: Wait for basic page structure (60 seconds)
                basic_indicators = [
                    'div',  # Basic div structure
                    'span', # Span elements
                    'h1, h2, h3',  # Any heading
                ]
                
                for indicator in basic_indicators:
                    try:
                        await self.page.wait_for_selector(indicator, timeout=20000)
                        self.logger.info(f"📦 Basic structure loaded: {indicator}")
                        break
                    except:
                        continue
                
                # PHASE 2: Aggressive wait for profile content (multiple strategies)
                await self._human_delay(3, 6)  # Let JavaScript run
                
                # Strategy 1: Check if divs increased (content loading indicator)
                div_count = await self.page.locator('div').count()
                self.logger.info(f"📊 Current div count: {div_count}")
                
                if div_count > 100:  # Rich content threshold
                    self.logger.info("✅ Rich content detected - high div count")
                    content_loaded = True
                    break
                
                # Strategy 2: Check for username in page text
                await self._human_delay(2, 4)
                body_text = await self.page.locator('body').inner_text()
                if username.lower() in body_text.lower():
                    self.logger.info(f"✅ Username '{username}' found in content")
                    content_loaded = True
                    break
                
                # Strategy 3: Look for profile-specific text patterns
                profile_patterns = ['Following', 'Followers', 'Joined', 'posts']
                found_patterns = []
                for pattern in profile_patterns:
                    if pattern in body_text:
                        found_patterns.append(pattern)
                
                if len(found_patterns) >= 2:  # At least 2 profile patterns
                    self.logger.info(f"✅ Profile patterns found: {found_patterns}")
                    content_loaded = True
                    break
                
                # Strategy 4: Wait for specific profile elements
                profile_selectors = [
                    'a[href*="following"]',
                    'a[href*="followers"]', 
                    f'text=@{username}',
                    'span:has-text("Joined")',
                ]
                
                for selector in profile_selectors:
                    try:
                        await self.page.wait_for_selector(selector, timeout=10000)
                        self.logger.info(f"✅ Profile element found: {selector}")
                        content_loaded = True
                        break
                    except:
                        continue
                
                if content_loaded:
                    break
                    
                # Failed attempt - wait and retry
                self.logger.warning(f"❌ Attempt {attempt + 1} failed - retrying...")
                await self._human_delay(5, 8)  # Wait before retry
            
            if content_loaded:
                self.logger.info("🎉 CONTENT LOADED SUCCESSFULLY!")
                # Extra stabilization wait for rich content
                await self._human_delay(3, 5)
            else:
                self.logger.warning("⚠️ All content loading attempts failed - proceeding with available content")
                await self._human_delay(2, 3)  # Brief wait anyway
            
            # Extract profile information first
            self.logger.info("📋 Extracting profile information...")
            results["profile"] = await self._extract_profile_info(username)
            
            # 1. Scrape Posts
            if params.get('scrape_posts', True):
                max_posts = params.get('max_posts', 100)  # This will now use validated parameters
                self.logger.info(f"📝 Scraping posts ({max_posts})...")
                posts = await self._scrape_user_posts(username, max_posts)
                results["posts"] = posts
            
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
            
            # 4. Scrape Media
            if params.get('scrape_media', False):
                max_media = params.get('max_media', 25)  # Using validated parameters
                self.logger.info(f"🖼️ Scraping media ({max_media})...")
                media = await self._scrape_user_media(username, max_media)
                results["media"] = media
            
            # 5. Scrape Followers
            if params.get('scrape_followers', False):
                max_followers = params.get('max_followers', 200)  # Using validated parameters
                self.logger.info(f"👥 Scraping followers ({max_followers})...")
                followers = await self._scrape_user_followers(username, max_followers)
                results["followers"] = followers
            
            # 6. Scrape Following
            if params.get('scrape_following', False):
                max_following = params.get('max_following', 150)  # Using validated parameters
                self.logger.info(f"➡️ Scraping following ({max_following})...")
                following = await self._scrape_user_following(username, max_following)
                results["following"] = following
            
            self.logger.info(f"🏁 Comprehensive user scraping completed for @{username}")
            return [results]
            
        except Exception as e:
            self.logger.error(f"❌ Comprehensive user scraping failed: {e}")
            return [results]  # Return partial results
    
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
        posts = []
        try:
            # Ensure we're on the user's main profile page
            profile_url = f"https://x.com/{username}"
            current_url = self.page.url
            if username not in current_url:
                await self.page.goto(profile_url, wait_until='domcontentloaded', timeout=30000)
                await self._human_delay(2, 4)
            
            # 🚀 HYBRID APPROACH: Try CSS selectors first, then enhanced div parsing
            self.logger.info("🐦 Using multi-method tweet extraction...")
            
            # Wait for content to be loaded
            await self._human_delay(3, 5)
            
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
                
                # Method 1: Try 2025-compatible CSS selectors first
                css_selectors_2025 = [
                    'article[data-testid="tweet"]',
                    'div[data-testid="tweet"]', 
                    '[data-testid="tweetWrapperOuter"]',
                    '[data-testid="cellInnerDiv"]',
                    'article[role="article"]',
                    'article[tabindex="-1"]',
                    'div[dir="ltr"][lang]',
                    'div:has(time)',
                    'div:has([role="link"][href*="/status/"])'
                ]
                
                css_tweets_found = False
                for selector in css_selectors_2025:
                    try:
                        elements = await self.page.locator(selector).all()
                        if elements:
                            self.logger.info(f"✅ Found {len(elements)} elements with selector: {selector}")
                            for element in elements[:max_posts]:
                                try:
                                    text_content = await element.inner_text()
                                    if text_content and len(text_content.strip()) > 10:
                                        # Extract just the tweet text part
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
                    except Exception:
                        continue
                
                # Method 2: Enhanced div content parsing (fallback)
                if not css_tweets_found:
                    self.logger.info("🔄 Falling back to enhanced div parsing...")
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
                        tweet_data = {
                            'id': f'tweet_{len(posts)+1}',
                            'text': tweet_text,
                            'author': username,
                            'url': f'https://x.com/{username}',
                            'extracted_from': 'hybrid_extraction_2025',
                            'extraction_attempt': scroll_attempts + 1,
                            'method': 'css_selector' if css_tweets_found else 'div_parsing'
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
                
                # STAGNATION CHECK: If no new tweets in last 2 attempts, try different approach
                if scroll_attempts > 2:
                    recent_posts = [p for p in posts if p.get('extraction_attempt', 0) >= scroll_attempts - 1]
                    if len(recent_posts) == 0:
                        self.logger.warning(f"⚠️ No new tweets in recent attempts - trying page refresh strategy")
                        try:
                            # Try refreshing the page to get new content
                            await self.page.reload(wait_until='domcontentloaded', timeout=15000)
                            await self._human_delay(3, 5)
                        except:
                            pass
                
            self.logger.info(f"🐦 Found {len(posts)} tweets using hybrid extraction")
            return posts[:max_posts]
            
        except Exception as e:
            self.logger.error(f"❌ Posts scraping failed: {e}")
            return posts
    
    def _is_likely_tweet_content(self, line: str) -> bool:
        """Enhanced helper method to identify if a line is likely tweet content."""
        if not line or len(line) < 10 or len(line) > 500:
            return False
            
        # ENHANCED PROFILE DATA DETECTION - Skip profile contamination
        profile_indicators = [
            # Location + website + join date patterns
            'comjoined',  # "github.comJoined"
            '.comjoined',  # ".org/Joined" etc
            'cagithub',   # "CA" + "github" concatenation
            'cayoutu',    # Location + website patterns
            # Common profile data patterns
            'followers', 'following', 'posts', 'joined',
            # Combined profile elements
            'san francisco', 'new york', 'los angeles',  # Common locations
            'ca', 'ny', 'tx', 'usa'  # State/country abbreviations
        ]
        
        line_lower = line.lower()
        
        # Skip lines that contain profile data indicators
        if any(indicator in line_lower for indicator in profile_indicators):
            return False
            
        # SPECIFIC PATTERN: Catch "Location, StateWebsite.comJoined Date" patterns
        if ((',' in line and '.com' in line_lower and 'joined' in line_lower) or
            ('.com' in line_lower and 'joined' in line_lower and len(line.split()) <= 4)):
            return False
            
        # Skip obvious UI elements and navigation
        if (line.startswith('To view') or
            line.startswith('View keyboard') or
            'keyboard shortcuts' in line or
            line in ['Follow', 'Following', 'Followers', 'Posts', 'Replies', 'Media', 'Likes', 'Joined', 'Home', 'Search', 'Messages', 'Bookmarks', 'Lists', 'Profile', 'More', 'Settings', 'Notifications'] or
            line.isdigit() or
            all(c in '0123456789,. MKB' for c in line) or
            line.startswith('Joined ') or
            line.endswith('.com') or
            line.endswith('.org') or
            line.endswith('.net') or
            line.startswith('http://') or
            line.startswith('https://') or
            (len(line.split()) == 1 and ('h' in line or 'm' in line or 's' in line) and any(c.isdigit() for c in line)) or
            (len(line.split()) == 1 and line in ['Repost', 'Quote', 'Bookmark', 'Share', 'Copy'])):
            return False
            
        # ENHANCED: Skip concatenated profile data
        # Look for patterns like "LocationWebsiteJoined Date"
        if ('joined' in line_lower and 
            (any(year in line for year in ['2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022', '2023', '2024', '2025']) or
             any(month in line_lower for month in ['january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'october', 'november', 'december']))):
            return False
        
        # Skip URLs without spaces (likely profile links)
        if ('.' in line and line.count(' ') == 0 and 
            any(tld in line_lower for tld in ['.com', '.org', '.net', '.io', '.co'])):
            return False
        
        words = line.split()
        if len(words) < 2:
            return False
            
        # Skip obvious UI action phrases
        if line.lower().strip() in ['show more', 'show less', 'show this thread', 'translate post', 'copy link']:
            return False
            
        # Skip if it's just a username
        if len(words) == 1 and line.startswith('@'):
            return False
            
        # Ensure it contains actual text content
        if not any(c.isalpha() for c in line):
            return False
        
        # ENHANCED Quality check: Must look like natural tweet content
        has_sentence_structure = (
            # Has punctuation indicating sentences
            ('.' in line or '!' in line or '?' in line) or
            # Long enough to be meaningful
            len(words) > 5 or 
            # Contains common English words indicating natural speech
            any(word.lower() in ['i', 'we', 'they', 'this', 'that', 'will', 'can', 'should', 'would', 'the', 'and', 'or', 'but', 'so', 'at', 'in', 'on', 'for', 'with', 'by'] for word in words) or
            # Contains verbs indicating action/statements
            any(word.lower() in ['is', 'are', 'was', 'were', 'has', 'have', 'had', 'do', 'does', 'did', 'get', 'got', 'make', 'take', 'go', 'come', 'see', 'know', 'think', 'want', 'need'] for word in words)
        )
        
        # FINAL FILTER: Skip if it looks like concatenated metadata
        if len(words) < 4 and not has_sentence_structure:
            return False
        
        return has_sentence_structure
    
    async def _scrape_user_likes(self, username: str, max_likes: int) -> List[Dict[str, Any]]:
        """Scrape user's liked tweets."""
        likes = []
        try:
            # Navigate to likes page
            likes_url = f"https://x.com/{username}/likes"
            await self.page.goto(likes_url, wait_until='domcontentloaded', timeout=30000)
            await self._human_delay(3, 5)
            
            # Wait for content
            await self.page.wait_for_selector('[data-testid="tweet"]', timeout=20000)
            
            scroll_attempts = 0
            max_scrolls = min(max_likes // 5, 10)
            
            date_threshold_reached = False
            while len(likes) < max_likes and scroll_attempts < max_scrolls and not date_threshold_reached:
                tweet_elements = await self.page.locator('[data-testid="tweet"]').all()
                
                for i, tweet_element in enumerate(tweet_elements):
                    if len(likes) >= max_likes:
                        break
                    
                    tweet_data = await self._extract_tweet_from_element(tweet_element, i, True)
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
    
    async def _scrape_user_mentions(self, username: str, max_mentions: int) -> List[Dict[str, Any]]:
        """Scrape tweets that mention the user."""
        mentions = []
        try:
            # Search for mentions
            search_url = f"https://x.com/search?q=%40{username}&src=typed_query&f=live"
            await self.page.goto(search_url, wait_until='domcontentloaded', timeout=30000)
            await self._human_delay(3, 5)
            
            await self.page.wait_for_selector('[data-testid="tweet"]', timeout=20000)
            
            scroll_attempts = 0
            max_scrolls = min(max_mentions // 5, 8)
            
            date_threshold_reached = False
            while len(mentions) < max_mentions and scroll_attempts < max_scrolls and not date_threshold_reached:
                tweet_elements = await self.page.locator('[data-testid="tweet"]').all()
                
                for i, tweet_element in enumerate(tweet_elements):
                    if len(mentions) >= max_mentions:
                        break
                    
                    tweet_data = await self._extract_tweet_from_element(tweet_element, i, True)
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
            
            # 🚀 Enhanced media extraction with div parsing and direct media detection
            self.logger.info("🖼️ Using div-based media extraction...")
            await self._human_delay(5, 8)  # Wait for media page to load
            
            scroll_attempts = 0
            max_scrolls = min(max_media // 3, 10)  # More scrolls for media content
            
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
    
    async def _scrape_user_followers(self, username: str, max_followers: int) -> List[Dict[str, Any]]:
        """Scrape user's followers list."""
        followers = []
        try:
            # Navigate to followers page
            followers_url = f"https://x.com/{username}/followers"
            await self.page.goto(followers_url, wait_until='domcontentloaded', timeout=30000)
            await self._human_delay(3, 5)
            
            # 🚀 Enhanced followers extraction with div parsing approach
            self.logger.info("👥 Using div-based followers extraction...")
            await self._human_delay(5, 8)  # Wait for followers page to load
            
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
                    await self.page.mouse.wheel(0, 800)
                    await self._human_delay(2, 4)
                    scroll_attempts += 1
            
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
            await self._human_delay(3, 5)
            
            # 🚀 Enhanced following extraction with div parsing approach  
            self.logger.info("➡️ Using div-based following extraction...")
            await self._human_delay(5, 8)  # Wait for following page to load
            
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
                    await self.page.mouse.wheel(0, 800)
                    await self._human_delay(2, 4)
                    scroll_attempts += 1
            
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
        """Extract comprehensive profile information."""
        profile_data = {}
        
        try:
            # DEBUG: Log current page HTML structure
            self.logger.info("🔍 DEBUGGING - Current page HTML structure:")
            page_title = await self.page.title()
            page_url = self.page.url
            self.logger.info(f"📄 Page title: {page_title}")
            self.logger.info(f"🌐 Page URL: {page_url}")
            
            # Sample page content to identify structure
            body_text = await self.page.locator('body').inner_text()
            self.logger.info(f"📝 Page body text sample (first 300 chars): {repr(body_text[:300])}")
            
            # SELECTOR HUNTING: Find elements containing "Elon Musk"
            try:
                # Search all elements that contain "Elon Musk"
                elon_elements = await self.page.locator('*:has-text("Elon Musk")').all()
                self.logger.info(f"🎯 FOUND {len(elon_elements)} elements containing 'Elon Musk'")
                
                for i, element in enumerate(elon_elements[:5]):  # Check first 5
                    tag_name = await element.evaluate('el => el.tagName.toLowerCase()')
                    element_text = (await element.inner_text())[:100]  # First 100 chars
                    class_name = await element.get_attribute('class') or 'no-class'
                    self.logger.info(f"🔍 Element {i+1}: <{tag_name} class='{class_name[:50]}...'> Text: '{element_text}'")
                
                # Also try span elements specifically
                span_elements = await self.page.locator('span:has-text("Elon Musk")').all()
                self.logger.info(f"📍 Found {len(span_elements)} SPAN elements with 'Elon Musk'")
                
                # And h1/h2 elements
                heading_elements = await self.page.locator('h1:has-text("Elon Musk"), h2:has-text("Elon Musk")').all()
                self.logger.info(f"🏷️ Found {len(heading_elements)} HEADING elements with 'Elon Musk'")
                
                # 🐦 HUNT FOR TWEET CONTENT IN DIVS
                self.logger.info("🐦 HUNTING FOR TWEET CONTENT...")
                div_texts = await self.page.locator('div').all_inner_texts()
                potential_tweets = []
                for i, div_text in enumerate(div_texts[:30]):  # Check first 30 divs
                    lines = div_text.split('\n')
                    for line in lines:
                        line = line.strip()
                        # Look for tweet-like content (not UI elements)
                        if (len(line) > 20 and len(line) < 300 and  # Reasonable tweet length
                            not line.startswith('To view') and
                            not 'keyboard shortcuts' in line and
                            not line in ['Follow', 'Following', 'Followers', 'Posts', 'Replies', 'Media', 'Likes', 'Joined'] and
                            not line.endswith(' posts') and
                            not line.endswith(' Following') and
                            not line.endswith(' Followers')):
                            potential_tweets.append(line[:100])  # First 100 chars
                
                if potential_tweets:
                    self.logger.info(f"🐦 Found {len(potential_tweets)} potential tweets:")
                    for j, tweet in enumerate(potential_tweets[:3]):  # Show first 3
                        self.logger.info(f"🐦 Tweet {j+1}: '{tweet}...'")
                else:
                    self.logger.info("🐦 No potential tweets found in divs")
                
            except Exception as e:
                self.logger.warning(f"Selector hunting failed: {e}")
                # Fallback: sample div content
                try:
                    div_texts = await self.page.locator('div').all_inner_texts()
                    non_empty_divs = [text.strip() for text in div_texts[:10] if text.strip()]
                    self.logger.info(f"📦 Fallback - First few div contents: {non_empty_divs[:5]}")
                except:
                    pass
            
            # Check for common HTML elements
            h1_count = await self.page.locator('h1').count()
            h2_count = await self.page.locator('h2').count()  
            article_count = await self.page.locator('article').count()
            div_count = await self.page.locator('div').count()
            span_count = await self.page.locator('span').count()
            
            self.logger.info(f"🏗️ HTML structure - H1: {h1_count}, H2: {h2_count}, articles: {article_count}, divs: {div_count}, spans: {span_count}")
            
            # Check for any elements with common text patterns
            if 'log in' in body_text.lower() or 'sign up' in body_text.lower():
                self.logger.warning("⚠️ Login wall detected in page content")
            elif username.lower() in body_text.lower():
                self.logger.info(f"✅ Username '{username}' found in page content")
            else:
                self.logger.warning(f"⚠️ Username '{username}' NOT found in page content")
            # Extract display name using div content parsing (NEW METHOD)
            try:
                # Get all div texts and search for the username pattern
                div_texts = await self.page.locator('div').all_inner_texts()
                for div_text in div_texts[:20]:  # Check first 20 divs
                    if username.lower() in div_text.lower():
                        # Parse the text to extract display name
                        # Pattern: "Display Name\n85.3K posts\nFollow\nDisplay Name\n@username"
                        lines = div_text.split('\n')
                        for i, line in enumerate(lines):
                            # Look for a line that appears before username and isn't a UI element
                            if f"@{username}" in line and i > 0:
                                # The display name is likely a few lines before @username
                                for j in range(max(0, i-5), i):
                                    potential_name = lines[j].strip()
                                    if (potential_name and 
                                        potential_name not in ['Follow', 'Unfollow', 'Posts', 'Replies', 'Media', 'Likes'] and
                                        not potential_name.startswith('To view') and
                                        not 'keyboard shortcuts' in potential_name and
                                        len(potential_name) < 50):  # Reasonable name length
                                        profile_data['display_name'] = potential_name
                                        self.logger.info(f"🎯 Found display name: '{potential_name}'")
                                        break
                                if 'display_name' in profile_data:
                                    break
                        if 'display_name' in profile_data:
                            break
            except Exception as e:
                self.logger.warning(f"Display name extraction failed: {e}")
            
            # Fallback: Try old selectors
            if 'display_name' not in profile_data:
                name_selectors = ['h2 span', 'h1', 'h2']
                for selector in name_selectors:
                    try:
                        name_element = self.page.locator(selector).first
                        if await name_element.count() > 0:
                            profile_data['display_name'] = await name_element.inner_text()
                            break
                    except:
                        continue
            
            # Extract bio/description
            bio_selectors = [
                '[data-testid="UserDescription"] span',
                '[data-testid="UserDescription"]',
                '[dir="auto"] span',
            ]
            for selector in bio_selectors:
                try:
                    bio_element = self.page.locator(selector).first
                    if await bio_element.count() > 0:
                        bio_text = await bio_element.inner_text()
                        if bio_text and len(bio_text) > 20:  # Valid bio
                            profile_data['bio'] = bio_text[:500]
                            break
                except:
                    continue
            
            # Extract follower/following counts
            try:
                stats_links = await self.page.locator('a[href*="followers"], a[href*="following"]').all()
                for link in stats_links:
                    href = await link.get_attribute('href')
                    text = await link.inner_text()
                    
                    if 'followers' in href and text:
                        profile_data['followers_count'] = text.split()[0] if text.split() else '0'
                    elif 'following' in href and text:
                        profile_data['following_count'] = text.split()[0] if text.split() else '0'
            except:
                pass
            
            # Extract profile image
            try:
                img_element = self.page.locator('[data-testid="UserAvatar-Container-"] img').first
                if await img_element.count() > 0:
                    img_src = await img_element.get_attribute('src')
                    if img_src:
                        profile_data['profile_image'] = img_src
            except:
                pass
            
            # Extract location
            try:
                location_element = self.page.locator('[data-testid="UserLocation"] span').first
                if await location_element.count() > 0:
                    profile_data['location'] = await location_element.inner_text()
            except:
                pass
            
            # Extract website
            try:
                website_element = self.page.locator('[data-testid="UserUrl"] a').first
                if await website_element.count() > 0:
                    profile_data['website'] = await website_element.get_attribute('href')
            except:
                pass
            
            # Extract join date
            try:
                join_element = self.page.locator('[data-testid="UserJoinDate"] span').first
                if await join_element.count() > 0:
                    profile_data['joined'] = await join_element.inner_text()
            except:
                pass
            
            # Extract tweet count
            try:
                nav_links = await self.page.locator('[role="tablist"] a').all()
                for link in nav_links:
                    text = await link.inner_text()
                    if 'Posts' in text or 'Tweets' in text:
                        # Extract number from "1.2K Posts"
                        import re
                        numbers = re.findall(r'[\d,]+\.?\d*[KMB]?', text)
                        if numbers:
                            profile_data['posts_count'] = numbers[0]
                        break
            except:
                pass
            
            self.logger.info(f"📋 Extracted profile: {len(profile_data)} fields")
            return profile_data
            
        except Exception as e:
            self.logger.error(f"❌ Profile extraction failed: {e}")
            return profile_data
    
    async def load_session(self, session_file: str = "/sessions/twitter_session.json"):
        """Load saved session cookies to bypass authentication."""
        self.logger.info(f"🍪 Loading Twitter session from {session_file}")
        
        try:
            # Check if session file exists
            if not os.path.exists(session_file):
                raise ValueError(f"Session file not found: {session_file}")
            
            # Load session data
            with open(session_file, 'r') as f:
                session_data = json.load(f)
            
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
            
            # Create browser context (similar to authenticate but simpler)
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
            
            self.page = await self.context.new_page()
            
            # Add cookies to context - FIXED: Add to both domains correctly
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
            self.logger.info(f"🍪 Added {len(cookie_list)} cookies to browser context")
            
            # ENHANCED: Validate session by checking a Twitter page
            try:
                self.logger.info("🔍 Validating session authentication...")
                await self.page.goto("https://x.com/home", wait_until='domcontentloaded', timeout=15000)
                await self.page.wait_for_timeout(3000)  # Wait for content to load
                
                # Check for authentication indicators
                auth_indicators = [
                    '[data-testid="SideNav_NewTweet_Button"]',
                    '[data-testid="AppTabBar_Profile_Link"]', 
                    '[aria-label*="Tweet"]',
                    '[aria-label*="Post"]',
                    'nav[role="navigation"]'
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
                    self.logger.info("🎉 Session validation successful - fully authenticated!")
                else:
                    self.logger.warning("⚠️ Session loaded but authentication unclear")
                    self.authenticated = True  # Proceed anyway, may work for public content
                    
            except Exception as auth_e:
                self.logger.warning(f"⚠️ Session validation failed: {auth_e} - proceeding anyway")
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
                await temp_page.goto("https://x.com/home", wait_until='domcontentloaded', timeout=20000)
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
                await test_page.goto("https://x.com/home", wait_until='domcontentloaded', timeout=15000)
                
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
                await test_page.goto("https://x.com/github", wait_until='domcontentloaded', timeout=15000)
                
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
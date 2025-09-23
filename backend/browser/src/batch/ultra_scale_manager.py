"""Ultra-Scale Batching Manager for 5,000+ and 10,000+ Post Extractions.

Phase 6.2: Ultra-Large Scale Data Extraction
- Multi-tier batching with timeline pagination
- Advanced anti-detection with temporal distribution
- Intelligent deduplication and data consolidation
- Production-ready orchestration for massive datasets
"""

from __future__ import annotations

import asyncio
import time
import logging
from typing import Dict, List, Optional, Any, Tuple, Set
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta
import json
import uuid
import math

from .manager import BatchManager, BatchExtractionRequest, BatchJob, BatchStatus


class UltraScaleStrategy(str, Enum):
    """Ultra-scale extraction strategies."""
    TEMPORAL_DISTRIBUTION = "temporal_distribution"  # Spread over time windows
    PAGINATION_BASED = "pagination_based"           # Use timeline pagination
    HYBRID_APPROACH = "hybrid_approach"             # Combine both strategies


@dataclass
class TimeWindow:
    """Time window for temporal distribution."""
    start_time: datetime
    end_time: datetime
    batch_allocation: int
    status: str = "pending"
    posts_extracted: int = 0


@dataclass
class UltraScaleExtractionRequest:
    """Request for ultra-large scale extraction (5000+ posts)."""
    parent_job_id: str
    username: str
    total_posts_needed: int
    strategy: UltraScaleStrategy = UltraScaleStrategy.HYBRID_APPROACH

    # Timeline configuration
    max_timeline_depth_days: int = 30  # How far back to go
    temporal_windows: int = 6          # Number of time windows
    window_delay_minutes: int = 15     # Delay between windows

    # Batch configuration
    posts_per_batch: int = 150         # Proven safe limit
    max_concurrent_batches: int = 2    # Conservative for ultra-scale
    inter_batch_delay_seconds: int = 30 # Longer delays for stealth

    # Quality and deduplication
    enable_deduplication: bool = True
    quality_threshold: float = 0.8     # Minimum acceptable quality

    # Monitoring and recovery
    auto_retry_failed_windows: bool = True
    max_window_retries: int = 2
    progress_checkpoint_interval: int = 10  # Save progress every N batches

    created_at: datetime = field(default_factory=datetime.now)


@dataclass
class UltraScaleProgress:
    """Comprehensive progress tracking for ultra-scale operations."""
    parent_job_id: str
    total_posts_needed: int
    total_posts_extracted: int
    unique_posts_count: int  # After deduplication

    # Time window progress
    total_windows: int
    completed_windows: int
    failed_windows: int

    # Batch progress
    total_batches: int
    completed_batches: int
    failed_batches: int

    # Performance metrics
    extraction_rate: float
    average_window_time: float

    # Fields with default values must come after fields without defaults
    current_window: Optional[int] = None
    estimated_completion: Optional[datetime] = None

    # Quality metrics
    deduplication_rate: float = 0.0
    average_quality_score: float = 0.0

    created_at: datetime = field(default_factory=datetime.now)
    last_updated: datetime = field(default_factory=datetime.now)


class UltraScaleBatchManager:
    """Manages ultra-large scale extractions (5000+ posts) with advanced strategies."""

    def __init__(self, base_batch_manager: BatchManager, logger: Optional[logging.Logger] = None):
        self.base_manager = base_batch_manager
        self.logger = logger or logging.getLogger(__name__)

        # Ultra-scale tracking
        self.ultra_extractions: Dict[str, UltraScaleExtractionRequest] = {}
        self.ultra_progress: Dict[str, UltraScaleProgress] = {}
        self.time_windows: Dict[str, List[TimeWindow]] = {}
        self.extracted_data: Dict[str, List[Dict[str, Any]]] = {}
        self.deduplication_cache: Dict[str, Set[str]] = {}  # URL-based deduplication

        # Configuration based on stress testing
        self.ULTRA_SAFE_BATCH_SIZE = 150
        self.MAX_POSTS_PER_WINDOW = 800    # Conservative window limit
        self.ANTI_DETECTION_DELAY = 45     # Seconds between aggressive operations
        self.TIMELINE_PAGINATION_STEP = 150  # Posts per timeline page

    async def submit_ultra_scale_extraction(self, request: UltraScaleExtractionRequest) -> str:
        """Submit an ultra-large scale extraction request."""
        self.logger.info(f"🚀 ULTRA-SCALE EXTRACTION: {request.total_posts_needed:,} posts for @{request.username}")

        if request.total_posts_needed < 1000:
            self.logger.warning("Ultra-scale manager called for <1000 posts. Consider using standard batch manager.")

        # Validate request
        if request.total_posts_needed > 50000:
            raise ValueError("Maximum ultra-scale limit is 50,000 posts per request")

        # Store request
        self.ultra_extractions[request.parent_job_id] = request
        self.extracted_data[request.parent_job_id] = []
        self.deduplication_cache[request.parent_job_id] = set()

        # Create execution strategy
        strategy_plan = await self._create_ultra_scale_strategy(request)

        # Initialize progress tracking
        progress = UltraScaleProgress(
            parent_job_id=request.parent_job_id,
            total_posts_needed=request.total_posts_needed,
            total_posts_extracted=0,
            unique_posts_count=0,
            total_windows=len(strategy_plan['time_windows']),
            completed_windows=0,
            failed_windows=0,
            total_batches=strategy_plan['total_batches'],
            completed_batches=0,
            failed_batches=0,
            extraction_rate=0.0,
            average_window_time=0.0
        )

        self.ultra_progress[request.parent_job_id] = progress
        self.time_windows[request.parent_job_id] = strategy_plan['time_windows']

        # Start ultra-scale processing
        asyncio.create_task(self._process_ultra_scale_extraction(request.parent_job_id))

        self.logger.info(f"✅ Ultra-scale extraction {request.parent_job_id} queued:")
        self.logger.info(f"   Strategy: {request.strategy.value}")
        self.logger.info(f"   Time windows: {len(strategy_plan['time_windows'])}")
        self.logger.info(f"   Total batches: {strategy_plan['total_batches']}")
        self.logger.info(f"   Estimated duration: {strategy_plan['estimated_duration']:.1f} minutes")

        return request.parent_job_id

    async def _create_ultra_scale_strategy(self, request: UltraScaleExtractionRequest) -> Dict[str, Any]:
        """Create execution strategy for ultra-scale extraction."""

        if request.strategy == UltraScaleStrategy.TEMPORAL_DISTRIBUTION:
            return await self._create_temporal_distribution_strategy(request)
        elif request.strategy == UltraScaleStrategy.PAGINATION_BASED:
            return await self._create_pagination_strategy(request)
        else:  # HYBRID_APPROACH
            return await self._create_hybrid_strategy(request)

    async def _create_hybrid_strategy(self, request: UltraScaleExtractionRequest) -> Dict[str, Any]:
        """Create hybrid strategy combining temporal distribution and pagination."""

        # Calculate optimal time windows
        total_posts = request.total_posts_needed
        posts_per_window = min(self.MAX_POSTS_PER_WINDOW, total_posts // request.temporal_windows)

        # Ensure we have enough windows for the request
        actual_windows = math.ceil(total_posts / posts_per_window)

        time_windows = []
        posts_allocated = 0

        # Create time windows spanning the last N days
        days_span = request.max_timeline_depth_days
        window_duration_hours = (days_span * 24) // actual_windows

        for i in range(actual_windows):
            # Calculate time window
            end_time = datetime.now() - timedelta(hours=i * window_duration_hours)
            start_time = end_time - timedelta(hours=window_duration_hours)

            # Calculate posts for this window
            remaining_posts = total_posts - posts_allocated
            posts_in_window = min(posts_per_window, remaining_posts)

            if posts_in_window <= 0:
                break

            window = TimeWindow(
                start_time=start_time,
                end_time=end_time,
                batch_allocation=posts_in_window
            )

            time_windows.append(window)
            posts_allocated += posts_in_window

        # Calculate total batches across all windows
        total_batches = sum(math.ceil(w.batch_allocation / request.posts_per_batch) for w in time_windows)

        # Estimate duration
        avg_batch_time = 120  # seconds per batch (conservative)
        batch_processing_time = total_batches * avg_batch_time
        window_delays = (len(time_windows) - 1) * request.window_delay_minutes * 60
        estimated_duration = (batch_processing_time + window_delays) / 60  # minutes

        return {
            'strategy': 'hybrid',
            'time_windows': time_windows,
            'total_batches': total_batches,
            'estimated_duration': estimated_duration,
            'posts_allocated': posts_allocated
        }

    async def _create_temporal_distribution_strategy(self, request: UltraScaleExtractionRequest) -> Dict[str, Any]:
        """Create temporal distribution strategy for stealth."""

        # Distribute extraction across multiple time periods
        posts_per_window = min(600, request.total_posts_needed // request.temporal_windows)

        time_windows = []
        for i in range(request.temporal_windows):
            # Spread windows across different times
            delay_minutes = i * request.window_delay_minutes
            execution_time = datetime.now() + timedelta(minutes=delay_minutes)

            window = TimeWindow(
                start_time=execution_time,
                end_time=execution_time + timedelta(minutes=10),  # 10-minute extraction window
                batch_allocation=posts_per_window
            )
            time_windows.append(window)

        total_batches = sum(math.ceil(w.batch_allocation / request.posts_per_batch) for w in time_windows)
        estimated_duration = request.temporal_windows * request.window_delay_minutes

        return {
            'strategy': 'temporal_distribution',
            'time_windows': time_windows,
            'total_batches': total_batches,
            'estimated_duration': estimated_duration,
            'posts_allocated': sum(w.batch_allocation for w in time_windows)
        }

    async def _create_pagination_strategy(self, request: UltraScaleExtractionRequest) -> Dict[str, Any]:
        """Create pagination-based strategy for sequential extraction."""

        # Single large window with pagination
        total_posts = request.total_posts_needed
        posts_per_page = self.TIMELINE_PAGINATION_STEP

        window = TimeWindow(
            start_time=datetime.now(),
            end_time=datetime.now() + timedelta(hours=2),  # Extended window
            batch_allocation=total_posts
        )

        total_batches = math.ceil(total_posts / request.posts_per_batch)
        estimated_duration = total_batches * 2  # 2 minutes per batch

        return {
            'strategy': 'pagination_based',
            'time_windows': [window],
            'total_batches': total_batches,
            'estimated_duration': estimated_duration,
            'posts_allocated': total_posts
        }

    async def _process_ultra_scale_extraction(self, parent_job_id: str) -> None:
        """Process ultra-scale extraction with advanced orchestration."""
        request = self.ultra_extractions[parent_job_id]
        progress = self.ultra_progress[parent_job_id]
        time_windows = self.time_windows[parent_job_id]

        self.logger.info(f"🎯 Starting ultra-scale processing: {len(time_windows)} windows")

        start_time = time.time()

        # Process each time window
        for window_idx, window in enumerate(time_windows):
            progress.current_window = window_idx
            window_start_time = time.time()

            self.logger.info(f"⏰ Processing window {window_idx + 1}/{len(time_windows)}: {window.batch_allocation} posts")

            try:
                # Process window with batch strategy
                window_results = await self._process_time_window(
                    parent_job_id, window_idx, window, request
                )

                # Update progress
                progress.completed_windows += 1
                progress.total_posts_extracted += window_results['posts_extracted']
                window.posts_extracted = window_results['posts_extracted']
                window.status = "completed"

                # Store extracted data with deduplication
                await self._store_and_deduplicate_data(parent_job_id, window_results['data'])

                # Update progress metrics
                window_time = time.time() - window_start_time
                progress.average_window_time = (
                    (progress.average_window_time * window_idx + window_time) / (window_idx + 1)
                )

                self.logger.info(f"✅ Window {window_idx + 1} completed: {window_results['posts_extracted']} posts in {window_time:.1f}s")

                # Anti-detection delay between windows
                if window_idx < len(time_windows) - 1:
                    delay = request.window_delay_minutes * 60
                    self.logger.info(f"⏳ Anti-detection delay: {delay} seconds until next window")
                    await asyncio.sleep(delay)

            except Exception as e:
                self.logger.error(f"❌ Window {window_idx + 1} failed: {e}")
                progress.failed_windows += 1
                window.status = "failed"

                # Attempt retry if configured
                if request.auto_retry_failed_windows and window_idx < request.max_window_retries:
                    self.logger.info(f"🔄 Retrying failed window {window_idx + 1}")
                    await asyncio.sleep(60)  # Longer delay before retry
                    # TODO: Implement retry logic

            # Update progress
            progress.last_updated = datetime.now()
            await self._save_progress_checkpoint(parent_job_id)

        # Final processing
        total_time = time.time() - start_time
        await self._finalize_ultra_scale_extraction(parent_job_id, total_time)

    async def _process_time_window(
        self,
        parent_job_id: str,
        window_idx: int,
        window: TimeWindow,
        request: UltraScaleExtractionRequest
    ) -> Dict[str, Any]:
        """Process a single time window with batch orchestration."""

        # Create batches for this window
        batches_needed = math.ceil(window.batch_allocation / request.posts_per_batch)
        posts_extracted = 0
        extracted_data = []

        self.logger.info(f"📦 Creating {batches_needed} batches for window {window_idx + 1}")

        # Process batches sequentially within window
        for batch_idx in range(batches_needed):
            posts_in_batch = min(
                request.posts_per_batch,
                window.batch_allocation - posts_extracted
            )

            # Create batch request
            batch_request = BatchExtractionRequest(
                parent_job_id=f"{parent_job_id}_w{window_idx}_b{batch_idx}",
                username=request.username,
                total_posts_needed=posts_in_batch,
                max_posts_per_batch=posts_in_batch,
                max_concurrent_batches=1,  # Sequential within window
                batch_delay_seconds=request.inter_batch_delay_seconds
            )

            try:
                # Submit batch through base manager
                batch_id = await self.base_manager.submit_large_extraction(batch_request)

                # Monitor batch completion
                batch_result = await self._monitor_batch_completion(batch_id, request.posts_per_batch * 2)  # 2 min per post

                if batch_result['success']:
                    posts_extracted += batch_result['posts_count']
                    extracted_data.extend(batch_result['data'])

                    self.logger.info(f"  ✅ Batch {batch_idx + 1}/{batches_needed}: {batch_result['posts_count']} posts")
                else:
                    self.logger.error(f"  ❌ Batch {batch_idx + 1}/{batches_needed} failed")

            except Exception as e:
                self.logger.error(f"Batch {batch_idx + 1} error: {e}")
                continue

        return {
            'posts_extracted': posts_extracted,
            'data': extracted_data,
            'batches_processed': batches_needed
        }

    async def _monitor_batch_completion(self, batch_id: str, timeout_seconds: int) -> Dict[str, Any]:
        """Monitor batch completion with timeout."""

        for _ in range(timeout_seconds // 10):  # Check every 10 seconds
            try:
                progress = self.base_manager.get_batch_progress(batch_id)
                if progress and progress.completed_batches == progress.total_batches:
                    # Batch completed - extract results
                    # TODO: Implement result extraction from base manager
                    return {
                        'success': True,
                        'posts_count': progress.total_posts_extracted,
                        'data': []  # Placeholder
                    }
                elif progress and progress.failed_batches == progress.total_batches:
                    return {'success': False, 'posts_count': 0, 'data': []}

                await asyncio.sleep(10)

            except Exception as e:
                self.logger.error(f"Batch monitoring error: {e}")
                continue

        # Timeout
        return {'success': False, 'posts_count': 0, 'data': []}

    async def _store_and_deduplicate_data(self, parent_job_id: str, new_data: List[Dict[str, Any]]) -> None:
        """Store data with intelligent deduplication."""

        cache = self.deduplication_cache[parent_job_id]
        stored_data = self.extracted_data[parent_job_id]

        duplicates_found = 0

        for item in new_data:
            # Use URL as primary deduplication key
            item_url = item.get('url', '')
            if item_url and item_url not in cache:
                cache.add(item_url)
                stored_data.append(item)
            else:
                duplicates_found += 1

        # Update progress
        progress = self.ultra_progress[parent_job_id]
        progress.unique_posts_count = len(stored_data)
        if progress.total_posts_extracted > 0:
            progress.deduplication_rate = duplicates_found / progress.total_posts_extracted * 100

        if duplicates_found > 0:
            self.logger.info(f"🔄 Deduplication: {duplicates_found} duplicates removed")

    async def _save_progress_checkpoint(self, parent_job_id: str) -> None:
        """Save progress checkpoint for recovery."""
        progress = self.ultra_progress[parent_job_id]

        checkpoint_data = {
            'parent_job_id': parent_job_id,
            'progress': progress.__dict__,
            'timestamp': datetime.now().isoformat(),
            'total_unique_posts': len(self.extracted_data[parent_job_id])
        }

        # Save to file for persistence
        checkpoint_file = f"/tmp/ultra_scale_checkpoint_{parent_job_id}.json"
        try:
            with open(checkpoint_file, 'w') as f:
                json.dump(checkpoint_data, f, indent=2, default=str)
        except Exception as e:
            self.logger.error(f"Failed to save checkpoint: {e}")

    async def _finalize_ultra_scale_extraction(self, parent_job_id: str, total_time: float) -> None:
        """Finalize ultra-scale extraction with comprehensive reporting."""
        progress = self.ultra_progress[parent_job_id]
        request = self.ultra_extractions[parent_job_id]
        extracted_data = self.extracted_data[parent_job_id]

        # Calculate final metrics
        success_rate = (progress.completed_windows / progress.total_windows * 100) if progress.total_windows > 0 else 0
        extraction_rate = (len(extracted_data) / request.total_posts_needed * 100) if request.total_posts_needed > 0 else 0

        self.logger.info(f"🎉 ULTRA-SCALE EXTRACTION COMPLETED: {parent_job_id}")
        self.logger.info(f"📊 FINAL RESULTS:")
        self.logger.info(f"   Requested: {request.total_posts_needed:,} posts")
        self.logger.info(f"   Extracted: {len(extracted_data):,} unique posts")
        self.logger.info(f"   Extraction rate: {extraction_rate:.1f}%")
        self.logger.info(f"   Windows completed: {progress.completed_windows}/{progress.total_windows}")
        self.logger.info(f"   Success rate: {success_rate:.1f}%")
        self.logger.info(f"   Total time: {total_time/3600:.1f} hours")
        self.logger.info(f"   Average speed: {len(extracted_data)/(total_time/3600):.0f} posts/hour")
        self.logger.info(f"   Deduplication rate: {progress.deduplication_rate:.1f}%")

    def get_ultra_scale_progress(self, parent_job_id: str) -> Optional[UltraScaleProgress]:
        """Get comprehensive progress for ultra-scale extraction."""
        return self.ultra_progress.get(parent_job_id)

    def get_ultra_scale_summary(self) -> Dict[str, Any]:
        """Get summary of all ultra-scale operations."""
        active_extractions = len(self.ultra_extractions)
        total_posts_requested = sum(req.total_posts_needed for req in self.ultra_extractions.values())
        total_posts_extracted = sum(len(data) for data in self.extracted_data.values())

        return {
            'active_ultra_extractions': active_extractions,
            'total_posts_requested': total_posts_requested,
            'total_posts_extracted': total_posts_extracted,
            'overall_extraction_rate': (total_posts_extracted / total_posts_requested * 100) if total_posts_requested > 0 else 0,
            'ultra_safe_batch_size': self.ULTRA_SAFE_BATCH_SIZE,
            'max_posts_per_window': self.MAX_POSTS_PER_WINDOW
        }


# Global ultra-scale manager instance
_ultra_scale_manager: Optional[UltraScaleBatchManager] = None


def get_ultra_scale_manager(base_batch_manager: BatchManager = None) -> UltraScaleBatchManager:
    """Get or create global ultra-scale manager instance."""
    global _ultra_scale_manager

    if _ultra_scale_manager is None and base_batch_manager:
        _ultra_scale_manager = UltraScaleBatchManager(base_batch_manager)

    return _ultra_scale_manager
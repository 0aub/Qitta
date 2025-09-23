"""Advanced Timeline Pagination and Deduplication System.

Phase 6.3: Advanced Batching with Timeline Pagination
- Intelligent timeline cursor management for sequential extraction
- Multi-level deduplication with content analysis
- Pagination state persistence and recovery
- Quality-aware data consolidation
- Advanced anti-detection temporal patterns
"""

from __future__ import annotations

import asyncio
import time
import logging
import hashlib
from typing import Dict, List, Optional, Any, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta
import json
import uuid

from .ultra_scale import UltraScaleManager, UltraScaleExtractionRequest


class PaginationStrategy(str, Enum):
    """Timeline pagination strategies."""
    CURSOR_BASED = "cursor_based"         # Use timeline cursors for sequential access
    DATE_RANGE = "date_range"             # Navigate by date ranges
    SCROLL_INFINITE = "scroll_infinite"   # Infinite scroll simulation
    HYBRID_CURSOR = "hybrid_cursor"       # Combine cursor and date range


@dataclass
class TimelineCursor:
    """Timeline cursor for pagination state."""
    cursor_id: str
    position: int
    timestamp: datetime
    is_valid: bool = True
    posts_extracted: int = 0
    last_post_id: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)


@dataclass
class DeduplicationMetrics:
    """Metrics for deduplication effectiveness."""
    total_items_processed: int = 0
    url_duplicates_removed: int = 0
    content_duplicates_removed: int = 0
    temporal_duplicates_removed: int = 0
    quality_filtered_items: int = 0
    final_unique_count: int = 0
    deduplication_rate: float = 0.0


@dataclass
class PaginationState:
    """Complete pagination state for recovery and persistence."""
    extraction_id: str
    strategy: PaginationStrategy
    current_page: int
    total_pages_estimated: int
    cursors: List[TimelineCursor]
    active_cursor_index: int
    posts_extracted_this_session: int
    pagination_metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)
    last_updated: datetime = field(default_factory=datetime.now)


@dataclass
class AdvancedPaginationRequest:
    """Request for advanced pagination-based extraction."""
    parent_job_id: str
    username: str
    total_posts_needed: int

    # Pagination configuration
    pagination_strategy: PaginationStrategy = PaginationStrategy.HYBRID_CURSOR
    posts_per_page: int = 50              # Conservative page size
    max_pages_per_session: int = 20       # Pages before cursor rotation
    cursor_rotation_interval: int = 15    # Minutes between cursor changes

    # Timeline configuration
    timeline_depth_days: int = 90         # How far back to paginate
    date_window_hours: int = 24           # Date range window size
    enable_temporal_gaps: bool = True     # Skip time periods strategically

    # Quality and deduplication
    enable_advanced_deduplication: bool = True
    content_similarity_threshold: float = 0.85  # Content similarity cutoff
    temporal_dedup_window_hours: int = 6  # Time window for temporal deduplication
    quality_score_threshold: float = 0.7  # Minimum quality threshold

    # Anti-detection and stealth
    randomize_pagination_timing: bool = True
    pagination_delay_range: Tuple[int, int] = (10, 30)  # Random delay range in seconds
    cursor_jitter_enabled: bool = True    # Add randomness to cursor positions

    # Recovery and persistence
    enable_state_persistence: bool = True
    checkpoint_interval_pages: int = 5    # Save state every N pages
    auto_resume_on_failure: bool = True

    created_at: datetime = field(default_factory=datetime.now)


class AdvancedPaginationManager:
    """Manages advanced timeline pagination with intelligent deduplication."""

    def __init__(self, base_ultra_manager: UltraScaleManager, logger: Optional[logging.Logger] = None):
        self.base_manager = base_ultra_manager
        self.logger = logger or logging.getLogger(__name__)

        # Pagination state tracking
        self.active_paginations: Dict[str, AdvancedPaginationRequest] = {}
        self.pagination_states: Dict[str, PaginationState] = {}
        self.timeline_cursors: Dict[str, List[TimelineCursor]] = {}

        # Deduplication systems
        self.url_fingerprints: Dict[str, Set[str]] = {}      # URL-based deduplication
        self.content_hashes: Dict[str, Set[str]] = {}        # Content hash deduplication
        self.temporal_clusters: Dict[str, List[Dict[str, Any]]] = {}  # Temporal deduplication
        self.extracted_data: Dict[str, List[Dict[str, Any]]] = {}     # Final deduplicated data

        # Deduplication metrics
        self.dedup_metrics: Dict[str, DeduplicationMetrics] = {}

        # Configuration
        self.MAX_CURSOR_LIFETIME_HOURS = 4
        self.CONTENT_SIMILARITY_ALGORITHM = "fuzzy_hash"
        self.PAGINATION_RECOVERY_ATTEMPTS = 3

    async def submit_advanced_pagination_extraction(self, request: AdvancedPaginationRequest) -> str:
        """Submit advanced pagination-based extraction."""
        self.logger.info(f"🔄 ADVANCED PAGINATION: {request.total_posts_needed:,} posts via {request.pagination_strategy.value}")

        # Validate request
        if request.total_posts_needed > 100000:
            raise ValueError("Maximum advanced pagination limit is 100,000 posts per request")

        # Initialize tracking
        self.active_paginations[request.parent_job_id] = request
        self.extracted_data[request.parent_job_id] = []
        self.url_fingerprints[request.parent_job_id] = set()
        self.content_hashes[request.parent_job_id] = set()
        self.temporal_clusters[request.parent_job_id] = []
        self.dedup_metrics[request.parent_job_id] = DeduplicationMetrics()

        # Create pagination strategy
        pagination_plan = await self._create_pagination_strategy(request)

        # Initialize pagination state
        state = PaginationState(
            extraction_id=request.parent_job_id,
            strategy=request.pagination_strategy,
            current_page=0,
            total_pages_estimated=pagination_plan['estimated_pages'],
            cursors=pagination_plan['initial_cursors'],
            active_cursor_index=0,
            posts_extracted_this_session=0,
            pagination_metadata=pagination_plan['metadata']
        )

        self.pagination_states[request.parent_job_id] = state
        self.timeline_cursors[request.parent_job_id] = pagination_plan['initial_cursors']

        # Start advanced pagination processing
        asyncio.create_task(self._process_advanced_pagination(request.parent_job_id))

        self.logger.info(f"✅ Advanced pagination {request.parent_job_id} initialized:")
        self.logger.info(f"   Strategy: {request.pagination_strategy.value}")
        self.logger.info(f"   Estimated pages: {pagination_plan['estimated_pages']}")
        self.logger.info(f"   Initial cursors: {len(pagination_plan['initial_cursors'])}")
        self.logger.info(f"   Posts per page: {request.posts_per_page}")

        return request.parent_job_id

    async def _create_pagination_strategy(self, request: AdvancedPaginationRequest) -> Dict[str, Any]:
        """Create intelligent pagination strategy based on request parameters."""

        if request.pagination_strategy == PaginationStrategy.CURSOR_BASED:
            return await self._create_cursor_based_strategy(request)
        elif request.pagination_strategy == PaginationStrategy.DATE_RANGE:
            return await self._create_date_range_strategy(request)
        elif request.pagination_strategy == PaginationStrategy.SCROLL_INFINITE:
            return await self._create_infinite_scroll_strategy(request)
        else:  # HYBRID_CURSOR
            return await self._create_hybrid_cursor_strategy(request)

    async def _create_hybrid_cursor_strategy(self, request: AdvancedPaginationRequest) -> Dict[str, Any]:
        """Create hybrid strategy combining cursor and date-based pagination."""

        # Calculate timeline segments
        total_posts = request.total_posts_needed
        posts_per_page = request.posts_per_page
        estimated_pages = (total_posts // posts_per_page) + 1

        # Create initial cursors spanning timeline depth
        timeline_days = request.timeline_depth_days
        cursor_count = min(10, max(3, timeline_days // 10))  # 3-10 cursors

        initial_cursors = []
        for i in range(cursor_count):
            # Distribute cursors across timeline
            days_back = (i * timeline_days) // cursor_count
            cursor_time = datetime.now() - timedelta(days=days_back)

            cursor = TimelineCursor(
                cursor_id=f"cursor_{i}_{uuid.uuid4().hex[:8]}",
                position=i * (total_posts // cursor_count),
                timestamp=cursor_time
            )
            initial_cursors.append(cursor)

        # Calculate estimated processing time
        pages_per_cursor = estimated_pages // len(initial_cursors)
        avg_page_time = 15  # seconds per page
        estimated_duration = estimated_pages * avg_page_time / 60  # minutes

        return {
            'strategy': 'hybrid_cursor',
            'estimated_pages': estimated_pages,
            'initial_cursors': initial_cursors,
            'pages_per_cursor': pages_per_cursor,
            'estimated_duration': estimated_duration,
            'metadata': {
                'cursor_count': len(initial_cursors),
                'timeline_span_days': timeline_days,
                'posts_per_page': posts_per_page,
                'anti_detection_enabled': request.randomize_pagination_timing
            }
        }

    async def _create_cursor_based_strategy(self, request: AdvancedPaginationRequest) -> Dict[str, Any]:
        """Create pure cursor-based pagination strategy."""

        # Single primary cursor with backup cursors
        primary_cursor = TimelineCursor(
            cursor_id=f"primary_{uuid.uuid4().hex[:8]}",
            position=0,
            timestamp=datetime.now()
        )

        backup_cursors = []
        for i in range(2):  # 2 backup cursors
            backup_cursor = TimelineCursor(
                cursor_id=f"backup_{i}_{uuid.uuid4().hex[:8]}",
                position=0,
                timestamp=datetime.now() - timedelta(hours=i*6)
            )
            backup_cursors.append(backup_cursor)

        initial_cursors = [primary_cursor] + backup_cursors
        estimated_pages = request.total_posts_needed // request.posts_per_page + 1

        return {
            'strategy': 'cursor_based',
            'estimated_pages': estimated_pages,
            'initial_cursors': initial_cursors,
            'estimated_duration': estimated_pages * 10 / 60,  # 10 seconds per page
            'metadata': {
                'primary_cursor_id': primary_cursor.cursor_id,
                'backup_cursors': len(backup_cursors),
                'sequential_extraction': True
            }
        }

    async def _create_date_range_strategy(self, request: AdvancedPaginationRequest) -> Dict[str, Any]:
        """Create date range-based pagination strategy."""

        # Divide timeline into date windows
        total_days = request.timeline_depth_days
        window_hours = request.date_window_hours
        windows_count = (total_days * 24) // window_hours

        cursors = []
        for i in range(windows_count):
            window_start = datetime.now() - timedelta(hours=(i+1) * window_hours)
            window_end = datetime.now() - timedelta(hours=i * window_hours)

            cursor = TimelineCursor(
                cursor_id=f"daterange_{i}_{uuid.uuid4().hex[:8]}",
                position=i,
                timestamp=window_start
            )
            cursors.append(cursor)

        estimated_pages = len(cursors) * 2  # 2 pages per date window on average

        return {
            'strategy': 'date_range',
            'estimated_pages': estimated_pages,
            'initial_cursors': cursors,
            'estimated_duration': estimated_pages * 12 / 60,  # 12 seconds per page
            'metadata': {
                'date_windows': windows_count,
                'window_size_hours': window_hours,
                'temporal_distribution': True
            }
        }

    async def _create_infinite_scroll_strategy(self, request: AdvancedPaginationRequest) -> Dict[str, Any]:
        """Create infinite scroll simulation strategy."""

        # Single continuous cursor with scroll positions
        scroll_cursor = TimelineCursor(
            cursor_id=f"scroll_{uuid.uuid4().hex[:8]}",
            position=0,
            timestamp=datetime.now()
        )

        # Estimate scroll positions needed
        estimated_scrolls = request.total_posts_needed // (request.posts_per_page * 2)  # 2x posts per scroll
        estimated_pages = estimated_scrolls

        return {
            'strategy': 'scroll_infinite',
            'estimated_pages': estimated_pages,
            'initial_cursors': [scroll_cursor],
            'estimated_duration': estimated_pages * 20 / 60,  # 20 seconds per scroll
            'metadata': {
                'scroll_simulation': True,
                'estimated_scrolls': estimated_scrolls,
                'continuous_extraction': True
            }
        }

    async def _process_advanced_pagination(self, extraction_id: str) -> None:
        """Process advanced pagination extraction with intelligent orchestration."""
        request = self.active_paginations[extraction_id]
        state = self.pagination_states[extraction_id]
        cursors = self.timeline_cursors[extraction_id]

        self.logger.info(f"🎯 Starting advanced pagination: {len(cursors)} cursors, {state.total_pages_estimated} estimated pages")

        start_time = time.time()
        session_start = datetime.now()

        try:
            # Process pages with intelligent cursor management
            for page_num in range(state.total_pages_estimated):
                state.current_page = page_num

                # Select optimal cursor for this page
                active_cursor = await self._select_optimal_cursor(extraction_id, page_num)
                if not active_cursor:
                    self.logger.warning(f"No valid cursor available for page {page_num}")
                    break

                # Anti-detection timing
                if request.randomize_pagination_timing:
                    delay = await self._calculate_anti_detection_delay(extraction_id, page_num)
                    if delay > 0:
                        self.logger.info(f"⏳ Anti-detection delay: {delay}s before page {page_num + 1}")
                        await asyncio.sleep(delay)

                # Extract page data
                page_data = await self._extract_pagination_page(
                    extraction_id, page_num, active_cursor, request
                )

                if page_data['success']:
                    # Process with advanced deduplication
                    unique_items = await self._process_with_advanced_deduplication(
                        extraction_id, page_data['items']
                    )

                    # Update state
                    state.posts_extracted_this_session += len(unique_items)
                    active_cursor.posts_extracted += len(page_data['items'])
                    active_cursor.position += len(page_data['items'])

                    if page_data.get('last_post_id'):
                        active_cursor.last_post_id = page_data['last_post_id']

                    self.logger.info(f"📄 Page {page_num + 1}: {len(page_data['items'])} items, {len(unique_items)} unique")

                    # Check if we have enough data
                    if state.posts_extracted_this_session >= request.total_posts_needed:
                        self.logger.info(f"🎯 Target reached: {state.posts_extracted_this_session} posts")
                        break

                    # Cursor rotation check
                    if page_num > 0 and page_num % request.max_pages_per_session == 0:
                        await self._rotate_cursors(extraction_id)
                else:
                    self.logger.warning(f"❌ Page {page_num + 1} extraction failed: {page_data.get('error')}")
                    # Mark cursor as potentially invalid
                    active_cursor.is_valid = False

                # Checkpoint state periodically
                if page_num % request.checkpoint_interval_pages == 0:
                    await self._save_pagination_checkpoint(extraction_id)

                # Update state timestamp
                state.last_updated = datetime.now()

        except Exception as e:
            self.logger.error(f"❌ Advanced pagination failed: {e}")
            if request.auto_resume_on_failure:
                self.logger.info("🔄 Attempting to resume from last checkpoint...")
                await self._attempt_pagination_recovery(extraction_id)

        # Finalize extraction
        total_time = time.time() - start_time
        await self._finalize_advanced_pagination(extraction_id, total_time)

    async def _select_optimal_cursor(self, extraction_id: str, page_num: int) -> Optional[TimelineCursor]:
        """Select the optimal cursor for the current page."""
        cursors = self.timeline_cursors[extraction_id]
        valid_cursors = [c for c in cursors if c.is_valid]

        if not valid_cursors:
            # Try to create a new cursor
            new_cursor = await self._create_emergency_cursor(extraction_id)
            if new_cursor:
                cursors.append(new_cursor)
                return new_cursor
            return None

        # Simple round-robin for now, could be enhanced with performance-based selection
        cursor_index = page_num % len(valid_cursors)
        return valid_cursors[cursor_index]

    async def _create_emergency_cursor(self, extraction_id: str) -> Optional[TimelineCursor]:
        """Create an emergency cursor when all others are invalid."""
        emergency_cursor = TimelineCursor(
            cursor_id=f"emergency_{uuid.uuid4().hex[:8]}",
            position=0,
            timestamp=datetime.now() - timedelta(hours=1)  # Start 1 hour back
        )

        self.logger.info(f"🆘 Created emergency cursor: {emergency_cursor.cursor_id}")
        return emergency_cursor

    async def _calculate_anti_detection_delay(self, extraction_id: str, page_num: int) -> int:
        """Calculate intelligent anti-detection delay."""
        request = self.active_paginations[extraction_id]

        if not request.randomize_pagination_timing:
            return 0

        # Base delay from configuration
        min_delay, max_delay = request.pagination_delay_range

        # Add variability based on page number and time
        import random
        random.seed(page_num + int(time.time()) % 100)

        base_delay = random.randint(min_delay, max_delay)

        # Increase delay for every 10th page (more cautious)
        if page_num > 0 and page_num % 10 == 0:
            base_delay = int(base_delay * 1.5)

        return base_delay

    async def _extract_pagination_page(
        self,
        extraction_id: str,
        page_num: int,
        cursor: TimelineCursor,
        request: AdvancedPaginationRequest
    ) -> Dict[str, Any]:
        """Extract a single page of data using the given cursor."""

        try:
            # Create a batch request for this page
            page_batch_request = {
                'username': request.username,
                'max_posts': request.posts_per_page,
                'pagination_cursor': cursor.cursor_id,
                'cursor_position': cursor.position,
                'extraction_method': 'timeline_pagination',
                'scrape_level': 4
            }

            # For now, simulate page extraction
            # In real implementation, this would call the base extraction system
            extracted_items = await self._simulate_page_extraction(page_batch_request)

            return {
                'success': True,
                'items': extracted_items,
                'page_num': page_num,
                'cursor_id': cursor.cursor_id,
                'last_post_id': extracted_items[-1].get('id') if extracted_items else None
            }

        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'page_num': page_num,
                'cursor_id': cursor.cursor_id
            }

    async def _simulate_page_extraction(self, batch_request: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Simulate page extraction for development purposes."""
        # This would be replaced with actual extraction logic
        simulated_items = []

        for i in range(batch_request['max_posts']):
            item = {
                'id': f"post_{uuid.uuid4().hex[:8]}",
                'url': f"https://x.com/{batch_request['username']}/status/{int(time.time())}{i}",
                'text': f"Simulated post {i} for pagination testing",
                'timestamp': datetime.now() - timedelta(minutes=i*10),
                'author': batch_request['username'],
                'extraction_cursor': batch_request['pagination_cursor'],
                'extraction_position': batch_request['cursor_position'] + i
            }
            simulated_items.append(item)

        return simulated_items

    async def _process_with_advanced_deduplication(
        self,
        extraction_id: str,
        items: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Process items with advanced multi-level deduplication."""

        url_cache = self.url_fingerprints[extraction_id]
        content_cache = self.content_hashes[extraction_id]
        extracted_data = self.extracted_data[extraction_id]
        metrics = self.dedup_metrics[extraction_id]

        unique_items = []

        for item in items:
            metrics.total_items_processed += 1

            # Level 1: URL-based deduplication
            item_url = item.get('url', '')
            if item_url in url_cache:
                metrics.url_duplicates_removed += 1
                continue

            # Level 2: Content-based deduplication
            content_hash = self._generate_content_hash(item)
            if content_hash in content_cache:
                metrics.content_duplicates_removed += 1
                continue

            # Level 3: Temporal deduplication
            if await self._is_temporal_duplicate(extraction_id, item):
                metrics.temporal_duplicates_removed += 1
                continue

            # Level 4: Quality filtering
            if not await self._meets_quality_threshold(extraction_id, item):
                metrics.quality_filtered_items += 1
                continue

            # Item passed all deduplication and quality checks
            url_cache.add(item_url)
            content_cache.add(content_hash)
            unique_items.append(item)

        # Store unique items
        extracted_data.extend(unique_items)
        metrics.final_unique_count = len(extracted_data)

        # Update deduplication rate
        if metrics.total_items_processed > 0:
            total_removed = (metrics.url_duplicates_removed +
                           metrics.content_duplicates_removed +
                           metrics.temporal_duplicates_removed +
                           metrics.quality_filtered_items)
            metrics.deduplication_rate = (total_removed / metrics.total_items_processed) * 100

        return unique_items

    def _generate_content_hash(self, item: Dict[str, Any]) -> str:
        """Generate content hash for deduplication."""
        # Combine key content fields for hashing
        content_fields = [
            item.get('text', ''),
            item.get('author', ''),
            str(item.get('timestamp', ''))
        ]

        content_string = '|'.join(content_fields).lower().strip()
        return hashlib.md5(content_string.encode()).hexdigest()

    async def _is_temporal_duplicate(self, extraction_id: str, item: Dict[str, Any]) -> bool:
        """Check for temporal duplicates (same content posted multiple times)."""
        request = self.active_paginations[extraction_id]

        if not request.enable_advanced_deduplication:
            return False

        item_time = item.get('timestamp')
        if not item_time:
            return False

        # Check against recently extracted items within the temporal window
        temporal_window = timedelta(hours=request.temporal_dedup_window_hours)
        extracted_data = self.extracted_data[extraction_id]

        for existing_item in extracted_data[-100:]:  # Check last 100 items for performance
            existing_time = existing_item.get('timestamp')
            if not existing_time:
                continue

            # Check if within temporal window
            if isinstance(item_time, str):
                item_time = datetime.fromisoformat(item_time.replace('Z', '+00:00'))
            if isinstance(existing_time, str):
                existing_time = datetime.fromisoformat(existing_time.replace('Z', '+00:00'))

            time_diff = abs((item_time - existing_time).total_seconds())
            if time_diff <= temporal_window.total_seconds():
                # Check content similarity
                similarity = self._calculate_content_similarity(item, existing_item)
                if similarity >= request.content_similarity_threshold:
                    return True

        return False

    def _calculate_content_similarity(self, item1: Dict[str, Any], item2: Dict[str, Any]) -> float:
        """Calculate content similarity between two items."""
        text1 = item1.get('text', '').lower().strip()
        text2 = item2.get('text', '').lower().strip()

        if not text1 or not text2:
            return 0.0

        # Simple similarity based on common words
        words1 = set(text1.split())
        words2 = set(text2.split())

        if not words1 or not words2:
            return 0.0

        intersection = words1.intersection(words2)
        union = words1.union(words2)

        return len(intersection) / len(union) if union else 0.0

    async def _meets_quality_threshold(self, extraction_id: str, item: Dict[str, Any]) -> bool:
        """Check if item meets quality threshold."""
        request = self.active_paginations[extraction_id]

        # Basic quality checks
        text = item.get('text', '')
        if len(text.strip()) < 10:  # Too short
            return False

        if text.count('@') > 5:  # Too many mentions (likely spam)
            return False

        # Check for common spam patterns
        spam_indicators = ['buy now', 'click here', 'free money', 'urgent']
        text_lower = text.lower()
        spam_count = sum(1 for indicator in spam_indicators if indicator in text_lower)

        if spam_count >= 2:  # Multiple spam indicators
            return False

        # Quality score calculation (simplified)
        quality_score = 1.0

        # Reduce score for short content
        if len(text) < 50:
            quality_score *= 0.8

        # Reduce score for excessive hashtags
        hashtag_count = text.count('#')
        if hashtag_count > 5:
            quality_score *= 0.7

        return quality_score >= request.quality_score_threshold

    async def _rotate_cursors(self, extraction_id: str) -> None:
        """Rotate cursors to avoid detection and maintain freshness."""
        cursors = self.timeline_cursors[extraction_id]
        state = self.pagination_states[extraction_id]

        # Mark old cursors as needing refresh
        for cursor in cursors:
            cursor_age = datetime.now() - cursor.created_at
            if cursor_age.total_seconds() > self.MAX_CURSOR_LIFETIME_HOURS * 3600:
                cursor.is_valid = False

        # Create new cursors if needed
        valid_cursors = [c for c in cursors if c.is_valid]
        if len(valid_cursors) < 2:
            new_cursor = TimelineCursor(
                cursor_id=f"rotated_{uuid.uuid4().hex[:8]}",
                position=state.posts_extracted_this_session,
                timestamp=datetime.now() - timedelta(minutes=30)
            )
            cursors.append(new_cursor)
            self.logger.info(f"🔄 Cursor rotated: {new_cursor.cursor_id}")

    async def _save_pagination_checkpoint(self, extraction_id: str) -> None:
        """Save pagination checkpoint for recovery."""
        state = self.pagination_states[extraction_id]
        cursors = self.timeline_cursors[extraction_id]
        metrics = self.dedup_metrics[extraction_id]

        checkpoint_data = {
            'extraction_id': extraction_id,
            'pagination_state': {
                'current_page': state.current_page,
                'posts_extracted': state.posts_extracted_this_session,
                'active_cursor_index': state.active_cursor_index,
                'last_updated': state.last_updated.isoformat()
            },
            'cursors': [
                {
                    'cursor_id': c.cursor_id,
                    'position': c.position,
                    'is_valid': c.is_valid,
                    'posts_extracted': c.posts_extracted,
                    'last_post_id': c.last_post_id
                }
                for c in cursors
            ],
            'deduplication_metrics': {
                'total_processed': metrics.total_items_processed,
                'final_unique': metrics.final_unique_count,
                'dedup_rate': metrics.deduplication_rate
            },
            'timestamp': datetime.now().isoformat()
        }

        checkpoint_file = f"/tmp/advanced_pagination_checkpoint_{extraction_id}.json"
        try:
            with open(checkpoint_file, 'w') as f:
                json.dump(checkpoint_data, f, indent=2)
            self.logger.info(f"💾 Pagination checkpoint saved: {checkpoint_file}")
        except Exception as e:
            self.logger.error(f"Failed to save pagination checkpoint: {e}")

    async def _attempt_pagination_recovery(self, extraction_id: str) -> bool:
        """Attempt to recover pagination from last checkpoint."""
        checkpoint_file = f"/tmp/advanced_pagination_checkpoint_{extraction_id}.json"

        try:
            with open(checkpoint_file, 'r') as f:
                checkpoint_data = json.load(f)

            # Restore state from checkpoint
            state = self.pagination_states[extraction_id]
            state.current_page = checkpoint_data['pagination_state']['current_page']
            state.posts_extracted_this_session = checkpoint_data['pagination_state']['posts_extracted']

            # Restore cursor states
            for cursor_data in checkpoint_data['cursors']:
                cursor_id = cursor_data['cursor_id']
                for cursor in self.timeline_cursors[extraction_id]:
                    if cursor.cursor_id == cursor_id:
                        cursor.position = cursor_data['position']
                        cursor.is_valid = cursor_data['is_valid']
                        cursor.posts_extracted = cursor_data['posts_extracted']
                        cursor.last_post_id = cursor_data.get('last_post_id')
                        break

            self.logger.info(f"🔄 Pagination recovery successful from page {state.current_page}")
            return True

        except Exception as e:
            self.logger.error(f"Pagination recovery failed: {e}")
            return False

    async def _finalize_advanced_pagination(self, extraction_id: str, total_time: float) -> None:
        """Finalize advanced pagination with comprehensive reporting."""
        request = self.active_paginations[extraction_id]
        state = self.pagination_states[extraction_id]
        extracted_data = self.extracted_data[extraction_id]
        metrics = self.dedup_metrics[extraction_id]

        # Calculate final metrics
        extraction_rate = (len(extracted_data) / request.total_posts_needed * 100) if request.total_posts_needed > 0 else 0
        pages_processed = state.current_page + 1
        avg_items_per_page = len(extracted_data) / max(pages_processed, 1)

        self.logger.info(f"🎉 ADVANCED PAGINATION COMPLETED: {extraction_id}")
        self.logger.info(f"📊 FINAL RESULTS:")
        self.logger.info(f"   Strategy: {request.pagination_strategy.value}")
        self.logger.info(f"   Requested: {request.total_posts_needed:,} posts")
        self.logger.info(f"   Extracted: {len(extracted_data):,} unique posts")
        self.logger.info(f"   Extraction rate: {extraction_rate:.1f}%")
        self.logger.info(f"   Pages processed: {pages_processed}")
        self.logger.info(f"   Average per page: {avg_items_per_page:.1f} items")
        self.logger.info(f"   Total time: {total_time/60:.1f} minutes")
        self.logger.info(f"   Speed: {len(extracted_data)/(total_time/60):.0f} posts/minute")

        self.logger.info(f"📈 DEDUPLICATION METRICS:")
        self.logger.info(f"   Total processed: {metrics.total_items_processed:,}")
        self.logger.info(f"   URL duplicates removed: {metrics.url_duplicates_removed:,}")
        self.logger.info(f"   Content duplicates removed: {metrics.content_duplicates_removed:,}")
        self.logger.info(f"   Temporal duplicates removed: {metrics.temporal_duplicates_removed:,}")
        self.logger.info(f"   Quality filtered: {metrics.quality_filtered_items:,}")
        self.logger.info(f"   Final unique count: {metrics.final_unique_count:,}")
        self.logger.info(f"   Deduplication rate: {metrics.deduplication_rate:.1f}%")

    def get_pagination_progress(self, extraction_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed pagination progress."""
        if extraction_id not in self.pagination_states:
            return None

        state = self.pagination_states[extraction_id]
        request = self.active_paginations[extraction_id]
        metrics = self.dedup_metrics[extraction_id]
        extracted_data = self.extracted_data[extraction_id]

        return {
            'extraction_id': extraction_id,
            'strategy': request.pagination_strategy.value,
            'current_page': state.current_page,
            'total_pages_estimated': state.total_pages_estimated,
            'posts_extracted': len(extracted_data),
            'posts_needed': request.total_posts_needed,
            'extraction_rate': (len(extracted_data) / request.total_posts_needed * 100) if request.total_posts_needed > 0 else 0,
            'deduplication_metrics': {
                'total_processed': metrics.total_items_processed,
                'deduplication_rate': metrics.deduplication_rate,
                'final_unique_count': metrics.final_unique_count
            },
            'cursors_active': len([c for c in self.timeline_cursors.get(extraction_id, []) if c.is_valid]),
            'last_updated': state.last_updated.isoformat()
        }


# Global advanced pagination manager instance
_advanced_pagination_manager: Optional[AdvancedPaginationManager] = None


def get_advanced_pagination_manager(base_ultra_manager: UltraScaleBatchManager = None) -> AdvancedPaginationManager:
    """Get or create global advanced pagination manager instance."""
    global _advanced_pagination_manager

    if _advanced_pagination_manager is None and base_ultra_manager:
        _advanced_pagination_manager = AdvancedPaginationManager(base_ultra_manager)

    return _advanced_pagination_manager
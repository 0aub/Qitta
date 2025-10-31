"""
Checkpoint Manager for Incremental Twitter Scraping
===================================================

Tracks the last scraped post ID for each username to enable incremental scraping.
Only new posts since the last checkpoint are extracted in subsequent scrapes.

Usage:
------
# Initial full scrape
checkpoint_mgr = CheckpointManager()
posts = scraper.extract_all_posts(username="PlayStationPark")
checkpoint_mgr.save_checkpoint("PlayStationPark", posts)

# Weekly incremental scrape
checkpoint = checkpoint_mgr.load_checkpoint("PlayStationPark")
last_post_id = checkpoint['last_scraped_post_id']
new_posts = scraper.extract_until(username="PlayStationPark", stop_at=last_post_id)
checkpoint_mgr.save_checkpoint("PlayStationPark", new_posts)
"""

import json
import os
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any


class CheckpointManager:
    """Manages checkpoints for incremental Twitter scraping."""

    def __init__(self, checkpoint_dir: str = "/tmp/checkpoints"):
        """
        Initialize checkpoint manager.

        Args:
            checkpoint_dir: Directory to store checkpoint files
        """
        self.checkpoint_dir = checkpoint_dir
        self.logger = logging.getLogger(__name__)

        # Create checkpoint directory if it doesn't exist
        os.makedirs(self.checkpoint_dir, exist_ok=True)
        self.logger.info(f"📁 Checkpoint directory: {self.checkpoint_dir}")

    def _get_checkpoint_path(self, username: str) -> str:
        """Get the file path for a username's checkpoint."""
        # Normalize username (remove @ if present)
        clean_username = username.lstrip('@')
        return os.path.join(self.checkpoint_dir, f"{clean_username}_checkpoint.json")

    def save_checkpoint(self, username: str, posts: List[Dict[str, Any]]) -> None:
        """
        Save checkpoint with the newest post ID from extracted posts.

        Args:
            username: Twitter username
            posts: List of extracted posts (newest first)
        """
        if not posts:
            self.logger.warning(f"⚠️ No posts to save checkpoint for @{username}")
            return

        # Get newest post (first in list - timeline is reverse chronological)
        newest_post = posts[0]
        post_id = newest_post.get('id') or newest_post.get('post_id')

        if not post_id:
            self.logger.error(f"❌ Cannot save checkpoint: No post ID found in newest post")
            return

        checkpoint = {
            "username": username,
            "last_scraped_post_id": post_id,
            "last_scraped_at": datetime.now(timezone.utc).isoformat(),
            "total_posts_in_scrape": len(posts),
            "last_post_date": newest_post.get('date') or newest_post.get('created_at'),
            "last_post_text": newest_post.get('text', '')[:100],  # First 100 chars
            "last_post_url": newest_post.get('url', ''),
        }

        checkpoint_path = self._get_checkpoint_path(username)

        try:
            with open(checkpoint_path, 'w') as f:
                json.dump(checkpoint, f, indent=2)

            self.logger.info(f"✅ Checkpoint saved for @{username}")
            self.logger.info(f"   Last post ID: {post_id}")
            self.logger.info(f"   Posts in scrape: {len(posts)}")
            self.logger.info(f"   File: {checkpoint_path}")

        except Exception as e:
            self.logger.error(f"❌ Failed to save checkpoint for @{username}: {e}")

    def load_checkpoint(self, username: str) -> Optional[Dict[str, Any]]:
        """
        Load checkpoint for a username.

        Args:
            username: Twitter username

        Returns:
            Checkpoint dict if exists, None otherwise
        """
        checkpoint_path = self._get_checkpoint_path(username)

        if not os.path.exists(checkpoint_path):
            self.logger.info(f"📭 No checkpoint found for @{username}")
            return None

        try:
            with open(checkpoint_path, 'r') as f:
                checkpoint = json.load(f)

            self.logger.info(f"📦 Checkpoint loaded for @{username}")
            self.logger.info(f"   Last scraped: {checkpoint.get('last_scraped_at')}")
            self.logger.info(f"   Last post ID: {checkpoint.get('last_scraped_post_id')}")

            return checkpoint

        except Exception as e:
            self.logger.error(f"❌ Failed to load checkpoint for @{username}: {e}")
            return None

    def get_last_post_id(self, username: str) -> Optional[str]:
        """
        Get the last scraped post ID for a username.

        Args:
            username: Twitter username

        Returns:
            Last scraped post ID if checkpoint exists, None otherwise
        """
        checkpoint = self.load_checkpoint(username)
        if checkpoint:
            return checkpoint.get('last_scraped_post_id')
        return None

    def checkpoint_exists(self, username: str) -> bool:
        """
        Check if a checkpoint exists for a username.

        Args:
            username: Twitter username

        Returns:
            True if checkpoint exists, False otherwise
        """
        checkpoint_path = self._get_checkpoint_path(username)
        return os.path.exists(checkpoint_path)

    def delete_checkpoint(self, username: str) -> bool:
        """
        Delete checkpoint for a username.

        Args:
            username: Twitter username

        Returns:
            True if checkpoint was deleted, False if it didn't exist
        """
        checkpoint_path = self._get_checkpoint_path(username)

        if os.path.exists(checkpoint_path):
            try:
                os.remove(checkpoint_path)
                self.logger.info(f"🗑️ Checkpoint deleted for @{username}")
                return True
            except Exception as e:
                self.logger.error(f"❌ Failed to delete checkpoint for @{username}: {e}")
                return False
        else:
            self.logger.info(f"📭 No checkpoint to delete for @{username}")
            return False

    def get_checkpoint_info(self, username: str) -> Optional[Dict[str, Any]]:
        """
        Get full checkpoint info including file metadata.

        Args:
            username: Twitter username

        Returns:
            Dict with checkpoint data and file info, or None if not found
        """
        checkpoint = self.load_checkpoint(username)
        if not checkpoint:
            return None

        checkpoint_path = self._get_checkpoint_path(username)
        file_stats = os.stat(checkpoint_path)

        return {
            **checkpoint,
            "checkpoint_file": checkpoint_path,
            "file_size_bytes": file_stats.st_size,
            "file_modified_at": datetime.fromtimestamp(file_stats.st_mtime, tz=timezone.utc).isoformat(),
        }

    def list_all_checkpoints(self) -> List[str]:
        """
        List all usernames that have checkpoints.

        Returns:
            List of usernames with checkpoints
        """
        if not os.path.exists(self.checkpoint_dir):
            return []

        checkpoints = []
        for filename in os.listdir(self.checkpoint_dir):
            if filename.endswith('_checkpoint.json'):
                username = filename.replace('_checkpoint.json', '')
                checkpoints.append(username)

        return checkpoints

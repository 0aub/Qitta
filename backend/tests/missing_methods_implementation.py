#!/usr/bin/env python3
"""
Missing Methods Implementation for Twitter Scraper
=================================================

This script contains the implementation of the 5 missing extraction methods
that are causing the Twitter scraper to fail:

1. _extract_user_likes
2. _extract_user_mentions
3. _extract_user_media
4. _extract_user_followers
5. _extract_user_following
"""

import asyncio
import random
from typing import List, Dict, Any


class MissingMethodsImplementation:
    """Contains implementations for all missing Twitter extraction methods."""

    def __init__(self, page=None, logger=None):
        self.page = page  # Playwright page object
        self.logger = logger or self._create_dummy_logger()

    def _create_dummy_logger(self):
        """Create a simple logger for testing."""
        class DummyLogger:
            def info(self, msg): print(f"INFO: {msg}")
            def warning(self, msg): print(f"WARNING: {msg}")
            def error(self, msg): print(f"ERROR: {msg}")
        return DummyLogger()

    async def _human_delay(self, min_seconds: float = 1.0, max_seconds: float = 3.0):
        """Simulate human-like delay between actions."""
        delay = random.uniform(min_seconds, max_seconds)
        await asyncio.sleep(delay)

    async def _extract_user_likes(self, username: str, max_likes: int = 10) -> List[Dict[str, Any]]:
        """
        Extract a user's liked tweets.

        Args:
            username: Twitter username (without @)
            max_likes: Maximum number of likes to extract

        Returns:
            List of liked tweet dictionaries
        """
        self.logger.info(f"💖 Extracting likes for @{username} (max: {max_likes})")

        try:
            # Navigate to user's likes page
            likes_url = f"https://x.com/{username}/likes"
            await self.page.goto(likes_url, wait_until='domcontentloaded', timeout=30000)
            await self._human_delay(3, 5)

            # Check if page is accessible (user might have protected tweets)
            if await self.page.locator('text=These posts are protected').is_visible():
                self.logger.warning(f"⚠️ @{username} has protected tweets - likes not accessible")
                return []

            # Look for the "Likes" tab and ensure we're on the right page
            likes_tab = self.page.locator('[role="tab"]:has-text("Likes")')
            if await likes_tab.is_visible():
                await likes_tab.click()
                await self._human_delay(2, 3)

            likes = []
            tweets_processed = 0

            # Scroll and collect liked tweets
            for scroll_attempt in range(5):  # Limit scrolling attempts
                if tweets_processed >= max_likes:
                    break

                # Find tweet articles
                tweet_elements = self.page.locator('[data-testid="tweet"]').all()

                for tweet_element in await tweet_elements:
                    if tweets_processed >= max_likes:
                        break

                    try:
                        # Extract tweet data
                        tweet_data = await self._extract_tweet_from_element(tweet_element)
                        if tweet_data:
                            # Add context that this is a liked tweet
                            tweet_data['interaction_type'] = 'like'
                            tweet_data['liked_by'] = username
                            likes.append(tweet_data)
                            tweets_processed += 1

                    except Exception as e:
                        self.logger.warning(f"⚠️ Failed to extract liked tweet: {e}")
                        continue

                # Scroll for more content
                if tweets_processed < max_likes:
                    await self.page.keyboard.press('PageDown')
                    await self._human_delay(2, 4)

            self.logger.info(f"✅ Extracted {len(likes)} liked tweets for @{username}")
            return likes

        except Exception as e:
            self.logger.error(f"❌ Failed to extract likes for @{username}: {e}")
            return []

    async def _extract_user_mentions(self, username: str, max_mentions: int = 10) -> List[Dict[str, Any]]:
        """
        Extract mentions of a user (tweets mentioning @username).

        Args:
            username: Twitter username (without @)
            max_mentions: Maximum number of mentions to extract

        Returns:
            List of mention tweet dictionaries
        """
        self.logger.info(f"@️⃣ Extracting mentions for @{username} (max: {max_mentions})")

        try:
            # Search for mentions of the user
            search_query = f"@{username}"
            search_url = f"https://x.com/search?q={search_query}&src=typed_query&f=live"

            await self.page.goto(search_url, wait_until='domcontentloaded', timeout=30000)
            await self._human_delay(3, 5)

            mentions = []
            tweets_processed = 0

            # Scroll and collect mentioning tweets
            for scroll_attempt in range(3):  # Limit scrolling for mentions
                if tweets_processed >= max_mentions:
                    break

                # Find tweet articles
                tweet_elements = self.page.locator('[data-testid="tweet"]').all()

                for tweet_element in await tweet_elements:
                    if tweets_processed >= max_mentions:
                        break

                    try:
                        # Extract tweet data
                        tweet_data = await self._extract_tweet_from_element(tweet_element)
                        if tweet_data and tweet_data.get('text'):
                            # Verify it actually mentions the user
                            if f"@{username}" in tweet_data['text'].lower():
                                tweet_data['interaction_type'] = 'mention'
                                tweet_data['mentions_user'] = username
                                mentions.append(tweet_data)
                                tweets_processed += 1

                    except Exception as e:
                        self.logger.warning(f"⚠️ Failed to extract mention tweet: {e}")
                        continue

                # Scroll for more content
                if tweets_processed < max_mentions:
                    await self.page.keyboard.press('PageDown')
                    await self._human_delay(2, 4)

            self.logger.info(f"✅ Extracted {len(mentions)} mentions for @{username}")
            return mentions

        except Exception as e:
            self.logger.error(f"❌ Failed to extract mentions for @{username}: {e}")
            return []

    async def _extract_user_media(self, username: str, max_media: int = 15) -> List[Dict[str, Any]]:
        """
        Extract media posts from a user (images, videos, etc.).

        Args:
            username: Twitter username (without @)
            max_media: Maximum number of media posts to extract

        Returns:
            List of media post dictionaries
        """
        self.logger.info(f"🖼️ Extracting media for @{username} (max: {max_media})")

        try:
            # Navigate to user's media page
            media_url = f"https://x.com/{username}/media"
            await self.page.goto(media_url, wait_until='domcontentloaded', timeout=30000)
            await self._human_delay(3, 5)

            # Check if user has media tab available
            media_tab = self.page.locator('[role="tab"]:has-text("Media")')
            if await media_tab.is_visible():
                await media_tab.click()
                await self._human_delay(2, 3)

            media_posts = []
            posts_processed = 0

            # Scroll and collect media posts
            for scroll_attempt in range(4):  # More scrolling for media
                if posts_processed >= max_media:
                    break

                # Find tweet articles that contain media
                tweet_elements = self.page.locator('[data-testid="tweet"]').all()

                for tweet_element in await tweet_elements:
                    if posts_processed >= max_media:
                        break

                    try:
                        # Check if tweet has media
                        media_container = tweet_element.locator('[data-testid="tweetPhoto"], [data-testid="videoPlayer"], [data-testid="card.wrapper"]')

                        if await media_container.count() > 0:
                            # Extract tweet data
                            tweet_data = await self._extract_tweet_from_element(tweet_element)
                            if tweet_data:
                                # Extract media information
                                media_info = await self._extract_media_from_tweet(tweet_element)
                                tweet_data['media'] = media_info
                                tweet_data['has_media'] = True
                                tweet_data['content_type'] = 'media_post'

                                media_posts.append(tweet_data)
                                posts_processed += 1

                    except Exception as e:
                        self.logger.warning(f"⚠️ Failed to extract media post: {e}")
                        continue

                # Scroll for more content
                if posts_processed < max_media:
                    await self.page.keyboard.press('PageDown')
                    await self._human_delay(2, 4)

            self.logger.info(f"✅ Extracted {len(media_posts)} media posts for @{username}")
            return media_posts

        except Exception as e:
            self.logger.error(f"❌ Failed to extract media for @{username}: {e}")
            return []

    async def _extract_user_followers(self, username: str, max_followers: int = 25) -> List[Dict[str, Any]]:
        """
        Extract a user's followers list.

        Args:
            username: Twitter username (without @)
            max_followers: Maximum number of followers to extract

        Returns:
            List of follower user dictionaries
        """
        self.logger.info(f"👥 Extracting followers for @{username} (max: {max_followers})")

        try:
            # Navigate to user's followers page
            followers_url = f"https://x.com/{username}/verified_followers"
            await self.page.goto(followers_url, wait_until='domcontentloaded', timeout=30000)
            await self._human_delay(3, 5)

            # Check if followers list is accessible
            if await self.page.locator('text=Who can see this account\'s posts').is_visible():
                self.logger.warning(f"⚠️ @{username} has protected account - followers not accessible")
                return []

            followers = []
            users_processed = 0

            # Scroll and collect followers
            for scroll_attempt in range(3):  # Limited scrolling for followers
                if users_processed >= max_followers:
                    break

                # Find user cells
                user_elements = self.page.locator('[data-testid="UserCell"]').all()

                for user_element in await user_elements:
                    if users_processed >= max_followers:
                        break

                    try:
                        # Extract user data
                        user_data = await self._extract_user_from_element(user_element)
                        if user_data:
                            user_data['relationship'] = 'follower'
                            user_data['follows_user'] = username
                            followers.append(user_data)
                            users_processed += 1

                    except Exception as e:
                        self.logger.warning(f"⚠️ Failed to extract follower: {e}")
                        continue

                # Scroll for more content
                if users_processed < max_followers:
                    await self.page.keyboard.press('PageDown')
                    await self._human_delay(2, 4)

            self.logger.info(f"✅ Extracted {len(followers)} followers for @{username}")
            return followers

        except Exception as e:
            self.logger.error(f"❌ Failed to extract followers for @{username}: {e}")
            return []

    async def _extract_user_following(self, username: str, max_following: int = 20) -> List[Dict[str, Any]]:
        """
        Extract users that a user is following.

        Args:
            username: Twitter username (without @)
            max_following: Maximum number of following users to extract

        Returns:
            List of following user dictionaries
        """
        self.logger.info(f"➡️ Extracting following for @{username} (max: {max_following})")

        try:
            # Navigate to user's following page
            following_url = f"https://x.com/{username}/following"
            await self.page.goto(following_url, wait_until='domcontentloaded', timeout=30000)
            await self._human_delay(3, 5)

            # Check if following list is accessible
            if await self.page.locator('text=Who can see this account\'s posts').is_visible():
                self.logger.warning(f"⚠️ @{username} has protected account - following not accessible")
                return []

            following = []
            users_processed = 0

            # Scroll and collect following
            for scroll_attempt in range(3):  # Limited scrolling for following
                if users_processed >= max_following:
                    break

                # Find user cells
                user_elements = self.page.locator('[data-testid="UserCell"]').all()

                for user_element in await user_elements:
                    if users_processed >= max_following:
                        break

                    try:
                        # Extract user data
                        user_data = await self._extract_user_from_element(user_element)
                        if user_data:
                            user_data['relationship'] = 'following'
                            user_data['followed_by_user'] = username
                            following.append(user_data)
                            users_processed += 1

                    except Exception as e:
                        self.logger.warning(f"⚠️ Failed to extract following user: {e}")
                        continue

                # Scroll for more content
                if users_processed < max_following:
                    await self.page.keyboard.press('PageDown')
                    await self._human_delay(2, 4)

            self.logger.info(f"✅ Extracted {len(following)} following users for @{username}")
            return following

        except Exception as e:
            self.logger.error(f"❌ Failed to extract following for @{username}: {e}")
            return []

    async def _extract_media_from_tweet(self, tweet_element) -> List[Dict[str, Any]]:
        """Extract media information from a tweet element."""
        media_items = []

        try:
            # Images
            images = tweet_element.locator('[data-testid="tweetPhoto"] img').all()
            for img in await images:
                try:
                    src = await img.get_attribute('src')
                    alt = await img.get_attribute('alt')
                    if src:
                        media_items.append({
                            'type': 'image',
                            'url': src,
                            'alt_text': alt or '',
                        })
                except:
                    continue

            # Videos
            videos = tweet_element.locator('[data-testid="videoPlayer"] video').all()
            for video in await videos:
                try:
                    src = await video.get_attribute('src')
                    poster = await video.get_attribute('poster')
                    if src or poster:
                        media_items.append({
                            'type': 'video',
                            'url': src or poster,
                            'poster_url': poster,
                        })
                except:
                    continue

            # Links/Cards
            cards = tweet_element.locator('[data-testid="card.wrapper"]').all()
            for card in await cards:
                try:
                    link = await card.locator('a').first.get_attribute('href')
                    if link:
                        media_items.append({
                            'type': 'link',
                            'url': link,
                        })
                except:
                    continue

        except Exception as e:
            self.logger.warning(f"⚠️ Failed to extract media: {e}")

        return media_items

    async def _extract_tweet_from_element(self, tweet_element) -> Dict[str, Any]:
        """Extract tweet data from a tweet element (placeholder - needs existing implementation)."""
        # This method should already exist in the Twitter scraper
        # This is just a placeholder to show the expected interface
        return {
            'text': 'Sample tweet text',
            'author': 'sample_user',
            'date': '2025-01-01',
            'likes': 0,
            'retweets': 0,
            'replies': 0
        }

    async def _extract_user_from_element(self, user_element) -> Dict[str, Any]:
        """Extract user data from a user element (placeholder - needs existing implementation)."""
        # This method should already exist in the Twitter scraper
        # This is just a placeholder to show the expected interface
        return {
            'username': 'sample_user',
            'display_name': 'Sample User',
            'bio': 'Sample bio',
            'verified': False,
            'followers_count': 100
        }


def generate_method_code_for_insertion():
    """Generate the actual method code to be inserted into twitter.py."""

    code = '''
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
                        tweet_data = await self._extract_tweet_from_element(tweet_element)
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
                        tweet_data = await self._extract_tweet_from_element(tweet_element)
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
                            tweet_data = await self._extract_tweet_from_element(tweet_element)
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

            if await self.page.locator('text=Who can see this account\\'s posts').is_visible():
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

            if await self.page.locator('text=Who can see this account\\'s posts').is_visible():
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
        """Extract media information from a tweet element."""
        media_items = []

        try:
            # Images
            images = await tweet_element.locator('[data-testid="tweetPhoto"] img').all()
            for img in images:
                try:
                    src = await img.get_attribute('src')
                    alt = await img.get_attribute('alt')
                    if src:
                        media_items.append({
                            'type': 'image',
                            'url': src,
                            'alt_text': alt or '',
                        })
                except:
                    continue

            # Videos
            videos = await tweet_element.locator('[data-testid="videoPlayer"] video').all()
            for video in videos:
                try:
                    src = await video.get_attribute('src')
                    poster = await video.get_attribute('poster')
                    if src or poster:
                        media_items.append({
                            'type': 'video',
                            'url': src or poster,
                            'poster_url': poster,
                        })
                except:
                    continue

        except Exception as e:
            self.logger.warning(f"⚠️ Failed to extract media: {e}")

        return media_items
'''

    return code.strip()


if __name__ == "__main__":
    print("🔧 MISSING METHODS IMPLEMENTATION READY")
    print("=" * 50)
    print("✅ 5 missing extraction methods implemented")
    print("✅ Proper error handling included")
    print("✅ Human-like delays included")
    print("✅ Type-safe return values")
    print("✅ Comprehensive logging")
    print("\n📋 Methods implemented:")
    print("   1. _extract_user_likes")
    print("   2. _extract_user_mentions")
    print("   3. _extract_user_media")
    print("   4. _extract_user_followers")
    print("   5. _extract_user_following")
    print("   6. _extract_media_from_tweet (helper)")
    print("\n🔧 Ready to be integrated into twitter.py")
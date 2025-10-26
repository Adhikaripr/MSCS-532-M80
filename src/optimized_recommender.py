"""
Optimized Recommendation Engine with Caching
Phase 3: Caching, precomputation, and parallel processing
"""

import heapq
import time
from functools import lru_cache
from typing import List, Dict, Any, Set, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing as mp
from collections import defaultdict

from user import UserStore
from product import ProductStore


class OptimizedRecommenderSystem:
    """
    Optimized recommendation system with:
    - Caching for similarity calculations
    - Precomputed similarity matrices
    - Parallel processing for batch operations
    - Memory-efficient recommendation generation
    """
    
    def __init__(self, user_store: UserStore, product_store: ProductStore):
        self.user_store = user_store
        self.product_store = product_store
        
        # Caching layers
        self.similarity_cache: Dict[Tuple[str, str], float] = {}
        self.recommendation_cache: Dict[Tuple[str, int, str], List[str]] = {}
        self.user_preference_cache: Dict[str, Dict[str, float]] = {}
        
        # Precomputed data
        self.similarity_matrix: Optional[Dict[Tuple[str, str], float]] = None
        self.precomputed_similarities: Dict[str, List[Tuple[str, float]]] = {}
        
        # Performance settings
        self.cache_ttl = 3600  # 1 hour
        self.cache_timestamps: Dict[str, float] = {}
        self.max_cache_size = 10000
        
        # Parallel processing
        self.num_workers = min(mp.cpu_count(), 8)  # Limit to 8 workers
    
    def _is_cache_valid(self, key: str) -> bool:
        """Check if cache entry is still valid"""
        if key not in self.cache_timestamps:
            return False
        return time.time() - self.cache_timestamps[key] < self.cache_ttl
    
    def _update_cache_timestamp(self, key: str):
        """Update cache timestamp"""
        self.cache_timestamps[key] = time.time()
    
    def _clean_cache(self):
        """Clean expired cache entries"""
        current_time = time.time()
        expired_keys = [
            key for key, timestamp in self.cache_timestamps.items()
            if current_time - timestamp >= self.cache_ttl
        ]
        
        for key in expired_keys:
            self.cache_timestamps.pop(key, None)
            self.similarity_cache.pop(key, None)
            self.recommendation_cache.pop(key, None)
            self.user_preference_cache.pop(key, None)
    
    @lru_cache(maxsize=1000)
    def _compute_cosine_similarity_cached(self, user1_id: str, user2_id: str) -> float:
        """Cached cosine similarity computation"""
        if user1_id not in self.user_store.users or user2_id not in self.user_store.users:
            return 0.0
        
        user1 = self.user_store.users[user1_id]
        user2 = self.user_store.users[user2_id]
        
        prefs1 = user1.get_preferences(normalized=True)
        prefs2 = user2.get_preferences(normalized=True)
        
        # Get common categories
        common_cats = set(prefs1.keys()) & set(prefs2.keys())
        if not common_cats:
            return 0.0
        
        # Calculate cosine similarity
        dot_product = sum(prefs1[cat] * prefs2[cat] for cat in common_cats)
        magnitude1 = sum(prefs1[cat] ** 2 for cat in common_cats) ** 0.5
        magnitude2 = sum(prefs2[cat] ** 2 for cat in common_cats) ** 0.5
        
        if magnitude1 == 0 or magnitude2 == 0:
            return 0.0
        
        return dot_product / (magnitude1 * magnitude2)
    
    def get_similar_users_cached(self, user_id: str, top_n: int = 10) -> List[Tuple[str, float]]:
        """
        Get similar users with caching
        Time Complexity: O(n) with cache hits, O(n²) for cache misses
        """
        cache_key = f"similar_users_{user_id}_{top_n}"
        
        if cache_key in self.precomputed_similarities and self._is_cache_valid(cache_key):
            return self.precomputed_similarities[cache_key][:top_n]
        
        # Compute similarities
        similarities = []
        for other_id in self.user_store.users.keys():
            if other_id == user_id:
                continue
            
            similarity = self._compute_cosine_similarity_cached(user_id, other_id)
            if similarity > 0:
                similarities.append((other_id, similarity))
        
        # Sort by similarity
        similarities.sort(key=lambda x: x[1], reverse=True)
        
        # Cache results
        self.precomputed_similarities[cache_key] = similarities
        self._update_cache_timestamp(cache_key)
        
        return similarities[:top_n]
    
    def precompute_similarity_matrix(self):
        """
        Precompute similarity matrix for all user pairs
        Time Complexity: O(n²) but done once
        """
        print("Precomputing similarity matrix...")
        user_ids = list(self.user_store.users.keys())
        self.similarity_matrix = {}
        
        total_pairs = len(user_ids) * (len(user_ids) - 1) // 2
        processed = 0
        
        for i, user1_id in enumerate(user_ids):
            for user2_id in user_ids[i+1:]:
                similarity = self._compute_cosine_similarity_cached(user1_id, user2_id)
                self.similarity_matrix[(user1_id, user2_id)] = similarity
                self.similarity_matrix[(user2_id, user1_id)] = similarity
                processed += 1
                
                if processed % 1000 == 0:
                    print(f"Processed {processed}/{total_pairs} pairs ({processed/total_pairs*100:.1f}%)")
        
        print("Similarity matrix precomputation completed!")
    
    def get_similar_users_fast(self, user_id: str, top_n: int = 10) -> List[Tuple[str, float]]:
        """
        Get similar users using precomputed matrix
        Time Complexity: O(n) with precomputed matrix
        """
        if not self.similarity_matrix:
            return self.get_similar_users_cached(user_id, top_n)
        
        similarities = []
        for other_id in self.user_store.users.keys():
            if other_id == user_id:
                continue
            
            similarity = self.similarity_matrix.get((user_id, other_id), 0.0)
            if similarity > 0:
                similarities.append((other_id, similarity))
        
        similarities.sort(key=lambda x: x[1], reverse=True)
        return similarities[:top_n]
    
    def recommend_by_category_cached(self, user_id: str, top_k: int = 5) -> List[str]:
        """
        Content-based recommendations with caching
        """
        cache_key = f"content_{user_id}_{top_k}"
        
        if cache_key in self.recommendation_cache and self._is_cache_valid(cache_key):
            return self.recommendation_cache[cache_key]
        
        if user_id not in self.user_store.users:
            return []
        
        user = self.user_store.users[user_id]
        user_items = set(user.get_items())
        recommendations = []
        
        # Get cached preferences
        if user_id in self.user_preference_cache and self._is_cache_valid(f"prefs_{user_id}"):
            preferences = self.user_preference_cache[user_id]
        else:
            preferences = user.get_preferences(normalized=True)
            self.user_preference_cache[user_id] = preferences
            self._update_cache_timestamp(f"prefs_{user_id}")
        
        # Sort categories by preference score
        sorted_categories = sorted(preferences.items(), key=lambda x: x[1], reverse=True)
        
        for category, score in sorted_categories:
            if len(recommendations) >= top_k:
                break
            
            # Use inverted index to get category items
            items_in_category = self.product_store.get_items_by_category(category)
            
            for item in items_in_category:
                if (item not in user_items and
                        item not in recommendations and
                        self._is_item_available(item)):
                    recommendations.append(item)
                
                if len(recommendations) >= top_k:
                    break
        
        result = recommendations[:top_k]
        
        # Cache result
        self.recommendation_cache[cache_key] = result
        self._update_cache_timestamp(cache_key)
        
        return result
    
    def recommend_by_similar_users_cached(self, user_id: str, top_k: int = 5) -> List[str]:
        """
        Collaborative filtering with caching
        """
        cache_key = f"collab_{user_id}_{top_k}"
        
        if cache_key in self.recommendation_cache and self._is_cache_valid(cache_key):
            return self.recommendation_cache[cache_key]
        
        if user_id not in self.user_store.users:
            return []
        
        user = self.user_store.users[user_id]
        user_items = set(user.get_items())
        item_scores = defaultdict(int)
        
        # Get similar users (use fast method if available)
        if self.similarity_matrix:
            similar_users = self.get_similar_users_fast(user_id, 20)
        else:
            similar_users = self.get_similar_users_cached(user_id, 20)
        
        for other_id, similarity in similar_users:
            other_user = self.user_store.users[other_id]
            other_items = set(other_user.get_items())
            
            # Score items from similar users
            for item in other_items - user_items:
                if self._is_item_available(item):
                    item_scores[item] += similarity
        
        # Use Priority Queue to get top-K items
        if not item_scores:
            return []
        
        top_items = heapq.nlargest(top_k, item_scores.items(), key=lambda x: (x[1], x[0]))
        result = [item for item, _ in top_items]
        
        # Cache result
        self.recommendation_cache[cache_key] = result
        self._update_cache_timestamp(cache_key)
        
        return result
    
    def recommend_hybrid_optimized(self, user_id: str, top_k: int = 5,
                                  content_weight: float = 0.7, collab_weight: float = 0.3) -> List[str]:
        """
        Optimized hybrid recommendations with caching
        """
        cache_key = f"hybrid_{user_id}_{top_k}_{content_weight}_{collab_weight}"
        
        if cache_key in self.recommendation_cache and self._is_cache_valid(cache_key):
            return self.recommendation_cache[cache_key]
        
        # Get recommendations from both methods
        content_recs = self.recommend_by_category_cached(user_id, top_k * 2)
        collab_recs = self.recommend_by_similar_users_cached(user_id, top_k * 2)
        
        scored_items = defaultdict(float)
        
        # Score content-based recommendations
        for i, item in enumerate(content_recs):
            score = content_weight * (1.0 - i / len(content_recs)) if content_recs else 0
            scored_items[item] += score
        
        # Score collaborative recommendations
        for i, item in enumerate(collab_recs):
            score = collab_weight * (1.0 - i / len(collab_recs)) if collab_recs else 0
            scored_items[item] += score
        
        # Get top-K items using priority queue
        top_items = heapq.nlargest(top_k, scored_items.items(), key=lambda x: x[1])
        result = [item for item, _ in top_items]
        
        # Cache result
        self.recommendation_cache[cache_key] = result
        self._update_cache_timestamp(cache_key)
        
        return result
    
    def recommend_batch_parallel(self, user_ids: List[str], top_k: int = 5) -> Dict[str, List[str]]:
        """
        Generate recommendations for multiple users in parallel
        Time Complexity: O(n/p) where p is number of workers
        """
        results = {}
        
        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            # Submit all tasks
            future_to_user = {
                executor.submit(self.recommend_hybrid_optimized, user_id, top_k): user_id
                for user_id in user_ids
            }
            
            # Collect results
            for future in as_completed(future_to_user):
                user_id = future_to_user[future]
                try:
                    results[user_id] = future.result()
                except Exception as e:
                    print(f"Error generating recommendations for {user_id}: {e}")
                    results[user_id] = []
        
        return results
    
    def _compute_chunk_similarities(self, user_chunk: List[str]) -> Dict[Tuple[str, str], float]:
        """Compute similarities for a chunk of users (for parallel processing)"""
        similarities = {}
        
        for user1_id in user_chunk:
            for user2_id in self.user_store.users.keys():
                if user1_id != user2_id:
                    similarity = self._compute_cosine_similarity_cached(user1_id, user2_id)
                    if similarity > 0:
                        similarities[(user1_id, user2_id)] = similarity
        
        return similarities
    
    def precompute_similarities_parallel(self):
        """
        Precompute user similarities using parallel processing
        """
        print("Precomputing similarities in parallel...")
        user_ids = list(self.user_store.users.keys())
        chunk_size = len(user_ids) // self.num_workers
        
        with mp.Pool(processes=self.num_workers) as pool:
            chunks = [user_ids[i:i + chunk_size] for i in range(0, len(user_ids), chunk_size)]
            results = pool.map(self._compute_chunk_similarities, chunks)
        
        # Merge results
        self.similarity_matrix = {}
        for result in results:
            self.similarity_matrix.update(result)
        
        print("Parallel similarity precomputation completed!")
    
    def _is_item_available(self, item_id: str) -> bool:
        """Check if item is available"""
        product = self.product_store.get_product(item_id)
        if not product:
            return False
        
        stock = product.get_info('stock')
        availability = product.get_info('availability')
        
        return (stock is None or stock > 0) and availability != 'out_of_stock'
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get caching statistics"""
        current_time = time.time()
        valid_entries = sum(1 for ts in self.cache_timestamps.values() 
                          if current_time - ts < self.cache_ttl)
        
        return {
            'similarity_cache_size': len(self.similarity_cache),
            'recommendation_cache_size': len(self.recommendation_cache),
            'preference_cache_size': len(self.user_preference_cache),
            'valid_entries': valid_entries,
            'total_entries': len(self.cache_timestamps),
            'cache_hit_ratio': self._calculate_cache_hit_ratio(),
            'memory_usage_mb': self._estimate_memory_usage()
        }
    
    def _calculate_cache_hit_ratio(self) -> float:
        """Calculate cache hit ratio (simplified)"""
        # This would need proper hit/miss tracking in a real implementation
        return 0.85  # Placeholder
    
    def _estimate_memory_usage(self) -> float:
        """Estimate memory usage in MB"""
        total_items = (len(self.similarity_cache) + 
                      len(self.recommendation_cache) + 
                      len(self.user_preference_cache))
        return total_items * 0.001  # Rough estimate
    
    def clear_cache(self):
        """Clear all caches"""
        self.similarity_cache.clear()
        self.recommendation_cache.clear()
        self.user_preference_cache.clear()
        self.cache_timestamps.clear()
        self.precomputed_similarities.clear()
    
    def cleanup_expired_cache(self):
        """Remove expired cache entries"""
        self._clean_cache()

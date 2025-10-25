import heapq
from collections import defaultdict
from typing import List, Dict, Any, Set
from src.user import UserStore
from src.product import ProductStore


class RecommenderSystem:
    def __init__(self, user_store: UserStore, product_store: ProductStore):
        self.user_store = user_store
        self.product_store = product_store

    def recommend_by_category(self, user_id: str, top_k: int = 5) -> List[str]:
        """
        Content-based filtering using:
        - User preferences (Hash Table)
        - Category → Items mapping (Inverted Index)
        - User purchase history (Linked List)
        """
        if user_id not in self.user_store:
            return []

        user = self.user_store.users[user_id]
        user_items = set(user.get_items())
        recommendations = []

        # Get normalized preferences
        preferences = user.get_preferences(normalized=True)

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

        return recommendations[:top_k]

    def recommend_by_similar_users(self, user_id: str, top_k: int = 5) -> List[str]:
        """
        Collaborative filtering using:
        - Hash Table of users
        - Linked List purchase histories
        - Priority Queue for top-K selection
        """
        if user_id not in self.user_store:
            return []

        user = self.user_store.users[user_id]
        user_items = set(user.get_items())
        item_scores = defaultdict(int)

        # Find similar users
        similar_users = self.user_store.get_similar_users(user_id)

        for other_id in similar_users:
            other_user = self.user_store.users[other_id]
            other_items = set(other_user.get_items())

            # Score items from similar users
            for item in other_items - user_items:
                if self._is_item_available(item):
                    item_scores[item] += 1

        # Use Priority Queue to get top-K items
        if not item_scores:
            return []

        top_items = heapq.nlargest(top_k, item_scores.items(), key=lambda x: (x[1], x[0]))
        return [item for item, _ in top_items]

    def recommend_hybrid(self, user_id: str, top_k: int = 5,
                         content_weight: float = 0.7, collab_weight: float = 0.3) -> List[str]:
        """
        Hybrid recommendation combining content-based and collaborative filtering
        """
        content_recs = self.recommend_by_category(user_id, top_k * 2)
        collab_recs = self.recommend_by_similar_users(user_id, top_k * 2)

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
        return [item for item, _ in top_items]

    def recommend_based_on_preferences(self, user_id: str, top_k: int = 5) -> List[str]:
        """Enhanced recommendation using explicit user preferences"""
        if user_id not in self.user_store:
            return []

        user = self.user_store.users[user_id]
        user_items = set(user.get_items())

        # Get user's preferred categories from info
        preferred_categories = []
        for i in range(1, 4):
            category = user.get_info(f'Favorite_Category_{i}')
            if category:
                preferred_categories.append(category)

        recommendations = []

        for category in preferred_categories:
            if len(recommendations) >= top_k:
                break

            items_in_category = self.product_store.get_items_by_category(category)
            for item in items_in_category:
                if (item not in user_items and
                        item not in recommendations and
                        self._is_item_available(item)):
                    recommendations.append(item)

                if len(recommendations) >= top_k:
                    break

        return recommendations[:top_k]

    def _is_item_available(self, item_id: str) -> bool:
        """Check if item is available using product store"""
        product = self.product_store.get_product(item_id)
        if not product:
            return False

        stock = product.get_info('stock')
        availability = product.get_info('availability')

        return (stock is None or stock > 0) and availability != 'out_of_stock'

    def get_recommendation_explanation(self, user_id: str, recommendations: List[str]) -> Dict[str, Any]:
        """Provide explanation for recommendations"""
        explanation = {
            'user_id': user_id,
            'total_recommendations': len(recommendations),
            'recommendations': []
        }

        user = self.user_store.users.get(user_id)
        if not user:
            return explanation

        for item_id in recommendations:
            product = self.product_store.get_product(item_id)
            if product:
                reason = self._explain_recommendation(user, product)
                explanation['recommendations'].append({
                    'item_id': item_id,
                    'name': product.get_info('name'),
                    'category': product.category,
                    'reason': reason
                })

        return explanation

    def _explain_recommendation(self, user, product) -> str:
        """Generate explanation for why item was recommended"""
        category = product.category

        if category in user.preferences:
            return f"Based on your interest in {category}"

        # Check if similar users bought it
        similar_users = self.user_store.get_similar_users(user.user_id)
        for other_id in similar_users:
            other_user = self.user_store.users[other_id]
            if other_user.has_purchased(product.item_id):
                return "Popular among users with similar interests"

        return "Recommended based on overall popularity"
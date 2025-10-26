from collections import defaultdict
from typing import Dict, List, Any, Optional
from linked_list import LinkedPurchaseHistory


class User:
    def __init__(self, user_id: str, info: Dict[str, Any] = None):
        self.user_id = user_id
        self.info = info or {}
        self.purchase_history = LinkedPurchaseHistory()
        self.preferences: Dict[str, float] = defaultdict(float)
        self.category_spending: Dict[str, float] = defaultdict(float)
        self.total_spent = 0.0

    def set_info(self, key: str, value: Any):
        self.info[key] = value

    def get_info(self, key: str) -> Any:
        return self.info.get(key)

    def get_all_info(self) -> Dict[str, Any]:
        return self.info.copy()

    def add_purchase(self, item_id: str, category: str, purchase_data: Dict[str, Any] = None):
        """Add purchase and update preferences based on spending"""
        self.purchase_history.add_purchase(item_id, purchase_data)

        # Update preferences based on spending
        amount = purchase_data.get('total_amount', 0) if purchase_data else 1.0
        self.preferences[category] += amount
        self.category_spending[category] += amount
        self.total_spent += amount

    def set_preference(self, category: str, score: float):
        self.preferences[category] = score

    def get_items(self) -> List[str]:
        return self.purchase_history.to_list()

    def get_preferences(self, normalized: bool = True) -> Dict[str, float]:
        if not normalized or not self.preferences:
            return dict(self.preferences)

        # Normalize preferences
        total = sum(self.preferences.values())
        return {cat: score / total for cat, score in self.preferences.items()}

    def get_top_categories(self, n: int = 3) -> List[str]:
        """Get top n preferred categories"""
        sorted_cats = sorted(self.preferences.items(), key=lambda x: x[1], reverse=True)
        return [cat for cat, _ in sorted_cats[:n]]

    def get_recent_purchases(self, count: int = 5) -> List[str]:
        return self.purchase_history.get_recent_purchases(count)

    def has_purchased(self, item_id: str) -> bool:
        return self.purchase_history.search_purchase(item_id) is not None


class UserStore:
    def __init__(self):
        self.users: Dict[str, User] = {}
        self.user_categories: Dict[str, List[str]] = defaultdict(list)

    def add_user(self, user_id: str, info: Dict[str, Any] = None):
        if user_id not in self.users:
            self.users[user_id] = User(user_id, info)

    def update_user_info(self, user_id: str, key: str, value: Any):
        self.add_user(user_id)
        self.users[user_id].set_info(key, value)

    def get_user_info(self, user_id: str, key: str) -> Any:
        return self.users[user_id].get_info(key) if user_id in self.users else None

    def get_all_user_info(self, user_id: str) -> Dict[str, Any]:
        return self.users[user_id].get_all_info() if user_id in self.users else {}

    def add_purchase(self, user_id: str, item_id: str, category: str, purchase_data: Dict[str, Any] = None):
        self.add_user(user_id)
        self.users[user_id].add_purchase(item_id, category, purchase_data)

        # Update category index
        if category not in self.user_categories[user_id]:
            self.user_categories[user_id].append(category)

    def set_preference(self, user_id: str, category: str, score: float):
        self.add_user(user_id)
        self.users[user_id].set_preference(category, score)

    def get_user_items(self, user_id: str) -> List[str]:
        return self.users[user_id].get_items() if user_id in self.users else []

    def get_user_preferences(self, user_id: str, normalized: bool = True) -> Dict[str, float]:
        return self.users[user_id].get_preferences(normalized) if user_id in self.users else {}

    def get_users_by_category(self, category: str) -> List[str]:
        """Get users who have purchased in this category"""
        return [user_id for user_id, categories in self.user_categories.items()
                if category in categories]

    def get_similar_users(self, user_id: str, min_common_categories: int = 2) -> List[str]:
        """Find users with similar purchase categories"""
        if user_id not in self.users:
            return []

        target_categories = set(self.user_categories.get(user_id, []))
        similar_users = []

        for other_id, categories in self.user_categories.items():
            if other_id == user_id:
                continue
            common_categories = target_categories.intersection(set(categories))
            if len(common_categories) >= min_common_categories:
                similar_users.append(other_id)

        return similar_users

    def __contains__(self, user_id: str) -> bool:
        return user_id in self.users
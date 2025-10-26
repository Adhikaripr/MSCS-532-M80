"""
Optimized Hash Table Implementation
Phase 3: Custom hash table with better collision handling and dynamic resizing
"""

from typing import Any, Optional, List, Tuple
import math


class OptimizedHashTable:
    """
    Custom hash table implementation with:
    - Dynamic resizing based on load factor
    - Chaining for collision resolution
    - Better hash distribution
    - Memory-efficient storage
    """
    
    def __init__(self, initial_capacity: int = 16):
        self.capacity = initial_capacity
        self.size = 0
        self.buckets: List[List[Tuple[str, Any]]] = [[] for _ in range(initial_capacity)]
        self.load_factor_threshold = 0.75
        self.max_load_factor = 0.9
        
    def _hash(self, key: str) -> int:
        """
        Improved hash function with better distribution
        Uses FNV-1a algorithm for better hash distribution
        """
        if not key:
            return 0
            
        hash_value = 2166136261  # FNV offset basis
        for char in key:
            hash_value ^= ord(char)
            hash_value *= 16777619  # FNV prime
            hash_value &= 0xffffffff  # Keep 32-bit
        
        return hash_value % self.capacity
    
    def _resize(self):
        """Resize hash table when load factor exceeds threshold"""
        old_buckets = self.buckets
        old_capacity = self.capacity
        
        # Double the capacity
        self.capacity = old_capacity * 2
        self.buckets = [[] for _ in range(self.capacity)]
        self.size = 0
        
        # Rehash all existing items
        for bucket in old_buckets:
            for key, value in bucket:
                self.insert(key, value)
    
    def insert(self, key: str, value: Any) -> bool:
        """
        Insert key-value pair with collision handling
        Returns True if new key, False if updated existing key
        """
        if not key:
            raise ValueError("Key cannot be empty")
        
        # Check if resize is needed
        if self.size / self.capacity > self.load_factor_threshold:
            self._resize()
        
        index = self._hash(key)
        bucket = self.buckets[index]
        
        # Check if key already exists
        for i, (existing_key, _) in enumerate(bucket):
            if existing_key == key:
                bucket[i] = (key, value)
                return False  # Updated existing key
        
        # Add new key-value pair
        bucket.append((key, value))
        self.size += 1
        return True  # New key
    
    def get(self, key: str) -> Optional[Any]:
        """Get value by key, returns None if not found"""
        if not key:
            return None
            
        index = self._hash(key)
        bucket = self.buckets[index]
        
        for existing_key, value in bucket:
            if existing_key == key:
                return value
        
        return None
    
    def delete(self, key: str) -> bool:
        """Delete key-value pair, returns True if deleted, False if not found"""
        if not key:
            return False
            
        index = self._hash(key)
        bucket = self.buckets[index]
        
        for i, (existing_key, _) in enumerate(bucket):
            if existing_key == key:
                del bucket[i]
                self.size -= 1
                return True
        
        return False
    
    def contains(self, key: str) -> bool:
        """Check if key exists in hash table"""
        return self.get(key) is not None
    
    def keys(self) -> List[str]:
        """Get all keys in the hash table"""
        all_keys = []
        for bucket in self.buckets:
            for key, _ in bucket:
                all_keys.append(key)
        return all_keys
    
    def values(self) -> List[Any]:
        """Get all values in the hash table"""
        all_values = []
        for bucket in self.buckets:
            for _, value in bucket:
                all_values.append(value)
        return all_values
    
    def items(self) -> List[Tuple[str, Any]]:
        """Get all key-value pairs in the hash table"""
        all_items = []
        for bucket in self.buckets:
            all_items.extend(bucket)
        return all_items
    
    def clear(self):
        """Clear all items from hash table"""
        self.buckets = [[] for _ in range(self.capacity)]
        self.size = 0
    
    def get_load_factor(self) -> float:
        """Get current load factor"""
        return self.size / self.capacity
    
    def get_stats(self) -> dict:
        """Get hash table statistics"""
        bucket_sizes = [len(bucket) for bucket in self.buckets]
        max_bucket_size = max(bucket_sizes) if bucket_sizes else 0
        avg_bucket_size = sum(bucket_sizes) / len(bucket_sizes) if bucket_sizes else 0
        
        return {
            'size': self.size,
            'capacity': self.capacity,
            'load_factor': self.get_load_factor(),
            'max_bucket_size': max_bucket_size,
            'avg_bucket_size': avg_bucket_size,
            'empty_buckets': sum(1 for bucket in self.buckets if not bucket)
        }
    
    def __len__(self) -> int:
        return self.size
    
    def __contains__(self, key: str) -> bool:
        return self.contains(key)
    
    def __getitem__(self, key: str) -> Any:
        value = self.get(key)
        if value is None:
            raise KeyError(f"Key '{key}' not found")
        return value
    
    def __setitem__(self, key: str, value: Any):
        self.insert(key, value)
    
    def __delitem__(self, key: str):
        if not self.delete(key):
            raise KeyError(f"Key '{key}' not found")


class OptimizedUserStore:
    """
    Optimized user store using custom hash table
    """
    
    def __init__(self, initial_capacity: int = 1000):
        self.users = OptimizedHashTable(initial_capacity)
        self.user_categories = OptimizedHashTable(initial_capacity)
    
    def add_user(self, user_id: str, info: dict = None):
        """Add user with optimized storage"""
        if user_id not in self.users:
            from user import User
            user = User(user_id, info)
            self.users.insert(user_id, user)
        else:
            # Update existing user info
            user = self.users.get(user_id)
            if info:
                for key, value in info.items():
                    user.set_info(key, value)
    
    def get_user(self, user_id: str):
        """Get user by ID"""
        return self.users.get(user_id)
    
    def update_user_info(self, user_id: str, key: str, value: Any):
        """Update user information"""
        user = self.get_user(user_id)
        if user:
            user.set_info(key, value)
        else:
            self.add_user(user_id, {key: value})
    
    def delete_user(self, user_id: str) -> bool:
        """Delete user and return success status"""
        return self.users.delete(user_id)
    
    def get_all_users(self) -> List[Any]:
        """Get all users"""
        return self.users.values()
    
    def get_user_count(self) -> int:
        """Get total number of users"""
        return len(self.users)
    
    def get_stats(self) -> dict:
        """Get store statistics"""
        return {
            'users': self.users.get_stats(),
            'categories': self.user_categories.get_stats()
        }


class OptimizedProductStore:
    """
    Optimized product store using custom hash table
    """
    
    def __init__(self, initial_capacity: int = 1000):
        self.items = OptimizedHashTable(initial_capacity)
        self.category_index = OptimizedHashTable(initial_capacity)
        self.brand_index = OptimizedHashTable(initial_capacity)
        self.price_index = OptimizedHashTable(initial_capacity)
    
    def add_product(self, item_id: str, category: str, info: dict = None):
        """Add product with optimized indexing"""
        if item_id not in self.items:
            from product import Product
            product = Product(item_id, category, info)
            self.items.insert(item_id, product)
            
            # Update indexes
            self._update_category_index(category, item_id)
            if info and 'brand' in info:
                self._update_brand_index(info['brand'], item_id)
            if info and 'price' in info:
                self._update_price_index(info['price'], item_id)
    
    def _update_category_index(self, category: str, item_id: str):
        """Update category index"""
        existing = self.category_index.get(category)
        if existing is None:
            self.category_index.insert(category, [item_id])
        else:
            if item_id not in existing:
                existing.append(item_id)
                self.category_index.insert(category, existing)
    
    def _update_brand_index(self, brand: str, item_id: str):
        """Update brand index"""
        existing = self.brand_index.get(brand)
        if existing is None:
            self.brand_index.insert(brand, [item_id])
        else:
            if item_id not in existing:
                existing.append(item_id)
                self.brand_index.insert(brand, existing)
    
    def _update_price_index(self, price: float, item_id: str):
        """Update price index"""
        price_range = self._get_price_range(price)
        existing = self.price_index.get(price_range)
        if existing is None:
            self.price_index.insert(price_range, [item_id])
        else:
            if item_id not in existing:
                existing.append(item_id)
                self.price_index.insert(price_range, existing)
    
    def _get_price_range(self, price: float) -> str:
        """Get price range for indexing"""
        if price < 10:
            return "0-10"
        elif price < 50:
            return "10-50"
        elif price < 100:
            return "50-100"
        elif price < 500:
            return "100-500"
        else:
            return "500+"
    
    def get_product(self, item_id: str):
        """Get product by ID"""
        return self.items.get(item_id)
    
    def get_items_by_category(self, category: str) -> List[str]:
        """Get items by category"""
        result = self.category_index.get(category)
        return result if result else []
    
    def get_items_by_brand(self, brand: str) -> List[str]:
        """Get items by brand"""
        result = self.brand_index.get(brand)
        return result if result else []
    
    def get_items_by_price_range(self, min_price: float, max_price: float) -> List[str]:
        """Get items by price range"""
        items = []
        for price_range, item_list in self.price_index.items():
            if self._range_overlaps(price_range, min_price, max_price):
                items.extend(item_list)
        return items
    
    def _range_overlaps(self, price_range: str, min_price: float, max_price: float) -> bool:
        """Check if price range overlaps with given range"""
        if price_range == "500+":
            return max_price >= 500
        elif "-" in price_range:
            range_min, range_max = map(float, price_range.split("-"))
            return not (max_price < range_min or min_price > range_max)
        return False
    
    def delete_product(self, item_id: str) -> bool:
        """Delete product and return success status"""
        product = self.get_product(item_id)
        if not product:
            return False
        
        # Remove from indexes
        category = product.category
        self._remove_from_index(self.category_index, category, item_id)
        
        if hasattr(product, 'info') and product.info:
            brand = product.info.get('brand')
            if brand:
                self._remove_from_index(self.brand_index, brand, item_id)
            
            price = product.info.get('price')
            if price:
                price_range = self._get_price_range(price)
                self._remove_from_index(self.price_index, price_range, item_id)
        
        return self.items.delete(item_id)
    
    def _remove_from_index(self, index: OptimizedHashTable, key: str, item_id: str):
        """Remove item from index"""
        items = index.get(key)
        if items and item_id in items:
            items.remove(item_id)
            if items:
                index.insert(key, items)
            else:
                index.delete(key)
    
    def get_stats(self) -> dict:
        """Get store statistics"""
        return {
            'items': self.items.get_stats(),
            'categories': self.category_index.get_stats(),
            'brands': self.brand_index.get_stats(),
            'prices': self.price_index.get_stats()
        }

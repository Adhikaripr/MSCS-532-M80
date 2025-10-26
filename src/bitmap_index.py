"""
Bitmap Index Implementation
Phase 3: Memory-efficient indexing for category/brand queries
"""

from typing import Set, List, Dict, Any, Optional
import array


class BitmapIndex:
    """
    Bitmap index for efficient set operations and memory usage
    Uses bit arrays to represent set membership
    """
    
    def __init__(self):
        self.bitmaps: Dict[str, Set[int]] = {}
        self.item_to_index: Dict[str, int] = {}
        self.index_to_item: Dict[int, str] = {}
        self.index_counter = 0
        self.total_items = 0
    
    def add_item(self, category: str, item_id: str) -> bool:
        """
        Add item to bitmap index
        Returns True if new item, False if already exists
        """
        # Get or create index for item
        if item_id not in self.item_to_index:
            self.item_to_index[item_id] = self.index_counter
            self.index_to_item[self.index_counter] = item_id
            self.index_counter += 1
            self.total_items += 1
        
        item_index = self.item_to_index[item_id]
        
        # Add to category bitmap
        if category not in self.bitmaps:
            self.bitmaps[category] = set()
        
        is_new = item_index not in self.bitmaps[category]
        self.bitmaps[category].add(item_index)
        
        return is_new
    
    def remove_item(self, category: str, item_id: str) -> bool:
        """
        Remove item from bitmap index
        Returns True if removed, False if not found
        """
        if item_id not in self.item_to_index:
            return False
        
        item_index = self.item_to_index[item_id]
        
        if category in self.bitmaps and item_index in self.bitmaps[category]:
            self.bitmaps[category].remove(item_index)
            
            # Clean up empty categories
            if not self.bitmaps[category]:
                del self.bitmaps[category]
            
            return True
        
        return False
    
    def get_items(self, category: str) -> List[str]:
        """
        Get all items in a category
        Time Complexity: O(k) where k is number of items in category
        """
        if category not in self.bitmaps:
            return []
        
        return [self.index_to_item[idx] for idx in self.bitmaps[category]]
    
    def get_items_intersection(self, categories: List[str]) -> List[str]:
        """
        Get items that belong to ALL specified categories
        Time Complexity: O(min(k1, k2, ..., kn)) where ki is items in category i
        """
        if not categories:
            return []
        
        # Start with first category
        result_indices = self.bitmaps.get(categories[0], set())
        
        # Intersect with remaining categories
        for category in categories[1:]:
            if category not in self.bitmaps:
                return []  # No items if any category is empty
            result_indices = result_indices.intersection(self.bitmaps[category])
        
        return [self.index_to_item[idx] for idx in result_indices]
    
    def get_items_union(self, categories: List[str]) -> List[str]:
        """
        Get items that belong to ANY of the specified categories
        Time Complexity: O(k1 + k2 + ... + kn) where ki is items in category i
        """
        if not categories:
            return []
        
        result_indices = set()
        for category in categories:
            if category in self.bitmaps:
                result_indices.update(self.bitmaps[category])
        
        return [self.index_to_item[idx] for idx in result_indices]
    
    def get_items_difference(self, category1: str, category2: str) -> List[str]:
        """
        Get items in category1 but not in category2
        Time Complexity: O(k1) where k1 is items in category1
        """
        if category1 not in self.bitmaps:
            return []
        
        indices1 = self.bitmaps[category1]
        indices2 = self.bitmaps.get(category2, set())
        
        result_indices = indices1 - indices2
        return [self.index_to_item[idx] for idx in result_indices]
    
    def get_category_count(self, category: str) -> int:
        """Get number of items in category"""
        return len(self.bitmaps.get(category, set()))
    
    def get_total_items(self) -> int:
        """Get total number of unique items"""
        return self.total_items
    
    def get_categories(self) -> List[str]:
        """Get all categories"""
        return list(self.bitmaps.keys())
    
    def get_item_categories(self, item_id: str) -> List[str]:
        """Get all categories for an item"""
        if item_id not in self.item_to_index:
            return []
        
        item_index = self.item_to_index[item_id]
        categories = []
        
        for category, bitmap in self.bitmaps.items():
            if item_index in bitmap:
                categories.append(category)
        
        return categories
    
    def get_stats(self) -> Dict[str, Any]:
        """Get bitmap index statistics"""
        category_sizes = [len(bitmap) for bitmap in self.bitmaps.values()]
        
        return {
            'total_items': self.total_items,
            'total_categories': len(self.bitmaps),
            'avg_items_per_category': sum(category_sizes) / len(category_sizes) if category_sizes else 0,
            'max_items_per_category': max(category_sizes) if category_sizes else 0,
            'min_items_per_category': min(category_sizes) if category_sizes else 0,
            'memory_efficiency': self._calculate_memory_efficiency()
        }
    
    def _calculate_memory_efficiency(self) -> float:
        """Calculate memory efficiency compared to storing full lists"""
        if not self.bitmaps:
            return 1.0
        
        # Estimate memory usage
        total_indices = sum(len(bitmap) for bitmap in self.bitmaps.values())
        total_categories = len(self.bitmaps)
        
        # Bitmap uses sets of integers (roughly 8 bytes per int + overhead)
        bitmap_memory = total_indices * 8 + total_categories * 8
        
        # Full lists would use strings (roughly 50 bytes per string + overhead)
        list_memory = total_indices * 50 + total_categories * 8
        
        return list_memory / bitmap_memory if bitmap_memory > 0 else 1.0


class OptimizedProductIndex:
    """
    Optimized product index using bitmap indexes for multiple attributes
    """
    
    def __init__(self):
        self.category_index = BitmapIndex()
        self.brand_index = BitmapIndex()
        self.price_range_index = BitmapIndex()
        self.availability_index = BitmapIndex()
        
        # Price range mapping
        self.price_ranges = {
            '0-10': (0, 10),
            '10-50': (10, 50),
            '50-100': (50, 100),
            '100-500': (100, 500),
            '500+': (500, float('inf'))
        }
    
    def add_product(self, item_id: str, category: str, brand: str = None, 
                   price: float = None, availability: str = None):
        """Add product to all relevant indexes"""
        self.category_index.add_item(category, item_id)
        
        if brand:
            self.brand_index.add_item(brand, item_id)
        
        if price is not None:
            price_range = self._get_price_range(price)
            self.price_range_index.add_item(price_range, item_id)
        
        if availability:
            self.availability_index.add_item(availability, item_id)
    
    def remove_product(self, item_id: str, category: str, brand: str = None,
                      price: float = None, availability: str = None):
        """Remove product from all relevant indexes"""
        self.category_index.remove_item(category, item_id)
        
        if brand:
            self.brand_index.remove_item(brand, item_id)
        
        if price is not None:
            price_range = self._get_price_range(price)
            self.price_range_index.remove_item(price_range, item_id)
        
        if availability:
            self.availability_index.remove_item(availability, item_id)
    
    def _get_price_range(self, price: float) -> str:
        """Get price range for indexing"""
        for range_name, (min_price, max_price) in self.price_ranges.items():
            if min_price <= price < max_price:
                return range_name
        return '500+'  # Default for very high prices
    
    def get_products_by_category(self, category: str) -> List[str]:
        """Get products by category"""
        return self.category_index.get_items(category)
    
    def get_products_by_brand(self, brand: str) -> List[str]:
        """Get products by brand"""
        return self.brand_index.get_items(brand)
    
    def get_products_by_price_range(self, min_price: float, max_price: float) -> List[str]:
        """Get products by price range"""
        matching_ranges = []
        for range_name, (range_min, range_max) in self.price_ranges.items():
            if not (max_price < range_min or min_price > range_max):
                matching_ranges.append(range_name)
        
        if not matching_ranges:
            return []
        
        # Union of all matching price ranges
        result = []
        for range_name in matching_ranges:
            result.extend(self.price_range_index.get_items(range_name))
        
        return result
    
    def get_products_by_availability(self, availability: str) -> List[str]:
        """Get products by availability"""
        return self.availability_index.get_items(availability)
    
    def get_products_multi_filter(self, category: str = None, brand: str = None,
                                 min_price: float = None, max_price: float = None,
                                 availability: str = None) -> List[str]:
        """
        Get products with multiple filters applied
        Uses bitmap intersection for efficient filtering
        """
        filters = []
        
        if category:
            filters.append(self.category_index.get_items(category))
        
        if brand:
            filters.append(self.brand_index.get_items(brand))
        
        if min_price is not None and max_price is not None:
            price_items = self.get_products_by_price_range(min_price, max_price)
            filters.append(price_items)
        
        if availability:
            filters.append(self.availability_index.get_items(availability))
        
        if not filters:
            return []
        
        # Start with first filter
        result = set(filters[0])
        
        # Intersect with remaining filters
        for filter_items in filters[1:]:
            result = result.intersection(set(filter_items))
        
        return list(result)
    
    def get_products_any_category(self, categories: List[str]) -> List[str]:
        """Get products in any of the specified categories"""
        return self.category_index.get_items_union(categories)
    
    def get_products_all_categories(self, categories: List[str]) -> List[str]:
        """Get products in all specified categories"""
        return self.category_index.get_items_intersection(categories)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive index statistics"""
        return {
            'category_index': self.category_index.get_stats(),
            'brand_index': self.brand_index.get_stats(),
            'price_range_index': self.price_range_index.get_stats(),
            'availability_index': self.availability_index.get_stats()
        }

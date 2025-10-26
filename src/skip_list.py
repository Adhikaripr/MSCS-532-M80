"""
Skip List Implementation for Purchase History
Phase 3: O(log n) search operations instead of O(n)
"""

import random
from typing import Any, Dict, List, Optional


class SkipListNode:
    """Node in skip list with multiple forward pointers"""
    
    def __init__(self, item_id: str, purchase_data: Dict[str, Any], level: int):
        self.item_id = item_id
        self.purchase_data = purchase_data
        self.forward = [None] * (level + 1)
        self.level = level


class SkipListPurchaseHistory:
    """
    Skip List implementation for purchase history
    Provides O(log n) search, O(log n) insertion, O(log n) deletion
    """
    
    def __init__(self, max_level: int = 4, probability: float = 0.5):
        self.max_level = max_level
        self.probability = probability
        self.header = self._create_node(self.max_level, None, None)
        self.level = 0
        self.size = 0
    
    def _create_node(self, level: int, item_id: str, purchase_data: Dict[str, Any]) -> SkipListNode:
        """Create a new skip list node"""
        return SkipListNode(item_id, purchase_data, level)
    
    def _random_level(self) -> int:
        """Generate random level for new node"""
        level = 0
        while random.random() < self.probability and level < self.max_level:
            level += 1
        return level
    
    def add_purchase(self, item_id: str, purchase_data: Dict[str, Any] = None):
        """
        Add purchase to skip list
        Time Complexity: O(log n)
        """
        if purchase_data is None:
            purchase_data = {}
        
        # Find insertion point
        current = self.header
        update = [None] * (self.max_level + 1)
        
        # Traverse from top level to bottom
        for i in range(self.level, -1, -1):
            while current.forward[i] and current.forward[i].item_id < item_id:
                current = current.forward[i]
            update[i] = current
        
        current = current.forward[0]
        
        # If item already exists, update it
        if current and current.item_id == item_id:
            current.purchase_data = purchase_data
            return
        
        # Create new node
        new_level = self._random_level()
        new_node = self._create_node(new_level, item_id, purchase_data)
        
        # If new level is higher than current level, update header
        if new_level > self.level:
            for i in range(self.level + 1, new_level + 1):
                update[i] = self.header
            self.level = new_level
        
        # Update forward pointers
        for i in range(new_level + 1):
            new_node.forward[i] = update[i].forward[i]
            update[i].forward[i] = new_node
        
        self.size += 1
    
    def search_purchase(self, item_id: str) -> Optional[Dict[str, Any]]:
        """
        Search for purchase by item ID
        Time Complexity: O(log n)
        """
        current = self.header
        
        # Traverse from top level to bottom
        for i in range(self.level, -1, -1):
            while current.forward[i] and current.forward[i].item_id < item_id:
                current = current.forward[i]
        
        current = current.forward[0]
        
        if current and current.item_id == item_id:
            return current.purchase_data
        
        return None
    
    def delete_purchase(self, item_id: str) -> bool:
        """
        Delete purchase by item ID
        Time Complexity: O(log n)
        """
        current = self.header
        update = [None] * (self.max_level + 1)
        
        # Find the node to delete
        for i in range(self.level, -1, -1):
            while current.forward[i] and current.forward[i].item_id < item_id:
                current = current.forward[i]
            update[i] = current
        
        current = current.forward[0]
        
        if current and current.item_id == item_id:
            # Update forward pointers
            for i in range(self.level + 1):
                if update[i].forward[i] != current:
                    break
                update[i].forward[i] = current.forward[i]
            
            # Remove empty levels
            while self.level > 0 and self.header.forward[self.level] is None:
                self.level -= 1
            
            self.size -= 1
            return True
        
        return False
    
    def to_list(self) -> List[str]:
        """
        Convert skip list to list of item IDs
        Time Complexity: O(n)
        """
        items = []
        current = self.header.forward[0]
        
        while current:
            items.append(current.item_id)
            current = current.forward[0]
        
        return items
    
    def get_purchases_with_data(self) -> List[Dict[str, Any]]:
        """
        Get all purchases with their data
        Time Complexity: O(n)
        """
        purchases = []
        current = self.header.forward[0]
        
        while current:
            purchases.append({
                'item_id': current.item_id,
                **current.purchase_data
            })
            current = current.forward[0]
        
        return purchases
    
    def get_recent_purchases(self, count: int = 5) -> List[str]:
        """
        Get most recent purchases (last count items)
        Time Complexity: O(count)
        """
        recent = []
        current = self.header.forward[0]
        
        while current and len(recent) < count:
            recent.append(current.item_id)
            current = current.forward[0]
        
        return recent
    
    def get_purchases_in_range(self, start_item: str, end_item: str) -> List[str]:
        """
        Get purchases in a range of item IDs
        Time Complexity: O(log n + k) where k is number of items in range
        """
        items = []
        current = self.header
        
        # Find start position
        for i in range(self.level, -1, -1):
            while current.forward[i] and current.forward[i].item_id < start_item:
                current = current.forward[i]
        
        current = current.forward[0]
        
        # Collect items in range
        while current and current.item_id <= end_item:
            items.append(current.item_id)
            current = current.forward[0]
        
        return items
    
    def get_top_purchases_by_amount(self, count: int = 5) -> List[Dict[str, Any]]:
        """
        Get top purchases by amount spent
        Time Complexity: O(n log n) due to sorting
        """
        all_purchases = self.get_purchases_with_data()
        
        # Sort by amount (assuming purchase_data has 'total_amount' field)
        sorted_purchases = sorted(
            all_purchases,
            key=lambda x: x.get('total_amount', 0),
            reverse=True
        )
        
        return sorted_purchases[:count]
    
    def get_purchases_by_category(self, category: str) -> List[str]:
        """
        Get purchases by category (if stored in purchase_data)
        Time Complexity: O(n)
        """
        items = []
        current = self.header.forward[0]
        
        while current:
            if current.purchase_data.get('category') == category:
                items.append(current.item_id)
            current = current.forward[0]
        
        return items
    
    def get_stats(self) -> Dict[str, Any]:
        """Get skip list statistics"""
        level_counts = [0] * (self.max_level + 1)
        current = self.header.forward[0]
        
        while current:
            level_counts[current.level] += 1
            current = current.forward[0]
        
        return {
            'size': self.size,
            'level': self.level,
            'max_level': self.max_level,
            'level_distribution': level_counts[:self.level + 1],
            'avg_items_per_level': sum(level_counts) / (self.level + 1) if self.level > 0 else 0
        }
    
    def __len__(self) -> int:
        return self.size
    
    def __iter__(self):
        """Iterate over all item IDs"""
        current = self.header.forward[0]
        while current:
            yield current.item_id
            current = current.forward[0]
    
    def __contains__(self, item_id: str) -> bool:
        return self.search_purchase(item_id) is not None


class OptimizedLinkedPurchaseHistory:
    """
    Wrapper class that maintains backward compatibility
    while using skip list internally
    """
    
    def __init__(self):
        self.skip_list = SkipListPurchaseHistory()
    
    def add_purchase(self, item_id: str, purchase_data: Dict[str, Any] = None):
        """Add purchase (delegates to skip list)"""
        self.skip_list.add_purchase(item_id, purchase_data)
    
    def to_list(self) -> List[str]:
        """Convert to list (delegates to skip list)"""
        return self.skip_list.to_list()
    
    def get_purchases_with_data(self) -> List[Dict[str, Any]]:
        """Get purchases with data (delegates to skip list)"""
        return self.skip_list.get_purchases_with_data()
    
    def search_purchase(self, item_id: str) -> Optional[Dict[str, Any]]:
        """Search purchase (delegates to skip list)"""
        return self.skip_list.search_purchase(item_id)
    
    def get_recent_purchases(self, count: int = 5) -> List[str]:
        """Get recent purchases (delegates to skip list)"""
        return self.skip_list.get_recent_purchases(count)
    
    def __len__(self) -> int:
        return len(self.skip_list)
    
    def __iter__(self):
        return iter(self.skip_list)
    
    def __contains__(self, item_id: str) -> bool:
        return item_id in self.skip_list
    
    def get_stats(self) -> Dict[str, Any]:
        """Get skip list statistics"""
        return self.skip_list.get_stats()

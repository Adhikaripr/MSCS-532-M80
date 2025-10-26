from typing import List, Optional, Dict, Any

class Node:
    def __init__(self, item_id: str, purchase_data: Dict[str, Any] = None):
        self.item_id = item_id
        self.purchase_data = purchase_data or {}
        self.next: Optional["Node"] = None

class LinkedPurchaseHistory:
    def __init__(self):
        self.head: Optional[Node] = None
        self.size = 0

    def add_purchase(self, item_id: str, purchase_data: Dict[str, Any] = None):
        """Add purchase to the beginning for O(1) insertion"""
        node = Node(item_id, purchase_data)
        node.next = self.head
        self.head = node
        self.size += 1

    def to_list(self) -> List[str]:
        """Convert linked list to list of item IDs"""
        items = []
        current = self.head
        while current:
            items.append(current.item_id)
            current = current.next
        return items

    def get_purchases_with_data(self) -> List[Dict[str, Any]]:
        """Get all purchases with their data"""
        purchases = []
        current = self.head
        while current:
            purchases.append({
                'item_id': current.item_id,
                **current.purchase_data
            })
            current = current.next
        return purchases

    def search_purchase(self, item_id: str) -> Optional[Dict[str, Any]]:
        """Search for specific purchase - O(n) but optimized for recent purchases"""
        current = self.head
        while current:
            if current.item_id == item_id:
                return current.purchase_data
            current = current.next
        return None

    def get_recent_purchases(self, count: int = 5) -> List[str]:
        """Get most recent purchases"""
        recent = []
        current = self.head
        while current and len(recent) < count:
            recent.append(current.item_id)
            current = current.next
        return recent

    def __len__(self):
        return self.size
from collections import defaultdict
from typing import Dict, List, Optional, Any


class Product:
    def __init__(self, item_id: str, category: str, info: Dict[str, Any] = None):
        self.item_id = item_id
        self.category = category
        self.info = info or {}

    def get_info(self, key: str) -> Any:
        return self.info.get(key)

    def update_info(self, key: str, value: Any):
        self.info[key] = value


class ProductStore:
    def __init__(self):
        self.items: Dict[str, Product] = {}
        self.category_index: Dict[str, List[str]] = defaultdict(list)
        self.brand_index: Dict[str, List[str]] = defaultdict(list)
        self.price_range_index: Dict[str, List[str]] = defaultdict(list)

    def add_product(self, item_id: str, category: str, info: Dict[str, Any] = None):
        """Add product with indexing for fast lookups"""
        product = Product(item_id, category, info)
        self.items[item_id] = product
        self.category_index[category].append(item_id)

        # Additional indexing for enhanced recommendations
        if info:
            brand = info.get('brand', 'unknown')
            self.brand_index[brand].append(item_id)

            # Price range indexing
            price = info.get('price', 0)
            price_range = self._get_price_range(price)
            self.price_range_index[price_range].append(item_id)

    def get_category(self, item_id: str) -> str:
        return self.items[item_id].category if item_id in self.items else "unknown"

    def get_product(self, item_id: str) -> Optional[Product]:
        return self.items.get(item_id)

    def get_items_by_category(self, category: str) -> List[str]:
        return self.category_index.get(category, [])

    def get_items_by_brand(self, brand: str) -> List[str]:
        return self.brand_index.get(brand, [])

    def get_items_by_price_range(self, min_price: float, max_price: float) -> List[str]:
        """Get items within price range using price range index"""
        items = []
        for price_range, item_list in self.price_range_index.items():
            range_min, range_max = self._parse_price_range(price_range)
            if range_min <= max_price and range_max >= min_price:
                items.extend(item_list)
        return items

    def _get_price_range(self, price: float) -> str:
        """Convert price to range for indexing"""
        if price < 50:
            return "0-50"
        elif price < 100:
            return "50-100"
        elif price < 200:
            return "100-200"
        else:
            return "200+"

    def _parse_price_range(self, price_range: str) -> tuple:
        """Parse price range string to min,max"""
        if price_range == "200+":
            return (200, float('inf'))
        parts = price_range.split('-')
        return (float(parts[0]), float(parts[1]))

    def get_all_categories(self) -> List[str]:
        return list(self.category_index.keys())

    def __contains__(self, item_id: str) -> bool:
        return item_id in self.items
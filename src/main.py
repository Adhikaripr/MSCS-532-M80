import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.user import UserStore
from src.product import ProductStore
from src.recommender import RecommenderSystem
from src.csv_loader import CSVLoader


def main():
    # Initialize data stores
    users = UserStore()
    products = ProductStore()
    recommender = RecommenderSystem(users, products)

    # Load data
    print("Loading data...")
    try:
        CSVLoader.load_users("data/users.csv", users)
        CSVLoader.load_products("data/products.csv", products)
        CSVLoader.load_purchases("data/purchase_history.csv", users, products)
        CSVLoader.load_preferences("data/preferences.csv", users)
        print("Data loaded successfully!\n")
    except Exception as e:
        print(f"Error loading data: {e}")
        return

    # Demonstrate system functionality
    print("RECOMMENDATION SYSTEM DEMONSTRATION")
    print("=" * 60)

    # Show sample users
    sample_users = list(users.users.keys())[:3]

    for user_id in sample_users:
        user = users.users[user_id]
        print(f"\n{'=' * 50}")
        print(f"USER: {user_id}")
        print(f"Name: {user.get_info('name')}")
        print(f"Total Purchases: {len(user.get_items())}")
        print(f"Top Categories: {user.get_top_categories(3)}")
        print(f"Total Spent: ${user.total_spent:.2f}")

        # Generate different types of recommendations
        print(f"\nRECOMMENDATIONS:")

        # Category-based
        cat_recs = recommender.recommend_by_category(user_id, 3)
        print(f"By Category ({len(cat_recs)}): {cat_recs}")

        # Similar users
        sim_recs = recommender.recommend_by_similar_users(user_id, 3)
        print(f"By Similar Users ({len(sim_recs)}): {sim_recs}")

        # Hybrid
        hybrid_recs = recommender.recommend_hybrid(user_id, 3)
        print(f"Hybrid ({len(hybrid_recs)}): {hybrid_recs}")

        # Preference-based
        pref_recs = recommender.recommend_based_on_preferences(user_id, 3)
        print(f"Preference-based ({len(pref_recs)}): {pref_recs}")

        # Explanation
        explanation = recommender.get_recommendation_explanation(user_id, hybrid_recs)
        print(f"\nExplanation for hybrid recommendations:")
        for rec in explanation['recommendations']:
            print(f"  - {rec['name']}: {rec['reason']}")

    # Demonstrate data structure operations
    print(f"\n{'=' * 50}")
    print("DATA STRUCTURE OPERATIONS DEMONSTRATION")
    print(f"{'=' * 50}")

    if sample_users:
        user_id = sample_users[0]
        user = users.users[user_id]

        # Linked List operations
        print(f"\nLinked List Operations for user {user_id}:")
        print(f"Purchase history length: {len(user.purchase_history)}")
        print(f"Recent purchases: {user.get_recent_purchases(3)}")

        # Hash Table operations
        print(f"\nHash Table Operations:")
        print(f"User exists: {user_id in users}")
        print(f"User preferences: {dict(user.preferences)}")

        # Inverted Index operations
        print(f"\nInverted Index Operations:")
        categories = products.get_all_categories()[:3]
        for category in categories:
            items = products.get_items_by_category(category)
            print(f"Category '{category}': {len(items)} items")


if __name__ == "__main__":
    main()
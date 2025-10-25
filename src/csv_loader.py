import csv
from typing import Dict, Any
from src.user import UserStore
from src.product import ProductStore


class CSVLoader:
    @staticmethod
    def load_users(path: str, user_store: UserStore):
        try:
            with open(path, 'r', encoding='utf-8') as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    if not row.get('Customer Id'):
                        continue

                    user_id = row['Customer Id'].strip()
                    info = {
                        'name': f"{row.get('First Name', '').strip()} {row.get('Last Name', '').strip()}",
                        'first_name': row.get('First Name', '').strip(),
                        'last_name': row.get('Last Name', '').strip(),
                        'company': row.get('Company', '').strip(),
                        'city': row.get('City', '').strip(),
                        'country': row.get('Country', '').strip(),
                        'email': row.get('Email', '').strip(),
                        'subscription_date': row.get('Subscription Date', '').strip(),
                        'website': row.get('Website', '').strip()
                    }
                    user_store.add_user(user_id, info)
            print(f"Loaded {len(user_store.users)} users from {path}")
        except Exception as e:
            print(f"Error loading users from {path}: {e}")

    @staticmethod
    def load_products(path: str, product_store: ProductStore):
        try:
            with open(path, 'r', encoding='utf-8') as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    if not row.get('Internal ID'):
                        continue

                    item_id = row['Internal ID'].strip()
                    category = row.get('Category', 'Unknown').strip()
                    info = {
                        'name': row.get('Name', '').strip(),
                        'description': row.get('Description', '').strip(),
                        'brand': row.get('Brand', '').strip(),
                        'price': float(row['Price']) if row.get('Price') and row['Price'].strip() else 0.0,
                        'currency': row.get('Currency', 'USD').strip(),
                        'stock': int(row['Stock']) if row.get('Stock') and row['Stock'].strip().isdigit() else 0,
                        'ean': row.get('EAN', '').strip(),
                        'color': row.get('Color', '').strip(),
                        'size': row.get('Size', '').strip(),
                        'availability': row.get('Availability', '').strip()
                    }
                    product_store.add_product(item_id, category, info)
            print(f"Loaded {len(product_store.items)} products from {path}")
        except Exception as e:
            print(f"Error loading products from {path}: {e}")

    @staticmethod
    def load_purchases(path: str, user_store: UserStore, product_store: ProductStore):
        try:
            with open(path, 'r', encoding='utf-8') as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    if not row.get('Customer_Id') or not row.get('Product_ID'):
                        continue

                    user_id = row['Customer_Id'].strip()
                    item_id = row['Product_ID'].strip()
                    category = row.get('Product_Category', 'Unknown').strip()

                    purchase_data = {
                        'purchase_id': row.get('Purchase_ID', '').strip(),
                        'product_name': row.get('Product_Name', '').strip(),
                        'purchase_date': row.get('Purchase_Date', '').strip(),
                        'quantity': int(row['Quantity']) if row.get('Quantity') and row[
                            'Quantity'].strip().isdigit() else 1,
                        'unit_price': float(row['Unit_Price']) if row.get('Unit_Price') and row[
                            'Unit_Price'].strip() else 0.0,
                        'total_amount': float(row['Total_Amount']) if row.get('Total_Amount') and row[
                            'Total_Amount'].strip() else 0.0,
                        'payment_method': row.get('Payment_Method', '').strip(),
                        'order_status': row.get('Order_Status', '').strip()
                    }

                    user_store.add_purchase(user_id, item_id, category, purchase_data)
            print(f"Loaded purchases from {path}")
        except Exception as e:
            print(f"Error loading purchases from {path}: {e}")

    @staticmethod
    def load_preferences(path: str, user_store: UserStore):
        try:
            with open(path, 'r', encoding='utf-8') as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    if not row.get('Customer_Id'):
                        continue

                    user_id = row['Customer_Id'].strip()

                    # Update user info with preference data
                    user_store.update_user_info(user_id, 'average_spending',
                                                float(row['Average_Spending']) if row.get('Average_Spending') else 0.0)
                    user_store.update_user_info(user_id, 'total_purchases',
                                                int(row['Total_Purchases']) if row.get('Total_Purchases') else 0)
                    user_store.update_user_info(user_id, 'preferred_payment',
                                                row.get('Preferred_Payment_Method', '').strip())
                    user_store.update_user_info(user_id, 'last_purchase_date',
                                                row.get('Last_Purchase_Date', '').strip())
                    user_store.update_user_info(user_id, 'customer_segment',
                                                row.get('Customer_Segment', '').strip())

                    # Set category preferences and store in user info
                    for i in range(1, 4):
                        category_key = f'Favorite_Category_{i}'
                        if category_key in row and row[category_key]:
                            category = row[category_key].strip()
                            # Store in user info for preference-based recommendations
                            user_store.update_user_info(user_id, category_key, category)
                            # Higher score for higher ranked categories
                            score = 1.0 - (i - 1) * 0.2  # 1.0, 0.8, 0.6
                            user_store.set_preference(user_id, category, score)

            print(f"Loaded preferences from {path}")
        except Exception as e:
            print(f"Error loading preferences from {path}: {e}")
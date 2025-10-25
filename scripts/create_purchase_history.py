import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Set random seed for reproducibility
np.random.seed(42)

# Read the CSV files
users_df = pd.read_csv('../data/users.csv')
products_df = pd.read_csv('../data/products.csv')

# Get user and product IDs
user_ids = users_df['Customer Id'].tolist()
product_ids = products_df['Internal ID'].tolist()
product_categories = products_df['Category'].unique().tolist()

# ============================================
# 1. CREATE PURCHASE HISTORY CSV
# ============================================

# Generate random purchase history
num_purchases = 2500  # Total number of purchases to generate

purchase_data = []

for i in range(num_purchases):
    # Random user
    user_id = np.random.choice(user_ids)

    # Random product
    product_idx = np.random.randint(0, len(products_df))
    product_id = products_df.iloc[product_idx]['Internal ID']
    product_name = products_df.iloc[product_idx]['Name']
    product_category = products_df.iloc[product_idx]['Category']
    product_price = products_df.iloc[product_idx]['Price']

    # Random purchase date (within last 2 years)
    days_ago = np.random.randint(0, 730)
    purchase_date = datetime.now() - timedelta(days=days_ago)

    # Random quantity (1-5)
    quantity = np.random.randint(1, 6)

    # Calculate total
    total_amount = product_price * quantity

    # Random payment method
    payment_method = np.random.choice(['Credit Card', 'Debit Card', 'PayPal', 'Bank Transfer'])

    # Random order status
    order_status = np.random.choice(['Completed', 'Completed', 'Completed', 'Completed', 'Cancelled', 'Returned'])

    purchase_data.append({
        'Purchase_ID': f'PUR{str(i + 1).zfill(6)}',
        'Customer_Id': user_id,
        'Product_ID': product_id,
        'Product_Name': product_name,
        'Product_Category': product_category,
        'Purchase_Date': purchase_date.strftime('%Y-%m-%d'),
        'Quantity': quantity,
        'Unit_Price': product_price,
        'Total_Amount': total_amount,
        'Payment_Method': payment_method,
        'Order_Status': order_status
    })

purchase_history_df = pd.DataFrame(purchase_data)

# Sort by purchase date
purchase_history_df = purchase_history_df.sort_values('Purchase_Date', ascending=False).reset_index(drop=True)

# Save to CSV
purchase_history_df.to_csv('purchase_history.csv', index=False)

print("=== PURCHASE HISTORY CSV CREATED ===")
print(f"Total purchases: {len(purchase_history_df)}")
print(f"\nFirst few rows:")
print(purchase_history_df.head(10))
print(f"\nShape: {purchase_history_df.shape}")
print(f"\nColumns: {purchase_history_df.columns.tolist()}")

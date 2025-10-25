import pandas as pd

# Initialize empty list
user_preferences_data = []

# Read the CSV files
users_df = pd.read_csv('../data/users.csv')
purchase_history_df = pd.read_csv('../data/purchase_history.csv')
products_df = pd.read_csv('../data/products.csv')

# Get user IDs from the users dataframe
user_ids = users_df['Customer Id'].tolist()

# Get product categories
product_categories = products_df['Category'].unique().tolist()

for user_id in user_ids:
    # Get user's purchase history
    user_purchases = purchase_history_df[
        (purchase_history_df['Customer_Id'] == user_id) &
        (purchase_history_df['Order_Status'] == 'Completed')
    ]

    if len(user_purchases) > 0:
        # Get favorite categories (top 3 most purchased)
        category_counts = user_purchases['Product_Category'].value_counts()
        favorite_categories = category_counts.head(3).index.tolist()

        # Average spending
        avg_spending = user_purchases['Total_Amount'].mean()

        # Total purchases
        total_purchases = len(user_purchases)

        # Preferred payment method (most used)
        preferred_payment = user_purchases['Payment_Method'].mode()[0] if len(user_purchases) > 0 else 'Credit Card'

        # Last purchase date
        last_purchase = user_purchases['Purchase_Date'].max()

        # Customer segment based on purchase behavior
        if total_purchases >= 10 and avg_spending >= 500:
            segment = 'Premium'
        elif total_purchases >= 5:
            segment = 'Regular'
        else:
            segment = 'Occasional'

    else:
        # New customer with no purchases
        import numpy as np
        favorite_categories = np.random.choice(product_categories, min(3, len(product_categories)), replace=False).tolist()
        avg_spending = 0
        total_purchases = 0
        preferred_payment = 'Credit Card'
        last_purchase = None
        segment = 'New'

    user_preferences_data.append({
        'Customer_Id': user_id,
        'Favorite_Category_1': favorite_categories[0] if len(favorite_categories) > 0 else None,
        'Favorite_Category_2': favorite_categories[1] if len(favorite_categories) > 1 else None,
        'Favorite_Category_3': favorite_categories[2] if len(favorite_categories) > 2 else None,
        'Average_Spending': round(avg_spending, 2),
        'Total_Purchases': total_purchases,
        'Preferred_Payment_Method': preferred_payment,
        'Last_Purchase_Date': last_purchase,
        'Customer_Segment': segment
    })

# Create DataFrame
user_preferences_df = pd.DataFrame(user_preferences_data)

# Save to CSV
user_preferences_df.to_csv('preferences.csv', index=False)

print("User preferences CSV created successfully!")
print(f"Total users: {len(user_preferences_df)}")
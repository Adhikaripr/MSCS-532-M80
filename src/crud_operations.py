import csv
import os
import sys
import psutil
import time
from datetime import datetime
from typing import Dict, List, Any, Optional

# Import from current directory since we're now inside src
from user import UserStore
from product import ProductStore
from csv_loader import CSVLoader


class SystemMetrics:
    """System metrics tracking for space, memory, and time"""
    
    def __init__(self):
        self.start_time = time.time()
        self.operation_times = {}
        self.memory_snapshots = []
        self.disk_usage_snapshots = []
    
    def get_memory_usage(self):
        """Get current memory usage in MB"""
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        return {
            'rss_mb': memory_info.rss / 1024 / 1024,  # Resident Set Size
            'vms_mb': memory_info.vms / 1024 / 1024,  # Virtual Memory Size
            'percent': process.memory_percent(),
            'available_mb': psutil.virtual_memory().available / 1024 / 1024
        }
    
    def get_disk_usage(self, path='.'):
        """Get disk usage for the given path"""
        try:
            usage = psutil.disk_usage(path)
            return {
                'total_gb': usage.total / 1024 / 1024 / 1024,
                'used_gb': usage.used / 1024 / 1024 / 1024,
                'free_gb': usage.free / 1024 / 1024 / 1024,
                'percent': (usage.used / usage.total) * 100
            }
        except Exception:
            return None
    
    def get_file_sizes(self):
        """Get sizes of data files"""
        files = ['../data/users.csv', '../data/products.csv', '../data/purchase_history.csv', '../data/preferences.csv']
        file_sizes = {}
        total_size = 0
        
        for file_path in files:
            if os.path.exists(file_path):
                size = os.path.getsize(file_path)
                file_sizes[file_path] = {
                    'size_mb': size / 1024 / 1024,
                    'size_bytes': size
                }
                total_size += size
            else:
                file_sizes[file_path] = {'size_mb': 0, 'size_bytes': 0}
        
        file_sizes['total_mb'] = total_size / 1024 / 1024
        return file_sizes
    
    def start_operation(self, operation_name):
        """Start timing an operation"""
        self.operation_times[operation_name] = time.time()
    
    def end_operation(self, operation_name):
        """End timing an operation and return duration"""
        if operation_name in self.operation_times:
            duration = time.time() - self.operation_times[operation_name]
            del self.operation_times[operation_name]
            return duration
        return 0
    
    def get_system_info(self):
        """Get comprehensive system information"""
        memory = self.get_memory_usage()
        disk = self.get_disk_usage()
        file_sizes = self.get_file_sizes()
        uptime = time.time() - self.start_time
        
        return {
            'uptime_seconds': uptime,
            'uptime_formatted': self._format_duration(uptime),
            'memory': memory,
            'disk': disk,
            'file_sizes': file_sizes,
            'cpu_percent': psutil.cpu_percent(interval=1),
            'active_operations': len(self.operation_times)
        }
    
    def _format_duration(self, seconds):
        """Format duration in human readable format"""
        if seconds < 60:
            return f"{seconds:.2f} seconds"
        elif seconds < 3600:
            return f"{seconds/60:.2f} minutes"
        else:
            return f"{seconds/3600:.2f} hours"
    
    def print_metrics(self):
        """Print simplified metrics - CPU, Memory, Time"""
        info = self.get_system_info()
        
        print("\n" + "="*50)
        print("PERFORMANCE METRICS")
        print("="*50)
        
        print(f"CPU USAGE: {info['cpu_percent']:.1f}%")
        print(f"MEMORY USAGE: {info['memory']['rss_mb']:.2f} MB ({info['memory']['percent']:.1f}%)")
        print(f"UPTIME: {info['uptime_formatted']}")
        print(f"ACTIVE OPERATIONS: {info['active_operations']}")
    
    def print_operation_metrics(self, operation_name, duration):
        """Print metrics for a specific operation"""
        memory = self.get_memory_usage()
        cpu = psutil.cpu_percent()  # Remove interval to avoid blocking
        
        print(f"\n--- {operation_name.upper()} METRICS ---")
        print(f"Time Taken: {duration:.3f} seconds")
        print(f"CPU Usage: {cpu:.1f}%")
        print(f"Memory Usage: {memory['rss_mb']:.2f} MB ({memory['percent']:.1f}%)")
        print("-" * 40)


class CRUDOperations:
    def __init__(self):
        self.users = UserStore()
        self.products = ProductStore()
        self.metrics = SystemMetrics()
        self.load_existing_data()
    
    def load_existing_data(self):
        """Load existing data from CSV files"""
        self.metrics.start_operation("load_data")
        try:
            CSVLoader.load_users("../data/users.csv", self.users)
            CSVLoader.load_products("../data/products.csv", self.products)
            CSVLoader.load_purchases("../data/purchase_history.csv", self.users, self.products)
            CSVLoader.load_preferences("../data/preferences.csv", self.users)
            load_time = self.metrics.end_operation("load_data")
            print(f"Existing data loaded successfully!")
            self.metrics.print_operation_metrics("data_loading", load_time)
        except Exception as e:
            self.metrics.end_operation("load_data")
            print(f"Error loading existing data: {e}")
    
    def save_data(self):
        """Save all data back to CSV files"""
        self.metrics.start_operation("save_data")
        try:
            self.save_users()
            self.save_products()
            self.save_purchases()
            self.save_preferences()
            save_time = self.metrics.end_operation("save_data")
            print(f"All data saved successfully!")
            self.metrics.print_operation_metrics("data_saving", save_time)
        except Exception as e:
            self.metrics.end_operation("save_data")
            print(f"Error saving data: {e}")
    
    # ==================== USER OPERATIONS ====================
    
    def add_user(self, user_id: str, name: str, email: str, **kwargs):
        """Add a new user"""
        self.metrics.start_operation("add_user")
        info = {
            'name': name,
            'email': email,
            'first_name': name.split()[0] if name else '',
            'last_name': ' '.join(name.split()[1:]) if len(name.split()) > 1 else '',
            'city': kwargs.get('city', ''),
            'country': kwargs.get('country', ''),
            'company': kwargs.get('company', ''),
            'subscription_date': datetime.now().strftime('%Y-%m-%d'),
            'website': kwargs.get('website', '')
        }
        self.users.add_user(user_id, info)
        add_time = self.metrics.end_operation("add_user")
        print(f"User {user_id} added successfully!")
        # self.metrics.print_operation_metrics("add_user", add_time)  # Disabled for bulk operations
    
    def get_user(self, user_id: str) -> Optional[Dict]:
        """Get user information"""
        if user_id not in self.users:
            return None
        user = self.users.users[user_id]
        return {
            'user_id': user_id,
            'info': user.get_all_info(),
            'purchases': user.get_items(),
            'preferences': dict(user.preferences),
            'total_spent': user.total_spent
        }
    
    def update_user(self, user_id: str, **kwargs):
        """Update user information"""
        if user_id not in self.users:
            print(f"User {user_id} not found!")
            return
        
        for key, value in kwargs.items():
            self.users.update_user_info(user_id, key, value)
        print(f"User {user_id} updated successfully!")
    
    def delete_user(self, user_id: str):
        """Delete a user"""
        if user_id not in self.users:
            print(f"User {user_id} not found!")
            return
        
        del self.users.users[user_id]
        if user_id in self.users.user_categories:
            del self.users.user_categories[user_id]
        print(f"User {user_id} deleted successfully!")
    
    def search_users(self, **criteria) -> List[Dict]:
        """Search users by criteria"""
        self.metrics.start_operation("search_users")
        results = []
        for user_id, user in self.users.users.items():
            match = True
            for key, value in criteria.items():
                if key == 'name' and value.lower() not in (user.get_info('name') or '').lower():
                    match = False
                elif key == 'email' and value.lower() not in (user.get_info('email') or '').lower():
                    match = False
                elif key == 'city' and value.lower() not in (user.get_info('city') or '').lower():
                    match = False
                elif key == 'country' and value.lower() not in (user.get_info('country') or '').lower():
                    match = False
            if match:
                results.append(self.get_user(user_id))
        search_time = self.metrics.end_operation("search_users")
        if search_time > 0.1:  # Only print if search took more than 100ms
            self.metrics.print_operation_metrics("user_search", search_time)
        return results
    
    # ==================== PRODUCT OPERATIONS ====================
    
    def add_product(self, product_id: str, name: str, category: str, price: float, **kwargs):
        """Add a new product"""
        self.metrics.start_operation("add_product")
        info = {
            'name': name,
            'description': kwargs.get('description', ''),
            'brand': kwargs.get('brand', ''),
            'price': price,
            'currency': kwargs.get('currency', 'USD'),
            'stock': kwargs.get('stock', 0),
            'ean': kwargs.get('ean', ''),
            'color': kwargs.get('color', ''),
            'size': kwargs.get('size', ''),
            'availability': kwargs.get('availability', 'in_stock')
        }
        self.products.add_product(product_id, category, info)
        add_time = self.metrics.end_operation("add_product")
        print(f"Product {product_id} added successfully!")
        # self.metrics.print_operation_metrics("add_product", add_time)  # Disabled for bulk operations
    
    def get_product(self, product_id: str) -> Optional[Dict]:
        """Get product information"""
        if product_id not in self.products:
            return None
        product = self.products.get_product(product_id)
        return {
            'product_id': product_id,
            'category': product.category,
            'info': product.info
        }
    
    def update_product(self, product_id: str, **kwargs):
        """Update product information"""
        if product_id not in self.products:
            print(f"Product {product_id} not found!")
            return
        
        product = self.products.get_product(product_id)
        for key, value in kwargs.items():
            product.update_info(key, value)
        print(f"Product {product_id} updated successfully!")
    
    def delete_product(self, product_id: str):
        """Delete a product"""
        if product_id not in self.products:
            print(f"Product {product_id} not found!")
            return
        
        product = self.products.get_product(product_id)
        category = product.category
        
        # Remove from all indexes
        del self.products.items[product_id]
        if product_id in self.products.category_index[category]:
            self.products.category_index[category].remove(product_id)
        
        # Remove from brand index
        brand = product.get_info('brand') or 'unknown'
        if product_id in self.products.brand_index[brand]:
            self.products.brand_index[brand].remove(product_id)
        
        print(f"Product {product_id} deleted successfully!")
    
    def search_products(self, **criteria) -> List[Dict]:
        """Search products by criteria"""
        results = []
        for product_id, product in self.products.items.items():
            match = True
            for key, value in criteria.items():
                if key == 'name' and value.lower() not in (product.get_info('name') or '').lower():
                    match = False
                elif key == 'category' and value.lower() != product.category.lower():
                    match = False
                elif key == 'brand' and value.lower() not in (product.get_info('brand') or '').lower():
                    match = False
                elif key == 'min_price' and (product.get_info('price') or 0) < value:
                    match = False
                elif key == 'max_price' and (product.get_info('price') or 0) > value:
                    match = False
                elif key == 'availability' and (product.get_info('availability') or '') != value:
                    match = False
            if match:
                results.append(self.get_product(product_id))
        return results
    
    # ==================== PURCHASE OPERATIONS ====================
    
    def add_purchase(self, user_id: str, product_id: str, quantity: int = 1, **kwargs):
        """Add a new purchase"""
        self.metrics.start_operation("add_purchase")
        if user_id not in self.users:
            self.metrics.end_operation("add_purchase")
            print(f"User {user_id} not found!")
            return
        
        if product_id not in self.products:
            self.metrics.end_operation("add_purchase")
            print(f"Product {product_id} not found!")
            return
        
        product = self.products.get_product(product_id)
        category = product.category
        unit_price = product.get_info('price') or 0
        total_amount = unit_price * quantity
        
        purchase_data = {
            'purchase_id': kwargs.get('purchase_id', f'PUR{len(self.users.users):06d}'),
            'product_name': product.get_info('name') or '',
            'purchase_date': kwargs.get('purchase_date', datetime.now().strftime('%Y-%m-%d')),
            'quantity': quantity,
            'unit_price': unit_price,
            'total_amount': total_amount,
            'payment_method': kwargs.get('payment_method', 'Credit Card'),
            'order_status': kwargs.get('order_status', 'Completed')
        }
        
        self.users.add_purchase(user_id, product_id, category, purchase_data)
        add_time = self.metrics.end_operation("add_purchase")
        print(f"Purchase added successfully!")
        # self.metrics.print_operation_metrics("add_purchase", add_time)  # Disabled for bulk operations
    
    def get_user_purchases(self, user_id: str) -> List[Dict]:
        """Get all purchases for a user"""
        if user_id not in self.users:
            return []
        
        user = self.users.users[user_id]
        purchases = []
        current = user.purchase_history.head
        
        while current:
            purchases.append({
                'product_id': current.item_id,
                'purchase_data': current.purchase_data
            })
            current = current.next
        
        return purchases
    
    def search_purchases(self, **criteria) -> List[Dict]:
        """Search purchases by criteria"""
        results = []
        for user_id, user in self.users.users.items():
            current = user.purchase_history.head
            while current:
                match = True
                for key, value in criteria.items():
                    if key == 'user_id' and user_id != value:
                        match = False
                    elif key == 'product_id' and current.item_id != value:
                        match = False
                    elif key == 'date' and current.purchase_data.get('purchase_date', '') != value:
                        match = False
                    elif key == 'status' and current.purchase_data.get('order_status', '') != value:
                        match = False
                if match:
                    results.append({
                        'user_id': user_id,
                        'product_id': current.item_id,
                        'purchase_data': current.purchase_data
                    })
                current = current.next
        return results
    
    # ==================== SAVE OPERATIONS ====================
    
    def save_users(self):
        """Save users to CSV"""
        with open('../data/users.csv', 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['Customer Id', 'First Name', 'Last Name', 'Company', 'City', 'Country', 
                         'Email', 'Subscription Date', 'Website']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for user_id, user in self.users.users.items():
                info = user.get_all_info()
                writer.writerow({
                    'Customer Id': user_id,
                    'First Name': info.get('first_name', ''),
                    'Last Name': info.get('last_name', ''),
                    'Company': info.get('company', ''),
                    'City': info.get('city', ''),
                    'Country': info.get('country', ''),
                    'Email': info.get('email', ''),
                    'Subscription Date': info.get('subscription_date', ''),
                    'Website': info.get('website', '')
                })
    
    def save_products(self):
        """Save products to CSV"""
        with open('../data/products.csv', 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['Index', 'Name', 'Description', 'Brand', 'Category', 'Price', 'Currency', 
                         'Stock', 'EAN', 'Color', 'Size', 'Availability', 'Internal ID']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for i, (product_id, product) in enumerate(self.products.items.items(), 1):
                info = product.info
                writer.writerow({
                    'Index': i,
                    'Name': info.get('name', ''),
                    'Description': info.get('description', ''),
                    'Brand': info.get('brand', ''),
                    'Category': product.category,
                    'Price': info.get('price', 0),
                    'Currency': info.get('currency', 'USD'),
                    'Stock': info.get('stock', 0),
                    'EAN': info.get('ean', ''),
                    'Color': info.get('color', ''),
                    'Size': info.get('size', ''),
                    'Availability': info.get('availability', 'in_stock'),
                    'Internal ID': product_id
                })
    
    def save_purchases(self):
        """Save purchases to CSV"""
        with open('../data/purchase_history.csv', 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['Purchase_ID', 'Customer_Id', 'Product_ID', 'Product_Name', 'Product_Category',
                         'Purchase_Date', 'Quantity', 'Unit_Price', 'Total_Amount', 'Payment_Method', 'Order_Status']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for user_id, user in self.users.users.items():
                current = user.purchase_history.head
                while current:
                    product = self.products.get_product(current.item_id)
                    writer.writerow({
                        'Purchase_ID': current.purchase_data.get('purchase_id', ''),
                        'Customer_Id': user_id,
                        'Product_ID': current.item_id,
                        'Product_Name': current.purchase_data.get('product_name', ''),
                        'Product_Category': product.category if product else 'Unknown',
                        'Purchase_Date': current.purchase_data.get('purchase_date', ''),
                        'Quantity': current.purchase_data.get('quantity', 1),
                        'Unit_Price': current.purchase_data.get('unit_price', 0),
                        'Total_Amount': current.purchase_data.get('total_amount', 0),
                        'Payment_Method': current.purchase_data.get('payment_method', ''),
                        'Order_Status': current.purchase_data.get('order_status', '')
                    })
                    current = current.next
    
    def save_preferences(self):
        """Save preferences to CSV"""
        with open('../data/preferences.csv', 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['Customer_Id', 'Favorite_Category_1', 'Favorite_Category_2', 'Favorite_Category_3',
                         'Average_Spending', 'Total_Purchases', 'Preferred_Payment_Method', 
                         'Last_Purchase_Date', 'Customer_Segment']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for user_id, user in self.users.users.items():
                info = user.get_all_info()
                writer.writerow({
                    'Customer_Id': user_id,
                    'Favorite_Category_1': info.get('Favorite_Category_1', ''),
                    'Favorite_Category_2': info.get('Favorite_Category_2', ''),
                    'Favorite_Category_3': info.get('Favorite_Category_3', ''),
                    'Average_Spending': info.get('average_spending', 0),
                    'Total_Purchases': info.get('total_purchases', 0),
                    'Preferred_Payment_Method': info.get('preferred_payment', ''),
                    'Last_Purchase_Date': info.get('last_purchase_date', ''),
                    'Customer_Segment': info.get('customer_segment', '')
                })


def display_menu():
    """Display the main menu"""
    print("\n" + "="*60)
    print("E-COMMERCE RECOMMENDATION SYSTEM - CRUD OPERATIONS")
    print("="*60)
    print("1. USER OPERATIONS")
    print("2. PRODUCT OPERATIONS") 
    print("3. PURCHASE OPERATIONS")
    print("4. SEARCH OPERATIONS")
    print("5. SAVE ALL DATA")
    print("6. VIEW STATISTICS")
    print("7. SYSTEM METRICS")
    print("8. EXIT")
    print("="*60)


def user_operations_menu(crud):
    """User operations submenu"""
    while True:
        print("\n" + "-"*40)
        print("USER OPERATIONS")
        print("-"*40)
        print("1. Add User")
        print("2. Get User Info")
        print("3. Update User")
        print("4. Delete User")
        print("5. Search Users")
        print("6. List All Users")
        print("7. User Analysis & Recommendations")
        print("8. Back to Main Menu")
        
        choice = input("\nSelect option (1-8): ").strip()
        
        if choice == '1':
            user_id = input("Enter User ID: ").strip()
            name = input("Enter Full Name: ").strip()
            email = input("Enter Email: ").strip()
            city = input("Enter City (optional): ").strip()
            country = input("Enter Country (optional): ").strip()
            crud.add_user(user_id, name, email, city=city, country=country)
            
        elif choice == '2':
            user_id = input("Enter User ID: ").strip()
            user_info = crud.get_user(user_id)
            if user_info:
                print(f"\n📋 User Information:")
                print(f"ID: {user_info['user_id']}")
                print(f"Name: {user_info['info'].get('name', 'N/A')}")
                print(f"Email: {user_info['info'].get('email', 'N/A')}")
                print(f"Total Spent: ${user_info['total_spent']:.2f}")
                print(f"Purchases: {len(user_info['purchases'])}")
            else:
                print("❌ User not found!")
                
        elif choice == '3':
            user_id = input("Enter User ID: ").strip()
            print("Enter new values (press Enter to skip):")
            name = input("Name: ").strip()
            email = input("Email: ").strip()
            city = input("City: ").strip()
            
            updates = {}
            if name: updates['name'] = name
            if email: updates['email'] = email
            if city: updates['city'] = city
            
            if updates:
                crud.update_user(user_id, **updates)
            else:
                print("No updates provided.")
                
        elif choice == '4':
            user_id = input("Enter User ID to delete: ").strip()
            confirm = input(f"Are you sure you want to delete user {user_id}? (y/N): ").strip().lower()
            if confirm == 'y':
                crud.delete_user(user_id)
                
        elif choice == '5':
            print("Search by:")
            name = input("Name (partial match): ").strip()
            email = input("Email (partial match): ").strip()
            city = input("City (partial match): ").strip()
            
            criteria = {}
            if name: criteria['name'] = name
            if email: criteria['email'] = email
            if city: criteria['city'] = city
            
            results = crud.search_users(**criteria)
            print(f"\n🔍 Found {len(results)} users:")
            for user in results[:10]:  # Show first 10
                print(f"  {user['user_id']}: {user['info'].get('name', 'N/A')} ({user['info'].get('email', 'N/A')})")
                
        elif choice == '6':
            print(f"\n📋 All Users ({len(crud.users.users)}):")
            for i, (user_id, user) in enumerate(list(crud.users.users.items())[:20], 1):
                print(f"  {i}. {user_id}: {user.get_info('name', 'N/A')}")
            if len(crud.users.users) > 20:
                print(f"  ... and {len(crud.users.users) - 20} more")
                
        elif choice == '7':
            user_analysis_menu(crud)
        elif choice == '8':
            break
        else:
            print("Invalid option!")


def user_analysis_menu(crud):
    """User analysis and recommendations menu"""
    while True:
        print("\n" + "-"*50)
        print("USER ANALYSIS & RECOMMENDATIONS")
        print("-"*50)
        print("1. Search and Analyze User")
        print("2. View User Purchase History")
        print("3. View User Preferences")
        print("4. Generate Recommendations")
        print("5. Complete User Profile")
        print("6. Back to User Operations")
        
        choice = input("\nSelect option (1-6): ").strip()
        
        if choice == '1':
            search_and_analyze_user(crud)
        elif choice == '2':
            view_user_purchases(crud)
        elif choice == '3':
            view_user_preferences(crud)
        elif choice == '4':
            generate_user_recommendations(crud)
        elif choice == '5':
            complete_user_profile(crud)
        elif choice == '6':
            break
        else:
            print("Invalid option!")


def search_and_analyze_user(crud):
    """Search for a user and show basic analysis"""
    print("\n" + "-"*30)
    print("SEARCH AND ANALYZE USER")
    print("-"*30)
    
    search_term = input("Enter user ID, name, or email to search: ").strip()
    if not search_term:
        print("Search term cannot be empty!")
        return
    
    # Search by different criteria
    results = []
    
    # Search by exact user ID
    if search_term in crud.users.users:
        results.append(crud.get_user(search_term))
    else:
        # Search by name
        name_results = crud.search_users(name=search_term)
        results.extend(name_results)
        
        # Search by email
        email_results = crud.search_users(email=search_term)
        results.extend(email_results)
    
    # Remove duplicates
    seen = set()
    unique_results = []
    for result in results:
        if result['user_id'] not in seen:
            seen.add(result['user_id'])
            unique_results.append(result)
    
    if not unique_results:
        print(f"No users found matching '{search_term}'")
        return
    
    print(f"\nFound {len(unique_results)} user(s):")
    for i, user in enumerate(unique_results, 1):
        print(f"{i}. {user['user_id']}: {user['info'].get('name', 'N/A')} ({user['info'].get('email', 'N/A')})")
    
    if len(unique_results) == 1:
        user = unique_results[0]
        print(f"\nSelected user: {user['user_id']}")
        show_user_summary(user)
    else:
        try:
            selection = int(input(f"\nSelect user (1-{len(unique_results)}): ")) - 1
            if 0 <= selection < len(unique_results):
                user = unique_results[selection]
                print(f"\nSelected user: {user['user_id']}")
                show_user_summary(user)
            else:
                print("Invalid selection!")
        except ValueError:
            print("Invalid selection!")


def show_user_summary(user):
    """Show a summary of user information"""
    print(f"\n" + "="*50)
    print(f"USER SUMMARY: {user['user_id']}")
    print("="*50)
    
    info = user['info']
    print(f"Name: {info.get('name', 'N/A')}")
    print(f"Email: {info.get('email', 'N/A')}")
    print(f"City: {info.get('city', 'N/A')}")
    print(f"Country: {info.get('country', 'N/A')}")
    print(f"Total Spent: ${user['total_spent']:.2f}")
    print(f"Total Purchases: {len(user['purchases'])}")
    
    if user['preferences']:
        print(f"Top Preferences: {list(user['preferences'].keys())[:3]}")
    else:
        print("No preferences recorded")


def view_user_purchases(crud):
    """View detailed purchase history for a user"""
    print("\n" + "-"*30)
    print("VIEW USER PURCHASE HISTORY")
    print("-"*30)
    
    user_id = input("Enter User ID: ").strip()
    if not user_id:
        print("User ID cannot be empty!")
        return
    
    user_info = crud.get_user(user_id)
    if not user_info:
        print(f"User {user_id} not found!")
        return
    
    purchases = crud.get_user_purchases(user_id)
    if not purchases:
        print(f"No purchases found for user {user_id}")
        return
    
    print(f"\nPURCHASE HISTORY for {user_info['info'].get('name', user_id)}")
    print("="*60)
    
    total_spent = 0
    for i, purchase in enumerate(purchases, 1):
        data = purchase['purchase_data']
        total_spent += data.get('total_amount', 0)
        
        print(f"\n{i}. Purchase ID: {data.get('purchase_id', 'N/A')}")
        print(f"   Product: {data.get('product_name', 'N/A')} (ID: {purchase['product_id']})")
        print(f"   Quantity: {data.get('quantity', 1)}")
        print(f"   Unit Price: ${data.get('unit_price', 0):.2f}")
        print(f"   Total Amount: ${data.get('total_amount', 0):.2f}")
        print(f"   Date: {data.get('purchase_date', 'N/A')}")
        print(f"   Payment Method: {data.get('payment_method', 'N/A')}")
        print(f"   Status: {data.get('order_status', 'N/A')}")
    
    print(f"\n" + "="*60)
    print(f"TOTAL PURCHASES: {len(purchases)}")
    print(f"TOTAL SPENT: ${total_spent:.2f}")
    print(f"AVERAGE PER PURCHASE: ${total_spent/len(purchases):.2f}")


def view_user_preferences(crud):
    """View user preferences and categories"""
    print("\n" + "-"*30)
    print("VIEW USER PREFERENCES")
    print("-"*30)
    
    user_id = input("Enter User ID: ").strip()
    if not user_id:
        print("User ID cannot be empty!")
        return
    
    user_info = crud.get_user(user_id)
    if not user_info:
        print(f"User {user_id} not found!")
        return
    
    print(f"\nPREFERENCES for {user_info['info'].get('name', user_id)}")
    print("="*50)
    
    # Show explicit preferences from CSV
    info = user_info['info']
    print(f"Favorite Category 1: {info.get('Favorite_Category_1', 'N/A')}")
    print(f"Favorite Category 2: {info.get('Favorite_Category_2', 'N/A')}")
    print(f"Favorite Category 3: {info.get('Favorite_Category_3', 'N/A')}")
    print(f"Average Spending: ${info.get('average_spending', 0):.2f}")
    print(f"Total Purchases: {info.get('total_purchases', 0)}")
    print(f"Preferred Payment: {info.get('preferred_payment', 'N/A')}")
    print(f"Last Purchase: {info.get('last_purchase_date', 'N/A')}")
    print(f"Customer Segment: {info.get('customer_segment', 'N/A')}")
    
    # Show calculated preferences from purchases
    if user_info['preferences']:
        print(f"\nCALCULATED PREFERENCES (from purchase history):")
        print("-"*50)
        sorted_prefs = sorted(user_info['preferences'].items(), key=lambda x: x[1], reverse=True)
        for category, score in sorted_prefs:
            print(f"{category}: {score:.2f}")
    else:
        print(f"\nNo calculated preferences available")


def generate_user_recommendations(crud):
    """Generate recommendations for a user"""
    print("\n" + "-"*30)
    print("GENERATE USER RECOMMENDATIONS")
    print("-"*30)
    
    user_id = input("Enter User ID: ").strip()
    if not user_id:
        print("User ID cannot be empty!")
        return
    
    if user_id not in crud.users.users:
        print(f"User {user_id} not found!")
        return
    
    # Import the recommender system
    try:
        from src.recommender import RecommenderSystem
        recommender = RecommenderSystem(crud.users, crud.products)
        
        print(f"\nRECOMMENDATIONS for {crud.users.users[user_id].get_info('name') or user_id}")
        print("="*60)
        
        # Generate different types of recommendations
        print(f"\n1. CATEGORY-BASED RECOMMENDATIONS:")
        cat_recs = recommender.recommend_by_category(user_id, 5)
        if cat_recs:
            for i, rec_id in enumerate(cat_recs, 1):
                product = crud.products.get_product(rec_id)
                if product:
                    print(f"   {i}. {product.get_info('name') or 'N/A'} - ${product.get_info('price') or 0:.2f}")
        else:
            print("   No category-based recommendations available")
        
        print(f"\n2. SIMILAR USERS RECOMMENDATIONS:")
        sim_recs = recommender.recommend_by_similar_users(user_id, 5)
        if sim_recs:
            for i, rec_id in enumerate(sim_recs, 1):
                product = crud.products.get_product(rec_id)
                if product:
                    print(f"   {i}. {product.get_info('name') or 'N/A'} - ${product.get_info('price') or 0:.2f}")
        else:
            print("   No similar users recommendations available")
        
        print(f"\n3. PREFERENCE-BASED RECOMMENDATIONS:")
        pref_recs = recommender.recommend_based_on_preferences(user_id, 5)
        if pref_recs:
            for i, rec_id in enumerate(pref_recs, 1):
                product = crud.products.get_product(rec_id)
                if product:
                    print(f"   {i}. {product.get_info('name') or 'N/A'} - ${product.get_info('price') or 0:.2f}")
        else:
            print("   No preference-based recommendations available")
        
        print(f"\n4. HYBRID RECOMMENDATIONS:")
        hybrid_recs = recommender.recommend_hybrid(user_id, 5)
        if hybrid_recs:
            for i, rec_id in enumerate(hybrid_recs, 1):
                product = crud.products.get_product(rec_id)
                if product:
                    print(f"   {i}. {product.get_info('name') or 'N/A'} - ${product.get_info('price') or 0:.2f}")
        else:
            print("   No hybrid recommendations available")
            
    except Exception as e:
        print(f"Error generating recommendations: {e}")


def complete_user_profile(crud):
    """Show complete user profile with all information"""
    print("\n" + "-"*30)
    print("COMPLETE USER PROFILE")
    print("-"*30)
    
    user_id = input("Enter User ID: ").strip()
    if not user_id:
        print("User ID cannot be empty!")
        return
    
    user_info = crud.get_user(user_id)
    if not user_info:
        print(f"User {user_id} not found!")
        return
    
    print(f"\n" + "="*60)
    print(f"COMPLETE PROFILE: {user_id}")
    print("="*60)
    
    # Basic Information
    info = user_info['info']
    print(f"\nBASIC INFORMATION:")
    print(f"  Name: {info.get('name', 'N/A')}")
    print(f"  Email: {info.get('email', 'N/A')}")
    print(f"  City: {info.get('city', 'N/A')}")
    print(f"  Country: {info.get('country', 'N/A')}")
    print(f"  Company: {info.get('company', 'N/A')}")
    print(f"  Website: {info.get('website', 'N/A')}")
    print(f"  Subscription Date: {info.get('subscription_date', 'N/A')}")
    
    # Purchase Statistics
    print(f"\nPURCHASE STATISTICS:")
    print(f"  Total Purchases: {len(user_info['purchases'])}")
    print(f"  Total Spent: ${user_info['total_spent']:.2f}")
    print(f"  Average per Purchase: ${user_info['total_spent']/max(len(user_info['purchases']), 1):.2f}")
    
    # Purchase History
    if user_info['purchases']:
        print(f"\nRECENT PURCHASES:")
        # Get detailed purchase data
        detailed_purchases = crud.get_user_purchases(user_id)
        for i, purchase in enumerate(detailed_purchases[:5], 1):
            data = purchase['purchase_data']
            print(f"  {i}. {data.get('product_name', 'N/A')} - ${data.get('total_amount', 0):.2f} ({data.get('purchase_date', 'N/A')})")
        if len(detailed_purchases) > 5:
            print(f"  ... and {len(detailed_purchases) - 5} more purchases")
    
    # Preferences
    print(f"\nPREFERENCES:")
    print(f"  Favorite Category 1: {info.get('Favorite_Category_1', 'N/A')}")
    print(f"  Favorite Category 2: {info.get('Favorite_Category_2', 'N/A')}")
    print(f"  Favorite Category 3: {info.get('Favorite_Category_3', 'N/A')}")
    print(f"  Preferred Payment: {info.get('preferred_payment', 'N/A')}")
    print(f"  Customer Segment: {info.get('customer_segment', 'N/A')}")
    
    if user_info['preferences']:
        print(f"\nCALCULATED PREFERENCES:")
        sorted_prefs = sorted(user_info['preferences'].items(), key=lambda x: x[1], reverse=True)
        for category, score in sorted_prefs[:5]:
            print(f"  {category}: {score:.2f}")
    
    # Generate recommendations
    try:
        from src.recommender import RecommenderSystem
        recommender = RecommenderSystem(crud.users, crud.products)
        
        print(f"\nRECOMMENDATIONS:")
        hybrid_recs = recommender.recommend_hybrid(user_id, 3)
        if hybrid_recs:
            for i, rec_id in enumerate(hybrid_recs, 1):
                product = crud.products.get_product(rec_id)
                if product:
                    print(f"  {i}. {product.get_info('name') or 'N/A'} - ${product.get_info('price') or 0:.2f}")
        else:
            print("  No recommendations available")
    except Exception as e:
        print(f"  Error generating recommendations: {e}")


def product_operations_menu(crud):
    """Product operations submenu"""
    while True:
        print("\n" + "-"*40)
        print("PRODUCT OPERATIONS")
        print("-"*40)
        print("1. Add Product")
        print("2. Get Product Info")
        print("3. Update Product")
        print("4. Delete Product")
        print("5. Search Products")
        print("6. List All Products")
        print("7. Back to Main Menu")
        
        choice = input("\nSelect option (1-7): ").strip()
        
        if choice == '1':
            product_id = input("Enter Product ID: ").strip()
            name = input("Enter Product Name: ").strip()
            category = input("Enter Category: ").strip()
            try:
                price = float(input("Enter Price: ").strip())
            except ValueError:
                print("❌ Invalid price!")
                continue
            description = input("Enter Description (optional): ").strip()
            brand = input("Enter Brand (optional): ").strip()
            stock = input("Enter Stock (optional, default 0): ").strip()
            stock = int(stock) if stock.isdigit() else 0
            
            crud.add_product(product_id, name, category, price, 
                           description=description, brand=brand, stock=stock)
            
        elif choice == '2':
            product_id = input("Enter Product ID: ").strip()
            product_info = crud.get_product(product_id)
            if product_info:
                print(f"\n📋 Product Information:")
                print(f"ID: {product_info['product_id']}")
                print(f"Name: {product_info['info'].get('name', 'N/A')}")
                print(f"Category: {product_info['category']}")
                print(f"Price: ${product_info['info'].get('price', 0):.2f}")
                print(f"Brand: {product_info['info'].get('brand', 'N/A')}")
                print(f"Stock: {product_info['info'].get('stock', 0)}")
            else:
                print("❌ Product not found!")
                
        elif choice == '3':
            product_id = input("Enter Product ID: ").strip()
            print("Enter new values (press Enter to skip):")
            name = input("Name: ").strip()
            price = input("Price: ").strip()
            stock = input("Stock: ").strip()
            
            updates = {}
            if name: updates['name'] = name
            if price and price.replace('.', '').isdigit():
                updates['price'] = float(price)
            if stock and stock.isdigit():
                updates['stock'] = int(stock)
            
            if updates:
                crud.update_product(product_id, **updates)
            else:
                print("No updates provided.")
                
        elif choice == '4':
            product_id = input("Enter Product ID to delete: ").strip()
            confirm = input(f"Are you sure you want to delete product {product_id}? (y/N): ").strip().lower()
            if confirm == 'y':
                crud.delete_product(product_id)
                
        elif choice == '5':
            print("Search by:")
            name = input("Name (partial match): ").strip()
            category = input("Category (exact match): ").strip()
            brand = input("Brand (partial match): ").strip()
            min_price = input("Min Price (optional): ").strip()
            max_price = input("Max Price (optional): ").strip()
            
            criteria = {}
            if name: criteria['name'] = name
            if category: criteria['category'] = category
            if brand: criteria['brand'] = brand
            if min_price and min_price.replace('.', '').isdigit():
                criteria['min_price'] = float(min_price)
            if max_price and max_price.replace('.', '').isdigit():
                criteria['max_price'] = float(max_price)
            
            results = crud.search_products(**criteria)
            print(f"\n🔍 Found {len(results)} products:")
            for product in results[:10]:  # Show first 10
                print(f"  {product['product_id']}: {product['info'].get('name', 'N/A')} - ${product['info'].get('price', 0):.2f}")
                
        elif choice == '6':
            print(f"\n📋 All Products ({len(crud.products.items)}):")
            for i, (product_id, product) in enumerate(list(crud.products.items.items())[:20], 1):
                print(f"  {i}. {product_id}: {product.get_info('name', 'N/A')} - ${product.get_info('price', 0):.2f}")
            if len(crud.products.items) > 20:
                print(f"  ... and {len(crud.products.items) - 20} more")
                
        elif choice == '7':
            break
        else:
            print("Invalid option!")


def purchase_operations_menu(crud):
    """Purchase operations submenu"""
    while True:
        print("\n" + "-"*40)
        print("PURCHASE OPERATIONS")
        print("-"*40)
        print("1. Add Purchase")
        print("2. Get User Purchases")
        print("3. Search Purchases")
        print("4. Back to Main Menu")
        
        choice = input("\nSelect option (1-4): ").strip()
        
        if choice == '1':
            user_id = input("Enter User ID: ").strip()
            product_id = input("Enter Product ID: ").strip()
            try:
                quantity = int(input("Enter Quantity (default 1): ").strip() or "1")
            except ValueError:
                print("❌ Invalid quantity!")
                continue
            payment_method = input("Payment Method (optional): ").strip()
            
            crud.add_purchase(user_id, product_id, quantity, 
                            payment_method=payment_method or 'Credit Card')
            
        elif choice == '2':
            user_id = input("Enter User ID: ").strip()
            purchases = crud.get_user_purchases(user_id)
            if purchases:
                print(f"\n🛍️  Purchases for User {user_id}:")
                for i, purchase in enumerate(purchases, 1):
                    data = purchase['purchase_data']
                    print(f"  {i}. Product {purchase['product_id']}: {data.get('product_name', 'N/A')}")
                    print(f"     Quantity: {data.get('quantity', 1)}, Total: ${data.get('total_amount', 0):.2f}")
                    print(f"     Date: {data.get('purchase_date', 'N/A')}, Status: {data.get('order_status', 'N/A')}")
            else:
                print("❌ No purchases found for this user!")
                
        elif choice == '3':
            print("Search by:")
            user_id = input("User ID (exact match): ").strip()
            product_id = input("Product ID (exact match): ").strip()
            date = input("Date (YYYY-MM-DD): ").strip()
            status = input("Order Status: ").strip()
            
            criteria = {}
            if user_id: criteria['user_id'] = user_id
            if product_id: criteria['product_id'] = product_id
            if date: criteria['date'] = date
            if status: criteria['status'] = status
            
            results = crud.search_purchases(**criteria)
            print(f"\n🔍 Found {len(results)} purchases:")
            for purchase in results[:10]:  # Show first 10
                data = purchase['purchase_data']
                print(f"  User {purchase['user_id']} -> Product {purchase['product_id']}: ${data.get('total_amount', 0):.2f}")
                
        elif choice == '4':
            break
        else:
            print("Invalid option!")


def view_statistics(crud):
    """Display system statistics"""
    print("\n" + "="*50)
    print("SYSTEM STATISTICS")
    print("="*50)
    
    # User statistics
    total_users = len(crud.users.users)
    users_with_purchases = sum(1 for user in crud.users.users.values() if len(user.get_items()) > 0)
    total_spent = sum(user.total_spent for user in crud.users.users.values())
    
    print(f"USERS:")
    print(f"  Total Users: {total_users:,}")
    print(f"  Users with Purchases: {users_with_purchases:,}")
    print(f"  Total Revenue: ${total_spent:,.2f}")
    print(f"  Average per User: ${total_spent/max(total_users, 1):,.2f}")
    
    # Product statistics
    total_products = len(crud.products.items)
    categories = len(crud.products.get_all_categories())
    
    print(f"\nPRODUCTS:")
    print(f"  Total Products: {total_products:,}")
    print(f"  Categories: {categories}")
    
    # Category breakdown
    category_counts = {}
    for product in crud.products.items.values():
        cat = product.category
        category_counts[cat] = category_counts.get(cat, 0) + 1
    
    print(f"\nTOP CATEGORIES:")
    for category, count in sorted(category_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"  {category}: {count} products")
    
    # Purchase statistics
    total_purchases = sum(len(user.get_items()) for user in crud.users.users.values())
    print(f"\nPURCHASES:")
    print(f"  Total Purchases: {total_purchases:,}")
    print(f"  Average per User: {total_purchases/max(total_users, 1):.2f}")
    
    # Display system metrics
    crud.metrics.print_metrics()


def main():
    """Main function"""
    print("Starting E-Commerce Recommendation System CRUD Operations...")
    
    # Initialize CRUD operations
    crud = CRUDOperations()
    
    while True:
        display_menu()
        choice = input("\nSelect option (1-7): ").strip()
        
        if choice == '1':
            user_operations_menu(crud)
        elif choice == '2':
            product_operations_menu(crud)
        elif choice == '3':
            purchase_operations_menu(crud)
        elif choice == '4':
            print("\nSEARCH OPERATIONS")
            print("Use the individual operation menus for searching.")
            print("Each operation (Users, Products, Purchases) has its own search functionality.")
        elif choice == '5':
            crud.save_data()
        elif choice == '6':
            view_statistics(crud)
        elif choice == '7':
            crud.metrics.print_metrics()
        elif choice == '8':
            print("\nSaving data before exit...")
            crud.save_data()
            print("Goodbye!")
            break
        else:
            print("Invalid option! Please select 1-8.")


if __name__ == "__main__":
    main()

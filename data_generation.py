import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta
import os

# Create data directory if it doesn't exist
os.makedirs('data', exist_ok=True)

fake = Faker()

# 1. dim_protein_source
sources = ['Whey', 'Chicken', 'Vegan', 'Beef', 'Casein']
dim_protein_source = pd.DataFrame({
    'source_id': range(1, len(sources) + 1),
    'source_name': sources
})

# 2. dim_users
num_users = 100
dim_users = pd.DataFrame({
    'user_id': range(1, num_users + 1),
    'age': [random.randint(18, 65) for _ in range(num_users)]
})

# 3. dim_time
start_date = datetime(2025, 1, 1)
num_days = 365
dates = [start_date + timedelta(days=i) for i in range(num_days)]
dim_time = pd.DataFrame({
    'time_id': range(1, num_days + 1),
    'log_date': [d.strftime('%Y-%m-%d') for d in dates],
    'log_month': [d.month for d in dates],
    'log_year': [d.year for d in dates]
})

# 4. fact_nutrition_recovery
num_records = 2000
fact_nutrition_recovery = pd.DataFrame({
    'user_id': [random.choice(dim_users['user_id']) for _ in range(num_records)],
    'source_id': [random.choice(dim_protein_source['source_id']) for _ in range(num_records)],
    'time_id': [random.choice(dim_time['time_id']) for _ in range(num_records)],
    'protein_intake': [round(random.uniform(50, 250), 2) for _ in range(num_records)], 
    'muscle_growth': [round(random.uniform(0.1, 1.5), 2) for _ in range(num_records)]
})

# Export to CSV inside the data folder
dim_protein_source.to_csv('data/dim_protein_source.csv', index=False)
dim_users.to_csv('data/dim_users.csv', index=False)
dim_time.to_csv('data/dim_time.csv', index=False)
fact_nutrition_recovery.to_csv('data/fact_nutrition_recovery.csv', index=False)

print("Mock data generated successfully in the /data folder.")
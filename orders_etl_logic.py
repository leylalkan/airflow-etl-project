import pandas as pd
import sqlalchemy as db
import os
from dotenv import load_dotenv

load_dotenv()

orders_table = '/Users/leylalkan/Desktop/Learning Path/ETL pipelines with python/H+ Sport Orders.xlsx'

engine = db.create_engine(os.getenv('DB_CONNECTION'))

def main():
    orders = pd.read_excel(orders_table, sheet_name='data')
    orders = orders[["OrderID", "Date", "TotalDue", "Status", "CustomerID", "SalespersonID"]]
    
    with engine.connect() as connection:  # ← wrap with a connection
        orders.to_sql('orders', connection,
                      if_exists='replace',
                      index=False)
    
    print("ETL script executed successfully.")

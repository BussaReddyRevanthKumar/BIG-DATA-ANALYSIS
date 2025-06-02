
import dask.dataframe as dd

df = dd.read_csv("synthetic_sales_data.csv", parse_dates=["transaction_date"])
df['total_price'] = df['price'] * df['quantity']

total_transactions = df.transaction_id.count().compute()
total_revenue = df.total_price.sum().compute()
average_transaction_value = total_revenue / total_transactions

top_categories = df.groupby('product_category').total_price.sum().compute().sort_values(ascending=False).head(5)
monthly_revenue = df.assign(month=df.transaction_date.dt.month).groupby('month').total_price.sum().compute().sort_index()

print(f"Total Transactions: {total_transactions}")
print(f"Total Revenue: ${total_revenue:,.2f}")
print(f"Average Transaction Value: ${average_transaction_value:,.2f}")

print("Top 5 Product Categories by Revenue:")
print(top_categories)

print("Monthly Revenue Trend:")
print(monthly_revenue)

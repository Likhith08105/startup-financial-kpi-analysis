import pandas as pd
from pathlib import Path

csv = 'data/raw/cac_ltv_model.csv'
print('Reading', csv)
df = pd.read_csv(csv)
df['date'] = pd.to_datetime(df['date'], format='%b-%y', errors='coerce')
df = df.dropna(subset=['date']).copy()
df['month'] = df['date'].dt.to_period('M')
monthly = df.groupby('month').agg(
    Revenue=('arpu','sum'),
    Customers=('customer_id','nunique'),
).reset_index()
first_month = df.groupby('customer_id')['month'].min().reset_index(name='first_month')
new_counts = first_month['first_month'].value_counts().rename_axis('month').reset_index(name='New_Customers')
new_counts['month'] = new_counts['month'].astype(str)
monthly['month'] = monthly['month'].astype(str)
monthly = monthly.merge(new_counts, how='left', on='month')
monthly['New_Customers'] = monthly['New_Customers'].fillna(0).astype(int)
monthly['ARPU'] = monthly['Revenue'] / monthly['Customers']
monthly['Churn_Rate'] = (monthly['Customers'].shift(1) - monthly['Customers']) / monthly['Customers'].shift(1)
monthly['Churn_Rate'] = monthly['Churn_Rate'].fillna(0)

out = Path('data/cleaned/monthly_kpis.csv')
out.parent.mkdir(parents=True, exist_ok=True)
monthly.to_csv(out, index=False)
print('Wrote', out, 'rows=', len(monthly))
print(monthly.head().to_string())

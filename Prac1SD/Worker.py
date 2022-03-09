import pandas as pd
import xmlrpc.client


df = pd.read_csv('cities.csv')
server = xmlrpc.client.ServerProxy('http://localhost:8000')


print(df.to_string())
print (df.columns.values[2])
print (df.at[0,"LatD"])
print (df.iat[0,0])
print(df['LonD'].max(axis=0))
print(df['LonD'].min(axis=0))
print(df.items)
print(df.groupby)
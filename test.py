import pymysql
import pandas as pd
import sqlalchemy as db

engine= db.create_engine('mysql+pymysql://root:root@localhost:3306/org')
myConnection=engine.connect()
metadata=db.MetaData()
account=db.Table('accountdetails',metadata,autoload=True,autoload_with=engine)
query=db.select([account])
result=(myConnection.execute(query)).fetchall()
print(result)

'''
print(engine.table_names)
df= pd.read_sql_table('accountdetails',engine)

print(df)
'''

df=pd.read_csv('AccountDetails.csv')
insertDetails=df[['ACCOUNT_ID','ACCOUNT_NAME']] [df['MODE'] =='I']
print(insertDetails)
insDet=insertDetails.values.tolist()
print(insDet)
print(type(insDet))
print(dict(insDet))
insDet=dict(insDet)

'''temp=[]
i=1
for index,row in insertDetails.iterrows():
        temp.append({'ACCOUNT_ID':row['ACCOUNT_ID'],'ACCOUNT_NAME':row['ACCOUNT_NAME']})
        i=2'''

for index,row in insertDetails.iterrows():
        query=db.insert(account).values(ACCOUNT_ID=row['ACCOUNT_ID'],ACCOUNT_NAME=row['ACCOUNT_NAME'])
        try:
                result=myConnection.execute(query,insDet)
                
        except Exception:
                print("Invalid Input provided. Please Check the Input File")

'''for index,row in insertDetails.iterrows():
        query=db.insert(account).values(ACCOUNT_ID=row['ACCOUNT_ID'],ACCOUNT_NAME=row['ACCOUNT_NAME'])
        try:
                result=myConnection.execute(query,insDet)
        except Exception:
                print("Invalid Input provided Please Check the Input File")
'''

import pymysql
import pandas as pd
import sqlalchemy as db
import threading

def threadDataHandling(fileName):
        engine= db.create_engine('mysql+pymysql://root:root@localhost:3306/organizationcatalog')
        myConnection=engine.connect()
        metadata=db.MetaData()
        account=db.Table('account_detail',metadata,autoload=True,autoload_with=engine)
        query=db.select([account])

        df=pd.read_csv(fileName)
        insertDetails=df[['ACCOUNT_ID','ACCOUNT_NAME']] [df['MODE'] =='I']
        #print(insertDetails)

        for index,row in insertDetails.iterrows():
                query=db.insert(account).values(ACCOUNT_ID=row['ACCOUNT_ID'],ACCOUNT_NAME=row['ACCOUNT_NAME'])
                try:
                        result=myConnection.execute(query)
                        if result.rowcount == 0:
                                raise(ValueError)
                except Exception as err: 
                        print(str(err))
                        print(row)
                

        updateDetails=df[['ACCOUNT_NAME','ACCOUNT_ID']] [df['MODE'] =='U']
        #print(updateDetails)

        for index,row in updateDetails.iterrows():
                query=db.update(account).values(ACCOUNT_NAME=row['ACCOUNT_NAME']).where(account.columns.ACCOUNT_ID==row['ACCOUNT_ID'])
                try:
                        result=myConnection.execute(query)
                        if result.rowcount == 0:
                                raise(ValueError)
                
                except ValueError:
                        print("Invalid Input provided for Updation. Please Check the Input File")

        deleteDetails=df[['ACCOUNT_NAME','ACCOUNT_ID']] [df['MODE'] =='D']
        #print(deleteDetails)

        for index,row in deleteDetails.iterrows():
                query=db.delete(account).where(account.columns.ACCOUNT_ID==row['ACCOUNT_ID'])
                try:
                        result=myConnection.execute(query)
                        if result.rowcount == 0:
                                raise(ValueError)
                
                except ValueError:
                        print("Invalid Input provided for deletion. Please Check the Input File")

if __name__ == "__main__":
        listOfThreads=[]
        for i in range(5):
                fileName=("files/Account_Details_"+str(i)+".csv")
                try:
                        thread=threading.Thread(target=threadDataHandling,args=(fileName,))
                except:
                        print("Error Occured while creating thread")
                thread.start()#thread Starting
                listOfThreads.append(thread)

        for thread in listOfThreads:
                thread.join()#combining threads to main thread
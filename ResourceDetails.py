import pymysql
import pandas as pd
import sqlalchemy as db
import threading

def threadDataHandling(fileName):
        engine= db.create_engine('mysql+pymysql://root:root@localhost:3306/organizationcatalog')
        myConnection=engine.connect()
        metadata=db.MetaData()
        resource=db.Table('resource_detail',metadata,autoload=True,autoload_with=engine)
        query=db.select([resource])

        df=pd.read_csv(fileName)
        insertDetails=df[['RESOURCE_ID','PROJECT_ID','RESOURCE_NAME','TECHNOLOGY','DOJ']] [df['MODE'] =='I']

        for index,row in insertDetails.iterrows():
                query=db.insert(resource).values(PROJECT_ID=row['PROJECT_ID'],RESOURCE_ID=row['RESOURCE_ID'],RESOURCE_NAME=row['RESOURCE_NAME'],TECHNOLOGY=row['TECHNOLOGY'],DOJ=row['DOJ'])
                try:
                        result=myConnection.execute(query)
                except Exception as err: 
                        print(str(err))
                

        updateDetails=df[['RESOURCE_ID','PROJECT_ID','RESOURCE_NAME','TECHNOLOGY','DOJ']] [df['MODE'] =='U']

        for index,row in updateDetails.iterrows():
                query=db.update(resource).values(PROJECT_ID=row['PROJECT_ID'],RESOURCE_NAME=row['RESOURCE_NAME'],TECHNOLOGY=row['TECHNOLOGY'],DOJ=row['DOJ'])
                query=query.where(resource.columns.RESOURCE_ID == row['RESOURCE_ID'])
                try:
                        result=myConnection.execute(query)        
                        if result.rowcount == 0:
                                raise(ValueError)
                except ValueError:
                        print("Invalid Input provided for Updation. Please Check the file")
                        print(row)

        deleteDetails=df[['RESOURCE_ID','PROJECT_ID','RESOURCE_NAME','TECHNOLOGY','DOJ']] [df['MODE'] =='D']

        for index,row in deleteDetails.iterrows():
                query=db.delete(resource).where(resource.columns.RESOURCE_ID == row['RESOURCE_ID'])
                try:
                        result=myConnection.execute(query)        
                        if result.rowcount == 0:
                                raise(ValueError)
                except ValueError:
                        print("Invalid Input provided for Deletion. Please Check the file")
                        print(row)


if __name__ == "__main__":
        listOfThreads=[]
        for i in range(5):
                fileName=("files/Resource_Details_"+str(i)+".csv")
                try:
                        thread=threading.Thread(target=threadDataHandling,args=(fileName,))
                except:
                        print("Error Occured while creating thread")
                thread.start()#thread Starting
                listOfThreads.append(thread)

        for thread in listOfThreads:
                thread.join()#combining threads to main thread
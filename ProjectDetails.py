import pymysql
import pandas as pd
import sqlalchemy as db
import threading

def threadDataHandling(fileName):
        engine= db.create_engine('mysql+pymysql://root:root@localhost:3306/organizationcatalog')
        myConnection=engine.connect()
        metadata=db.MetaData()
        project=db.Table('project_detail',metadata,autoload=True,autoload_with=engine)
        query=db.select([project])

        df=pd.read_csv(fileName)
        insertDetails=df[['PROJECT_ID','ACCOUNT_ID','PROJECT_NAME','START_DATE','END_DATE']] [df['MODE'] =='I']

        for index,row in insertDetails.iterrows():
                query=db.insert(project).values(PROJECT_ID=row['PROJECT_ID'],ACCOUNT_ID=row['ACCOUNT_ID'],PROJECT_NAME=row['PROJECT_NAME'],START_DATE=row['START_DATE'],END_DATE=row['END_DATE'])
                try:
                        result=myConnection.execute(query)
                except Exception as err: 
                        print("Input Invalid.Please Check the Input File")
                except:
                        print("Record Already present")

        updateDetails=df[['PROJECT_ID','ACCOUNT_ID','PROJECT_NAME','START_DATE','END_DATE']] [df['MODE'] =='U']

        for index,row in updateDetails.iterrows():
                query=db.update(project).values(ACCOUNT_ID=row['ACCOUNT_ID'],PROJECT_NAME=row['PROJECT_NAME'],START_DATE=row['START_DATE'],END_DATE=row['END_DATE'])
                query=query.where(project.columns.PROJECT_ID==row['PROJECT_ID'])
                try:
                        result=myConnection.execute(query)
                        if result.rowcount == 0:
                                raise(ValueError)
                
                except ValueError:
                        print("Invalid Input provided for Updation. Please Check the Input File")
                        print(row)
                


        deleteDetails=df[['PROJECT_ID','ACCOUNT_ID','PROJECT_NAME','START_DATE','END_DATE']] [df['MODE'] =='D']

        for index,row in deleteDetails.iterrows():
                query=db.delete(project).where(project.columns.PROJECT_ID==row['PROJECT_ID'])
                try:
                        result=myConnection.execute(query)
                        if result.rowcount == 0:
                                raise(ValueError)
                
                except ValueError:
                        print("Invalid Input provided for Deletion. Please Check the Input File")
                        print(row)

if __name__ == "__main__":
        listOfThreads=[]
        for i in range(5):
                fileName=("files/Project_Details_"+str(i)+".csv")
                try:
                        thread=threading.Thread(target=threadDataHandling,args=(fileName,))
                except:
                        print("Error Occured while creating thread")
                thread.start()#thread Starting
                listOfThreads.append(thread)

        for thread in listOfThreads:
                thread.join()#combining threads to main thread
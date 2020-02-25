import mysql.connector
import pandas as pd
myConnection =mysql.connector.connect(host="localhost", user="root", passwd="root",database="organizationcatalog")

myCursor= myConnection.cursor()
'''myCursor.execute("create database organizationcatalog")
myCursor.execute("use organizationcatalog")
myCursor.execute("CREATE TABLE accountdetails ( \
  `ACCOUNT_ID` INT NOT NULL,\
  `ACCOUNT_NAME` VARCHAR(45) NOT NULL,\
  PRIMARY KEY (`ACCOUNT_ID`)); \'''
")'''

#myCursor.execute("select * from accountdetails")
accountDetails=pd.read_csv('AccountDetails.csv')
'''print(accountDetails)'''

for index,row in accountDetails.iterrows():
  print(str(row["ACCOUNT_ID"])+"  "+row["ACCOUNT_NAME"])
  if(row["MODE"]=='I'):
    values=(str(row["ACCOUNT_ID"]),row["ACCOUNT_NAME"])
    insertQuery="INSERT INTO `accountdetails`(ACCOUNT_ID,ACCOUNT_NAME) VALUES("+"%s"+","+"%s)"
    try:
      myCursor.execute(insertQuery,values)
    except:
      print("SQL Error:- Rrcord Already Present")
      
  elif(row["MODE"]=='U'):
    values=(row["ACCOUNT_NAME"],str(row["ACCOUNT_ID"]))
    updateQuery="UPDATE accountdetails SET ACCOUNT_NAME = %s WHERE ACCOUNT_ID=%s"
    try:
      myCursor.execute(updateQuery,values)
    except:
      print("SQL Error:- Record Already Present")
  else:
    print("DELETE")

myConnection.commit()
import mysql.connector
import pandas as pd
myConnection =mysql.connector.connect(host="localhost", user="root", passwd="root",database="organizationcatalog")

myCursor= myConnection.cursor()
#myCursor.execute("create database organizationcatalog")
myCursor.execute("use organizationcatalog")
'''myCursor.execute("CREATE TABLE accountdetails ( \
  `ACCOUNT_ID` INT NOT NULL,\
  `ACCOUNT_NAME` VARCHAR(45) NOT NULL,\
  PRIMARY KEY (`ACCOUNT_ID`)); \
")'''

#myCursor.execute("select * from accountdetails")
accountDetails=pd.read_csv('AccountDetails.csv')
print(accountDetails)

insertDetails=accountDetails[['ACCOUNT_ID','ACCOUNT_NAME']] [accountDetails['MODE'] =='I']
print("----------------------------------")
print(insertDetails)
print("-----------------------------------")
for index,row in insertDetails.iterrows():
        insertQuery="INSERT INTO `accountdetails`(ACCOUNT_ID,ACCOUNT_NAME) VALUES ("+"%s"+","+"%s) "
        try:
                myCursor.execute(insertQuery,tuple(row))
                print("Record Inserted Successfully")
        except:
                print("Record Exists")
        rowsAffected=myCursor.rowcount
        if(rowsAffected==0):
                raise TypeError("InValid Data")
        

'''updateDetails=accountDetails[['ACCOUNT_NAME','ACCOUNT_ID']] [accountDetails['MODE'] =='U']
for index,row in updateDetails.iterrows():
        updateQuery="UPDATE `accountdetails`SET ACCOUNT_NAME=%s WHERE ACCOUNT_ID=%s "
        try:
                myCursor.execute(updateQuery,tuple(row))
                rowsAffected=myCursor.rowcount
                if(rowsAffected==0):
                       raise TypeError("Failed to Update Record or InValid Data")
                else:
                        print("Record Updated Successfully")        
        except:
                print("Failed to Update Record "+str(rowsAffected))
'''
'''deleteDetails=accountDetails[['ACCOUNT_ID']] [accountDetails['MODE'] =='D']
for index,row in deleteDetails.iterrows():
        deleteQuery="DELETE FROM `accountdetails` WHERE ACCOUNT_ID=%s"
        try:
                myCursor.execute(deleteQuery,tuple(row))
        except:
                print("Failed to Delete Record")
'''
'''insertDetails=accountDetails[['ACCOUNT_ID','ACCOUNT_NAME']] [accountDetails['MODE']=='I']
print(insertDetails)

insertDetails=accountDetails[['ACCOUNT_ID','ACCOUNT_NAME']] [accountDetails['MODE']=='I']
print(insertDetails)'''
myConnection.commit()
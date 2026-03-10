import pandas as pd
import mysql.connector as m_sql
from sqlalchemy import create_engine as c_engine
from dotenv import load_dotenv
import os
import matplotlib.pyplot as pl

# load dataset
data = pd.read_csv(
    "D:\\DA_project\\indian_diseases_dataset\\indian_diseases_dataset.csv")

print(data.describe())
print(data.info())
print(data.isnull().sum())
print(data["comorbidity"].isnull().sum())

print(data['age_group'].unique())
data["age_group"] = data["age_group"].replace("19-oct", "0-20")
data["age_group"] = data["age_group"].replace("80+", "80-90")
print(data["comorbidity"].head(100))

data["comorbidity"] = data["comorbidity"].fillna("never")
data["alcohol_use"] = data["alcohol_use"].fillna("never")
# print(data[["comorbidity","alcohol_use"]])

data["cause_of_death"] = data["cause_of_death"].fillna("Unknown")

data["recovery_days"] = (
    data.groupby("disease_name")["recovery_days"]
    .transform(lambda x: x.fillna(x.median()))
)

data["recovery_days"] = (
    data.groupby("disease_name")["recovery_days"]
    .transform(lambda x: x.fillna(x.median()))
    .fillna(data["recovery_days"].median())
)
print(data[["cause_of_death", "recovery_days"]])
# print(data["comorbidity"].unique())

print(data["age_group"].unique())

print(data["alcohol_use"].unique())

print(data["cause_of_death"].unique())

print(data["comorbidity"].unique())

print(data["recovery_days"].unique())

print(data["blood_group"].unique())

print(data["bmi"].unique())

print(data["symptoms"].unique())

print(data["diagnosis_date"].head(100))

data["diagnosis_date"] = pd.to_datetime(data["diagnosis_date"])
print(data["diagnosis_date"].dtype)


# load env
load_dotenv()
dataBase = m_sql.connect(
    host=os.getenv("MYSQL_HOST"),
    user=os.getenv("MYSQL_USER"),
    passwd=os.getenv("MYSQL_PASSWORD")
    # database=os.getenv("MYSQL_DB")
)
"""#1.step 
cursor_object=dataBase.cursor()
query = "CREATE DATABASE IF NOT EXISTS indian_diseases_data"
cursor_object.execute(query)
dataBase.commit()"""

# 2.step
createengine = c_engine(
    'mysql+mysqlconnector://root:vivek@localhost/indian_diseases_data')
query = "select *from indian_diseases_data"

new_data = pd.DataFrame(data)
new_data.to_sql("indian_diseases_data", createengine,
                if_exists="append", index=False)
df = pd.read_sql(query, createengine)
print(df)
data = pd.DataFrame(df)
print("database successfully created:-")
dataBase.close()

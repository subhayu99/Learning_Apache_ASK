import pandas as pd

df: pd.DataFrame = pd.read_pickle("/home/subhayu/Downloads/Learning_Apache_ASK/Airflow/data/df.pkl")


print(df.head())
print(df.columns)
print(df.shape)
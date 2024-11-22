import datetime
print(datetime.datetime.now())

import pandas
df = pandas.read_csv("/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/student.txt")
print(df)
## print(df.filter(df["grade"]<5))
faildf = df.query("grade<5")
faildf.to_csv("/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/out.csv",
              sep="|",
              index=False,
              header=True, 
              columns=["id","name","subject","grade"],
              mode="w")
print("something")
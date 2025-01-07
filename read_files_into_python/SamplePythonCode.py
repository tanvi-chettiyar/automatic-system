import datetime
print(datetime.datetime.now())

import pandas
df = pandas.read_csv("/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/data/student.txt")
print(df)
## print(df.filter(df["grade"]<5))
faildf = df.query("grade<5")
faildf2 = df.loc(df['grade'] < 5, ['id','name'])
faildf3 = df[df['grade'].isin([0,1,2,3,4])]
faildf.to_csv("/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/out.csv",
              sep="|",
              index=False,
              
              header=True, 
              columns=["id","name","subject","grade"],
              mode="w")

print(datetime.datetime.now())
print("Tests")

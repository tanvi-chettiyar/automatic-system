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

## import pandas library using pip install pandas in terminal and then import is into code

##     dataframe = pandas.read_cvs("path")
##          it reads a comma separated values into a dataframe 
##          (two-dimensional labeled data structure in pandas, similar to a spreadsheet or a SQL table)

##     newdataframe = dataframe.query("condition")
##          using SQL-esque language to filter data which is helpful if conditions are complex
##          use logical operators AND, OR, NOT to write a conditional statement
	
##     newdataframe = dataframe.loc(dataframe['column'] boolean operator condition, ['column', etc.]
##          used for more complex filtering which can filter both rows and columns with specifying conditionals
##          this command filters those with failing grades and select the specified columns from the filtered data

##     newdataframe = dataframe[dataframe['column'].isin(['value1', 'value2', etc])]
##          used when a list of values is given to check against column values
##          a new dataframe is wrapped around to create a new filtered dataframe

##     dataframe.to_cvs("path", sep= "(_)", index=(True or False), index_label=["label1", "label2", etc.] or False, ...
##     header=(True or False), columns= ["label1", "label2", etc.] 
##          output dataframe into a cvs file with the given specifications into the specified folder


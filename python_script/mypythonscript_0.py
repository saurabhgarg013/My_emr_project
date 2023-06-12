import pandas as pd

# Dict object
courses = {'Courses':['Spark','PySpark','Java','PHP'],
           'Fee':[10000,20000,3000,40000],
           'Duration':['10','20','30','20']}

# Create DataFrame from dict
df = pd.DataFrame.from_dict(courses)
print(df)
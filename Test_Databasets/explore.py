
import pandas as pd
import chardet


df_1= pd.read_csv(r'Scholar.csv')



# Detect encoding
with open('DBLP1.csv', 'rb') as file:
    result = chardet.detect(file.read())

encoding = result['encoding']

# Read the CSV file with the detected encoding
df_2 = pd.read_csv('DBLP1.csv', encoding=encoding)


print(df_1.columns)
print(df_2.columns)

print(len(df_1))
print(len(df_2))

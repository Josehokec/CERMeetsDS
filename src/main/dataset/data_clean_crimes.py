"""
this program aims to clean crimes dataset
crimes file download URL: https://data.cityofchicago.org/stories/s/5cd6-ry5g
(clik dots then click export data)
please change filename
"""

import pandas as pd
import numpy as np

filename = 'CrimesXXXX.csv'
df = pd.read_csv(filename)

# df_cleaned = df.dropna(axis=1)
df['Date'] = pd.to_datetime(df['Date'], format="%m/%d/%Y %I:%M:%S %p")
df['Date'] = df['Date'].astype(int) // 10**9
# use '_' to replace ' ' charecters
df['Primary Type'] = df['Primary Type'].str.replace(' ', '_', regex=False)
df['Primary Type'] = df['Primary Type'].str.replace('INTERFERENCE_WITH_PUBLIC_OFFICER', 'INTERFERENCE_OFFICER', regex=False)
df['Primary Type'] = df['Primary Type'].str.replace('OFFENSE_INVOLVING_CHILDREN', 'OFFENSE_IN_CHILDREN', regex=False)
df['Primary Type'] = df['Primary Type'].str.replace('CONCEALED_CARRY_LICENSE_VIOLATION', 'CONCEALED_CARRY_LICENSE', regex=False)
df['Primary Type'] = df['Primary Type'].str.replace('NON-CRIMINAL_(SUBJECT_SPECIFIED)', 'NON-CRIMINAL', regex=False)

saved_column_names = ['Primary Type', 'ID', 'Case Number', 'IUCR', 'Beat', 'District', 'Latitude', 'Longitude', 'FBI Code', 'Date']

# remove the columns that have most of null values, or redundant values
selected_columns = df[saved_column_names]
for i in range(len(saved_column_names)):
    column_name = saved_column_names[i]
    selected_columns = selected_columns[selected_columns[column_name].notna()]

"""
INTERFERENCE_WITH_PUBLIC_OFFICER    -> INTERFERENCE_OFFICER
OFFENSE_INVOLVING_CHILDREN          -> OFFENSE_IN_CHILDREN
CONCEALED_CARRY_LICENSE_VIOLATION   -> CONCEALED_CARRY_LICENSE
NON-CRIMINAL_(SUBJECT_SPECIFIED)    -> NON-CRIMINAL
"""


selected_columns['Beat'].astype(int)
selected_columns['District'].astype(int)

# save new file
selected_columns.to_csv('crimes.csv', index=False)
# print(selected_columns.head())


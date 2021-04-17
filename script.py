import pandas as pd
import re

print('Reading data from file...')
col_names = ['S/N', 'POSTING DATE', 'VALUE DATE', 'DESCRIPTION', 'DEBIT', 'CREDIT', 'BALANCE']
extra_cols = ['EXTRA1', 'EXTRA2', 'EXTRA3', 'EXTRA4']
df = pd.read_csv('raw/account_stmt_01May2020_05Mar2021.csv', skiprows=2, names=col_names+extra_cols, usecols=[1,2,3,4,5,6,7,8,9,10])
# start index from 1 instead of 0
df.index += 1
print('Data read from file successfully.')

print()
print('Processing data...')
print()

mandates = {
    'MANDATE': [],
    'CREDIT': []
}

count = 0
for index, row in df.iterrows():
    if not pd.isnull(row['EXTRA1']):
        if not pd.isnull(row['EXTRA4']):
            debit, credit, balance = row['EXTRA2'], row['EXTRA3'], row['EXTRA4']
        elif not pd.isnull(row['EXTRA3']):
            debit, credit, balance = row['EXTRA1'], row['EXTRA2'], row['EXTRA3']
        elif not pd.isnull(row['EXTRA2']):
            debit, credit, balance = row['BALANCE'], row['EXTRA1'], row['EXTRA2']
        else:
            debit, credit, balance = row['CREDIT'], row['BALANCE'], row['EXTRA1']

        df.loc[index, 'DEBIT'], df.loc[index, 'CREDIT'], df.loc[index, 'BALANCE'] = debit, credit, balance

    if 'PayDay' in row['DESCRIPTION']:
        mandate_number = re.search('/.*?:(.*?)-PayDay', row['DESCRIPTION']).group(1)
        mandates['MANDATE'].append(mandate_number)
        mandates['CREDIT'].append(row['CREDIT'])

    count += 1
    print(count, ' row(s) processed.')

print()
print(count, ' row(s) were processed successfully.')

# delete extra columns
df.drop(extra_cols, axis=1, inplace=True)

df_mandates = pd.DataFrame(mandates)
# start index from 1 instead of 0
df_mandates.index += 1

print()
print('Writing cleaned data to file...')
# export cleaned data to csv
df.to_csv('new/account_stmt_01May2020_05Mar2021_cleaned_new.csv')
print('Cleaned data written to file successfully.')

print('Writing mandates to file...')
# export mandates to csv
df_mandates.to_csv('new/account_stmt_01May2020_05Mar2021_cleaned_manadates_new.csv')
print('Mandates written to file successfully.')
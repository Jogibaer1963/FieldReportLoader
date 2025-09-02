from pymongo import MongoClient
import pandas as pd


client = MongoClient("mongodb://localhost:27017/")


# Connect to a specific database
db = client['reports']

# Connect to a collection (similar to a SQL table)
collection = db['field-reports']

# replace "VP:" in the first row with empty space
def main():
    df = pd.read_excel('field-report.xlsx')
    df.columns = df.columns.str.replace('VP: ', '')
    df = df.fillna("")

    for column in df.select_dtypes(include=['datetime64[ns]']).columns:
        df[column] = df[column].astype(str).replace('NaT', '')

    df['long text failure1'] = (df['long text failure1'].fillna('').astype(str) +
                                ' ' + df['long text failure2'].fillna('').astype(str) +
                                ' ' + df['long text failur3'].fillna('').astype(str) +
                                ' ' + df['long text failur4'].fillna('').astype(str) +
                                ' ' + df['long text failur5'].fillna('').astype(str) +
                                ' ' + df['long text failur6'].fillna('').astype(str)
                                )
    columns_to_drop = ['long text failure2', 'long text failur3', 'long text failur4',
                       'long text failur5', 'long text failur6']
    df = df.drop(columns_to_drop, axis=1)

    df['long text remedy1'] = (df['long text remedy1'].fillna('').astype(str) +
                               ' ' + df['long text remedy2'].fillna('').astype(str) +
                               ' ' + df['long text remedy3'].fillna('').astype(str) +
                               ' ' + df['long text remedy4'].fillna('').astype(str) +
                               ' ' + df['long text remedy5'].fillna('').astype(str) +
                               ' ' + df['long text remedy6'].fillna('').astype(str)
                               )
    columns_to_drop = ['long text remedy2', 'long text remedy3', 'long text remedy4',
                       'long text remedy5', 'long text remedy6']
    df = df.drop(columns_to_drop, axis=1)

    df['long text reason1'] = (df['long text reason1'].fillna('').astype(str) +
                               ' ' + df['long text reason2'].fillna('').astype(str)
                               )
    columns_to_drop = ['long text reason2']
    df = df.drop(columns_to_drop, axis=1)

    df['long text diag1'] = (df['long text diag1'].fillna('').astype(str) +
                             ' ' + df['long text diag2'].fillna('').astype(str) +
                             ' ' + df['long text diag3'].fillna('').astype(str) +
                             ' ' + df['long text diag4'].fillna('').astype(str) +
                             ' ' + df['long text diag5'].fillna('').astype(str) +
                             ' ' + df['long text diag6'].fillna('').astype(str)
                             )
    columns_to_drop = ['long text diag2', 'long text diag3', 'long text diag4',
                       'long text diag5', 'long text diag6']
    df = df.drop(columns_to_drop, axis=1)

    df['long text com.1'] = (df['long text com.1'].fillna('').astype(str) +
                             ' ' + df['long text com.2'].fillna('').astype(str) +
                             ' ' + df['long text com.3'].fillna('').astype(str)
                             )
    columns_to_drop = ['long text com.2', 'long text com.3']
    df = df.drop(columns_to_drop, axis=1)

    df['long text ext. Com.1'] = (df['long text ext. Com.1'].fillna('').astype(str) +
                                  ' ' + df['long text ext. Com.2'].fillna('').astype(str) +
                                  ' ' + df['long text ext. Com.3'].fillna('').astype(str)
                                  )
    columns_to_drop = ['long text ext. Com.2', 'long text ext. Com.3']

    df = df.drop(columns_to_drop, axis=1)

    df.to_excel('field-report_updated.xlsx', index=False)

    # updated_df = pd.read_excel('field-report_updated.xlsx')

    # Konvertierung des DataFrames in eine Liste von Dictionaries
    #    records = df.to_dict('records')

    # Löschen der vorhandenen Dokumente in der Collection (optional)
    collection.delete_many({})
    # Einfügen der neuen Dokumente
    result = collection.insert_many(df.to_dict('records'))

    print(f"Erfolgreich {len(result.inserted_ids)} Dokumente in MongoDB eingefügt")



if __name__ == "__main__":
    main()
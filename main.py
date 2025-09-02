import sys
import logging
from pathlib import Path

from pymongo import MongoClient
import pandas as pd
from uuid import uuid4
import re



client = MongoClient("mongodb://localhost:27017/")


# Connect to a specific database
db = client['reports']

# Connect to a collection (similar to a SQL table)
collection = db['field-reports']

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    filename=LOG_DIR / "app.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

INPUT = Path('C:/files/field-report.xlsx')


def main():
    try:
        # 2) Robust einlesen
        df = pd.read_excel(
            INPUT,
            engine="openpyxl",
            header=0,
            dtype=str,  # alles als String -> nichts "verschwindet"
            keep_default_na=False  # "NA"/"N/A" etc. bleiben Strings, nicht NaN
        )

        # 3) Header normalisieren (trim, NBSP entfernen, Whitespaces vereinheitlichen)
        def clean_header(s: str) -> str:
            if s is None: return ""
            s = str(s)
            s = (s.replace("\u00A0", " ").replace("\u2009", " ")
                 .replace("\t", " "))
            s = re.sub(r"\s+", " ", s).strip()
            return s

        # eliminate VP:
        df.columns = df.columns.str.replace('VP: ', '')
        #Shortn empty space
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
                                 ' ' + df['long text com.2'].fillna('').astype(str)
                                 )
        columns_to_drop = ['long text com.2', 'long text com.3']
        df = df.drop(columns_to_drop, axis=1)

        df['long text ext. Com.1'] = (df['long text ext. Com.1'].fillna('').astype(str) +
                                      ' ' + df['long text ext. Com.2'].fillna('').astype(str) +
                                      ' ' + df['long text ext. Com.3'].fillna('').astype(str)
                                      )
        columns_to_drop = ['long text ext. Com.2', 'long text ext. Com.3']

        df = df.drop(columns_to_drop, axis=1)


        df.to_excel('C:/files/field-report_updated.xlsx', index=False)

        # updated_df = pd.read_excel('field-report_updated.xlsx')

        # Konvertierung des DataFrames in eine Liste von Dictionaries
        records = df.to_dict('records')

        for r in records:
            r['_id'] = str(uuid4())  # statt ObjectId: plain String

        # Löschen der vorhandenen Dokumente in der Collection (optional)
        collection.delete_many({})

        # Einfügen der neuen Dokumente
        result = collection.insert_many(records)

        logging.info("Successfully inserted %d documents into MongoDB", len(result.inserted_ids))

    except FileNotFoundError:
        logging.exception("Required file missing: %s", INPUT)  # writes stack trace to logs/app.log
        sys.exit(1)  # end script with non-zero exit code




if __name__ == "__main__":
    main()
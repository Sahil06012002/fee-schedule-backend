import pandas as pd
import sqlalchemy
import json
import uuid
from datetime import datetime
import psycopg2
from enums import Operations
import hashlib
import numpy as np

# Database connection details
DATABASE = "demo_db"
HOST = "ep-falling-wildflower-a5hi1gh6-pooler.us-east-2.aws.neon.tech"
USER = "demo_db_owner"
PASSWORD = "9GbDkis2zVYX"
PORT = "5432"







def store_comparison_result(changes, column_changes, table_name):
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        dbname=DATABASE,
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT
    )
    cur = conn.cursor()

    
    
    # Insert the comparison metadata into the comparisons table
    comparison_id = str(uuid.uuid4())
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    cur.execute(
        "INSERT INTO table_versions (id, timestamp, table_name) VALUES (%s, %s, %s)",
        (comparison_id, timestamp, table_name)
    )

    # Insert column-level changes into the column_changes table
    for col_change in column_changes:
        cur.execute(
            "INSERT INTO column_changes (comparison_id, type, columns) VALUES ( %s, %s, %s)",
            (comparison_id, col_change['type'], col_change['columns'])
        )

    # Insert row-level changes into the changes table
    for change in changes:
        cur.execute(
            "INSERT INTO cell_changes (comparison_id, type, code, column_name, old_value, new_value) VALUES (%s, %s, %s, %s, %s, %s)",
            (comparison_id, change['type'], change['code'], change['column_name'], change.get('old_value'), change.get('new_value'))
        )

    
    
    conn.commit()
    cur.close()
    conn.close()
#helper functions----------------------------------------------------------------------------------------
def dtype_to_postgres(dtype):
        if pd.api.types.is_string_dtype(dtype) or dtype == 'object':
            return 'text'
        elif pd.api.types.is_numeric_dtype(dtype):
            return 'numeric'
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return 'timestamp'
        else:
            return 'text'

def hash_row(row, columns):
    row_str = ''.join([str(row[col]) for col in columns])
    hash_result = hashlib.md5(row_str.encode()).hexdigest()
    return hash_result[:10]

#funciton to update the df with with the hash values
def add_hash_col(df) : 
    df['hash'] = df.apply(lambda row: hash_row(row, ['role' , 'code']), axis=1)
    return df

def convert_to_python_type(value):
    if isinstance(value, (np.int64, np.float64)):
        return int(value)  # Convert numpy int/float to Python int
    elif isinstance(value, (np.bool_, np.object_)):
        return value.item()  # Convert numpy objects to Python native types
    return value


#helper functions----------------------------------------------------------------------------------------
def create_table(table_name, df):
  df.columns = [col.lower() for col in df.columns]

  engine = sqlalchemy.create_engine('postgresql://demo_db_owner:9GbDkis2zVYX@ep-falling-wildflower-a5hi1gh6-pooler.us-east-2.aws.neon.tech/demo_db?sslmode=require')
  df.to_sql(table_name, engine, index=False, if_exists='replace')
  engine.dispose()

def read_excel(excel_file_path): 
    conn = psycopg2.connect(
        dbname=DATABASE,
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT
    )
    cur = conn.cursor()  
    df = pd.read_excel(excel_file_path)

    df = add_hash_col(df)
    print(df)


    table_name = "demo" #this should come dynamically

    query = """
    SELECT EXISTS (
        SELECT 1 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        AND table_name = %s
    );
    """
    cur.execute(query, (table_name,))



    # Fetch the result
    table_exists = cur.fetchone()[0]
    cur.close()
    conn.close()
    if not table_exists:
        create_table(table_name,df)
        
    # else:
    #     update_table(table_name,df)




# read_excel(r'C:\Users\sahmad\Desktop\root\python\web-scrapping\desktop\demo.xlsx')




def compare_db_and_excel(db_table_name, excel_file_path):
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        dbname=DATABASE,
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT
    )
    cur = conn.cursor()
    
    # Load the data from the database table
    cur.execute(f"SELECT * FROM {db_table_name}")

    old_df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
    cur.close()
    conn.close()

    #extract the column names that would be hashed


    # curr.execute(f"SELECT columns from table_details")
    # column = curr.cur.fetchone()[0]
    # cur.close()
    # conn.close()

    # hashable_cols = column.split(",")

    
    # Load the data from the new Excel file
    new_df = pd.read_excel(excel_file_path)
    print(new_df)

    #create hash of each row and add a column in the df with respective hashes
    # new_df['hash'] = new_df.apply(lambda row: hash_row(row, hashable_cols), axis=1)

    new_df = add_hash_col(new_df)
    print(new_df)


    changes = []
    column_changes = []

    # Set the unique code column as the index for comparison
    old_df.set_index("hash", inplace=True)
    new_df.set_index("hash", inplace=True)

    # Extract unique codes
    old_codes = old_df.index
    new_codes = new_df.index

    # Detect added columns
    added_columns = {
        col: dtype_to_postgres(new_df[col].dtype) for col in new_df.columns if col not in old_df.columns
    }
    if added_columns:
        column_changes.append({
            "type": Operations.ADD.name,
            "columns": json.dumps(added_columns)
        })
    
    # Detect deleted columns
    deleted_columns = {
        col: dtype_to_postgres(old_df[col].dtype) for col in old_df.columns if col not in new_df.columns
    }
    if deleted_columns:
        column_changes.append({
            "type": Operations.DELETE.name,
            "columns": json.dumps(deleted_columns)
        })
    
    # Detect modifications at the cell level
    # old_code = list of hash val of existing table
    for code in old_codes:
        if code in new_codes:
            old_row = old_df.loc[code]
            new_row = new_df.loc[code]
            
            # before comparing make a check on the new_df ,if the col is present in  new_df, if not(col is deleted) new_val would be None
            for col in old_row.index:

                old_value = convert_to_python_type(old_row[col])
                if col not in new_row.index:  
                    new_value = None
                    changes.append({
                        "type": Operations.DELETE.name,
                        "code": code,
                        "column_name": col,
                        "old_value": old_value,
                        "new_value": new_value
                    })
                else : 
                    new_value = convert_to_python_type(new_row[col])
                    if old_value != new_value:
                        changes.append({
                            "type": Operations.UPDATE.name,
                            "code": code,
                            "column_name": col,
                            "old_value": old_value,
                            "new_value": new_value
                        })

            #this iteration is for the cols that are present in the new df but are not in the old df(cols are added)
            for col in new_row.index:
                if col not in old_row.index:
                    new_value = convert_to_python_type(new_row[col])
                    changes.append({
                        "type": Operations.ADD.name,
                        "code": code,
                        "column_name": col,
                        "old_value": None,  # No old value since the column is new
                        "new_value": new_value
                    })


    # Detect added rows (added codes)
    for code in new_codes:
        if code not in old_codes:
            for col in new_df.columns:
                new_value = convert_to_python_type(new_df.at[code, col])
                changes.append({
                    "type": Operations.ADD.name,
                    "code": code,
                    "column_name": col,
                    "old_value" : None,
                    "new_value": new_value
                })

    # Detect deleted rows (deleted codes)
    for code in old_codes:
        if code not in new_codes:
            for col in old_df.columns:
                old_value = convert_to_python_type(old_df.at[code, col])
                changes.append({
                    "type": Operations.DELETE.name,
                    "code": code,
                    "column_name": col,
                    "old_value": old_value,
                    "new_val" : None
                })

    import pprint
    pprint.pprint(changes)
    print("-----------------------------------------------")
    pprint.pprint(column_changes)
    store_comparison_result(changes,column_changes,"demo")
    return {"changes": changes, "column_changes": column_changes}

# ------------------------------------------------------------------------------------

compare_db_and_excel("demo",r"C:\Users\sahmad\Desktop\root\python\web-scrapping\desktop\demo.xlsx")





# -------------------------------------------------------------------------------

def apply_col_changes(conn, comparison_id, table_name):
    cur = conn.cursor()
    
    cur.execute("SELECT columns, type FROM column_changes WHERE comparison_id = %s", (comparison_id,))
    column_changes = cur.fetchall()
    print(column_changes)


    for change in column_changes:
        columns, change_type = change
        # columns = json.loads(columns)

        if change_type == Operations.ADD.name:
            for column, data_type in columns.items():
                column_quoted = f'"{column}"'
                sql = f"ALTER TABLE {table_name} ADD COLUMN {column_quoted} {data_type};"
                cur.execute(sql)        
        elif change_type == Operations.DELETE.name:
            for column, _ in columns.items():
                column_quoted = f'"{column}"'
                sql = f"ALTER TABLE {table_name} DROP COLUMN {column_quoted};"
                cur.execute(sql)

    conn.commit()
    cur.close()

def apply_cell_changes(conn, comparison_id, table_name):
    #extract the column name in which all the codes are present
    cur = conn.cursor()
    index_col_name = '"code"'
    
    cur.execute("SELECT type, code, column_name, old_value, new_value FROM cell_changes WHERE comparison_id = %s", (comparison_id,))
    cell_changes = cur.fetchall()

    for change in cell_changes:
        change_type = change[0]
        code = change[1]
        column_name = f'"{change[2]}"'
        old_value = change[3]
        new_value = change[4] 

        if change_type == Operations.UPDATE.name:
            cur.execute(f'UPDATE {table_name} SET {column_name} = %s WHERE {index_col_name} = %s', (new_value, code))
        elif change_type == Operations.ADD.name:
            cur.execute(f'INSERT INTO {table_name} ({index_col_name}, {column_name}) VALUES (%s, %s) ON CONFLICT ({index_col_name}) DO UPDATE SET {column_name} = %s', (code, new_value, new_value))
        elif change_type == Operations.DELETE.name:
            cur.execute(f'DELETE FROM {table_name} WHERE "Code" = %s', (code,))
    conn.commit()
    cur.close()

def apply_changes(comparison_id, table_name):

    conn = psycopg2.connect(
        dbname=DATABASE,
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT
    )
    
    try:
        apply_col_changes(conn, comparison_id, table_name)
        apply_cell_changes(conn, comparison_id, table_name)
    except Exception as e:
        conn.rollback()
        print(f"An error occurred: {e}")
        raise
    finally:
        conn.close()

# Example usage
# apply_changes('863b1fbc-da94-4c63-b307-5b80c26a3e09', "demo")


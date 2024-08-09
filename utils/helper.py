import hashlib
import pandas as pd
import numpy as np

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
def add_hash_col(df,hashable_cols) : 
    df['hash'] = df.apply(lambda row: hash_row(row, hashable_cols), axis=1)
    return df



def convert_to_python_type(value):
    if isinstance(value, (np.int64, np.float64)):
        return int(value)  # Convert numpy int/float to Python int
    elif isinstance(value, (np.bool_, np.object_)):
        return value.item()  # Convert numpy objects to Python native types
    return value
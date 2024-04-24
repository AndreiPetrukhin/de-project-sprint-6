import pandas as pd
import vertica_python
from typing import Dict, List, Optional
import os
import glob

class VerticaUtils:
    def __init__(self, host: str, user: str, password: str, port=5433):
        self.conn_info = {
            'host': host,
            'user': user,
            'password': password,
            'port': port,
            'connection_timeout': 5
        }

    def connection(self):
        """Establish a connection to the Vertica database."""
        return vertica_python.connect(**self.conn_info)

    def execute_sql_scripts(self, script_directory: str):
        script_files = sorted(glob.glob(f"{script_directory}/*.sql"))
        with self.connection() as conn, conn.cursor() as cur:
            for script_file in script_files:
                print(script_file)
                with open(script_file, 'r') as file:
                    sql_script = file.read()
                if sql_script.strip():  # Check if script is not empty
                    try:
                        cur.execute(sql_script)
                        conn.commit()
                        print(f"Successfully executed script: {script_file}")
                    except Exception as e:
                        print(f"Error executing script {script_file}: {e}")
                        raise
                else:
                    print(f"Script file {script_file} is empty or only contains comments.")


    def load_dataset_file_to_vertica(self, dataset_path: str, schema: str, table: str, columns: List[str], type_override: Optional[Dict[str, str]] = None):
        """Loads data from a CSV file into a Vertica table."""
        try:
            df = pd.read_csv(dataset_path)
            print("Initial NA counts by column:")
            print(df.isna().sum())

            if type_override:
                for col, dtype in type_override.items():
                    if 'int' in dtype:
                        df[col] = df[col].fillna(-1).astype(dtype)
                    elif 'datetime' in dtype:
                        df[col] = df[col].fillna(pd.Timestamp('1945-05-09 00:00:00.000')).astype(dtype)
                    else:
                        df[col] = df[col].fillna(-1)
            print("Data types after handling NAs and conversions:")
            print(df.dtypes)
            print("Sample data after conversions:")
            print(df.head())

            num_rows = len(df)
            columns_str = ', '.join(columns)
            copy_expr = f"COPY {schema}.{table} ({columns_str}) FROM STDIN DELIMITER ',' ENCLOSED BY '\"'"
            chunk_size = num_rows // 100 if num_rows > 100 else num_rows
            
            with self.connection() as conn, conn.cursor() as cur:
                for start in range(0, num_rows, chunk_size):
                    end = min(start + chunk_size, num_rows)
                    buffer = df.iloc[start:end].to_csv(index=False, header=False)
                    cur.copy(copy_expr, buffer)
                    conn.commit()
                    print(f"loaded rows {start+1}-{end}")
        except pd.errors.EmptyDataError:
            error_msg = "No data: The file was empty."
            print(error_msg)
            raise ValueError(error_msg)
        except pd.errors.ParserError as e:
            error_msg = f"Error parsing data: {e}"
            print(error_msg)
            raise ValueError(error_msg)
        except Exception as e:
            error_msg = f"An error occurred: {e}"
            print(error_msg)
            raise Exception(error_msg)

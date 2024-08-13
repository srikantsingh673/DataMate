from flask import Flask, render_template, request, jsonify
from flask.views import MethodView
import pymysql
import pandas as pd
import os

app = Flask(__name__)

def get_db_connection():
    """
    Establish a database connection.
    """
    return pymysql.connect(
        host=request.form.get('host'),
        user=request.form.get('user'),
        password=request.form.get('password'),
        database=request.form.get('database')
    )

class DashboardAPI(MethodView):
    def get(self):
        return render_template('index.html')

class ExportToCSVAPI(MethodView):
    def get(self):
        return render_template('export.html')

    def post(self):
        try:
            tables = [table.strip() for table in request.form.get('tables').split(',')]
            output_directory = './exported_files'
            os.makedirs(output_directory, exist_ok=True)
            
            with get_db_connection() as connection:
                for table in tables:
                    query = f"SELECT * FROM {table}"
                    df = pd.read_sql(query, connection)
                    if not df.empty:
                        csv_filename = os.path.join(output_directory, f'{table}.csv')
                        df.to_csv(csv_filename, index=False)
            
            return jsonify({"message": "Tables exported successfully", "status": "success"}), 200

        except Exception as e:
            return jsonify({"message": str(e), "status": "error"}), 500

class MigrateCSVToDBAPI(MethodView):
    def get(self):
        return render_template('migrate.html')

    def post(self):
        try:
            table_name = request.form.get('table_name')
            csv_file = request.files['csv_file']
            df = pd.read_csv(csv_file)

            # Process the DataFrame
            df.fillna({
                column: 0 if pd.api.types.is_numeric_dtype(df[column]) 
                else pd.Timestamp('1970-01-01') if pd.api.types.is_datetime64_any_dtype(df[column]) 
                else 'null'
                for column in df.columns
            }, inplace=True)

            with get_db_connection() as connection:
                cursor = connection.cursor()
                cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
                if not cursor.fetchone():
                    # Create table if it doesn't exist
                    column_definitions = [
                        f"`{col}` {self._get_column_type(df[col])}"
                        for col in df.columns
                    ]
                    create_table_query = f"CREATE TABLE `{table_name}` ({', '.join(column_definitions)})"
                    cursor.execute(create_table_query)

                # Insert data into the table
                columns = ', '.join([f"`{col}`" for col in df.columns])
                placeholders = ', '.join(['%s'] * len(df.columns))
                insert_query = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"
                data_to_insert = [tuple(x) for x in df.to_numpy()]
                cursor.executemany(insert_query, data_to_insert)
                connection.commit()

            return jsonify({"message": "CSV data migrated successfully", "status": "success"}), 200

        except Exception as e:
            return jsonify({"message": str(e), "status": "error"}), 500

    def _get_column_type(self, series):
        """
        Determine SQL data type based on pandas Series dtype.
        """
        
        if pd.api.types.is_numeric_dtype(series):
            return 'DOUBLE'
        elif pd.api.types.is_datetime64_any_dtype(series):
            return 'DATETIME'
        else:
            return 'VARCHAR(255)'

# Register the views
app.add_url_rule('/', view_func=DashboardAPI.as_view('dashboard'))
app.add_url_rule('/export', view_func=ExportToCSVAPI.as_view('export_to_csv_api'))
app.add_url_rule('/migrate', view_func=MigrateCSVToDBAPI.as_view('migrate_csv_to_db_api'))

if __name__ == '__main__':
    app.run(debug=True)

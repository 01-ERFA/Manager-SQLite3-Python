
import sqlite3, os, csv, json, re
class DataBaseSQLite:
    def __init__(self, path: str = './db_sqlite3.db') -> None:
        class GetElements:
            def __init__(self, db, cursor, table_name : str = '') -> None:
                self._db = db
                self._cursor = cursor
                self._table_name =  table_name if isinstance(table_name, str) else ''
        
                self._get_value : dict = { 'status': False, 'columns': [], 'values': [] }
                self._get_return : dict = { 'column': '', 'value': None }

            def get_value(self, column_name : str = '', value = None):
                
                def to_find(column_name : str = '', value = None) -> list:
                    result : list = []

                    if not column_name in self._get_value['columns']:
                        return result
                    
                    if not column_name in self._get_value['columns']:
                        return result
                    
                    i = self._get_value['columns'].index(column_name)
                    for element in self._get_value['values']:
                        if element[i] == value:
                            result.append(element)

                    return result
                
                def bad_end() -> bool:
                    self._get_value['values'] = []

                self._get_return['column'] = column_name
                self._get_return['value'] = value

                if self._get_value['status'] == True:
                    self._get_value['values'] = to_find(column_name, value)
                    return self 
                else:
                    self._get_value['status'] = True

                if not isinstance(column_name, str):
                    bad_end()
                    return self
                
                self._cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{self._table_name}'")
                if self._cursor.fetchone() is None:
                    bad_end()
                    return self

                try:
                    self._cursor.execute(f"PRAGMA table_info({self._table_name})")
                    columns_name : list = [column[1] for column in self._cursor.fetchall()]

                    if not column_name in columns_name:
                        bad_end()
                        return self

                    if value == None:
                        self._cursor.execute(f"SELECT * FROM {self._table_name} WHERE {column_name} IS NULL")
                    elif isinstance(value, str):
                        sql : str = f'"{value}"'
                        self._cursor.execute(f"SELECT * FROM {self._table_name} WHERE {column_name} = {sql}")
                    else:
                        self._cursor.execute(f"SELECT * FROM {self._table_name} WHERE {column_name} = {value}")

                    self._get_value['status'] = True
                    self._get_value['columns'] = columns_name
                    self._get_value['values'] = self._cursor.fetchall()

                except:
                    bad_end()
                return self

            def first(self):

                result : dict = {}

                try:
                    if self._get_value['status'] == False:

                        self._cursor.execute(f"SELECT * FROM {self._table_name} LIMIT 1")
                        row = self._cursor.fetchone()

                        self._cursor.execute(f"PRAGMA table_info({self._table_name})")
                        columns_name : list = [column[1] for column in self._cursor.fetchall()]

                        if row is None:
                            return None

                        while len(result) < len(columns_name):
                            i : int = len(result)
                            result[columns_name[i]] = row[i]

                    else: 

                        self._cursor.execute(f"SELECT * FROM {self._table_name} WHERE {self._get_return['column']} = {self._get_return['value']}")
                        self._get_value['values'][0] = self._cursor.fetchone()

                        while len(result) < len(self._get_value['columns']):
                            i : int = len(result)
                            result[self._get_value['columns'][i]] = self._get_value['values'][0][i]

                except:
                    pass

                return result
                    
            def all(self):

                result : list = []

                try:
                    if self._get_value['status'] == False:
                        self._cursor.execute(f"SELECT * FROM {self._table_name}")
                        rows = self._cursor.fetchall()

                        self._cursor.execute(f"PRAGMA table_info({self._table_name})")
                        columns_name : list = [column[1] for column in self._cursor.fetchall()]


                        if rows is None:
                            return None
                        
                        while len(result) < len(rows):
                            i : int = len(result)
                            new_value : dict = {}

                            while len(new_value) < len(columns_name):
                                j : int = len(new_value)
                                new_value[columns_name[j]] = rows[i][j]
                            result.append(new_value)

                    else:

                        self._cursor.execute(f"SELECT * FROM {self._table_name} WHERE {self._get_return['column']} = {self._get_return['value']}")
                        self._get_value['values'] = self._cursor.fetchall()

                        while len(result) < len(self._get_value['values']):
                            i : int = len(result)
                            new_value : dict = {}

                            while len(new_value) < len(self._get_value['columns']):
                                j : int = len(new_value)
                                new_value[self._get_value['columns'][j]] = self._get_value['values'][i][j]
                            result.append(new_value)
                except:
                    pass

                return result

            def edit(self, column_name : str = '', new_value = None, integrity : bool = True) -> bool:

                if not isinstance(column_name, str):
                    return False

                if self._get_value['status'] == False:

                    try:
                        self._cursor.execute(f"PRAGMA table_info({self._table_name})")
                        fetch : list = self._cursor.fetchall()

                        columns_name : list = [column[1] for column in fetch]
                        columns_type : list = [column[2] for column in fetch]

                        if not column_name in columns_name:
                            return False
                        
                        if integrity == True:
                            match columns_type[columns_name.index(column_name)]:
                                case 'INTEGER':
                                    if not isinstance(new_value, int):
                                        return False
                                case 'REAL':
                                    if not isinstance(new_value, float):
                                        return False
                                case 'TEXT':
                                    if not isinstance(new_value, str):
                                        return False
                                case 'BLOB':
                                    try:
                                        sqlite3.Binary(new_value) 
                                        pass
                                    except:
                                        return False
                                case _:
                                    pass

                        sql : str = f"UPDATE {self._table_name} SET {column_name} = ? WHERE {columns_name[0]} = ?"
                        
                        self._cursor.execute(f"SELECT * FROM {self._table_name}")
                        

                        for element in self._cursor.fetchall():
                            self._cursor.execute(sql, (new_value, element[0]))

                        self._db.commit()

                        return True
                    except:
                        return False 

                if not column_name in self._get_value['columns']:
                    print('...', column_name, self._get_value)
                    return False
                
                try:
                    self._cursor.execute(f"PRAGMA table_info({self._table_name})")
                    columns_type : list = [column[2] for column in self._cursor.fetchall()]

                    if integrity == True:
                        match columns_type[self._get_value['columns'].index(column_name)]:
                            case 'INTEGER':
                                if not isinstance(new_value, int):
                                    return False
                            case 'REAL':
                                if not isinstance(new_value, float):
                                    return False
                            case 'TEXT':
                                if not isinstance(new_value, str):
                                    return False
                            case 'BLOB':
                                try:
                                    sqlite3.Binary(new_value) 
                                    pass
                                except:
                                    return False
                            case _:
                                pass
                    
                    sql : str = f"UPDATE {self._table_name} SET {column_name} = ? WHERE {self._get_value['columns'][0]} = ?"
                    
                    for element in self._get_value['values']:
                        self._cursor.execute(sql, (new_value, element[0]))

                    self._db.commit()

                    return True
                except:
                    return False
                                
            def delete(self) -> bool:
                try:
                    if self._get_value['status'] == False:
                        self._cursor.execute(f"DELETE FROM {self._table_name}")
                    else:
                        self._cursor.execute(f"DELETE FROM {self._table_name} WHERE {self._get_return['column']} = {self._get_return['value']}")

                    self._db.commit()
                    return True
                except:
                    return False

        class ExportDB:
            def __init__(self, db, cursor, path_file : str) -> None:

                self._db = db
                self._cursor = cursor
                self._path = path_file if isinstance(path_file, str) else None

            def sql(self) -> bool:

                if not isinstance(self._path, str):
                    return False

                dir_path : str = os.path.dirname(self._path)
                
                if self._path[-4:] != '.sql':
                    self._path = f"{self._path}.sql"         
        
                if not os.path.exists(dir_path) or not os.path.isdir(dir_path):
                    os.makedirs(dir_path)

                try:
                    with open(self._path, 'w') as _file:
                        for line in self._db.iterdump():
                            _file.write(f"{line}\n")
                except:
                    return False
                
                try:                    
                    if os.path.exists(self._path):
                        return True if os.path.getsize(self._path) > 0 else False
                    else:
                        return False
                except:
                    return False

            def csv(self) -> bool:

                if not isinstance(self._path, str):
                    return False

                dir_path : str = os.path.dirname(self._path)
                
                if self._path[-4:] != '.csv':
                    self._path = f"{self._path}.csv"         
        
                if not os.path.exists(dir_path) or not os.path.isdir(dir_path):
                    os.makedirs(dir_path)

                try:
                    self._cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                    tables_name = self._cursor.fetchall()

                    with open(self._path, 'w', newline='') as file_csv:
                        writer_csv = csv.writer(file_csv)

                        for table in tables_name:
                            writer_csv.writerow('')
                            writer_csv.writerow(table)

                            self._cursor.execute(f"PRAGMA table_info({table[0]})")

                            writer_csv.writerow(self._cursor.fetchall())

                            self._cursor.execute(f"SELECT * FROM {table[0]}")
                            results = self._cursor.fetchall()

                            for row in results:
                                writer_csv.writerow(row)

                    return True
                except:
                    return False

            def json(self) -> bool:
    
                if not isinstance(self._path, str):
                    return False

                new_json : dict = {}
                dir_path : str = os.path.dirname(self._path)
                
                if self._path[-5:] != '.json':
                    self._path = f"{self._path}.json"         
        
                if not os.path.exists(dir_path) or not os.path.isdir(dir_path):
                    os.makedirs(dir_path)

                try:
                    self._cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                    tables_name = self._cursor.fetchall()

                    for table in tables_name:

                        new_table : dict = {}

                        self._cursor.execute(f"PRAGMA table_info({table[0]})")
                        new_table['columns_info'] = self._cursor.fetchall()

                        self._cursor.execute(f"SELECT * FROM {table[0]}")
                        new_table['values'] = self._cursor.fetchall()

                        new_json[table[0]] = new_table

                    with open(self._path, 'w') as file:
                        json.dump(new_json, file, indent=4)

                    return True
                except:
                    return False

        class ImportDB:
            def __init__(self, self_parent, path_file : str, delete_old_tables : bool) -> None:

                self._db = self_parent.db
                self._cursor = self_parent.cursor
                self._insert_value = self_parent.insert_value

                self._path = path_file if isinstance(path_file, str) else None
                self._delete_old_tables = delete_old_tables
            
            def sql(self):

                if not isinstance(self._path, str) or self._path == '':
                    return False

                if self._path[-4:] != '.sql':
                    self._path = f"{self._path}.sql"
                
                if not os.path.exists(self._path):
                    return False
                
                try:
                    with open(self._path, 'r') as file:
                        sql : str = file.read()

                    tables_name = re.findall(r'CREATE\s+TABLE\s+(\w+)\s+\(', sql)

                    if self._delete_old_tables == True:
                        for table_name in tables_name:
                            self._cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                            self._db.commit()

                    self._db.executescript(sql)
                    self._db.commit()
                    
                
                    return True
                except:
                    return False
                
            def json(self):

                if not isinstance(self._path, str) or self._path == '':
                    return False
                
                if self._path[-5:] != '.json':
                    self._path = f"{self._path}.json"
                    
                if not os.path.exists(self._path):
                    return False
                
                json_read = None
                sql_execute : list = []

                with open(self._path, 'r') as file:
                    try:
                        json_read = json.loads(file.read())
                    except:
                        return False

                for table_name in json_read.keys():

                    sql : str = f"CREATE TABLE {table_name} (?)"
                    sql_columns : str = ''

                    for col_info in json_read[table_name]['columns_info']:

                        col_id, col_name, col_type, col_no_null, col_default_value, col_pk = col_info 

                        sql_columns += f"{col_name} {col_type}"

                        if col_no_null:
                            sql_columns += " NOT NULL"

                        if col_default_value is not None:
                            sql_columns += f" DEFAULT {col_default_value}"

                        if col_pk:
                            sql_columns += " PRIMARY KEY"
                        
                        sql_columns += ", "

                    sql = sql.replace('?', sql_columns.rstrip(', '))

                    i : int = len(sql_execute)

                    sql_execute.append({ 'status': False, 'table_name': table_name, 'sql': sql, 'values': json_read[table_name]['values'], 'cols_info': json_read[table_name]['columns_info'] })

                    if self._delete_old_tables == True:
                        self._cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                        self._db.commit()

                i : int = 0
                result : list = []
                while i < len(sql_execute):

                    table_name = sql_execute[i]['table_name']

                    try:
                        self._cursor.execute(sql_execute[i]['sql'])
                        self._db.commit()
                        sql_execute[i]['status'] = True
                    except:
                        pass

                    for value in sql_execute[i]['values']:

                        if sql_execute[i]['status']:
                            result.append(self._insert_value(table_name, value))
                        else:
                            self._cursor.execute(f"PRAGMA table_info({table_name})")

                            status : bool = True
                            cols_info = self._cursor.fetchall()
                            
                            j : int = 0

                            for col_info in cols_info:
                                if col_info != tuple(sql_execute[i]['cols_info'][j]):
                                    status = False
                                j = j+1

                            result.append(self._insert_value(table_name, value) if status else None)
                    i = i+1
                
                return result
                
 
            def csv(self):
                pass
            


        self.path : str = path
        self.db = self._connect_db()
        self.cursor = self.db.cursor()
        
        self.query = lambda table_name = '' : GetElements(self.db, self.cursor, table_name) 
        self.export_db = lambda path_file = './export' : ExportDB(self.db, self.cursor, path_file)
        self.import_db = lambda path_file = './', delete_old_tables = False : ImportDB(self, path_file, delete_old_tables)

    def _connect_db(self):
        dir_path : str = os.path.dirname(self.path)
        
        if not os.path.exists(dir_path) or not os.path.isdir(dir_path):
            os.makedirs(dir_path)
        
        conn = sqlite3.connect(self.path)
        return conn
    
    def close_db(self):
        self.db.close()

    def table_exists(self, table_name: str) -> bool:

        if not isinstance(table_name, str):
            return False
        
        sql = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
        self.cursor.execute(sql)
        result = self.cursor.fetchone()

        if result is not None:
            return True
        else:
            return False

    # example: [ {name: 'id', value: 'INTEGER', properties: 'PRIMARY KEY'} ]
    def create_table(self, name : str, content : list) -> bool:

        def filter_dicts(list_: list) -> list[dict]:
            dict_only = [
                element
                for element in list_
                if isinstance(element, dict)
                and all(key in element for key in ["name", "value", "properties"])
            ]
            return dict_only
        
        def convert_to_str(list_ : list) -> str:
            result = ''
            for element in list_:
                if isinstance(element, dict) and all(key in element for key in ["name", "value", "properties"]):
                    result += '{} {} {}, '.format(element['name'], element['value'], element['properties'])
            result = result.rstrip(', ') 
            return result

        if name == '' or not isinstance(content, list) or not isinstance(name, str):
            return False
        
        content = filter_dicts(content)
        result_content: str  = convert_to_str(content)
        
        self.cursor.execute(f''' SELECT name FROM sqlite_master WHERE type='table' AND name='{name}'; ''')
        result = self.cursor.fetchone()

        if result is not None:
            return False
        
        self.cursor.execute(f''' CREATE TABLE IF NOT EXISTS {name} ({result_content}); ''')
        self.db.commit()

        self.cursor.execute(f''' SELECT name FROM sqlite_master WHERE type='table' AND name='{name}'; ''')
        result = self.cursor.fetchone()

        if result is not None:
            return True
        else:
            return False

    def delete_table(self, table_name : str):
        try:
            if not isinstance(table_name, str):
                return False
            
            if not self.table_exists(table_name):
                return False

            self.cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.db.commit()

            if not self.table_exists(table_name):
                return True
        except:
            return False
    
    def print_database(self): # basic (no relations)

        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = self.cursor.fetchall()

        for table in tables:
            table_name = table[0]

            self.cursor.execute(f"PRAGMA table_info({table_name})")
            columns : list = self.cursor.fetchall()

            table_info : dict = {}
            for column in columns:
                table_info[column[1]] = column[2]
            
            print(" - Table: {} \n   {} \n {}".format(table_name, str(table_info), "-" * 20))

            self.cursor.execute(f"SELECT * FROM {table_name};")
            records = self.cursor.fetchall()

            for record in records:
                print(f'   {record}')

            print("\n")

    # supported_data_types : list = ['INTEGER', 'REAL', 'TEXT', 'BLOB', 'NULL']
    def insert_value(self, table_name : str, values: tuple | list) -> bool:

        def check_value(type_value : str, value) -> bool:
            type_value = type_value.upper()
            match type_value:
                case 'INTEGER':
                    if isinstance(value, int):
                        return True
                    else: 
                        return False
                case 'REAL':
                    if isinstance(value, float):
                        return True
                    else: 
                        return False
                case 'TEXT':
                    if isinstance(value, str):
                        return True
                    else: 
                        return False
                case 'NULL':
                    if value == None:
                        return True
                    else:
                        return False
                case 'BLOB':
                    try:
                        sqlite3.Binary(value) 
                        return True
                    except:
                        return False
                case _:
                    return False
            
        try:
            self.cursor.execute(f"PRAGMA table_info({table_name})")
            columns : list = self.cursor.fetchall()

            name_columns : list = [column[1] for column in columns]
            type_columns : list = [type_column[2] for type_column in columns]

            new_data : dict = {} 
            new_values : list = []

            placeholders : str = ", ".join(["?" for _ in name_columns])
            sql = f"INSERT INTO {table_name} VALUES ({placeholders});"

            while len(new_data) < len(name_columns):
                try:
                    i : int = len(new_data)
                    new_data[name_columns[i]] = {'type': type_columns[i], 'value': values[i] }
                except IndexError:
                    i : int = len(new_data)
                    new_data[name_columns[i]] = {'type': type_columns[i], 'value': None }

            for element in new_data:
                if check_value(new_data[element]['type'], new_data[element]['value']):
                    new_values.append(new_data[element]['value'])
                elif new_data[element]['value'] == None:
                    new_values.append('' if new_data[element]['type'] == 'TEXT' else None)
                elif new_data[element]['value'] is not None:
                    return False

            self.cursor.execute(sql, tuple(new_values))
            self.db.commit()

            if self.cursor.rowcount == 1:
                return True
            else: 
                return False
        except:
            return False

    def rename_table(self, table_name : str = '', new_table_name : str = '') -> bool:
        if not isinstance(table_name, str) or not isinstance(new_table_name, str):
            return False
        try:
            self.cursor.execute(f"ALTER TABLE {table_name} RENAME TO {new_table_name}")
            self.db.commit()
        except:
            return False
        return True

    def add_col_table(self, table_name : str = '', column_name : str = '', column_type : str = '') -> bool:
        if not isinstance(table_name, str) or not isinstance(column_name, str) or not isinstance(column_type, str):
            return False
        if not column_type in ['INTEGER', 'REAL', 'TEXT', 'BLOB']:
            return False

        try:
            self.cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")
            self.db.commit()
            return True
        except:
            return False

    def get_table(self, table_name : str = '') -> bool | list:
        
        data_table : list = [] 

        if not isinstance(table_name, str):
            return False

        try:
            self.cursor.execute(f"SELECT * FROM {table_name}")
            values : list = self.cursor.fetchall()

            self.cursor.execute(f"PRAGMA table_info({table_name})")
            column_names : list = [name[1] for name in self.cursor.fetchall()]

            while len(data_table) < len(values):
                i : int = len(data_table)
                new_ : dict = {}
                while len(new_) < len(column_names):
                    j : int = len (new_)
                    new_[column_names[j]] = values[i][j]
                data_table.append(new_)

            return data_table
        except:
            return False
        
    def delete_col_table(self, table_name : str = '', column_name : str = '', integrity : bool = True) -> bool:

        if not isinstance(table_name, str) or not isinstance(column_name, str):
            return False
        
        try:
            self.cursor.execute(f"PRAGMA table_info({table_name});")
            columns_info : list = self.cursor.fetchall()
            columns_names : list = [name[1] for name in columns_info]

            if not column_name in columns_names:
                return False
            
            if integrity != True:
                if columns_info[columns_names.index(column_name)][5] == 1 or columns_info[columns_names.index(column_name)][3] == 1:

                    query : str = "CREATE TABLE {} AS SELECT {} FROM {};".format(table_name*5, ", ".join(element for element in columns_names if element != column_name), table_name)
                    self.cursor.execute(query)

                    query = f"DROP TABLE {table_name};"
                    self.cursor.execute(query)

                    query = "ALTER TABLE {} RENAME TO {};".format(table_name*5, table_name)
                    self.cursor.execute(query)

                else:
                    self.cursor.execute(f"ALTER TABLE {table_name} DROP COLUMN {column_name}")
            else:
                self.cursor.execute(f"ALTER TABLE {table_name} DROP COLUMN {column_name}")
                
            self.cursor.execute(f"PRAGMA table_info({table_name});")
            columns_names = [name[1] for name in self.cursor.fetchall()]

            if column_name in columns_names:
                return False
            
            self.db.commit()

            return True
        
        except:
            return False

    def backup_db(self, path : str = './backup_db_sqlite3.db', remove : bool = False):

        if not isinstance(path, str):
            return False
        
        dir_path : str = os.path.dirname(path)
        
        if path[-3:] != '.db':
            path = f"{path}.db"

        if not os.path.exists(dir_path) or not os.path.isdir(dir_path):
            os.makedirs(dir_path)

        if remove != False:
            try:
                os.remove(path)
            except OSError:
                pass
        
        try:
            backup_db = DataBaseSQLite(path)

            backup_sql = "\n".join(self.db.iterdump())

            backup_db.cursor.executescript(backup_sql)
            backup_db.db.commit()
            backup_db.db.close()

            return True
        except:
            return False

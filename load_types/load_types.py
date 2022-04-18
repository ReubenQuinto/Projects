import sqlite3
import pandas as pd

class Load_program():
    def __init__(self):
        self.logical_db = ""
        self.table_name = ""
        self.column_names = []
        self.primary_keys = []

    def read_table(self, table_name, connection = "None"):
        """
        This function returns 'columns & primarky keys' by querying SQLite's systems table
        """

        ############################################## sql code

        sql_query_01 = f"""
        PRAGMA table_info( {table_name} )
        """

        ############################################## execute

        connection = sqlite3.connect( connection, timeout=1 )
        # cursor = connection.cursor()

        df = pd.read_sql_query( sql_query_01, connection )

        ############################################## set variables

        # qc: check if table exists
        assert len( df['name'] ) != 0, f'Table {table_name} does not exist in current database.'

        # set locial_db
        self.logical_db = connection

        # set table_name
        self.table_name = table_name

        # set column_names
        self.column_names = list( df['name'] )

        # set primary_keys (check sqlite system table. primary keys are NOT 0)
        self.primary_keys = [ df['name'][i] for i in range(len(df)) if df['pk'][i] ]

    def get_db_name(self):
        return self.logical_db

    def get_table_name(self):
        return self.table_name

    def get_column_names(self):
        return self.column_names

    def get_primary_keys(self):
        return self.primary_keys


def generate_sql(destination_table="None", load_table="None", load_type = "None"):
    if load_type == 'merge_upsert_update':
        """
        data model:
            UPDATE { destination_table }
                SET actual_sale = b.actual_sale
            FROM { load_table } AS b
            WHERE target_table.date = b.date
                AND target_table.product = b.product
                AND target_table.state = b.state
                AND target_table.addr_id = b.addr_id
                AND target_table.actual_sale != b.actual_sale /* <---- SUPER IMPORTANT */;
        """

        ##### SET CLAUSE ##################################################
        # notes: ls_set is a list of column names that is not a primary key

        str_set_statement = ""

        ls_set = [ x for x in destination_table.get_column_names() if x not in destination_table.get_primary_keys() ]

        for x in ls_set:
            if x != ls_set[-1]:
                s = f'{x} = {load_table.get_table_name()}.{x}, '
                str_set_statement += s
            else:
                s = f'{x} = {load_table.get_table_name()}.{x}'
                str_set_statement += s

        ##### WHERE CLAUSE ################################################

        str_where_statement = ""

        for x in destination_table.get_primary_keys():
            if x != destination_table.get_primary_keys()[-1]:
                s = f'{destination_table.get_table_name()}.{x} = {load_table.get_table_name()}.{x} AND '
                str_where_statement += s
            else:
                s = f'{destination_table.get_table_name()}.{x} = {load_table.get_table_name()}.{x}'
                str_where_statement += s

        ###################################################################

        rv = f"""
                  UPDATE {destination_table.get_table_name()}
                      SET {str_set_statement}
                  FROM {load_table.get_table_name()}
                  WHERE {str_where_statement} ;
              """

        return rv

    if load_type == 'merge_upsert_insert':
        """
        data model:
            INSERT INTO testtable
            SELECT newvals.id, newvals.somedata
            FROM newvals
            LEFT OUTER JOIN testtable ON ( testtable.id = newvals.id )
            WHERE testtable.id IS NULL ;
        """
        ##### SELECT CLAUSE ###############################################

        str_select_statement = ""

        for x in load_table.get_column_names():
            if x != load_table.get_column_names()[-1]:
                s = f'{load_table.get_table_name()}.{x}, '
                str_select_statement += s
            else:
                s = f'{load_table.get_table_name()}.{x}'
                str_select_statement += s

        ##### ON CLAUSE ###################################################

        str_on_statement = ""

        for x in destination_table.get_primary_keys():
            if x != destination_table.get_primary_keys()[-1]:
                s = f'{destination_table.get_table_name()}.{x} = {load_table.get_table_name()}.{x} AND '
                str_on_statement += s
            else:
                s = f'{destination_table.get_table_name()}.{x} = {load_table.get_table_name()}.{x}'
                str_on_statement += s

        ##### WHERE CLAUSE ################################################

        str_where_statement = ""

        for x in destination_table.get_primary_keys():
            if x != destination_table.get_primary_keys()[-1]:
                s = f'{destination_table.get_table_name()}.{x} = {load_table.get_table_name()}.{x} AND '
                str_on_statement += s
            else:
                s = f'{destination_table.get_table_name()}.{x} = {load_table.get_table_name()}.{x}'
                str_on_statement += s

        return rv

    if load_type == 'append':
        """
        data mode:
            INSERT INTO target_table
            SELECT {cols}
            FROM load_table ;
        """

        ##### SELECT CLAUSE ###############################################

        str_select_statement = ""

        for x in load_table.get_column_names():
            if x != load_table.get_column_names()[-1]:
                s = f'{load_table.get_table_name()}.{x}, '
                str_select_statement += s
            else:
                s = f'{load_table.get_table_name()}.{x}'
                str_select_statement += s

        ###################################################################

        rv = f"""
                INSERT INTO {destination_table.get_table_name()}
                SELECT {str_select_statement}
                FROM {load_table.get_table_name()} ;
              """

        return rv

    if load_type == 'delete':
        """
        data model:
            DELETE FROM table_name;
        """

        rv = f"""
              DELETE FROM {destination_table.get_table_name()} ;
              """

        return rv

def load(destination_table="None", load_table="None", load_type="None"):

    connection = sqlite3.connect('tutorial.db', timeout=10)
    cursor = connection.cursor()

    try:
        if load_type=="merge_upsert":
            # update statement
            sql_update = generate_sql( destination_table=destination_table, load_table=load_table, load_type='merge_upsert_update')
            connection.execute(sql_update)

            # insert statement
            sql_insert = generate_sql( destination_table=destination_table, load_table=load_table, load_type='merge_upsert_insert')
            connection.execute(sql_insert)

            connection.commit()

        elif load_type=="merge_insert":
            # insert statement
            sql_insert = generate_sql( destination_table=destination_table, load_table=load_table, load_type='merge_upsert_insert')
            connection.execute(sql_insert)

            connection.commit()

        elif load_type=="merge_update":
            # update statement
            sql_update = generate_sql( destination_table=destination_table, load_table=load_table, load_type='merge_upsert_update')
            connection.execute(sql_update)

            connection.commit()

        elif load_type=="insert_append":
            # append statement
            sql_append = generate_sql( destination_table=destination_table, load_table=load_table, load_type='append')
            connection.execute(sql_append)

            connection.commit()
        elif load_type=="insert_table":
            # delete statement
            sql_delete = generate_sql( destination_table=destination_table, load_table=load_table, load_type='delete')
            connection.execute(sql_delete)

            # append statement
            sql_append = generate_sql( destination_table=destination_table, load_table=load_table, load_type='append')
            connection.execute(sql_append)

            connection.commit()

        elif load_type=="insert_partition":
            pass

        else:
            print('Error: Must choose an existing load type')

    except Exception as e:
        print(e)
        print('Transaction failed. Rolled Back.')
        connection.rollback()

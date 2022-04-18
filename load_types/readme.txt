goal:
	create an etl tool that automates loads

how to run code:
	table_1 = {obj}.read_table( {destination_table}, connection={db_name} )
	table_2 = {obj}.read_table( {load_table}, connection={db_name} )

	{obj}.load(destination_table={table_1}, load_table={table_2}, load_type="merge_upsert") 

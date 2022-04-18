goal:
	create an etl tool that automates loads

notes:
	- there are 3 main modules:
		1. read table (class that will obtain table name, primary keys, and column names)
		2. generate sql (function that dynamically generates sql statements)
		3. load (main function that execute the load)

how to run code:
	table_1 = {obj}.read_table( {destination_table}, connection={db_name} )
	table_2 = {obj}.read_table( {load_table}, connection={db_name} )

	{obj}.load(destination_table={table_1}, load_table={table_2}, load_type="merge_upsert") 

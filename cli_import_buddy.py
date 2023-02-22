from datetime import datetime
import sys, os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from rich import print as rprint
from rich.console import Console
from dotenv import load_dotenv


if __name__ == "__main__":
    num_arg = len(sys.argv)-1
    if num_arg == 0:
        # show user the correct usage if run without arguments
        rprint(f"[underline red]\nMissing arguments, correct usage:[/]\n[bold cyan]{os.getcwd()}\\python[/] [bold yellow]{os.path.basename(sys.argv[0])}[/] [bold green]file_1.csv file_2.csv[/] [white]...[/] [bold green]file_N.csv[/]\n")
    else:
        # begin tool if arguments present
        console = Console()
        console.print("[bold cyan]-[/]" * console.width)
        console.print('[bold cyan]Welcome to Import Buddy CLI Tool![/]', justify="center")
        console.print("[bold cyan]-[/]" * console.width)
        print('')
        import_count = 1
        check_relationship_flag = False # allows user to define relationship between two or more tables (ie in the case of this exercise to define primary & foreign key relationships)
        if num_arg > 1:
            num_arg += 1 # add 1 more step to define relationship
            check_relationship_flag = True
        tables_created = {} # format of {"input_file.csv" : "imported_as_table"}
        table_columns_created = {} # format of {"imported_as_table" : ["columnId1 ... columnIdX"]}
        for file in sys.argv[1:]:
            rprint(f"[bold magenta]{import_count}/{num_arg} imports for file {file}[/] [white]...[/]")
            print(f"... importing {file} into local dataframe")
            df_file = pd.read_csv(file)
            num_rows = len(df_file.index)
            print(f"... successfully read data into memory, {num_rows} rows")
            file_headers = list(df_file.columns.values)
            # if "Unnamed" in [f"{i}_{x[:7]}" for i,x in enumerate(file_headers)]:
            missing_col = [e for e in file_headers if e.startswith("Unnamed")]
            if len(missing_col) >= 1:
                rprint(f"[white]... [/][yellow]warning:[/] [white]it looks like you forgot to name a column in your csv file: [yellow]{file}[/]")
                rprint(f"[white]...[/][yellow] renaming: [/][white]\"{missing_col[0]}\" --> \"pngUrl\"[/]")
                print("... the column name will be inferred this time, but its best practice to properly structure your data")
                df_file.rename(columns={missing_col[0]: "pngUrl"}, inplace=True)
            print(f"... ready to import file \"{file}\" with random 10 row sample below:\n")
            print(df_file.sample(n=10))
            rprint("\n[white]...[/] does the above sample of the data look correct? (y/N)")
            user_confirmation = input("... ").lower()
            while user_confirmation not in ("n", "y"):
                rprint("[white]... [/][bold yellow]invalid response:[/] [white]please enter \"y\" to confirm or \"N\" to start over[/]")
                user_confirmation = input("... ").lower()
            if user_confirmation == "n":
                rprint("[white]...[/][bold red] Process aborted:[/] please exit the program and correct the data before retrying")
                system_pause = input("... press [Enter] to quit or simply close the window")
                print('')
                today_datetime = datetime.now().strftime("%m-%d-%Y %H:%M:%S")
                console.print("[bold cyan]-[/]" * console.width)
                console.print(f"[bold cyan]Aborted import processes at {today_datetime}[/]", justify="center")
                console.print("[bold cyan]-[/]" * console.width)
                sys.exit()
            elif user_confirmation == "y":
                print(f"... confirmed, generating postgres table schema")
                raw_column_dtypes = list(zip(list(df_file.columns.values),list(df_file.dtypes.values))) # creates a zipped list containing the csvs column names and data types to get ready to create psql schema
                validated_column_dtypes_list = []
                for col in raw_column_dtypes:
                    validated_col_name = f"\"{col[0]}\"" # use parts & products csv headers as table headers for the purpose of this exercise, allowing a generic approach to import any csv table
                    validated_col_dtype = f"{col[1]}".replace("object", "text").replace("int64", "integer").replace("float64", "float8") # rename dtypes from python to psql types
                    validated_column_dtypes_list.append(f"{validated_col_name} {validated_col_dtype}")
                # prepares generalized columns dtype matching for psql create statement by checking which col dtype pairing should be labelled as primary/foreign key using shape of input data
                if len(validated_column_dtypes_list) <= 3:
                    validated_column_dtypes_list = [x.replace("\"productId\" integer","\"productId\" integer primary key") for x in validated_column_dtypes_list]
                create_stg = ", ".join(validated_column_dtypes_list)
                load_dotenv() # load env variables
                DB_HOST = os.getenv("DB_HOST")
                DB_DATABASE = os.getenv("DB_DATABASE")
                DB_USERNAME = os.getenv("DB_USERNAME")
                DB_PASSWORD = os.getenv("DB_PASSWORD")
                DB_CONNECTION = os.getenv("DB_CONNECTION")
                psql_db_conn = psycopg2.connect(host=DB_HOST,database=DB_DATABASE,user=DB_USERNAME,password=DB_PASSWORD)
                psql_c = psql_db_conn.cursor()
                table_name = os.path.splitext(file)[0]
                create_stmt = f"""create table if not exists "{table_name}" ({create_stg});"""
                psql_c.execute(f"drop table if exists \"{table_name}\"; ")
                psql_db_conn.commit()
                psql_c.execute(create_stmt)
                psql_db_conn.commit()
                print("... successfully created table using extrapolated schema:")
                print(f"... {create_stmt}")
                psql_engine = create_engine(DB_CONNECTION, isolation_level="AUTOCOMMIT", connect_args= {'options': '-c lock_timeout=15000'})
                print("... generating engine for bulk import into table")
                df_file.to_sql(table_name, psql_engine, if_exists='append', index=False,)
                rprint(f"[white]... [/][bold green]successfully imported {file} as table {table_name}[/]")
                print("")
                tables_created.update({file: table_name})
                table_columns_created.update({table_name: list(df_file.columns.values)})
                import_count += 1
        # no more files to loop through, display time completed and end program if no relationship requested
        # ID foreign key if more than 1 table was created
        if check_relationship_flag:
            rprint(f"[bold magenta]{num_arg}/{num_arg} define relations for tables[/] [white]...[/]")
            print(f"... checking tables {' & '.join(list(tables_created.values()))} for matching columns")
            all_columns_used = []
            for key, value in tables_created.items():
                all_columns_used.append(table_columns_created[value]) # creates list of all columns using first dict of tables created, ie parts table from parts.csv, and extracts their corresponding columns from the second dict as the value from the first is linked as the key to the second and so on
            for i in range(len(all_columns_used)): # recursively iterate through i and i+1 index to check for matching columns between 2 or more tables and break with column name if found
                matched_col = set(all_columns_used[i]) & set(all_columns_used[i+1])
                if len(matched_col) >= 1: # if potential foreign key found, break loop and return with matching column between i and i+1 tables
                    break
            rprint(f"[white]... [/][yellow]potential relation[/] found with column: [yellow]{next(iter(matched_col))}[/]") # extract item in set to display to the user to confirm
            rprint("[white]... [/]would you like to define a relation using this column? (y/N)")
            user_confirmation = input("... ").lower()
            while user_confirmation not in ("n", "y"):
                rprint("[white]... [/][bold yellow]invalid response:[/] [white]please enter \"y\" to confirm or \"N\" to ignore[/]")
                user_confirmation = input("... ").lower()
            if user_confirmation == "n":
                print("... confirmed, potential relation ignored and concluding process")
                print("")
            elif user_confirmation == "y":
                fk_relation_col = next(iter(matched_col)) # grab fk relation after user confirms requirement
                print("... confirmed, please select which table should contain the foreign key relationship:")
                relation_map = {v: k for v, k in enumerate(tables_created.values())}
                relation_list = list(list(enumerate(tables_created.values())))
                formatted_dir_dict = '\n'.join(f'[white]...[/][bold cyan] {key+1}: {value}[/]' for key, value in relation_map.items())
                rprint(formatted_dir_dict)
                print(f'... type the corresponding number and press [Enter]')
                user_map_selection = input('... ')
                while user_map_selection not in [str(i) for i in range(1,len(tables_created)+1)]:
                    rprint("[white]... [/][bold yellow]invalid response:[/] [white]please enter a valid option displayed above[/]")
                    user_map_selection = input('... ')
                # once user makes valid selection:
                foreign_key_relation = relation_map[int(user_map_selection)-1]
                print(f'... confirmed, table "{foreign_key_relation}" will be defined as the foreign key relation')
                print("... executing corresponding sql to define relation between selected tables:")
                for x in relation_list:
                    if int(user_map_selection)-1 != x[0]:
                        primary_key_relation = x[1] # finding the other table for the relation using a generic approach not tied or hardcoded to this exercise
                        break
                relation_stmt = f"""alter table \"{foreign_key_relation}\" add constraint \"fk_{fk_relation_col}\" foreign key (\"{fk_relation_col}\") references \"{primary_key_relation}\" (\"{fk_relation_col}\"); """
                try:
                    psql_c.execute(relation_stmt)
                    psql_db_conn.commit()
                    psql_db_conn.close()
                    print(f"... {relation_stmt}")
                    rprint(f"[white]... [/][bold green]successfully defined relation using {fk_relation_col} key[/]")
                    print("")
                except:
                    rprint(f"[white]... [/][yellow]warning:[/] it looks like you choose a table without a corresponding pk relationship")
                    psql_db_conn.close()
                    print(f"... no relation defined, please ensure the correct pk exists in the relation table before trying again")
                    print("")
        else:
            psql_db_conn.commit() # no further database transactions
        today_datetime = datetime.now().strftime("%m-%d-%Y %H:%M:%S")
        console.print("[bold cyan]-[/]" * console.width)
        console.print(f"[bold cyan]Successfully completed processes at {today_datetime}[/]", justify="center")
        console.print("[bold cyan]-[/]" * console.width)
        print("")

"""
Version 1.0

    - created read & write credential and spun up new dedicated psql instance for this project
    - changed import system to be generic over any csv instead of hardcoding for parts.csv & products.csv
    - changed dataframe display to derive from random sample for potential future use cases
    - added missing column error handling since unclear if intentional for exercise
    - added third step when more than 1 tables are imported to allow user to define relation
    - created sql statements manually to demonstrate transaction without orm
    - added rich formatting and try/except block for missing pk relation on defining block
    - added .env and environment variables and added extension to gitignore

"""
#!/usr/bin/python
import pathlib as pl
import meta_utilz
import duckdb


class CollateBirthDeath:
    def __init__(self, path_db, con):
        self.path_db = path_db
        self.con = con
        self.table_name = "birthdeath_db"
        self.test_path_1 = '/home/micha/Documents/post_thermasim_results/climate_exps/Texas_Current/Results/rep_117600/BirthDeath.csv'
        self.test_path_2 = '/home/micha/Documents/post_thermasim_results/climate_exps/Canada_1/Results/rep_463312/BirthDeath.csv'

    def create_table(self):
        self.con.execute(f"""
        CREATE OR REPLACE TABLE {self.table_name} (
            Study_Site TEXT,
            Experiment TEXT,
            sim_id TEXT,
            Time_Step INTEGER,
            Agent_id TEXT,
            Species TEXT,
            Age DOUBLE,
            Sex TEXT,
            Mass DOUBLE,
            Birth_Counter DOUBLE,
            Death_Counter DOUBLE,
            Alive BOOLEAN,
            Event_Type TEXT,
            Cause_Of_Death TEXT,
            Litter_Size INTEGER,
            Body_Temperature DOUBLE,
            ct_min DOUBLE,
            ct_max DOUBLE
        );
        """)
        return
    
    def insert_csv(self, csv_path, site, experiment,sim_id):
        self.con.execute(f"""
                INSERT INTO {self.table_name}
                SELECT
                    '{site}' AS Study_Site,
                    '{experiment}' AS Experiment,
                    {sim_id} AS sim_id,
                    Time_Step, 
                    Agent_id,
                    Species,
                    Age,
                    Sex,
                    Mass,
                    Birth_Counter,
                    Death_Counter,
                    Alive,
                    Event_Type,
                    Cause_Of_Death,
                    Litter_Size,
                    Body_Temperature,
                    ct_min,
                    ct_max
                FROM read_csv_auto('{csv_path}')
            """)
        return
    
    def insert_test(self):
        list_of_csv_paths = [self.test_path_1,
                             self.test_path_2]
        for path in list_of_csv_paths:
            path = pl.Path(path)
            site = meta_utilz.extract_site(path)
            experiment = meta_utilz.extract_experiment_name(path)
            sim_id = meta_utilz.extract_sim_id(path)
            print(f'file {path}, site {site}, experiment {experiment}, simid {sim_id}')
            try:
                self.insert_csv(str(path), site, experiment, sim_id)
            except Exception as e:
                print(f"[WARN] Failed to process {path}: {e}")
            print(f"Inserted {path} into {self.table_name}")
        return

    def insert_all(self, csv_list):
        for file in csv_list:
            try:
                site = meta_utilz.extract_site(file)
                experiment = meta_utilz.extract_experiment_name(file)
                sim_id = meta_utilz.extract_sim_id(file)
                self.insert_csv(str(file), site, experiment, sim_id)
            except Exception as e:
                print(f"[WARN] Failed to process {file}: {e}")
        return

    def query_bd_table(self, query):
        """
        Execute a query on the model table and return the results as a DataFrame.
        """
        return self.con.execute(query).fetchdf()
    
if __name__ == "__main__":
    con = duckdb.connect(database=":memory:")
    path_db = {}
    birthdeath_db = CollateBirthDeath(path_db=path_db, con=con)
    birthdeath_db.create_table()
    birthdeath_db.insert_test()
    query = "SELECT *" \
    "        FROM birthdeath_db" \
    "        ORDER BY Litter_Size" \
    "        DESC LIMIT 10;"
    df = birthdeath_db.query_bd_table(query)
    print(df)

    
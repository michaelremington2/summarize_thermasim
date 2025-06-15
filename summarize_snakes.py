# -*- coding: utf-8 -*-
import main
import duckdb
import pathlib as pl

class CollateRattlesnake:
    def __init__(self, path_db, con):
        self.path_db = path_db
        self.con = con
        self.table_name = "rattlesnake_db"
        self.test_path_1 = '/home/micha/Documents/post_thermasim_results/climate_exps/Texas_Current/Results/rep_117600/Rattlesnake.csv'
        self.test_path_2 = '/home/micha/Documents/post_thermasim_results/climate_exps/Canada_1/Results/rep_463312/Rattlesnake.csv'

    def create_table(self):
        self.con.execute(f"""
        CREATE OR REPLACE TABLE {self.table_name} (
            Study_Site TEXT,
            Experiment TEXT,
            sim_id INTEGER,
            Time_Step INTEGER, 
            Hour INTEGER, 
            Day INTEGER, 
            Month INTEGER, 
            Year INTEGER, 
            Agent_id INTEGER, 
            Active BOOLEAN,
            Alive BOOLEAN, 
            Behavior TEXT, 
            Microhabitat TEXT, 
            Body_Temperature DOUBLE, 
            Mass DOUBLE,
            Metabolic_State DOUBLE, 
            Handling_Time DOUBLE, 
            Attack_Rate DOUBLE, 
            Prey_Density DOUBLE,
            Prey_Encountered DOUBLE, 
            Prey_Consumed DOUBLE,
        );
        """)

    def insert_csv(self, csv_path, site, experiment, sim_id):
        self.con.execute(f"""
            INSERT INTO {self.table_name}
            SELECT
                '{site}' AS Study_Site,
                '{experiment}' AS Experiment,
                {sim_id} AS sim_id,
                Time_Step, 
                Hour, 
                Day, 
                Month, 
                Year, 
                Agent_id, 
                Active,
                Alive, 
                Behavior, 
                Microhabitat, 
                Body_Temperature, 
                Mass,
                Metabolic_State, 
                Handling_Time, 
                Attack_Rate, 
                Prey_Density,
                Prey_Encountered, 
                Prey_Consumed,
            FROM read_csv_auto('{csv_path}')
        """)
        return


    
    def insert_test(self):
        list_of_csv_paths = [self.test_path_1,
                             self.test_path_2]
        for path in list_of_csv_paths:
            path = pl.Path(path)
            site = main.extract_site(path)
            experiment = main.extract_experiment_name(path)
            sim_id = main.extract_sim_id(path)
            print(f'file {path}, site {site}, experiment {experiment}, simid {sim_id}')
            try:
                self.insert_csv(str(path), site, experiment, sim_id)
            except Exception as e:
                print(f"[WARN] Failed to process {path}: {e}")
            print(f"Inserted {path} into {self.table_name}")
        return


    def insert_all(self):
        for site, exps in self.path_db.items():
            for experiment, sims in exps.items():
                for sim_id, file_dict in sims.items():
                    path = file_dict.get("Rattlesnake")
                    if path is None or not path.exists():
                        continue
                    try:
                        self.insert_csv(str(path), site, experiment, sim_id)
                    except Exception as e:
                        print(f"[WARN] Failed to process {path}: {e}")

    def query_snake_table(self, query):
        """
        Execute a query on the rattlesnake table and return the results as a DataFrame.
        """
        return self.con.execute(query).fetchdf()


if __name__ == "__main__":
    con = duckdb.connect(database=":memory:")
    path_db = {}
    snake_db = CollateRattlesnake(path_db=path_db, con=con)
    snake_db.create_table()
    #snake_db.insert_test()
    snake_db.insert_all()
    query = "SELECT * FROM rattlesnake_db LIMIT 10;"
    df = snake_db.query_snake_table(query)
    print(df)
    
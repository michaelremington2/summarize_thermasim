#!/usr/bin/python
import pathlib as pl
import meta_utilz
import duckdb



class CollateModel:
    def __init__(self, path_db, con):
        self.path_db = path_db
        self.con = con
        self.table_name = "model_db"
        self.test_path_1 = '/home/micha/Documents/post_thermasim_results/climate_exps/Texas_Current/Results/rep_117600/Model.csv'
        self.test_path_2 = '/home/micha/Documents/post_thermasim_results/climate_exps/Canada_1/Results/rep_463312/Model.csv'

    def create_table(self):
        self.con.execute(f"""
        CREATE OR REPLACE TABLE {self.table_name} (
        Study_Site TEXT,
        Experiment TEXT,
        Time_Step INTEGER, 
        Hour INTEGER,
        Day INTEGER, 
        Month INTEGER, 
        Year INTEGER, 
        Rattlesnakes INTEGER,
        Krats INTEGER,
        Rattlesnakes_Density DOUBLE, 
        Krats_Density DOUBLE,
        Rattlesnakes_Active INTEGER,
        Krats_Active INTEGER,
        Foraging INTEGER, 
        Thermoregulating INTEGER, 
        Resting INTEGER, 
        Searching INTEGER, 
        Brumating INTEGER,
        Snakes_in_Burrow INTEGER, 
        Snakes_in_Open INTEGER,
        mean_thermal_quality DOUBLE, 
        mean_thermal_accuracy DOUBLE, 
        count_interactions INTEGER, 
        count_successful_interactions INTEGER,
        seed INTEGER, 
        sim_id INTEGER
        );
        """)
        return

    def insert_csv(self, csv_path, site, experiment):
        self.con.execute(f"""
                INSERT INTO {self.table_name}
                SELECT
                    '{site}' AS Study_Site,
                    '{experiment}' AS Experiment,
                    Time_Step, 
                    Hour, 
                    Day, 
                    Month, 
                    Year,
                    Rattlesnakes, 
                    Krats, 
                    Rattlesnakes_Density, 
                    Krats_Density, 
                    Rattlesnakes_Active, 
                    Krats_Active,
                    Foraging, 
                    Thermoregulating, 
                    Resting, 
                    Searching, 
                    Brumating,
                    Snakes_in_Burrow, 
                    Snakes_in_Open,
                    mean_thermal_quality, 
                    mean_thermal_accuracy, 
                    count_interactions, 
                    count_successful_interactions,
                    seed, 
                    sim_id
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
                #sim_id = meta_utilz.extract_sim_id(file)
                self.insert_csv(str(file), site, experiment)
            except Exception as e:
                print(f"[WARN] Failed to process {file}: {e}")
        return

    def query_model_table(self, query):
        """
        Execute a query on the model table and return the results as a DataFrame.
        """
        return self.con.execute(query).fetchdf()
    
if __name__ == "__main__":
    con = duckdb.connect(database=":memory:")
    path_db = {}
    model_db = CollateModel(path_db=path_db, con=con)
    model_db.create_table()
    model_db.insert_test()
    query = "SELECT * FROM model_db LIMIT 10;"
    df = model_db.query_model_table(query)
    print(df)

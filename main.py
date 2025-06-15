#!/usr/bin/python
import polars as po
import duckdb
import numpy as np
import os
import pathlib as pl
import re
from summarize_snakes import CollateRattlesnake
from summarize_bd import CollateBirthDeath
from summarize_model import CollateModel

# Helper functions to extract metadata from file paths
def extract_site(path):
    site_exp = path.parts[-4]
    site = site_exp.split('_')[0] 
    return site

def extract_experiment_name(path):
    site_exp = path.parts[-4]
    exp = site_exp.split('_')[1]
    if exp == 'Current':
        return 0
    return int(exp)

def extract_sim_id(path):
    rep_simid = path.parts[-2]
    sim_id = rep_simid.split('_')[1]
    return int(sim_id)

class SimSummarizer:
    '''
    Builds path_db from simulation output directory
    and optionally coalesces all raw `.csv` files into a DuckDB database.
    '''
    def __init__(self, parent_directory, site_names , db_path=None):
        self.parent_directory = pl.Path(parent_directory)
        self.site_names = site_names
        self.path_db = self.make_path_db()
        if db_path:
            db_path = pl.Path(db_path).resolve()
            db_path.parent.mkdir(parents=True, exist_ok=True)
            print(f"[INFO] Using persistent DuckDB at {db_path}")
            self.con = duckdb.connect(str(db_path))
        else:
            print(f"[INFO] Using in-memory DuckDB database")
            self.con = duckdb.connect(database=":memory:")

    def make_path_db(self):
        path_db = {}
        for subdir in self.parent_directory.iterdir():
            if subdir.is_dir():
                for site in self.site_names:
                    if subdir.name.startswith(site + "_"):
                        if site not in path_db:
                            path_db[site] = {}
                        experiment = subdir.name[len(site) + 1:] 
                        results_folder = subdir / "Results"
                        path_db[site][experiment] = {}

                        if results_folder.exists():
                            for sim_dir in results_folder.iterdir():
                                if sim_dir.is_dir():
                                    sim_id = sim_dir.name
                                    path_db[site][experiment][sim_id] = {}

                                    for file in sim_dir.glob("*.csv"):
                                        csv_type = file.stem  # filename without extension
                                        path_db[site][experiment][sim_id][csv_type] = file.resolve()

        return path_db
    
    def make_metadata_path_db(self):
        path_db = {}
        for subdir in self.parent_directory.iterdir():
            if subdir.is_dir():
                for site in self.site_names:
                    if subdir.name.startswith(site + "_"):
                        if site not in path_db:
                            path_db[site] = {}
                        experiment = subdir.name[len(site) + 1:] 
                        results_folder = subdir / "Results"
                        path_db[site][experiment] = {}
        return None

    def get_path_db(self):
        return self.path_db

    def get_connection(self):
        return self.con
    
    def make_model_csvlist(self):
        return None
    
    def make_rattlesnake_csvlist(self):
        return None
    
    def make_bd_csvlist(self):
        return None
    
    def initialize_tables(self, model=True, rattlesnake=True, bd=True):
        """
        Initialize the DuckDB tables for model, rattlesnake, and birth-death data.
        """
        if model:
            self.con.execute("""
                CREATE TABLE IF NOT EXISTS model_raw (
                    site VARCHAR,
                    experiment INT,
                    sim_id INT,
                    Time_Step INT,
                    Agent_id INT,
                    Mass FLOAT,
                    Body_Temperature FLOAT,
                    Metabolic_State VARCHAR
                );
            """)
        
        if rattlesnake:
            CollateRattlesnake(self.path_db, self.con).create_table()
        
        if bd:
            CollateBirthDeath(self.path_db, self.con).create_table()
    
    def query_sim_table(self, query):
        """
        Execute a query on the simulation table and return the results as a DataFrame.
        """
        return self.con.execute(query).fetchdf()
    
    



if __name__ == "__main__":
    from summarize_bd import CoalateBirthDeath

    site_names = ["Texas", "Nebraska", "Canada"]
    simsum = SimSummarizer(parent_directory="../climate_exps/", site_names=site_names)

    # shared in-memory DuckDB
    con = simsum.get_connection()

    # run birth-death coalescence into DuckDB
    cbd = CoalateBirthDeath(simsum.get_path_db(), con)
    cbd.create_table()
    cbd.insert_all()

    # verify or query immediately
    df = con.execute("SELECT site, event_type, COUNT(*) FROM birthdeath_raw GROUP BY site, event_type").fetchdf()
    print(df)

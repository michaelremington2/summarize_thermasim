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


class SimSummarizer:
    '''
    Builds path_db from simulation output directory
    and optionally coalesces all raw `.csv` files into a DuckDB database.
    '''
    def __init__(self, parent_directory, site_names , db_path=None):
        self.parent_directory = pl.Path(parent_directory)
        self.site_names = site_names
        self.results_paths = {'model': [],
                              'rattlesnake': [],
                              'birthdeath': []}
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
                                        if csv_type == "Model":
                                            self.results_paths['model'].append(file.resolve())
                                        elif csv_type == "Rattlesnake":
                                            self.results_paths['rattlesnake'].append(file.resolve())
                                        elif csv_type == "BirthDeath":
                                            self.results_paths['birthdeath'].append(file.resolve())
                                        elif csv_type == "KangarooRat":
                                            pass
                                        else:
                                            print(f"[WARN] Unrecognized CSV type '{csv_type}' in {file}")
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

    
    
    def initialize_tables(self, model=True, rattlesnake=True, bd=True):
        """
        Initialize the DuckDB tables for model, rattlesnake, and birth-death data.
        """
        if model:
            self.mdb = CollateModel(self.path_db, self.con)
            self.mdb.create_table()
            model_csvs = self.results_paths['model']
            self.mdb.insert_all(csv_list=model_csvs)
        if rattlesnake:
            self.rdb = CollateRattlesnake(self.path_db, self.con)
            self.rdb.create_table()
            snake_csvs = self.results_paths['rattlesnake']
            self.rdb.insert_all(csv_list=snake_csvs)
        if bd:
            self.bddb = CollateBirthDeath(self.path_db, self.con)
            self.bddb.create_table()
            bd_csvs = self.results_paths['birthdeath']
            self.bddb.insert_all(csv_list=bd_csvs)
    
    def query_sim_table(self, query):
        """
        Execute a query on the simulation table and return the results as a DataFrame.
        """
        return self.con.execute(query).fetchdf()
    
    



if __name__ == "__main__":
    site_names = ["Texas", "Nebraska", "Canada"]
    thermadb_path = "/mnt/d/Documents/therma_sim.duckdb"
    parent_directory = pl.Path("/mnt/d/Documents/climate_exps/")

    simsum = SimSummarizer(
        parent_directory=parent_directory,
        site_names=site_names,
        db_path=thermadb_path
    )

    # Optional: create tables
    simsum.initialize_tables(model=True, rattlesnake=False, bd=True)

    # # Query
    # df = simsum.query_sim_table("""
    #     SELECT Study_Site, Experiment, Year, Month, Day, AVG(Rattlesnakes) AS Avg_Rattlesnakes, AVG(Krats) as Avg_Krats, AVG(Rattlesnakes_Density) AS Avg_Rattlesnakes_Density, AVG(Krats_Density) AS Avg_Krats_Density, AVG(Rattlesnakes_Active) AS Avg_Rattlesnakes_Active, AVG(Krats_Active) AS Avg_Krats_Active, count(distinct Sim_ID) AS Num_Sims
    #     FROM model_db
    #     group by Study_Site, Experiment, Year, Month, Day
    # """)
    df = simsum.query_sim_table("""
        SELECT Study_Site,
            Experiment,
            Cause_Of_Death, 
            Time_Step,
            Species,
            AVG(Mass) As mean_mass,
            Count(Distinct Agent_id) AS num_agents,
            AVG(Age) AS mean_age,
            AVG(Body_Temperature) AS mean_body_temp
        FROM birthdeath_db
        WHERE Event_Type = 'Death'
        AND Species = 'Rattlesnake'
        group by Study_Site, Experiment, Cause_Of_Death, Time_step, Species
    """)

    # Write to CSV
    output_path = "/mnt/d/Documents/summary_avg_bd.csv"
    df.to_csv(output_path)

    print(f"[INFO] Query results written to: {output_path}")


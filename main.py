#!/usr/bin/python
import polars as po
import duckdb
import numpy as np
import os
import pathlib as pl
import re
from summarize_snakes import CoalateRattlesnake
from summarize_bd import CoalateBirthDeath


class SimSummarizer:
    '''
    Builds path_db from simulation output directory
    and optionally coalesces all raw `.csv` files into a DuckDB database.
    '''
    def __init__(self, parent_directory, site_names, con=None):
        self.parent_directory = pl.Path(parent_directory)
        self.site_names = site_names
        self.path_db = self.make_path_db()
        self.con = duckdb.connect(database=":memory:")  # default: in-memory

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

    def get_path_db(self):
        return self.path_db

    def get_connection(self):
        return self.con



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

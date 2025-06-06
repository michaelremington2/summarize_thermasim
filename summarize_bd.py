#!/usr/bin/python
import polars as po
import duckdb as db
import numpy as np
import os
import pathlib as pl
import re
from tqdm import tqdm


class CoalateBirthDeath:
    def __init__(self, path_db, con):
        self.path_db = path_db
        self.con = con
        self.table_name = "birthdeath"

    def create_table(self):
        self.con.execute(f"""
        CREATE OR REPLACE TABLE {self.table_name} (
            site TEXT,
            experiment TEXT,
            sim_id TEXT,
            time_step INTEGER,
            agent_id TEXT,
            species TEXT,
            age DOUBLE,
            sex TEXT,
            mass DOUBLE,
            birth_counter INTEGER,
            death_counter INTEGER,
            alive BOOLEAN,
            event_type TEXT,
            cause_of_death TEXT,
            litter_size INTEGER,
            body_temperature DOUBLE,
            ct_min DOUBLE,
            ct_max DOUBLE
        );
        """)

    def insert_all(self):
        with self.con.appender(self.table_name) as app:
            for site, exps in tqdm(self.path_db.items(), desc="Sites"):
                for experiment, sims in exps.items():
                    for sim_id, file_dict in sims.items():
                        bd_path = file_dict.get("BirthDeath")
                        if bd_path is None:
                            continue

                        try:
                            df = pl.read_csv(str(bd_path), infer_schema_length=1000)

                            for row in df.iter_rows(named=True):
                                app.append((
                                    site,
                                    experiment,
                                    sim_id,
                                    row.get("Time_Step"),
                                    row.get("Agent_id"),
                                    row.get("Species"),
                                    row.get("Age"),
                                    row.get("Sex"),
                                    row.get("Mass"),
                                    row.get("Birth_Counter"),
                                    row.get("Death_Counter"),
                                    row.get("Alive"),
                                    row.get("Event_Type"),
                                    row.get("Cause_Of_Death"),
                                    row.get("Litter_Size"),
                                    row.get("Body_Temperature"),
                                    row.get("ct_min"),
                                    row.get("ct_max"),
                                ))

                        except Exception as e:
                            print(f"[WARN] Failed to process {bd_path}: {e}")

    
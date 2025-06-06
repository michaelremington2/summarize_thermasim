import polars as pl

class CoalateRattlesnake:
    def __init__(self, path_db, con):
        self.path_db = path_db
        self.con = con
        self.table_name = "rattlesnake_raw"

    def create_table(self):
        self.con.execute(f"""
        CREATE OR REPLACE TABLE {self.table_name} (
            site TEXT,
            experiment TEXT,
            sim_id TEXT,
            time_step INTEGER,
            agent_id TEXT,
            mass DOUBLE,
            body_temperature DOUBLE,
            metabolic_state DOUBLE
        );
        """)

    def insert_all(self):
        for site, exps in self.path_db.items():
            for experiment, sims in exps.items():
                for sim_id, file_dict in sims.items():
                    path = file_dict.get("Rattlesnake")
                    if path is None or not path.exists():
                        continue

                    try:
                        # Read only needed columns for memory safety
                        df = pl.read_csv(str(path)).select([
                            "Time_Step", "Agent_id", "Mass", "Body_Temperature", "Metabolic_State"
                        ])

                        # Add metadata columns
                        df = df.with_columns([
                            pl.lit(site).alias("site"),
                            pl.lit(experiment).alias("experiment"),
                            pl.lit(sim_id).alias("sim_id"),
                        ])

                        # Reorder to match DuckDB schema
                        df = df.select([
                            "site", "experiment", "sim_id",
                            "Time_Step", "Agent_id", "Mass",
                            "Body_Temperature", "Metabolic_State"
                        ])

                        # Register and insert
                        self.con.register("temp_rattlesnake", df.to_pandas())
                        self.con.execute(f"INSERT INTO {self.table_name} SELECT * FROM temp_rattlesnake")
                        self.con.unregister("temp_rattlesnake")

                    except Exception as e:
                        print(f"[WARN] Failed to process {path}: {e}")

import polars as pl


class CoalateModel:
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

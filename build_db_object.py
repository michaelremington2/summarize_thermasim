#!/usr/bin/python
import main
import pathlib as pl


site_names = ["Texas", "Nebraska", "Canada"]
thermadb_path = "../post_thermasim/therma_sim.duckdb"
parent_directory = pl.Path("../run_experiments/climate_exps/")

simsum = main.SimSummarizer(
    parent_directory=parent_directory,
    site_names=site_names,
    db_path=thermadb_path
)

# Optional: create tables
simsum.initialize_tables(model=True, rattlesnake=True, bd=True)
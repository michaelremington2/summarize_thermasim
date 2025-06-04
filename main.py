#!/usr/bin/python
import polars as po
import numpy as np
import os
import pathlib as pl
import re


class SimSummarizer(object):
    '''
    This is the main script for handling navigating file directories and coalating output scripts into a central database of information.
    '''
    def __init__(self, parent_directory, site_names):
        self.parent_directory = pl.Path(parent_directory)
        self.site_names = site_names
        self.path_db = self.make_path_db()
    #####
    # Dictionary: {site : { Temperature: sim_results_directory}}}
    #####
    def make_path_db(self):
        path_db = {}
        for subdir in self.parent_directory.iterdir():
            if subdir.is_dir():
                for site in self.site_names:
                    if subdir.name.startswith(site + "_"):
                        if site not in path_db:
                            path_db[site] = {}
                        variant = subdir.name[len(site) + 1:] 
                        exp_dir = subdir.resolve()
                        results_folder = exp_dir + '/Results'
                        path_db[site][variant] = {}

        return path_db






if __name__ ==  "__main__":
    site_names = ["Texas", "Nebraska", "Canada"]
    simsum = SimSummarizer(parent_directory="../climate_exps/", site_names=site_names)
    print(simsum.path_db)
import pathlib as pl

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

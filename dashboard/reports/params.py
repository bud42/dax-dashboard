import os
import yaml


def load_params_list(paramsdir):
    plist = os.listdir(paramsdir)

    # Make full paths
    plist = [os.path.join(paramsdir, f) for f in plist]

    # Only yaml files
    plist = [f for f in plist if f.endswith('.yaml') and os.path.isfile(f)]

    return sorted(plist)


def load_project_params(paramsfile):

    # Load the parameters from yaml file
    with open(paramsfile, 'r') as f:
        params = yaml.load(f, Loader=yaml.Loader)

    if 'use_secondary' not in params:
        params['use_secondary'] = False

    return params

def get_projects(paramsdir):
    plist = load_params_list(paramsdir)

    projects = [os.path.splitext(os.path.basename(x))[0] for x in plist]

    return sorted(projects)

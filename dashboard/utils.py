import redcap
import os
import pandas as pd
import logging
import json

from dax import XnatUtils


def make_options(values):
    return [{'label': x, 'value': x} for x in values]


def make_columns(values):
    return [{'name': x, 'id': x} for x in values]


def get_json(xnat, uri):
    return json.loads(xnat._exec(uri, 'GET'))


def get_user_favorites(xnat):
    FAV_URI = '/data/archive/projects?favorite=True'
    fav_json = get_json(xnat, FAV_URI)
    data = [x['id'] for x in fav_json['ResultSet']['Result']]

    return data


def get_user_projects(xnat, username):

    uri = '/xapi/users/{}/groups'.format(username)

    # get from xnat and convert to list
    data = get_json(xnat, uri)

    # format of group name is PROJECT_ROLE,
    # so we split on the underscore
    data = sorted([x.rsplit('_', 1)[0] for x in data])

    return data


def get_projectkey(projectid, keyfile):
    # Load the dictionary
    d = {}
    with open(keyfile) as f:
        for line in f:
            if line == '':
                continue

            try:
                (i, k, n) = line.strip().split(',')
                d[i] = k
            except:
                pass

    return d.get(projectid, None)


def get_projectid(projectname, keyfile):
    # Load the dictionary mapping name to id
    d = {}
    with open(keyfile) as f:
        for line in f:
            if line == '':
                continue

            try:
                (i, k, n) = line.strip().split(',')
                # Map name to id
                d[n] = i
            except:
                pass

    # Return the project id for given project name
    return d.get(projectname, None)


def get_projectkeybyname(projectname, keyfile):
    i = get_projectid(projectname, keyfile)
    return get_projectkey(i, keyfile)


def download_file(project, record_id, event_id, field_id, filename, repeat_id=None):
    try:
        (cont, hdr) = project.export_file(
            record=record_id,
            event=event_id,
            field=field_id,
            repeat_instance=repeat_id)

        if cont == '':
            raise redcap.RedcapError
    except redcap.RedcapError as err:
        logging.error(f':downloading file:{err}')
        return None

    try:
        with open(filename, 'wb') as f:
            f.write(cont)

        return filename
    except FileNotFoundError as err:
        logging.error(f'file not found:{filename}:{err}')
        return None


def upload_file(
    project, record_id, event_id, field_id, filename, repeat_id=None
):

    with open(filename, 'rb') as f:
        project.import_file(
            record=record_id,
            field=field_id,
            file_name=os.path.basename(filename),
            event=event_id,
            repeat_instance=repeat_id,
            file_object=f)


def read_data(filename):
    df = pd.read_pickle(filename)
    return df


def save_data(df, filename):
    # save to cache
    df.to_pickle(filename)


def match_repeat(mainrc, record_id, repeat_name, match_field, match_value):

    # Load potential matches
    records = mainrc.export_records(records=[record_id])

    # Find records with matching vaue
    matches = [x for x in records if x[match_field] == match_value]

    # Return ids of matches
    return [x['redcap_repeat_instance'] for x in matches]


def session_label_list(xnat, project_name):
    _uri = f'/REST/experiments?columns=label,modality&project={project_name}'
    _json = XnatUtils.get_json(xnat, _uri)
    label_list = [x['label'] for x in _json['ResultSet']['Result']]
    return label_list


def session_vuiisid_list(xnat, projectid):
    dcmtag = 'dcmPatientId'
    post_uri = '/REST/projects/{0}/experiments?columns=label,xnat:imagesessiondata/{1}'
    post_uri = post_uri.format(projectid, dcmtag)

    sess_json = get_json(xnat, post_uri)
    sess_list = sess_json['ResultSet']['Result']

    vuiisid_list = [x[dcmtag].split('_')[1] for x in sess_list if '_' in x[dcmtag]]

    return vuiisid_list


def get_unmatched(params):
    unmatched = []

    with XnatUtils.InterfaceTemp(xnat_retries=0) as xnat:
        for p in params['pimap']:
            vuiisid_list = []
            pi = p['pi']
            main = p['main']
            ignore = p['ignore']
            label_list = session_label_list(xnat, pi)
            label_list = [x for x in label_list if x not in ignore]

            for m in main:
                vuiisid_list += session_vuiisid_list(xnat, m)

            pi_unmatched = [x for x in label_list if x not in vuiisid_list]

            unmatched += [(pi+'_'+x) for x in pi_unmatched]

    return unmatched
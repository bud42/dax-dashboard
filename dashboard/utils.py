# utility functions


def make_options(values):
    return [{'label': x, 'value': x} for x in values]


def make_columns(values):
    return [{'name': x, 'id': x} for x in values]


def get_json(xnat, uri):
    import json

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

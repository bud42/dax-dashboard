import redcap
import os


def download_file(project, record_id, event_id, field_id, filename):
    try:
        (cont, hdr) = project.export_file(
            record=record_id, event=event_id, field=field_id)

        if cont == '':
            raise redcap.RedcapError
    except redcap.RedcapError as err:
        print('ERROR:downloading file', err)
        return None

    try:
        with open(filename, 'wb') as f:
            f.write(cont)

        return filename
    except FileNotFoundError as err:
        print('file not found', filename, str(err))
        return None


def upload_file(project, record_id, event_id, field_id, filename, repeat_id=None):
    with open(filename, 'rb') as f:
        project.import_file(
            record=record_id,
            field=field_id,
            file_name=os.path.basename(filename),
            event=event_id,
            repeat_instance=repeat_id,
            file_object=f)


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

    # Return the key id for given project id
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

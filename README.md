# dax-dashboard
Dashboard for DAX

Try it now with:
```
docker run -ti --rm -p 8050:8050 -e USER -e XNAT_HOST -v $HOME/.netrc:/root/.netrc bud42/daxdashboard
```

This assumes you have already configured your environment to use DAX. You will have set XNAT_HOST to your xnat host url and your credentials will have been stored in .netrc in your home directory. The first time you run, docker will need to build the container so it could take a few minutes to start. When the startup is complete, browse to http://0.0.0.0:8050/.


dax-dashboard will load all projects from XNAT that you have designated as a favorite. You can add/remove from your favorites in XNAT by browsing to project page and clicking in the Actions box.

If you are using the dax module Module_redcap_sync, you can view these data by mounting a yaml file that contain your REDCap keys.
The file should be mounted to docker home director in /root/redcap.yaml.
```
docker run -ti --rm -p 8050:8050 -e USER -e XNAT_HOST -v $HOME/.netrc:/root/.netrc -v $HOME/dashboard.redcap.yaml:/root/redcap.yaml bud42/daxdashboard
```

The format of the redcap yaml file should be the same as the file generated by Module_redcap_sync.
```
api_url: <redcap server url>
projects:
- key: <key>
  name: <project>-<proctype>-<resource>
```

The stats page will also use your XNAT favorites to filter projects.

# qa page

changing pivot
filtering using dropdowns
exporting to csv
Refresh data


# stats page


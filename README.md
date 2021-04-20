# dax-dashboard
Dashboard for DAX

Try it now with:
```
docker run -ti --rm -p 8050:8050 -e USER -e XNAT_HOST -v $HOME/.netrc:/root/.netrc bud42/daxdashboard
```

This assumes you have already configured your environment to use DAX. You will have set XNAT_HOST to your xnat host url and your credentials will have been stored in .netrc in your home directory. The first time you run, docker will need to build the container so it could take a minute or so to start.

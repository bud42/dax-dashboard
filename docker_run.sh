docker run -ti --rm --env USER -p 8050:8050 -e XNAT_HOST="https://xnat.vanderbilt.edu/xnat" -v $HOME/.netrc:/root/.netrc -v $HOME/dashboard.redcap.yaml:/root/redcap.yaml  bud42/daxdashboard

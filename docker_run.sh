docker run -ti --rm --env USER -p 8050:8050 -e XNAT_HOST="http://129.59.135.143:8080/xnat" -v $HOME/.netrc:/root/.netrc -v $HOME/dashboard.redcap.yaml:/root/redcap.yaml  bud42/dashboard

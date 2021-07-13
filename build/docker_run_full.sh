docker run \
-ti \
--rm \
-p 8050:8050 \
-e USER \
-e XNAT_HOST \
-v $HOME/.netrc:/root/.netrc \
-v $HOME/dashboard.redcap.yaml:/root/redcap.yaml \
-v $HOME/qaparams.yaml:/root/qaparams.yaml \
-v $HOME/issues.csv:/root/issues.csv \
-v $HOME/completed.log:/root/completed.log \
bud42/daxdashboard

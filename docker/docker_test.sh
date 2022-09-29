docker run \
-ti \
--rm \
-p 8050:8050 \
-e USER \
-e XNAT_HOST \
-v $HOME/.netrc:/root/.netrc \
-v $HOME/dashboard.redcap.yaml:/root/redcap.yaml \
-v $HOME/git/ccmparams/qaparams.yaml:/root/qaparams.yaml \
-v $HOME/git/ccmparams/statsparams.yaml:/root/statsparams.yaml \
-v $HOME/git/ccmparams/params:/root/PARAMS \
-v $HOME/REPORTS:/root/REPORTS \
-v $HOME/demogparams.yaml:/root/demogparams.yaml \
bud42/daxdashboard:v1

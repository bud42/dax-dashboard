docker run \
-ti \
--rm \
-p 8050:8050 \
-e USER \
-e XNAT_HOST \
-v $HOME/.netrc:/root/.netrc \
-v $HOME/git/ccmparams/qaparams.yaml:/root/qaparams.yaml \
-v $HOME/git/ccmparams/params:/root/PARAMS \
-v $HOME/REPORTS:/root/REPORTS \
bud42/daxdashboard


docker run \
-ti \
--rm \
-p 8050:8050 \
-e USER \
-e XNAT_HOST \
-e TZ=America/Chicago \
-v $HOME/.netrc:/root/.netrc \
-v $HOME/.redcap.txt:/root/.redcap.txt \
-v $HOME/dashboard.redcap.yaml:/root/redcap.yaml \
-v $HOME/git/ccmparams/params:/root/PARAMS \
bud42/daxdashboard:v1


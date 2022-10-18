docker run \
-ti \
--rm \
-p 8050:8050 \
-e USER \
-e XNAT_HOST \
-e TZ=America/Chicago \
-v $HOME/.netrc:/root/.netrc \
-v $HOME/.redcap.txt:/root/.redcap.txt \
-v $HOME/REPORTS:/opt/dashboard/DATA \
bud42/daxdashboard:v1

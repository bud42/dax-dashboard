docker run -ti --rm --env USER -p 8050:8050 -e XNAT_HOST="https://xnat.vanderbilt.edu/xnat" -v $HOME/.netrc:/root/.netrc bud42/daxdashboard:v1

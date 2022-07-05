These bash scripts are used to interact with the dashboard docker image
using various docker commands to build and run for testing and production.

To test new code, you'll want to first build the docker image locally with:
docker_build.sh

Then you'll want to test it locally with docker_test.sh.

Then if you want to publish a new version of the docker image on docker hub,
you would push the new docker images that you build with:
docker_push.sh

And finally to run the newly uploaded image, shut down the running dockers and
then update by running docker_pull.sh and then docker_run.sh.

Again, it's:
docker_build.sh
docker_test.sh
docker_push.sh
docker_run.sh

Outside of this loop, you might also want to inspect the docker container. For
this, you can create an interactive shell inside the container with docker_bash.sh

You can also pull the new image in the docker gui by going to Images, click
the 3 dots by the dashbaord image and choose Pull.

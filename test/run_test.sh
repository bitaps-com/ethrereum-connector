docker build -t test-connector .
docker run --rm \
           --name test-connector \
           -v $(pwd)/../ethereumconnector/:/ethereumconnector/ \
           -v $(pwd)/../test:/test/ \
           --net=host \
           -it test-connector
# Cattle DB with AWS IoT #

This example shows how to use the database to ingest weather data from AWS IoT and query the database to show a graph.

## Building the Image

```
docker build -t pyimg -f examples/aws_iot/Dockerfile .


# run container into container
docker run -it --entrypoint bash pyimg

# in another terminal copy:
docker cp XXXXX:/var/task/build/docker_layer.zip ./
```

With this zip create a lambda layer and use it for all needed functions.


## Setting up the database

Can be done with the cattledb CLI
```
cattledb -c ingest.py initdb
cattledb -c ingest.py dbinfo
```


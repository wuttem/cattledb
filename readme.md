# Cattle DB #

[![Build Status](https://travis-ci.org/wuttem/cattledb.svg?branch=master)](https://travis-ci.org/wuttem/cattledb)

Fast BigTable Time Series Database Implementation


## Installation
Recursive Option is needed to build the C Speedups.
```
git clone --recursive https://github.com:wuttem/cattledb.git
pip install ./cattledb
```

## Run Docker Emulator for Tests on Machine

```bash
docker run -it -p 8080:8080 spotify/bigtable-emulator:latest
export BIGTABLE_EMULATOR_HOST=localhost:8080
pytest tests
```


## Build and Start Tests in docker
```
docker build . -t cattledb-test
docker run -it cattledb-test bash
service bigtable-server start
export BIGTABLE_EMULATOR_HOST="localhost:8080"
pytest tests
```


## Compile python protobuf file
```bash
# RUN
python -m grpc.tools.protoc --python_out=./cattledb/grpcserver --grpc_python_out=./cattledb/grpcserver --proto_path=./protos cdb.proto
# cd to .\cattledb\grpcserver\cdb_pb2_grpc.py
# change line
import cdb_pb2 as cdb__pb2
# to
from . import cdb_pb2 as cdb__pb2
```

## Build and Push cattledb docker
```bash
docker build . -t mths/cattledb:latest
docker tag mths/cattledb:latest mths/cattledb:0.1
docker push mths/cattledb
```
# Cattle DB #

[![Build Tests](https://github.com/wuttem/cattledb/actions/workflows/python-package.yml/badge.svg)](https://github.com/wuttem/cattledb/actions/workflows/python-package.yml)
[![Mentioned in Awesome Bigtable](https://awesome.re/mentioned-badge-flat.svg)](https://github.com/zrosenbauer/awesome-bigtable)


Fast Time Series Database Implementation.

CattleDB can store timeseries data in various cloud noSQL databases.
At the moment bigtable and dynamodb storage backends are implemented.
Data can be queried by time ranges very efficiently.

CattleDB can be used as a library in projects or as a standalone service with a REST/gRPC backend to put/get data.


## Installation
Recursive Option is needed to build the C Speedups.
```
git clone --recursive https://github.com:wuttem/cattledb.git
pip install ./cattledb
```

## Run Docker Emulator for Tests on Machine

Bigtable

```bash
docker run -it -p 8080:8080 spotify/bigtable-emulator:latest
export BIGTABLE_EMULATOR_HOST=localhost:8080
pytest tests
```

DynamoDB

```bash
docker run -it -p 8000:8000 amazon/dynamodb-local:latest
export DYNAMO_EMULATOR_HOST=localhost:8000
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

## Development Build / Upload
```
python setup.py sdist
twine upload dist/*
```

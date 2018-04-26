# Cattle DB #

Fast BigTable Time Series Database Implementation

## Run Docker Emulator for Tests

```bash
docker run -it -p 8080:8080 spotify/bigtable-emulator:latest
export BIGTABLE_EMULATOR_HOST=localhost:8080
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

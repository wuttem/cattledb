## -*- docker-image-name: "spotify/bigtable-emulator" -*-
FROM spotify/bigtable-emulator as emu

RUN ls /go

FROM python:3.6

COPY --from=emu /go /go
COPY bigtable-emu/bigtable-server /etc/init.d/bigtable-server
RUN  chmod 700 /etc/init.d/bigtable-server

RUN apt-get update
RUN apt-get install -y ca-certificates

# APP Data Req
RUN mkdir -p /app
WORKDIR /app
COPY requirements.txt /app/requirements.txt

# Updates
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /app/requirements.txt
RUN pip install pytest mock

COPY . /app

CMD ["bash"]
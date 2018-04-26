FROM python:3.6

RUN apt-get update
RUN apt-get install -y ca-certificates

# APP Data Req
RUN mkdir -p /app
RUN mkdir -p /var/log/smaxtec
WORKDIR /app
COPY requirements.txt /app/requirements.txt

# Updates
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /app/requirements.txt
RUN pip install gunicorn

COPY . /app

CMD ["bash", "-c", "/app/start.sh"]
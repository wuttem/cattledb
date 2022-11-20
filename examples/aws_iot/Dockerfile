FROM public.ecr.aws/lambda/python:3.8

# Copy function code
# COPY app.py ${LAMBDA_TASK_ROOT}

# Install the function's dependencies using file requirements.txt
# from your project folder.

RUN yum -y install gcc gcc-c++ make zip

RUN pip3 install cmake
COPY requirements.txt  requirements.txt
RUN  pip3 install -r requirements.txt --target /var/task/build/python
COPY . ./cattledb/
RUN cd cattledb && pip3 install . --target /var/task/build/python
RUN cd /var/task/build && zip -r docker_layer.zip .
#RUN cd cattledb && python setup.py install --prefix /var/task/build/python


# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
#CMD [ "app.handler" ] 
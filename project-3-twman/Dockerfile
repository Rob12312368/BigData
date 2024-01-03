FROM ubuntu:22.04
RUN apt-get update && apt-get install -y unzip python3 python3-pip
RUN pip3 install pandas==2.1.0
RUN pip3 install torch==2.0.1 --index-url https://download.pytorch.org/whl/cpu
RUN pip3 install grpcio==1.58.0 grpcio-tools==1.58.0
COPY server.py /server.py
COPY modelserver_pb2_grpc.py /modelserver_pb2_grpc.py
COPY modelserver_pb2.py /modelserver_pb2.py
# COPY client.py /client.py
# COPY workload /workload
COPY docker_autograde.py /docker_autograde.py
COPY tester.py /tester.py
CMD ["python3", "/server.py"]
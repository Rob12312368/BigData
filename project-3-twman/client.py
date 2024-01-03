import grpc
import modelserver_pb2, modelserver_pb2_grpc
import threading
import sys
import pandas as pd
import torch

def threadfunc(workload):
    lock = threading.Lock()
    for row in workload:
        #stub = modelserver_pb2_grpc.ModelServerStub
        result = stub.Predict(modelserver_pb2.PredictRequest(X = row))
        with lock:
            statistics[1] += 1
            if result.hit:
                statistics[0] += 1


# read argument

arglen = len(sys.argv)
port = sys.argv[1]
coef = [float(i) for i in sys.argv[2].split(',')]
files = []
for i in range(3,arglen):
    files.append(sys.argv[i])

#sys.exit(0)
# split volumn of work
init = torch.empty(0)
for f in files:
    init = torch.cat([init,torch.tensor(pd.read_csv(f,header=None).to_numpy(), dtype = torch.float32)], dim = 0)
#sys.exit(0)
# create three threads and pass into the workload respectively
partition = len(init) // 3
t1 = threading.Thread(target=threadfunc,args=(init[:partition],))
t2 = threading.Thread(target=threadfunc,args=(init[partition:2*partition],))
t3 = threading.Thread(target=threadfunc,args=(init[2*partition:],))

statistics = [0,0]
channel = grpc.insecure_channel("localhost:" + port)
stub = modelserver_pb2_grpc.ModelServerStub(channel)
stub.SetCoefs(modelserver_pb2.SetCoefsRequest(coefs = coef ))

t1.start()
t2.start()
t3.start()
t1.join()
t2.join()
t3.join()

print(statistics[0]/statistics[1])
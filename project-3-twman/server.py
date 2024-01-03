import torch
import threading
import modelserver_pb2, modelserver_pb2_grpc
import grpc
from concurrent import futures

class ModelServer(modelserver_pb2_grpc.ModelServerServicer):
    def __init__(self):
        self.cache = PredictionCache()
    def SetCoefs(self, request, context):
        #print(type(request.coefs)) #<class 'google._upb._message.RepeatedScalarContainer'>
        self.cache.SetCoefs(torch.FloatTensor(request.coefs).to(torch.float32))
        return modelserver_pb2.SetCoefsResponse(error = '')
    def Predict(self, request, context):
        result = self.cache.Predict(torch.FloatTensor(request.X).to(torch.float32))
        return modelserver_pb2.PredictResponse(y = float(result[0]), hit = result[1], error = '')

class PredictionCache():
    def __init__(self):
        self.cache = {}
        self.order = []
        self.maxsize = 10

    def SetCoefs(self,coefs):
        lock = threading.Lock()
        with lock:
            self.cache = {}
            self.order = []
            self.coefs = coefs

    def Predict(self,X):
        X = torch.round(X, decimals = 4)
        dickeyX = tuple(X.flatten().tolist()) #given solution on github is weird
        lock = threading.Lock()

        with lock:
            if dickeyX in self.cache:
                # if in cache put that X to the most recent used side (rightmost) of the LRU
                self.order.remove(dickeyX)
                self.order.append(dickeyX)
                return (self.cache[dickeyX], True)
            else:
                # if not in cache, add into it, but need to check the size limit
                self.order.append(dickeyX)
                self.cache[dickeyX] = X @ self.coefs
                # if exceed limit, must evict
                if len(self.order) > 10:
                    victim = self.order.pop(0)
                    self.cache.pop(victim) 
                return (self.cache[dickeyX], False)
            

print('server start')
server = grpc.server(futures.ThreadPoolExecutor(max_workers=4), options=(('grpc.so_reuseport', 0),))
modelserver_pb2_grpc.add_ModelServerServicer_to_server(ModelServer(), server)
server.add_insecure_port("[::]:5440", )
server.start()
server.wait_for_termination()
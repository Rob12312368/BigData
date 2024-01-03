import grpc
import station_pb2, station_pb2_grpc
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from concurrent import futures

class StationServer(station_pb2_grpc.StationServicer):
    def __init__(self):
        cluster = Cluster(['p6-db-1', 'p6-db-2', 'p6-db-3'])
        cass = cluster.connect()
        self.cass = cass
    def RecordTemps(self, request, context):
        insert_cql = self.cass.prepare('''
        insert into weather.stations (id, date, record)
        values(?, ?, {tmin:?, tmax:?})                      
        ''')
        insert_cql.consistency_level = ConsistencyLevel.ONE
        self.cass.execute(insert_cql, (request.station, request.date, request.tmin, request.tmax))
        return station_pb2.RecordTempsReply(error = '')
    def StationMax(self, request, context):
        max_cql = self.cass.prepare('''
            select max(record.tmax)
            from weather.stations
            where id = ?
        ''')
        errormessage = ''
        try:
            max_cql.consistency_level = ConsistencyLevel.THREE
            result = self.cass.execute(max_cql, (request.station,)).one()[0]
            return station_pb2.StationMaxReply(tmax = result, error = errormessage)
        except Exception as e:
            errormessage = f'need {e.required_replicas} replicas, but only have {e.alive_replicas}'
            return station_pb2.StationMaxReply(error = errormessage)
        
    

print('server start')
server = grpc.server(futures.ThreadPoolExecutor(max_workers=4), options=(('grpc.so_reuseport', 0),))
station_pb2_grpc.add_StationServicer_to_server(StationServer(), server)
server.add_insecure_port("[::]:5440")
server.start()
server.wait_for_termination()
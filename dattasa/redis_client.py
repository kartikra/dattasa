import redis
import csv


class RedisClient():
    '''
    Connects to redis cache and loads data to a cache
    '''
    def __init__(self, config_file, db_credentials, db_host, debug=False):
        self.config_file = config_file
        self.db_credentials = db_credentials
        self.db_host = db_host
        self.debug = debug
        return

    def get_db_conn(self, other_db=0):
        redis_credentials = self.db_credentials[self.db_host]
        try:
            if other_db == 0:
                conn = redis.Redis(redis_credentials['host'],
                                   redis_credentials['port'],
                                   redis_credentials['database']
                                   )
            else:
                conn = redis.Redis(redis_credentials['host'],
                                   redis_credentials['port'],
                                   other_db
                                   )
        except redis.Error as e:
            print("Unable to connect using redis!")
            print(e.diag.message_detail)
        except Exception as e:
            print(e, e.args)
        return conn

    def __load_data_from_csv(self, csv_file, file_delimiter, ik, iv):
        with open(csv_file) as csvf:
            csv_data = csv.reader(csvf, delimiter=file_delimiter)
            return [(r[ik], r[iv]) for r in csv_data]

    def __store_data(self, conn, data):
        for i in data:
            conn.setnx(i[0], i[1])
        return

    def clear_redis_data(self, conn, clear_all=False):
        if clear_all:
            conn.flushall()
        else:
            conn.flushdb()
        return

    def get_redis_data(self, conn, lookup_key):
        lookup_value = conn.get(lookup_key)
        if lookup_value is None:
            return 'Not-Found'
        else:
            return lookup_value

    def load_csv_to_redis(self, conn, file_name, key_position,
                          value_position, csv_delimiter=','):
        data = self.__load_data_from_csv(file_name, csv_delimiter, key_position,
                                         value_position)
        self.__store_data(conn, data)
        return

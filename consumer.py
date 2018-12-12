import psycopg2
from config import config

from kafka import KafkaClient, KafkaConsumer
from kafka.errors import KafkaError
from sys import argv
import json
import logging
import traceback
import os
import sys

class ConsumerService:
    def __init__(self, topic: str, process: callable, **config):
        if not callable(process):
            raise TypeError("Consumer process parameter must be type callable.")
        self.process = process
        self.logger = logging.getLogger(__name__)
        self.config = config or {}
        self.topic = topic
        self.consumer = self.create_consumer()
        self.conn = self.connect_db()
        # consider moving validate to init (self.validate)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.consumer.commit()
        self.consumer.close()
        # close the connection with the PostgreSQL
        if self.conn is not None:
            self.cur.close()
            self.conn.close()

        if args[0]:
            traceback.print_exception(args[0], args[1], args[2])

    def create_consumer(self):
        return KafkaConsumer(self.topic, **self.config)

    def consume(self):
        messages_consumed = 0

    def connect_db(self):
        try:
            self.conn = None
            # Load connection parameters
            params = config()
     
            # Connectonnect to the PostgreSQL server
            self.conn = psycopg2.connect(**params)
     
            # Create a cursor
            self.cur = self.conn.cursor()
            
            # Drop the table if it is there
            self.cur.execute('DROP TABLE IF EXISTS stream')
            self.conn.commit()
            
            # Create stream table to hold data
            self.cur.execute('CREATE TABLE stream (theData jsonb NULL)')
            self.conn.commit()
            
            return self.conn
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def insert_data(self, message):
        self.cur.execute('INSERT INTO stream (theData) SELECT jsonb_array_elements(\'['+message.value+']\'::jsonb) AS subsection')
        self.conn.commit()
        
    def update_data(self, message):
        self.cur.execute('UPDATE stream set theData = (SELECT jsonb_array_elements(\'['+message.value+']\'::jsonb) AS subsection)')
        self.conn.commit()
        
    def run(self, max_messages=-1):
        try:
            counter = 1
            
            for message in self.consumer:
                print(message.value)
                if counter == 1:
                    self.insert_data(message)
                else:
                    self.update_data(message)
                
                if max_messages != -1 and counter >= max_messages:
                    print('Maximum messages processed')
                    break
                    
                counter += 1
              
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if self.conn is not None:
                self.conn.close()
                print('Database connection closed.')
 
def main():
    KAFKA_BOOTSTRAP_SERVER = os.environ.get("KAFKA_BOOTSTRAP_SERVER") or 'localhost:9092'
    DEFAULT_TOPIC = os.environ.get("KAFKA_DEFAULT_TOPIC") or 'poc'
    DEFAULT_GROUP_ID = os.environ.get("KAFKA_DEFAULT_TOPIC") or 'poc'

    max_msg = -1
    if len(sys.argv) > 1:
       max_msg = int(sys.argv[1])
    
    kwargs = dict(bootstrap_servers=[KAFKA_BOOTSTRAP_SERVER],
                  auto_offset_reset='latest',
                  enable_auto_commit=False,
                  value_deserializer=lambda m: json.loads(m.decode('ascii')),
                  group_id=DEFAULT_GROUP_ID
                  )

    with ConsumerService(topic=DEFAULT_TOPIC, process=print, **kwargs) as consumer_service:
        consumer_service.run(max_msg)
   
if __name__ == '__main__':
    main()
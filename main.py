import pandas as pd
import numpy as np
import psycopg2
import psycopg2.extras
import threading
import time

conn = psycopg2.connect(
   database="tazi", user='postgres', password='', host='localhost'
)
cursor = conn.cursor()

def migrate_data():
    psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)
    with pd.read_csv('tazi-se-interview-project-data.csv',chunksize=300) as read_chunk:
        for chunk in read_chunk:
            chunk.drop(columns=['id'],inplace=True)
            data = chunk.to_records(index=False)
            insert_query = 'insert into predictions (given_label,model1_a,model1_b,model2_a,model2_b,model3_a,model3_b) values %s'
            psycopg2.extras.execute_values (
                cursor, insert_query, data, template=None, page_size=300
            )
            conn.commit()
            time.sleep(1)
            break

def calculations():
    pass


migrator = threading.Thread(target=migrate_data)
calculator = threading.Thread(target=calculations)
migrator.start()
calculator.start()
migrator.join()
calculator.join()
conn.close()

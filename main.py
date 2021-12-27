from collections import deque
import pandas as pd
import numpy as np
import psycopg2
import psycopg2.extras
import threading
import time

conn = psycopg2.connect(
   database="", user='', password='', host=''
)
cursor = conn.cursor()

def migrate_data():
    conn = psycopg2.connect(
   database="", user='', password='', host=''
)
    cursor = conn.cursor()
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

def calculate_matrix_values(df):
    df['A_pred'] = 0.5*df['model1_a'].astype(float) + 0.6*df['model2_a'].astype(float) + 0.7*df['model3_a'].astype(float)
    df['B_pred'] = 0.5*df['model1_b'].astype(float) + 0.6*df['model2_b'].astype(float) + 0.7*df['model3_b'].astype(float)
    df.loc[df['A_pred'] >= df['B_pred'], 'pred'] = 'A'
    df.loc[df['A_pred'] < df['B_pred'], 'pred'] = 'B'
    df.loc[df['pred'] == df['given_label'], 'correct'] = 1
    df.loc[df['pred'] != df['given_label'], 'correct'] = 0
    tp = int(df.loc[df['given_label']=='A']['correct'].sum())
    fn = int(len(df.loc[df['given_label']=='A']) - tp)
    tn = int(df.loc[df['given_label']=='B']['correct'].sum())
    fp = int(len(df.loc[df['given_label']=='B']) - tn)

    return tp,fn,tn,fp

def calculations(window_size=1000):
    conn2 = psycopg2.connect(
    database="", user='', password='', host=''
)
    conn2.autocommit=True
    cursor2 = conn2.cursor()
    while(True):
        cursor2.execute("SELECT n_live_tup FROM pg_stat_user_tables where relname='predictions'")
        rows = cursor2.fetchone()
        current_row = rows[0]
        if current_row >= window_size:
            break
    first_index = 1
    cursor2.execute(f"SELECT * from predictions where id>={first_index} and id<{first_index+window_size}")
    rows = cursor2.fetchall()
    first_index = rows[-1][0]
    data = deque(rows)
    last_row = rows[-1]
    while(True):
        if last_row:
            df = pd.DataFrame(data, columns =['id','given_label','model1_a','model1_b','model2_a','model2_b','model3_a','model3_b'])
            tp,fn,tn,fp = calculate_matrix_values(df)
            cursor2.execute(f"INSERT INTO confusion_matrix (a_a,a_b,b_a,b_b) VALUES ({tp},{fn},{fp},{tn})")
            first_index = last_row[0]
            
        cursor2.execute(f"SELECT * from predictions where id>{first_index} limit 1")
        last_row = cursor2.fetchone()
        if last_row:
            data.popleft()
            data.append(last_row)

if __name__ =='__main__':
    migrator = threading.Thread(target=migrate_data)
    calculator = threading.Thread(target=calculations)
    migrator.start()
    calculator.start()
    migrator.join()
    calculator.join()
# from datetime import datetime
# import uuid
# from airflow import DAG
# #  NEW CORRECT IMPORT
# from airflow.operators.python import PythonOperator

# default_args = {
#     'owner': 'haoj',
#     'start_date': datetime(2026, 5, 25, 10, 00)
# }


# def get_data():
#     import requests
    
#     res = requests.get('https://randomuser.me/api/')
#     data = res.json()
#     res = data['results'][0]
    
#     return res

# def format_data(res):
#     data = {}
#     location = res['location']  
#     data['id'] = str(uuid.uuid4())
#     data['first_name'] = res['name']['first']
#     data['last_name'] = res['name']['last']
#     data['gender'] = res['gender']
#     data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
#                       f"{location['city']}, {location['state']}, {location['country']}"
#     data['post_code'] = location['postcode']
#     data['email'] = res['email']
#     data['username'] = res['login']['username']
#     data['dob'] = res['dob']['date']
#     data['registered_date'] = res['registered']['date']
#     data['phone'] = res['phone']
#     data['picture'] = res['picture']['medium']

#     return data

# def stream_data():
#     import json 
#     from kafka import KafkaProducer
#     import time
#     import logging
#     # import requests
#     # res = requests.get('https://randomuser.me/api/')
#     # # print(res.json())
#     # res.json()
#     # data = res.json()
#     # res = data['results'][0]
#     # print(json.dumps(res, indent=3))

#     producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000) #max timeout
#     curr_time = time.time()

#     while True:
#         if time.time() > curr_time + 60:
#             break
#         try:
#             res = get_data()
#             res = format_data(res)
#             producer.send('user_created', json.dumps(res).encode('utf-8'))
#         except Exception as e:
#             logging.error(f'An error occured: {e}')
#             continue

# with DAG('user_automation',
#          default_args=default_args,
#          schedule_interval='@daily',
#          catchup=False) as dag:
    
#     streaming_task = PythonOperator(
#         task_id='stream_data_from_api',
#         python_callable=stream_data
#     )


from datetime import datetime
import uuid
from airflow import DAG
#  NEW CORRECT IMPORT
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'haoj',
    'start_date': datetime(2026, 5, 25, 0, 0)
}


def get_data():
    import requests
    
    res = requests.get('https://randomuser.me/api/')
    data = res.json()
    res = data['results'][0]
    
    return res

def format_data(res):
    data = {}
    location = res['location']  
    data['id'] = str(uuid.uuid4())
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data

def stream_data():
    import json 
    from kafka import KafkaProducer
    import time
    import logging

    # 1. Initialize Producer
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000) 
    curr_time = time.time()

    # Callback helpers to track delivery status in Airflow task logs
    def on_success(record_metadata):
        logging.info(f"Successfully sent to topic {record_metadata.topic} partition {record_metadata.partition}")

    def on_error(excp):
        logging.error(f"Failed to deliver message: {excp}")

    logging.info("Starting 60-second streaming loop...")
    
    while True:
        if time.time() > curr_time + 60:
            break
        try:
            res = get_data()
            res = format_data(res)
            
            # 2. Send message and attach callbacks for explicit logging
            producer.send('users_created', json.dumps(res).encode('utf-8')).add_callback(on_success).add_errback(on_error)
            
            # Optional: Add a slight pause so you don't aggressively spam the API inside the 60s window
            time.sleep(1) 
            
        except Exception as e:
            logging.error(f'An error occurred during runtime extraction: {e}')
            continue

    # 3. 🌟 CRITICAL FIX: Flush remaining buffered messages to Kafka before shutting down the task!
    logging.info("Flushing remaining messages to Kafka broker...")
    producer.flush()
    logging.info("Streaming session finished cleanly.")

with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
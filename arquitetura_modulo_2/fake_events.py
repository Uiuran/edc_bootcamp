from boto3 import Session
import boto3
import json
from fake_web_events import Simulation

boto_sess = Session(profile_name='awsadm')
client = boto_sess.client('firehose',region_name='sa-east-1')

def put_record(event: dict):
    data = json.dumps(event) + "\n"
    response = client.put_record(
            DeliveryStreamName='desafio-bootcamp',
            Record={"Data":data}
            )
    print(event)
    return response

simulation = Simulation(user_pool_size=20,sessions_per_day=100000)
events = simulation.run(duration_seconds=100000)

for event in events:
    put_record(event)

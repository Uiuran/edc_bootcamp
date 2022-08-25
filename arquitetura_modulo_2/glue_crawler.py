from boto3 import Session
session = Session(profile_name='awsadm')
glue = session.client('glue',region_name='sa-east-1')

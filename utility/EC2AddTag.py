import boto3
import datetime
import RDSMethods as rdsM

def addLoadStatusTag(newTag, instanceId):
    client = boto3.client('ec2')
    response = client.create_tags(
        Resources=[
            instanceId,
            ],
        Tags=[
            {
                'Key': 'loadstatus',
                'Value': newTag
            },
            ]
        )
    if newTag == 'complete':
        addNextLaunchTag(instanceId)

def addNextLaunchTag(instanceId):
    client = boto3.client('ec2')
    now = datetime.datetime.now()
    devIP = rdsM.getDevIP()
    now_plus = now + datetime.timedelta(minutes = int(rdsM.getConfig(devIP,'EC2CheckMinutes')))
    now_plus_string = now_plus.strftime('%Y%m%d%H')
    response = client.create_tags(
        Resources=[
            instanceId,
            ],
        Tags=[
            {
                'Key': 'nextlaunch',
                'Value': now_plus_string
            },
            ]
        )

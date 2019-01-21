import boto3
import pymysql as mdb

def getRDSClient():
    return boto3.client('rds',
        region_name='us-east-2')

def getRDSInstances():
	return getRDSClient().describe_db_instances()

def isServerRunning(key, value):
    countOfRunning = 0
    for instance in getRDSInstances()['DBInstances']:
        response = getRDSClient().list_tags_for_resource(ResourceName=instance['DBInstanceArn'])
        for tag in response['TagList']:
            if tag['Key'] == key and tag['Value'] == value:
                countOfRunning += 1
    if countOfRunning > 0:
        return True
    else:
        return False

def stopRDS(type):
    client = getRDSClient()
    if type == 'dev':
        snapId = 'vast-dev-final-snapshot'
        getId = getFirstDBIdent('server','dev')
        if doesSnapshotExist(snapId):
             deleteDBSnapshot(snapId)
        response = client.delete_db_instance(DBInstanceIdentifier = getId, SkipFinalSnapshot = False,  FinalDBSnapshotIdentifier = snapId, DeleteAutomatedBackups = True)
    else:
        print('We havent set up a routine for this yet')

def deleteDBSnapshot(DBSnapIdent):
    getRDSClient().delete_db_snapshot(DBSnapshotIdentifier=DBSnapIdent)

def getFirstDBIdent(key, value):
    client = getRDSClient()
    instances = getRDSInstances()
    for instance in instances['DBInstances']:
        response = client.list_tags_for_resource(ResourceName=instance['DBInstanceArn'])
        for tag in response['TagList']:
            if tag['Key'] == key and tag['Value'] == value:
                return instance['DBInstanceIdentifier']
            else:
                return False

def doesSnapshotExist(snapId):
    client = getRDSClient()
    snapshots = client.describe_db_snapshots()['DBSnapshots']
    for snapshot in snapshots:
        if snapshot['DBSnapshotIdentifier'] == snapId:
            return True
    return False

def getDevIP():
    instances = getRDSInstances()
    client = getRDSClient()
    for instance in instances['DBInstances']:
        response = client.list_tags_for_resource(ResourceName=instance['DBInstanceArn'])
        for tag in response['TagList']:
            if tag['Key'] == 'server' and tag['Value'] == 'dev':
                return instance['Endpoint']['Address']

def getConfig(databaseip, key):
    con = mdb.connect(databaseip, 'admin', 'Welcome123$','vast', local_infile = 1)
    cur = con.cursor()
    query = "SELECT ConfigValue FROM t_Config WHERE ConfigKey = '"+key+"';"
    print(query)
    cur.execute(query)
    return cur.fetchone()[0]

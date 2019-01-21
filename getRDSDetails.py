import boto3
import time
import vastloadlog
import getFilesToLoad
import deleteOldTables
import utility.RDSMethods as rdsM

#####GET DEV IP#####
def getDevIP():
    instances = rdsM.getRDSInstances()
    client = rdsM.getRDSClient()
    for instance in instances['DBInstances']:
        response = client.list_tags_for_resource(ResourceName=instance['DBInstanceArn'])
        for tag in response['TagList']:
            if tag['Key'] == 'server' and tag['Value'] == 'dev':
                return instance['Endpoint']['Address']


def getDevStatus():
    instances = rdsM.getRDSInstances()
    client = rdsM.getRDSClient()
    for instance in instances['DBInstances']:
        response = client.list_tags_for_resource(ResourceName=instance['DBInstanceArn'])
        for tag in response['TagList']:
            if tag['Key'] == 'server' and tag['Value'] == 'dev':
                return instance['DBInstanceStatus']


def startServerFromSnapshot():
    client = rdsM.getRDSClient()
    snapshots = client.describe_db_snapshots()
    exists_response = snapshotExists('vast-dev-final-snapshot',snapshots['DBSnapshots'])
    if exists_response[0]:
        print("We have found the final snapshot")
        response = client.restore_db_instance_from_db_snapshot(
            DBInstanceIdentifier='vast-dev',
            DBSnapshotIdentifier='vast-dev-final-snapshot',
            Tags=[{'Key': 'server', 'Value': 'dev'}, {'Key': 'stopstart', 'Value': 'office'}],
            VpcSecurityGroupIds=['sg-025adefb25d0f6a4c'],
            CopyTagsToSnapshot=True
        )
        instanceId = response['DBInstance']['DBInstanceIdentifier']
        while True:
            retrievedInstance = client.describe_db_instances(DBInstanceIdentifier = instanceId)['DBInstances'][0]
            if retrievedInstance['DBInstanceStatus']  == 'available':
                break
        else:
            print(retrievedInstance['DBInstanceStatus'])
            print('Waiting to check status again...')
            time.sleep(10)
    else:
        raise ValueError("No Final Snapshot found")

def snapshotExists(snapID, snapshots):
    for snapshot in snapshots:
        if snapID == snapshot[DBSnapshotIdentifier]:
            return [True, snapshot]
    return [False, None]

def getFirstDBIdent(key, value):
    client = rdsM.getRDSClient()
    instances = rdsM.getRDSInstances()
    for instance in instances['DBInstances']:
        response = client.list_tags_for_resource(ResourceName=instance['DBInstanceArn'])
        for tag in response['TagList']:
            if tag['Key'] == key and tag['Value'] == value:
                return instance['DBInstanceIdentifier']
            else:
                return False

def setUpRDSForLoad():
    if rdsM.isServerRunning('server','dev'):
        print('Server is already running')
        vastloadlog.updateLoadLog()
        getFilesToLoad.loadFiles()
        deleteOldTables.deleteOldTablesFromData()
    else:
        print("Server isn't running, starting server....")
        startServerFromSnapshot()
        while True:
            time.sleep(10)
            print("is Running?")
            if getDevStatus() == 'available':
		break
            print(getDevIP())
        vastloadlog.updateLoadLog()
        getFilesToLoad.loadFiles()
        deleteOldTables.deleteOldTablesFromData()

def main():
    setUpRDSForLoad()

if __name__ == "__main__": main()

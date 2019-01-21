import paramiko
import datetime
import pymysql as mdb
import sys
import os
import socket
from datetime import datetime as dt
from datetime import date, timedelta
import getRDSDetails as devDet


def updateLoadLog():

    pwd = os.path.abspath('..')
    key_file = pwd + '/key/pk.ppk'  # NOT .pub


    host = 'sftp.barb.2cnt.net'
    port = 22
    username = 'barbvast-uksftp'
    ip = socket.gethostbyname(socket.gethostname())
    prodip = '172.16.193.111'
    databaseip = devDet.getDevIP()
    print(databaseip)
    print("Pointing the script towards " + databaseip)

    try:
        #mysqlConnection
        con = mdb.connect(databaseip, 'admin', 'Welcome123$','vast')
        cur = con.cursor()
        cur.execute("SELECT VERSION();")
        ver = cur.fetchone()
        print (ver)
        print("Getting file list from SFTP....")
        my_key = paramiko.RSAKey.from_private_key_file(key_file)
        transport = paramiko.Transport((host, port))
        transport.connect(username=username, pkey=my_key)
        sftp = paramiko.SFTPClient.from_transport(transport)
        dateFromQ = "SELECT ConfigValue FROM t_Config WHERE ConfigKey = 'VastSftpMetaDataToGet';"
        cur.execute(dateFromQ)
        dateFrom = cur.fetchone()
        start_date = (date.today() - timedelta(int(dateFrom[0]))).strftime('%Y-%m-%d')
        start_date_string = dt.strptime(start_date, '%Y-%m-%d')
        print("We will look for files after: "+str(start_date_string))
        print (sftp.listdir())
        folder_list = sftp.listdir(path='/usage')
        print(len(folder_list))
        for folder in folder_list:
	    print(folder)
            if folder.startswith('usage-20'):
	        folderdate = folder[6:16]
                datetime_object = dt.strptime(folderdate, '%Y-%m-%d')
                if start_date_string < datetime_object:
                    rc = 0
                    files_in_folder = sftp.listdir_attr(path=('/usage/'+folder))
            	    for file in files_in_folder:
                        if file.filename.endswith('.gz'):
                            remotePath = '/usage/'+folder+ '/' + file.filename
                            fileDate = folder[6:16]
                            fileHour = folder[17:19]
                            fileName = file.filename[0:6]
                            fileRevision = file.filename[7]
                            lastModified = datetime.datetime.fromtimestamp(file.st_mtime)
                            fileSize = file.st_size
                            insertStatement = 'INSERT INTO t_FileLogSftp (RemotePath, FileDate, FileName, FileRevision, LastModified, FileSize,  UpdateTime, FileHour) VALUES \n'
                            insertStatement = insertStatement + '(\'' + remotePath + '\', \'' + fileDate + '\', \''+ fileName +'\', \'' + str(fileRevision)
                            insertStatement = insertStatement + '\', \'' + str(lastModified) + '\', ' + str(fileSize) + ', NOW(), ' + str(fileHour) + ')\n'
                            insertStatement = insertStatement + 'ON DUPLICATE KEY UPDATE RemotePath = \'' + remotePath + '\', FileRevision = \'' + str(fileRevision)
                            insertStatement = insertStatement + '\', LastModified = \'' + str(lastModified) + '\', FileSize =' + str(fileSize) + ',  UpdateTime = NOW();'
                            cur.execute(insertStatement)
                            rc = rc + cur.rowcount
                            con.commit()
	                    print ("Commited %d rows for folder %s" % (rc, folder))

    except Exception as e:
        print ("Error %s" % str(e.args))
        sys.exit(1)


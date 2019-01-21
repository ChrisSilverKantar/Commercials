import datetime
import sys
import paramiko
import sys
import os
import socket
import gzip
import shutil
import urllib2
import pymysql as mdb
from time import sleep
import getRDSDetails as devDet
import utility.EC2AddTag as ECTag

def loadFiles():
    databaseip = devDet.getDevIP()
    pwd = os.path.abspath('..')
    key_file = pwd + '/key/pk.ppk'  # NOT .pub


    host = 'sftp.barb.2cnt.net'
    port = 22
    username = 'barbvast-uksftp'
    ip = socket.gethostbyname(socket.gethostname())


    try:

        my_key = paramiko.RSAKey.from_private_key_file(key_file)
        transport = paramiko.Transport((host, port))
        transport.connect(username=username, pkey=my_key)
        sftp = paramiko.SFTPClient.from_transport(transport)
        con = mdb.connect(databaseip, 'admin', 'Welcome123$','vast', local_infile = 1)
        cur = con.cursor()
        cur.execute("SELECT VERSION();")
        ver = cur.fetchone()
        print (ver)
        cur.execute("call sp_GetFileListToLoad()")
        result = cur.fetchall()
        if len(result) > 0:
            selfInstanceID = urllib2.urlopen('http://169.254.169.254/latest/meta-data/instance-id').read()
            response = ECTag.addLoadStatusTag('loading',selfInstanceID)
            print("We have "+ str(len(result))+" files to load.")
	    for file in result:
	        truncateQuery = "TRUNCATE TABLE t_Stg_Emedia;"
	        print("Truncating Table " + truncateQuery)
	        cur.execute(truncateQuery)
	        con.commit()
	        print("Start Loading " + file[0])
	        filepath = file[0]
                localpath = '../temp/'+file[2]
	        rowcount = 0
	        print("Downloading File....")
	        sftp.get(filepath, localpath)
	        print("File Downloaded")
	        with gzip.open(localpath, 'rb') as f_in:
		    print("GZIPR OPEN")
    		    with open(localpath.replace('.gz',''), 'wb') as f_out:
        	        print("COPY FILE")
		        shutil.copyfileobj(f_in, f_out)
		        print("COPIED FILE")
	        f_out.close()
	        f_in.close()
	        loadquer = "LOAD DATA LOCAL INFILE '" + os.getcwd()+"/"+f_out.name + "' INTO TABLE t_Stg_Emedia FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n';"
    	        print(os.getcwd())
                print(loadquer)
	        cur.execute(loadquer)
	        con.commit()
	        loadToTable = "CALL sp_Loadfile(NULL,'" + file[0] +"','" + str(file[3]) + "','" + str(file[4]) +"');"
                print(loadToTable)
	        cur.execute(loadToTable)
	        con.commit()
	    ECTag.addLoadStatusTag('complete',selfInstanceID)
        else:
            selfInstanceID = urllib2.urlopen('http://169.254.169.254/latest/meta-data/instance-id').read()
            ECTag.addLoadStatusTag('complete',selfInstanceID)
            print("No more files to load")

    except Exception as e:
        print ("Error %s" % str(e.args))
        sys.exit(1)

import socket
import paramiko
import os

dirname = os.path.dirname(__file__)

def connectFTP(type):
    if type == "reports":
        return connectReports()
    else:
        raise ValueError('Invalid SFTP name')

def connectReports():
    host = 'sftp.barb.2cnt.net'
    username = 'barbtvprsftp'
    ip = socket.gethostbyname(socket.gethostname())
    pwd = os.path.dirname(__file__)
    print(pwd)
    port = 22
    key_file = pwd + '/../../key/pk.ppk'  # NOT .pub
    print(key_file)
    try:
        my_key = paramiko.RSAKey.from_private_key_file(key_file)
        transport = paramiko.Transport((host, port))
        transport.connect(username=username, pkey=my_key)
        return paramiko.SFTPClient.from_transport(transport)

    except Exception as e:
        print ("Exception in the sftp get step")
        print ("Error %s" % str(e.args))

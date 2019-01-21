import utility.sftpUtility as sfU
import datetime
import getRDSDetails as rdDet
import pymysql as mdb

def getAllTicketData():
    print("Getting ticket data from sftp")
    sftp = sfU.connectFTP('reports')
    folder_list = sftp.listdir(path='/reports')
    ticket_list = [f for f in folder_list if '_tickets.gz' in f]
    #Get Sunday's date to compare to ticket list and see if we have the most recent Sunday's date
    today = datetime.date.today()
    idx = (today.weekday() + 1) % 7
    sun = today - datetime.timedelta(idx)
    sun_str = sun.strftime("%Y%m%d")
    if len([f for f in ticket_list if sun_str + '_tickets.gz' == f]):
        print("We have found last sunday's ticket data")
        ticket = [f for f in ticket_list if sun_str + '_tickets.gz' == f][0]
        print(ticket)
        #sftp.get("/reports/"+ticket, "../temp/"+ticket)
        truncateTable()
        loadTickets("../temp/"+ticket)
    else:
        print("We can't find the needed ticket data")
        raise ValueError("Can't find any ticket data for date")

def truncateTable():
    databaseip = rdDet.getDevIP()
    print(databaseip)
    print("Pointing the script towards " + str(databaseip))
    query = "TRUNCATE TABLE t_ticketDataAnalysis;"
    try:
        #mysqlConnection
        con = mdb.connect(databaseip, 'admin', 'Welcome123$','vast')
        cur = con.cursor()
        cur.execute("SELECT VERSION();")
        ver = cur.fetchone()
        print (ver)
        cur.execute(query)
        con.commit()
    except Exception as e:
        print ("Error %s" % str(e.args))
        sys.exit(1)

def loadTickets(file):
    query = "LOAD DATA LOCAL INFILE '" + str(file) + "' INTO TABLE `t_ticketDataAnalysis`  FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';"
    databaseip = rdDet.getDevIP()
    print("Pointing the script towards " + str(databaseip))
    try:
        #mysqlConnection
        con = mdb.connect(databaseip, 'admin', 'Welcome123$','vast')
        cur = con.cursor()
        cur.execute("SELECT VERSION();")
        ver = cur.fetchone()
        print (ver)
        cur.execute(query)
        con.commit()
    except Exception as e:
        print ("Error %s" % str(e.args))
        sys.exit(1)

if __name__ == "__main__":
    #getAllTicketData()
    truncateTable()

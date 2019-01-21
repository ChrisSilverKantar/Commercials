import datetime
import pymysql as mdb
import sys
import os
import socket
from datetime import datetime as dt
from datetime import date, timedelta
import getRDSDetails as rdDet


def deleteOldTablesFromData():

    databaseip = rdDet.getDevIP()

    print("Pointing the script towards " + databaseip)

    try:
        #mysqlConnection
        con = mdb.connect(databaseip, 'admin', 'Welcome123$','vast')
        cur = con.cursor()
        cur.execute("SELECT VERSION();")
        ver = cur.fetchone()
        print (ver)
        dateFromQ = "SELECT ConfigValue FROM t_Config WHERE ConfigKey = 'VastEmediaHistoryToKeepDays';"
        cur.execute(dateFromQ)
        dateFrom = cur.fetchone()
        start_date = (date.today() - timedelta(int(dateFrom[0]))).strftime('%Y-%m-%d')
        start_date_string = dt.strptime(start_date, '%Y-%m-%d')
        print("We will Delete Tables before: "+str(start_date_string))

        getTableDates = "SELECT TABLE_NAME, REPLACE(TABLE_NAME,'t_VastEmedia_','') FROM information_schema.tables where TABLE_SCHEMA = 'vast' and TABLE_NAME like 't_VastEmedia_%' and DATE(REPLACE(TABLE_NAME,'t_VastEmedia_','')) <= '"+str(start_date_string)+"';"
        cur.execute(getTableDates)
        deleteTableNames = cur.fetchall()
        for tableName in deleteTableNames:
	    print("Deleteing table: " + tableName[0])
	    deleteQuery = "DROP TABLE `" + tableName[0] + "`;"
	    print(deleteQuery)
	    cur.execute(deleteQuery)
	    con.commit()
	    deleteFromLog = "DELETE FROM t_FileLog where FileDate = '" + tableName[1]+"'"
	    print(deleteFromLog)
	    cur.execute(deleteFromLog)


    except Exception as e:
        print ("Error %s" % str(e.args))
        sys.exit(1)

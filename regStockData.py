import os
import datetime as dt
import dbAccess as db
import sys

import glob
import csv


DATAPATH = "./Data/*"

# Stockデータファイルの一覧作成
files = glob.glob(DATAPATH)
if len(files) == 0:
    sys.exit()

#KABU-DBへ接続
dbobj = db.kabudb('./dbconfig.ini')
ret = dbobj.dbConnect()
if ret == False :
    sys.exit()

for filename in files:
    print(filename)
    with open(filename, encoding='utf8', newline='') as f:
        csvreader = csv.reader(f)
        cnt = 0
        for row in csvreader:
            if cnt == 0:
                cnt += 1
                continue
            
            #list = [<RDATE>,<CODE>,<OPEN>,<High>,<LOW>,<CLOSE>,<VALUME>]
            dbobj.insStockinfo(row)
            
        # １社づつコミットする
        dbobj.dbCommit()
    
dbobj.dbClose()

import csv
import dbAccess as db
import sys
import argparse

def getArgument() :
    argp = argparse.ArgumentParser(
        prog = 'getBrandName.py',
        usage = 'getBrandName.py -M <マーケットコード> -F <ファイル名>',
        description = '企業一覧を作成'
    )
    argp.add_argument('-M', '--market', required=False, default='TSE', help='マーケットコード')
    argp.add_argument('-F', '--filename', required=False, default='data_j.csv', help='ファイル名')

    args = argp.parse_args()
    return (args)


# 起動パラメータの取得
args = getArgument()

# 企業一覧のファイルを設定
MARKET = args.market
DATA_FILE = args.filename

# データベースへの接続
dbobj = db.kabudb('./dbconfig.ini')
ret = dbobj.dbConnect()
if ret == False :
    sys.exit()

with open(DATA_FILE, encoding='utf8', newline='') as f:
    csvreader = csv.reader(f)
    cnt = 0
    for row in csvreader:
        if cnt == 0:
            cnt = cnt + 1
            continue
        
        for cnt in range(len(row)) :
            if row[cnt-1] == '-' :
                row[cnt-1] = ''
                
        segment_id = dbobj.getSegment_id(row[3])
        
        # [<code>, <name>, <market_abb>, <segment_id>, <industory_id>, <industory17_id>, <valid>, <rdate>]
        ret = dbobj.insBrandName([row[1], row[2], MARKET, segment_id, row[4], row[6], 1, row[0]])
 
dbobj.dbCommit()
dbobj.dbClose()
        

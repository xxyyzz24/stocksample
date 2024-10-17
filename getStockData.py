import os
import datetime as dt
import pandas_datareader.data as web
import dbAccess as db
import sys
import argparse

def getArgument() :
    argp = argparse.ArgumentParser(
        prog = 'getStockData.py',
        usage = 'getStockData.py -I <業種コード> -17 <17業種コード> -st <取得開始日時> -ed <取得終了日時>',
        description = '株価データを【Stooq】から取得する'
    )
    end = dt.date.today().strftime('%Y/%m/%d')
    argp.add_argument('-I', '--industory', required=False, default=0, help='業種コード')
    argp.add_argument('-17', '--industory17', required=False, default=0, help='17業種コード')
    argp.add_argument('-st', '--stDate', required=False, default='1990/1/1', help='開始日時')
    argp.add_argument('-ed', '--edDate', required=False, default=end, help='終了日時')

    args = argp.parse_args()
    return (args)
    
def main():
    args = getArgument()

    GET_INDUSTORY=int(args.industory)    # 0: ALL
    GET_INDUSTORY17=int(args.industory17)  # 0: ALL

    dbobj = db.kabudb('./dbconfig.ini')
    ret = dbobj.dbConnect()
    if ret == False :
        sys.exit()
        
    rows = dbobj.getCodeList()

    for row in rows :
        
        # 業種を指定
        if GET_INDUSTORY != 0 and GET_INDUSTORY != row[1] :
            continue
        # 17業種を指定
        if GET_INDUSTORY17 != 0 and GET_INDUSTORY17 != row[2] :
            continue

        #銘柄コード入力(7177はGMO-APです。)
        ticker_symbol=str(row[0])
        ticker_symbol_dr=ticker_symbol + ".JP"
    
        #株価情報を取得する範囲（日付）を設定
        start = None
        tmpList = dbobj.getLastDate(row[0])  # コード毎に取得済みの日付を取得
        if len(tmpList) > 0 and tmpList[0][1] == None:
            start=args.stDate
        else:
            start = tmpList[0][1].strftime('%Y/%m/%d')
        end = args.edDate
    
        #データ取得
        df = web.DataReader(ticker_symbol_dr, data_source='stooq', start=start,end=end)
        #2列目に銘柄コード追加
        df.insert(0, "code", ticker_symbol, allow_duplicates=False)
        #csv保存
        df.to_csv( os.path.dirname(__file__) + '/Data/s_stock_data_'+ ticker_symbol + '.csv')
        print('s_stock_data_'+ ticker_symbol + '.csv')
        
    dbobj.dbClose()

# コマンドライン起動時
if __name__ == '__main__':
    main()
import mplfinance as mpf
import pandas as pd
import dbAccess as db
import sys
import argparse

start_date = '1990-01-01'
end_date = None
vol = None
moving = None
filename = None

def getArgument() :
    argp = argparse.ArgumentParser(
        prog = 'createGraph.py',
        usage = 'createGraph.py -S <開始日 YYYY[-MM][-DD]> -E <終了日 YYYY[-MM][-DD]> -V -M -F <filename>',
        description = '株価データからグラフを作成する'
    )
    argp.add_argument('-S', '--startdate', required=False, default='1990-01-01', help='グラフの開始日を指定')
    argp.add_argument('-E', '--enddate', required=False, help='ブラフの最終日を指定（空白：最新日）')
    argp.add_argument('-V', '--volume', required=False, action='store_true', help='Volumeの表示')
    argp.add_argument('-M', '--mal', required=False, action='store_true', help='移動平均線の表示')
    argp.add_argument('-F', '--filename', required=False, help='クラフ画像の保存ファイル名')

    args = argp.parse_args()
    return (args)

def create_graph():
    
    # DBへの接続
    dbobj = db.kabudb('./dbconfig.ini')
    ret = dbobj.dbConnect()
    if ret == False :
        sys.exit()
    
    # データの取得
    rows = dbobj.getStockData("5162")
    columns1 = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
    df = pd.DataFrame(data=rows, columns=columns1)
#    df['Date'] = pd.to_datetime(df['Date'])
    df.set_index('Date',inplace=True)
    print(df)

    # グラフ画像の作成
    params = {}
    if moving != None:
        params["mav"] = moving
    if vol != None:
        params["volume"] = True
    if filename != None:
        params["savefig"] = filename
    else:
        params["savefig"] = 'candlestick_mpf.png'
    mpf.plot(df.loc[start_date:end_date], type='candle', **params)

    # DBのClose
    dbobj.dbClose()


def main():
    create_graph()
    
if __name__ == '__main__':
    # 引数の解析
    args = getArgument()
    if args.startdate != None:
        start_date = args.startdate
    if args.enddate != None:
        end_date = args.enddate
    if args.mal != None:
        moving = (5, 25, 75)
    if args.volume != None:
        vol = True
    if args.filename != None:
        filename = args.filename
    else:
        filename = 'candlestick_mpf.png'
   
    main()
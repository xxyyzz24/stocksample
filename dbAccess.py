import confile
import oracledb
import datetime as dt

class dbAccess():
    def __init__(self, con_file='./dbconfig.ini'):
        self.con_file = con_file
        self.db = None
        self.con = None
        self.cur = None

        try:
            self.db_config = confile.read_config(self.con_file)
        except:
            print('db_config.ini : Read ERROR.')

    def dbConnect(self):
        
        # データベース毎のモジュール設定
        if self.db_config.get_property('general', 'dbtype') == 'Oracle':
            db = oracledb
            userid = self.db_config.get_property('Oracle', 'user')
            pw = self.db_config.get_property('Oracle', 'password')
            host = self.db_config.get_property('Oracle', 'host')
            dbname = self.db_config.get_property('Oracle', 'db')
            dbname = host + "/" + dbname
        
        try:
            self.con = db.connect (user=userid, password=pw, dsn=dbname)
            self.cur = self.con.cursor()
            return True

        except:
            print('DB: connect Eroor.')
            #カーソルがオープン状態ならClose
            if self.cur != None:
                self.cur.close()
            #DBがオープン状態ならClose
            if self.con != None:
                self.con.close()
            return False
                
    def dbClose(self):
         #カーソルがオープン状態ならClose
         if self.cur != None:
            self.cur.close()
         #DBがオープン状態ならClose
         if self.con != None:
            self.con.close()
            
    def dbCommit(self):
        #DBがオープン状態ならCommit処理を実行
        if self.con != None:
            self.con.commit()
       
                
    def execSelect(self, selectString, values=None):
        try:
            self.cur.execute(selectString, values)
            rows = self.cur.fetchall()
            return rows
        except Exception as e:
            print(e)
            return None
            
    def execMerge(self, insertString, values):
        try:
            self.cur.execute(insertString, values)
            return True
        except Exception as e:
            print(e)
            return False

    def execInsert(self, insertString, values):
        try:
            self.cur.execute(insertString, values)
            return True
        except Exception as e:
            print(e)
            return False


class kabudb(dbAccess) :
    
    def __init__(self, con_file) :
        self.segment_obj = None
        
        # 基底クラスの初期化メソッド呼び出し
        super().__init__(con_file)
    
    def insStockinfo(self, stockDataList):
        """KABUDB Stockデータをimportする

        Args:
            stockDataList (__list__): Stock情報のリスト
            [<RDATE>,<CODE>,<OPEN>,<High>,<LOW>,<CLOSE>,<VALUME>]
        """
        if len(stockDataList) != 7:
            return False
        
        sql_string = 'insert into STOCKS '\
            '( CODE,RDATE,O_PRICE,H_PRICE,L_PRICE,E_PRICE,VOLUME ) ' \
            'VALUES '\
            "( :code, to_date(:rdate, 'YYYY-MM-DD'), :open, :high, :low, :close, :volume )"
        bind_variables = {"code":stockDataList[1], "rdate":stockDataList[0], "open":stockDataList[2], "high":stockDataList[3], "low":stockDataList[4],"close":stockDataList[5], "volume":stockDataList[6]}
        return self.execInsert(sql_string, bind_variables)

    
    
    def insBrandName(self, datalist):
        """KABUDB BRAND_NAMEへのデータ登録関数
        
        Parameters
        ----------
        datalist : list
            BRAND_NAMEテーブルのデータをリスト形式で設定
            [<code>, <name>, <market_abb>, <segment_id>, <industory_id>, <industory17_id>, <valid>, <rdate>]
        """
#        tstr = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        tstr = dt.datetime.now().strftime('%Y-%m-%d')
        
        sql_string = 'merge into brand_name b '\
        "using (select :code as code from dual) t "\
        "on (b.code = t.code) " \
        "when matched then " \
        "update set b.name = :name, " \
        "b.market_abb = :market_abb, " \
        "b.segment_id = :segment_id, " \
        "b.industory_id = :industory_id, " \
        "b.industory17_id = :industory17_id, " \
        "b.valid = :valid, " \
        "b.rdate = :rdate, " \
        "b.udate = :udate"
        "when not matched then " \
        "insert (b.code, b.name, b.market_abb, b.segment_id, b.industory_id, b.industory17_id, b.valid, b.rdate) " \
        "values (:code, :name, :market_abb, :segment_id, :industory_id, :industory17_id, :valid, :rdate)"
        bind_variables = {"code":datalist[0], "name":datalist[1], "market_abb":datalist[2], "segment_id":datalist[3], "industory_id":datalist[4], "industory17_id":datalist[5], "valid":datalist[6], "rdate":datalist[7], "udate":tstr}
        return self.execMerge(sql_string, bind_variables)
    
        
    def getSegment_id(self, segname, renew = False):
        """Segment_idを取得する
        
        Parameters
        ----------
        segname : string
            区分名をインプットに、segmentテーブルにあるsegment_idを取得する
        return : int
            Segment_idを返却する
        """
        # Segmentデータ未取得の場合、再取得
        if self.segment_obj == None or renew == True :
            sql_string = "select segment_id, name from segment"
            self.segment_obj = self.execSelect(sql_string)
            
        for row in self.segment_obj:
            if row[1] == segname :
                return row[0]
            
        return 0
     
    def getCodeList(self) :
        """日本株式のコード一覧を取得する（東証）
         
        Parameters
         ---------
        None
        return : list
            東証の証券コード一覧（List形式）で返却する
            [<code>, <industory_id>, <industory17_id>, <valid>]
        """
        if self.cur != None :
            dataList = []
            sql_string = "select code, industory_id, industory17_id, valid from brand_name order by code"
            rows = self.execSelect(sql_string)
            if rows != None:
                for row in rows :
                    dataList.append([row[0],row[1],row[2],row[3]])
            
            return dataList
            
    def getLastDate(self, code):
        """最終Stock取得日を取得
        
        Parameters
        -----------
        code : integer
            コードを指定
        return : date
            最終取得日（データが存在しない場合、Noneを返却）
        """
        if self.cur == None:
            return None
        
        dataList = []
        sql_string = "select max(code), max(rdate) from stocks where code = :code"
        bind_valiable = {"code": code}
        
        rows = self.execSelect(sql_string, bind_valiable)
        if rows != None:
            for row in rows :
                dataList.append([row[0],row[1]])
        
        return dataList
    
    def getStockData(self, code, start=None, end=None):
        """指定期間の株価情報を取得

        Args:
            code (_type_): _description_
            start (_type_): _description_
            end (_type_): _description_
        """
        if self.cur == None:
            return None
        
        dataList = []
        sql_string = 'select rdate, o_price, h_price, l_price, e_price, volume from stocks where code = :code'
        bind_valiable = {"code": code}
        if start != None:
            sql_string = sql_string + " and rdate > :lstart" 
            bind_valiable.update(lstart=start)
        if end != None:
            sql_string = sql_string + " and rdate < :lend"
            bind_valiable.update(lend=end)
        #sql_string = sql_string + ";"
 
        rows = self.execSelect(sql_string, bind_valiable)
        if rows != None:
            for row in rows :
                tmp = []
#                tmp.append(row[0].strftime('%Y-%m-%d'))
                tmp.append(row[0])
                tmp.append(row[1])
                tmp.append(row[2])
                tmp.append(row[3])
                tmp.append(row[4])
                tmp.append(row[5])
                dataList.append(tmp)
        
        return dataList

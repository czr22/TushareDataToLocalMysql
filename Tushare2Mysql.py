import pymysql
import sys
import tushare as ts
import time
import pandas as pd
assert pymysql.__version__ , "请使用pip install pymysql 下载 pymysql"

class Mysql:
    def __init__(self,host='localhost',user='root',password='',charset="utf-8",db=''):
        assert password != '','请输入数据库密码'
        assert db != '', '请输入数据库名称'
        self.host=host
        self.user = user
        self.pwd = password
        self.charset = charset
        self.db = db
    #连接数据库并获取操作游标
    def connect(self):
        print('正在连接数据库......')
        try:
            self.conn = pymysql.connect(host=self.host,user=self.user,passwd=self.pwd,db=self.db)
            self.cursor = self.conn.cursor()
            print("数据库连接成功......")
            print('='*100)
        except :
            print("数据库连接失败")
            sys.exit(0)
    def createTable(self,tName='',sql=''):
        assert tName != '','请输入数据表名称'
        assert sql != '请输入SQL语句'
        self.cursor.execute('SHOW TABLES;')
        tables =[table[0] for table in self.cursor.fetchall()]
        while tName in tables:
            tName = input(f'数据表名称重复，请重新输入名称。\n已有的数据表:{tables}\n:')
        try:
            self.cursor.execute(sql)
            print(f'数据表{tName}建立成功')
        except Exception as e:
            print(f'数据表{tName}建立失败,请检查SQL语句')
            print(e)
            self.close()
            sys.exit(0)
    def insertValues(self,sql='',values=[]):
        assert values!=[],'请输入数据'
        try:
            self.cursor.executemany(sql,values)
            self.conn.commit()
            print('数据插入成功')
        except Exception as e:
            print("数据插入失败")
            print(e)
            self.close()
            sys.exit(0)
    def fetchValues(self,tName,sql=''):
        self.cursor.execute(f'select COLUMN_NAME from information_schema.COLUMNS where table_name = "{tName}" and table_schema = "{self.db}";')
        columns = [col[0] for col in self.cursor.fetchall()]
        self.cursor.execute(sql)
        df = pd.DataFrame(self.cursor.fetchall())
        df.columns =columns
        return df
    def close(self):
        self.cursor.close()
        self.conn.close()
    # 进入with语句自动执行
    def __enter__(self):
        self.connect()
        return self
    # 退出with语句自动执行
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


#生成插入数据SQL语句
def insertValueSQL(tName,df):
    columns = df.columns
    return f'insert into {tName}({",".join(columns.values.tolist())}) values({",".join("%s" for _ in range(len(columns)))})'

if __name__ == "__main__":
    # 连接至tushare
    token = 'e8df84bd1b25a8a2a2ceb7edf7ad41f2c3a1d3ec604bb8abd40321f4'
    ts.set_token(token)
    pro = ts.pro_api()
    with Mysql(password='12345678',db='tushare') as mysql:
        #获取mysql版本号
        mysql.cursor.execute("SELECT VERSION()")
        version = mysql.cursor.fetchone()[0]
        print(f'Database version :{version}')
        stockList = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_status,is_hs')
        """
        创建股票列表数据表
        createTableSql = f'CREATE TABLE STOCKLIST({" VARCHAR(30),".join(stockList.columns.values.tolist())} VARCHAR(30))ENGINE=innoDB DEFAULT CHARSET=utf8;'
        print(createTableSql)
        mysql.createTable(tName='stockList',sql=createTableSql)
        """
        insertValuesSql = insertValueSQL(tName='STOCKLIST',df=stockList)
        mysql.insertValues(sql=insertValuesSql,values=[tuple(val) for val in stockList.values.tolist()])


        df = pro.daily(ts_code='000001.SZ',fields='ts_code,trade_date,open,high,low,close,pct_chg,vol')
        columns = df.columns.values.tolist()
        """
        创建日线行情数据表
        sql = f'CREATE TABLE DAILY (\n'
        columnlen = len(columns)
        for idx,col in enumerate(columns):
            if 'code' in col :
                sql += col+ ' VARCHAR(30) NOT NULL,'
            elif 'date' in col:
                sql += col+ ' DATE,'
            else:
                sql += col+ ' FLOAT,' if idx != columnlen-1 else col +' FLOAT'
        sql += ')ENGINE=innoDB DEFAULT CHARSET=utf8;'
        mysql.createTable(tName='DAILY',sql=sql)
        """
        # 插入数值
        data = [tuple(val) for val in df.values.tolist()]
        sql = f'insert into DAILY({",".join(columns)}) values({",".join(["%s" for _ in range(len(columns))])});'
        mysql.interValues(sql, data)
        data = mysql.fetchValues(tName="DAILY",sql = "select * from DAILY limit 100;")
        print(data)



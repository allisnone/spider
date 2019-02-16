# -*- coding: utf-8 -*-
#__Author__= allisnone #2019-02-16
#https://docs.python.org/3/library/sqlite3.html
import sqlite3
import os

class Mysqlite():
    def __init__(self,db_path):
        self.connect = None
        self.cursor = None
        self.create_sqlite(db_path)
        
    def create_sqlite(self, path):
        conn = None
        if os.path.exists(path) and os.path.isfile(path):
            print('硬盘上面:[{}]'.format(path))
            self.connect =  sqlite3.connect(path)
        else:
            print('内存上面:[:memory:]')
            self.connect = sqlite3.connect(':memory:')
        self.cursor = self.connect.cursor()
        
    def close(self):
        self.connect.close()
        self.cursor.close()
        
    def form_sql(self,table_name,oper_type='query',columns=(),select_field=None,where_condition=None,insert_field=None,update_field=None,update_value=None):
        """
        :param table_name: string type, db_name.table_name
        :param select_field: string type, like 'id,type,value'
        :param where_condition: string type, like 'field_value>50'
        :param insert_field: string type, like '(date_time,measurement_id,value)'
        :param update_field: string type, like 'value' or  '(measurement_id,value)'
        :param update_value: value or string type, like '1000' or "'normal_type'"
        :return: sql string
        :use example:
        :query: sql_q=form_sql(table_name='stock.account',oper_type='query',select_field='acc_name,initial',where_condition="acc_name='36005'")
        :insert: sql_insert=form_sql(table_name='stock.account',oper_type='insert',insert_field='(acc_name,initial,comm)')
        :update: sql_update=form_sql(table_name='stock.account',oper_type='update',update_field='initial',where_condition='initial=2900019000',set_value_str='29000')
        :delete: sql_delete=form_sql(table_name='stock.account',oper_type='delete',where_condition="initial=14200.0")
        """
        sql=''
        if table_name=='' or not table_name:
            return sql
        if oper_type=='create':
            #con.execute("create table person (id integer primary key, firstname varchar unique)")
            if isinstance(columns, str):
                sql = 'create table %s (%s) ' % (table_name,columns)
            elif isinstance(columns, tuple):
                sql = 'create table %s %s ' % (table_name,columns)
            else:
                pass
        elif oper_type=='query':
            field='*'
            if select_field:
                field=select_field
            condition=''
            if where_condition:
                condition=' where %s' % where_condition
            sql='select %s from %s'%(field,table_name) + condition +';'
        elif oper_type=='insert' and insert_field:
            num=len(insert_field.split(','))
            value_tail='%s,'*num
            value_tail='('+value_tail[:-1]+')'
            sql='insert into %s '% table_name +insert_field +' values'+ value_tail + ';'
        elif oper_type=='update' and where_condition and update_field:
            """
            update_value_str=str(update_value)
            if isinstance(update_value, str):
                update_value_str="'%s'"%update_value
            """
            sql='update %s set %s='%(table_name,update_field)+ update_value + ' where '+  where_condition + ';'
            """
            sql=''
            num=len(update_field.split(','))
            if num==1:
                sql='update %s set %s='%(table_name,update_field)+ update_value + ' where '+  where_condition + ';'
            elif num>1:
                value_tail='%s,'*num
                value_tail='('+value_tail[:-1]+')'
                update_sql="update test set " + update_field +value_tail + ':'
            else:
                pass
            """
        elif oper_type=='delete':
            condition=''
            if where_condition:
                condition=' where %s' % where_condition
            sql='delete from %s'%table_name + condition + ';'
        else:
            pass
        # print('%s_sql=%s'%(oper_type,sql))
        return sql
    
    def create_table(self,table_sql,parama=''):
        con.execute("create table person (id integer primary key, firstname varchar unique)")
        cursor.execute(sql,{'st_name':name, 'st_username':username, 'id_num':id_num})
        columns = ('id integer primary key', 'firstname varchar unique')
        sql = self.form_sql('test1', oper_type, columns, select_field, where_condition, insert_field, update_field, update_value)
        conn.commit()
        return  
    
    def get_tables(self,sql,condition=''):  
        sql = "select rowid,  username from students"
        # 执行语句
        results = self.cursor.execute(sql)
        # 遍历打印输出
        datas = results.fetchall()
        self.cursor.close()
        return  datas
    
    def drop_table(self):
        return
    
    def insert(self):
        sql = "select rowid,  username from students"
        cursor.execute(sql,{'st_name':name, 'st_username':username, 'id_num':id_num})
        conn.commit()
        return
    
    def update(self):
        return
    
    def delete(self):
        return
    
    

#python sqlite

#Author : Hongten
#MailTo : hongtenzone@foxmail.com
#QQ     : 648719819
#Blog   : http://www.cnblogs.com/hongten
#Create : 2013-08-09
#Version: 1.0

#DB-API 2.0 interface for SQLite databases

import sqlite3
import os
'''SQLite数据库是一款非常小巧的嵌入式开源数据库软件，也就是说
没有独立的维护进程，所有的维护都来自于程序本身。
在python中，使用sqlite3创建数据库的连接，当我们指定的数据库文件不存在的时候
连接对象会自动创建数据库文件；如果数据库文件已经存在，则连接对象不会再创建
数据库文件，而是直接打开该数据库文件。
    连接对象可以是硬盘上面的数据库文件，也可以是建立在内存中的，在内存中的数据库
    执行完任何操作后，都不需要提交事务的(commit)

    创建在硬盘上面： conn = sqlite3.connect('c:\\test\\test.db')
    创建在内存上面： conn = sqlite3.connect('"memory:')

    下面我们一硬盘上面创建数据库文件为例来具体说明：
    conn = sqlite3.connect('c:\\test\\hongten.db')
    其中conn对象是数据库链接对象，而对于数据库链接对象来说，具有以下操作：

        commit()            --事务提交
        rollback()          --事务回滚
        close()             --关闭一个数据库链接
        cursor()            --创建一个游标

    cu = conn.cursor()
    这样我们就创建了一个游标对象：cu
    在sqlite3中，所有sql语句的执行都要在游标对象的参与下完成
    对于游标对象cu，具有以下具体操作：

        execute()           --执行一条sql语句
        executemany()       --执行多条sql语句
        close()             --游标关闭
        fetchone()          --从结果中取出一条记录
        fetchmany()         --从结果中取出多条记录
        fetchall()          --从结果中取出所有记录
        scroll()            --游标滚动

'''

#global var
#数据库文件绝句路径
DB_FILE_PATH = ''
#表名称
TABLE_NAME = ''
#是否打印sql
SHOW_SQL = True

def get_conn(path):
    '''获取到数据库的连接对象，参数为数据库文件的绝对路径
    如果传递的参数是存在，并且是文件，那么就返回硬盘上面改
    路径下的数据库文件的连接对象；否则，返回内存中的数据接
    连接对象'''
    
    if os.path.exists(path) and os.path.isfile(path):
        print('硬盘上面:[{}]'.format(path))
        return sqlite3.connect(path)
    else:
        conn = None
        print('内存上面:[:memory:]')
        return sqlite3.connect(':memory:')

def get_cursor(conn):
    '''该方法是获取数据库的游标对象，参数为数据库的连接对象
    如果数据库的连接对象不为None，则返回数据库连接对象所创
    建的游标对象；否则返回一个游标对象，该对象是内存中数据
    库连接对象所创建的游标对象'''
    if conn is not None:
        return conn.cursor()
    else:
        return get_conn('').cursor()

###############################################################
####            创建|删除表操作     START
###############################################################
def drop_table(conn, table):
    '''如果表存在,则删除表，如果表中存在数据的时候，使用该
    方法的时候要慎用！'''
    if table is not None and table != '':
        sql = 'DROP TABLE IF EXISTS ' + table
        if SHOW_SQL:
            print('执行sql:[{}]'.format(sql))
        cu = get_cursor(conn)
        cu.execute(sql)
        conn.commit()
        print('删除数据库表[{}]成功!'.format(table))
        close_all(conn, cu)
    else:
        print('the [{}] is empty or equal None!'.format(sql))

def create_table(conn, sql):
    '''创建数据库表：student'''
    if sql is not None and sql != '':
        cu = get_cursor(conn)
        if SHOW_SQL:
            print('执行sql:[{}]'.format(sql))
        cu.execute(sql)
        conn.commit()
        print('创建数据库表[student]成功!')
        close_all(conn, cu)
    else:
        print('the [{}] is empty or equal None!'.format(sql))

###############################################################
####            创建|删除表操作     END
###############################################################

def close_all(conn, cu):
    '''关闭数据库游标对象和数据库连接对象'''
    try:
        if cu is not None:
            cu.close()
    finally:
        if cu is not None:
            cu.close()

###############################################################
####            数据库操作CRUD     START
###############################################################

def save(conn, sql, data):
    '''插入数据'''
    if sql is not None and sql != '':
        if data is not None:
            cu = get_cursor(conn)
            for d in data:
                if SHOW_SQL:
                    print('执行sql:[{}],参数:[{}]'.format(sql, d))
                cu.execute(sql, d)
                conn.commit()
            close_all(conn, cu)
    else:
        print('the [{}] is empty or equal None!'.format(sql))

def fetchall(conn, sql):
    '''查询所有数据'''
    if sql is not None and sql != '':
        cu = get_cursor(conn)
        if SHOW_SQL:
            print('执行sql:[{}]'.format(sql))
        cu.execute(sql)
        r = cu.fetchall()
        if len(r) > 0:
            for e in range(len(r)):
                print(r[e])
    else:
        print('the [{}] is empty or equal None!'.format(sql)) 

def fetchone(conn, sql, data):
    '''查询一条数据'''
    if sql is not None and sql != '':
        if data is not None:
            #Do this instead
            d = (data,) 
            cu = get_cursor(conn)
            if SHOW_SQL:
                print('执行sql:[{}],参数:[{}]'.format(sql, data))
            cu.execute(sql, d)
            r = cu.fetchall()
            if len(r) > 0:
                for e in range(len(r)):
                    print(r[e])
        else:
            print('the [{}] equal None!'.format(data))
    else:
        print('the [{}] is empty or equal None!'.format(sql))

def update(conn, sql, data):
    '''更新数据'''
    if sql is not None and sql != '':
        if data is not None:
            cu = get_cursor(conn)
            for d in data:
                if SHOW_SQL:
                    print('执行sql:[{}],参数:[{}]'.format(sql, d))
                cu.execute(sql, d)
                conn.commit()
            close_all(conn, cu)
    else:
        print('the [{}] is empty or equal None!'.format(sql))

def delete(conn, sql, data):
    '''删除数据'''
    if sql is not None and sql != '':
        if data is not None:
            cu = get_cursor(conn)
            for d in data:
                if SHOW_SQL:
                    print('执行sql:[{}],参数:[{}]'.format(sql, d))
                cu.execute(sql, d)
                conn.commit()
            close_all(conn, cu)
    else:
        print('the [{}] is empty or equal None!'.format(sql))
###############################################################
####            数据库操作CRUD     END
###############################################################


###############################################################
####            测试操作     START
###############################################################
def drop_table_test():
    '''删除数据库表测试'''
    print('删除数据库表测试...')
    conn = get_conn(DB_FILE_PATH)
    drop_table(conn, TABLE_NAME)

def create_table_test():
    '''创建数据库表测试'''
    print('创建数据库表测试...')
    create_table_sql = '''CREATE TABLE `student` (
                          `id` int(11) NOT NULL,
                          `name` varchar(20) NOT NULL,
                          `gender` varchar(4) DEFAULT NULL,
                          `age` int(11) DEFAULT NULL,
                          `address` varchar(200) DEFAULT NULL,
                          `phone` varchar(20) DEFAULT NULL,
                           PRIMARY KEY (`id`)
                        )'''
    conn = get_conn(DB_FILE_PATH)
    create_table(conn, create_table_sql)

def save_test():
    '''保存数据测试...'''
    print('保存数据测试...')
    save_sql = '''INSERT INTO student values (?, ?, ?, ?, ?, ?)'''
    data = [(1, 'Hongten', '男', 20, '广东省广州市', '13423****62'),
            (2, 'Tom', '男', 22, '美国旧金山', '15423****63'),
            (3, 'Jake', '女', 18, '广东省广州市', '18823****87'),
            (4, 'Cate', '女', 21, '广东省广州市', '14323****32')]
    conn = get_conn(DB_FILE_PATH)
    save(conn, save_sql, data)

def fetchall_test():
    '''查询所有数据...'''
    print('查询所有数据...')
    fetchall_sql = '''SELECT * FROM student'''
    conn = get_conn(DB_FILE_PATH)
    fetchall(conn, fetchall_sql)

def fetchone_test():
    '''查询一条数据...'''
    print('查询一条数据...')
    fetchone_sql = 'SELECT * FROM student WHERE ID = ? '
    data = 1
    conn = get_conn(DB_FILE_PATH)
    fetchone(conn, fetchone_sql, data)

def update_test():
    '''更新数据...'''
    print('更新数据...')
    update_sql = 'UPDATE student SET name = ? WHERE ID = ? '
    data = [('HongtenAA', 1),
            ('HongtenBB', 2),
            ('HongtenCC', 3),
            ('HongtenDD', 4)]
    conn = get_conn(DB_FILE_PATH)
    update(conn, update_sql, data)

def delete_test():
    '''删除数据...'''
    print('删除数据...')
    delete_sql = 'DELETE FROM student WHERE NAME = ? AND ID = ? '
    data = [('HongtenAA', 1),
            ('HongtenCC', 3)]
    conn = get_conn(DB_FILE_PATH)
    delete(conn, delete_sql, data)

###############################################################
####            测试操作     END
###############################################################

def init():
    '''初始化方法'''
    #数据库文件绝句路径
    global DB_FILE_PATH
    DB_FILE_PATH = 'c:\\test\\hongten.db'
    #数据库表名称
    global TABLE_NAME
    TABLE_NAME = 'student'
    #是否打印sql
    global SHOW_SQL
    SHOW_SQL = True
    print('show_sql : {}'.format(SHOW_SQL))
    #如果存在数据库表，则删除表
    drop_table_test()
    #创建数据库表student
    create_table_test()
    #向数据库表中插入数据
    save_test()
    

def main():
    init()
    fetchall_test()
    print('#' * 50)
    fetchone_test()
    print('#' * 50)
    update_test()
    fetchall_test()
    print('#' * 50)
    delete_test()
    fetchall_test()

if __name__ == '__main__':
    main()
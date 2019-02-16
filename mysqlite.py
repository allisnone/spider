# -*- coding: utf-8 -*-
#__Author__= allisnone #2019-02-16
#https://docs.python.org/3/library/sqlite3.html
import sqlite3
import os

class Mysqlite():
    def __init__(self,db_path,memory=False):
        self.connect = None
        self.cursor = None
        self.create_sqlite(db_path,memory)
        
    def create_sqlite(self, path,memory=False):
        conn = None
        if not memory:
            print('硬盘上面:[{}]'.format(path))
            self.connect =  sqlite3.connect(path)
        else:
            print('内存上面:[:memory:]')
            self.connect = sqlite3.connect(':memory:')
        self.cursor = self.connect.cursor()
        
    def close(self):
        self.cursor.close()
        self.connect.close()
        
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
    
    def create_table(self,table,columns=[]):
        #con.execute("create table person (id integer primary key, firstname varchar unique)")
        #cursor.execute(sql,{'st_name':name, 'st_username':username, 'id_num':id_num})
        #columns = ('id integer primary key', 'firstname varchar unique')
        #sql = self.form_sql(table, oper_type='create', columns=columns)
        #print('sql=',sql)
        sql = "create table %s (%s);" % (table,self._join_str(columns))
        try:
            self.cursor.execute(sql)
            self.connect.commit()
        except Exception as e:
            print('create_table Exception: ',e)
        return  
    
    def _join_str(self,fields, default='',specify=','):
        """
        把list或者tuple转化为str, 以逗号或者其他特殊字符连接
        """
        re = default
        if fields:
            if isinstance(fields,str):
                re = fields
            elif isinstance(fields, list) or isinstance(fields, tuple):
                re = specify.join(fields)
            else:
                pass
        return re
    
    def get_table(self,table, fields='',where={'firsname':'b'}):  
        #where_str = self._join_str(fields, default='',specify=',')
        
        sql = "select %s from %s" % (self._join_str(fields,'*'),table)
        a = ';'
        if where:
            a = ' '.join(['%s=%s'%(k,v) for k,v in where.items()]) + ';'
        datas = []
        try: 
            results = self.cursor.execute(sql+a)# 遍历打印输出
            datas = results.fetchall()
        except Exception as e:
            print('get_table Exception: ',e)
        #self.cursor.close()
        return  datas
    
    def drop_table(self,table):
        sql = 'DROP TABLE IF EXISTS ' + table
        try:
            self.cursor.execute(sql)
            self.connect.commit()
        except Exception as e:
            print('drop_table Exception: ',e)
        return
    
    def insert(self,table,datas):
        """
        table: str, table name
        datas: list, each element will be tuple type
        """
        if isinstance(datas,list) or isinstance(datas,tuple):
            pass
        else:
            return 
        col_count = len(datas[0])
        col_str = ','.join(['?']*col_count)
        sql = 'INSERT INTO %s VALUES (%s)' % (table, col_str)
        print('sql=',sql)
        try:
            self.cursor.executemany(sql,datas)
            self.connect.commit()
        except Exception as e:
            print('insert Exception: ',e)
        #cursor.execute(sql,{'st_name':name, 'st_username':username, 'id_num':id_num})
        return
    
    def update(self,table,field,where=[], datas=[]):
        where_str = self._join_str(where, default='',specify='=? ') + '=?;'
        update_sql = 'UPDATE %s SET %s=? ' % (table,field) + ' where ' +  where_str
        print('update_sql=',update_sql)
        try:
            self.cursor.executemany(update_sql,datas)
            self.connect.commit()
        except Exception as e:
            print('update Exception: ',e)
        return
    
    def delete(self,table,where=[], datas=[]):
        #delete_sql = 'DELETE FROM student WHERE NAME = ? AND ID = ? '
        where_str = self._join_str(where, default='',specify='=? ') + '=?;'
        delete_sql = 'DELETE FROM %s' % table + ' where ' +  where_str
        try:
            if not where:
                self.cursor.executemany(delete_sql,datas)
            else:
                self.cursor.executemany('DELETE FROM %s' % table)
            self.connect.commit()
        except Exception as e:
            print('delete Exception: ',e)
        return
    
    def execute_script(self,sql):
        sql = """
        create table person(
            firstname,
            lastname,
            age
        );
    
        create table book(
            title,
            author,
            published
        );
        CREATE TABLE `student`(
            `id` int(11) NOT NULL,
            `name` varchar(20) NOT NULL,
            `gender` varchar(4) DEFAULT NULL,
            `age` int(11) DEFAULT NULL,
            `address` varchar(200) DEFAULT NULL,
            `phone` varchar(20) DEFAULT NULL,
            PRIMARY KEY (`id`)
        );
        insert into book(title, author, published)
        values (
            'Dirk Gently''s Holistic Detective Agency',
            'Douglas Adams',
            1987
        );
        """
        self.cursor.executescript(sql)
        self.connect.commit()
        return
    
    def backup(self,new='backup.db'):
        def progress(status, remaining, total):
            print('Copied {total-remaining} of {total} pages...')
            return
        with sqlite3.connect(new) as bck:
            self.connect.backup(bck, pages=1, progress=progress)
        return
    
if __name__ == '__main__':
    sql3 = Mysqlite(db_path='test.db',memory=False)
    columns = ('id integer primary key', 'firstname varchar unique')
    sql3.create_table('table2', columns)
    datas1 = [(1,'a'),(2,'b'),(3,'c')]
    datas2 = [(1,'e'),(2,'f'),(3,'g')]
    sql3.insert('table2', datas1)
    table1_data = sql3.get_table(table='table2', fields='', where='')
    print('table1_data=',table1_data)
    sql3.insert('person', datas2)
    table2_data = sql3.get_table(table='person', fields='', where='')
    print('table2_data=',table2_data)
    sql3.update(table='table2',field='firstname',where=['id'], datas=[('h',2)])
    table2_data1 = sql3.get_table(table='table2', fields='', where='')
    print('table2_data1=',table2_data1)
    sql3.connect.commit()
    sql3.close()
    
import pymysql
"""
用于千万级别的单表，思路：将两个连接分别拆开，避免数据库等待时间过长而中断连接。

需要注意的是，因为这里用的是自适应字段，需要提前将空白的那个表的字段id主键先去掉，迁移完再加上（这样也保证解决了存在其他表使用外键的情况）
"""
def read(table, num_1, num_2, cursor1):
    sql = 'SELECT * FROM {0} LIMIT {1}, {2} '.format(table, num_1, num_2)
    sql_column = "SELECT column_name FROM information_schema.`COLUMNS` WHERE TABLE_NAME ='{0}'".format(table)
    cursor1.execute(sql_column)
    column = cursor1.fetchall()
    cursor1.execute(sql)
    res = cursor1.fetchall()
    return res, column

def insert(table, res, column, cursor2):
    sql = 'INSERT INTO ' + table + ' VALUES(%s'
    for i in range((int)(len(column)) - 1):
        sql += ',%s'
    sql += ')'
    print(sql)
    for re in res:
        print(re[0])
        cursor2.execute(sql, re)

if __name__ == '__main__':
    for num in range(0, 200000000, 50000):		# 根据数据库的量提前调整
        connection1 = pymysql.connect(host='',
                                      user='',
                                      password='',
                                      db='',
                                      port=3306,
                                      charset='utf8')  # 注意是utf8不是utf-8



        # # 获取游标
        cursor1 = connection1.cursor()
        print('开始迁移')
        res, column = read('TableName', num, num + 50000, cursor1)
        if not res:			# 设置中断环节
            break
        connection2 = pymysql.connect(host='',
                                      user='',
                                      password='',
                                      db='',
                                      port=3306,
                                      charset='')  # 注意是utf8不是utf-8
        cursor2 = connection2.cursor()
        sql = 'SET innodb_lock_wait_timeout=1000;'
        cursor2.execute(sql)
        insert('TableName', res, column, cursor2)
        connection2.commit()
        print('部分数据已存储')
        connection1.close()
        connection2.close()
    print('存储完毕')
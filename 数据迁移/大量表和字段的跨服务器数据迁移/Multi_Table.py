import pymysql
"""
用于多个表的迁移，只需要将表名放入列表即可。

需要注意的是，因为这里用的是自适应字段，需要提前将空白的那个表的字段id主键先去掉，迁移完再加上（这样也保证解决了存在其他表使用外键的情况）
"""
# 有数据的地方
connection1 = pymysql.connect(host='',
                             user='',
                             password='',
                             db='',
                             port=3306,
                             charset='utf8')  # 注意是utf8不是utf-8
# 没数据的地方
connection2 = pymysql.connect(host='',
                             user='',
                             password='',
                             db='',
                             port=3306,
                             charset='utf8')  # 注意是utf8不是utf-8


# # 获取游标
cursor1 = connection1.cursor()
cursor2 = connection2.cursor()

list_table = []	# 这里填迁移表的名


def read(table):
    sql = 'SELECT * FROM {0}'.format(table)
    sql_column = "SELECT column_name FROM information_schema.`COLUMNS` WHERE TABLE_NAME ='{0}'".format(table)
    cursor1.execute(sql_column)
    column = cursor1.fetchall()
    cursor1.execute(sql)
    res = cursor1.fetchall()
    return res, column

def insert(table, res, column):
    sql = 'INSERT INTO ' + table + ' VALUES(%s'
    for i in range((int)(len(column)) - 1):
        sql += ',%s'
    sql += ')'
    print(sql)
    for re in res:
        print(re[0])
        cursor2.execute(sql, re)

if __name__ == '__main__':
    try:
        print('开始迁移数据')
        for table in list_table:
            try:
                res, column = read(table)
                insert(table, res, column)
                connection2.commit()
            except Exception as e:
                connection2.rollback()
                print('异常：', e)
        print('迁移结束')
    finally:
        cursor1.close()
        connection1.close()
        cursor2.close()
        connection2.close()
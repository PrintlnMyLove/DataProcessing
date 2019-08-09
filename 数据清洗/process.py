
import pymysql


def trans_data():
    """根据限制条件转移筛选后的数据到其他数据库   DataA => DataB"""

    table = 'fd_samples'
    where_ = "sp_s_70='抽检监测（转移地方）' OR sp_s_70='抽检监测（本级三司）'"
    step = 10000
    for num in range(0, 200000, step):
        # 配置连接1  有数据的地方
        connection1 = pymysql.connect(
            host='47.95.237.70',
            user='root',
            password='zqc1982',
            db='risk_data',
            port=3306,
            charset='utf8')  # 注意是utf8不是utf-8

        # 查询语句(按情况修改)
        sql_select = "SELECT * FROM {0} WHERE {1} LIMIT {2}, {3}".format(table, where_, num, step)
        sql_column = "SELECT column_name FROM information_schema.`COLUMNS` WHERE TABLE_NAME ='{0}'".format(table)

        # 创建游标
        cursor1 = connection1.cursor()

        # 执行语句,获取数据及数据的字段
        cursor1.execute(sql_column)
        column = cursor1.fetchall()
        cursor1.execute(sql_select)
        res = cursor1.fetchall()

        if not res: # 添加中断
            break
        # 字段名动态获取
        sql_insert = 'INSERT INTO ' + table + ' VALUES(%s'
        for i in range((int)(len(column)/2) - 1):
            sql_insert += ',%s'
        sql_insert += ')'

        # 配置连接2 配置没数据的地方
        connection2 = pymysql.connect(
            host='47.95.237.70',
            user='root',
            password='zqc1982',
            db='process_risk_data',
            port=3306,
            charset='utf8')  # 注意是utf8不是utf-8

        cursor2 = connection2.cursor()

        # 遍历执行插入语句
        print('总共有：', len(res), '条数据')
        for re in res:
            print(re[0])
            cursor2.execute(sql_insert, re)

        # 提交插入数据
        connection2.commit()

        connection1.close()
        connection2.close()

    print('数据筛选迁移完毕！')

def select_data():
    """一对多数据迁移（一条数据查多条进行迁移）"""

    #
    table_1 = 'fd_samples'  # 索引表
    table_2 = 'xr_gc_test_info' # 需要迁移的

    sql_column = "SELECT column_name FROM information_schema.`COLUMNS` WHERE TABLE_NAME ='{0}'".format(table_2)

    # 配置连接2 配置没数据的地方
    connection2 = pymysql.connect(
        host='47.95.237.70',
        user='root',
        password='zqc1982',
        db='process_risk_data',
        port=3306,
        charset='utf8')  # 注意是utf8不是utf-8
    # 获取数据索引表
    cursor2 = connection2.cursor()
    sql_select_index = "SELECT * FROM {0}".format(table_1)
    cursor2.execute(sql_select_index)
    dataIndex = cursor2.fetchall()

    # 设置插入语句
    cursor2.execute(sql_column)
    column = cursor2.fetchall()
    # 字段名动态获取
    sql_insert = 'INSERT INTO ' + table_2 + ' VALUES(%s'
    for i in range((int)(len(column) / 2) - 1):
        sql_insert += ',%s'
    sql_insert += ')'
    # print(sql_insert, column)
    # 配置连接1  有数据的地方
    connection1 = pymysql.connect(
        host='47.95.237.70',
        user='root',
        password='zqc1982',
        db='risk_data',
        port=3306,
        charset='utf8')  # 注意是utf8不是utf-8
    cursor1 = connection1.cursor()

    i = 0
    # 循环连接查询插入
    for idx, dataIdx in enumerate(dataIndex):
        # 查询语句
        sql_select = "SELECT * FROM {0} WHERE sp_bsb_id={1} ".format(table_2, dataIdx[0])

        # 执行筛选查询
        cursor1.execute(sql_select)
        res = cursor1.fetchall()

        # 插入
        for re in res:
            i += 1
            print(idx, re[0], i)
            cursor2.execute(sql_insert, re)
            connection2.commit()

    print('全部迁移完毕')
    # 关闭连接
    connection1.close()
    connection2.close()

if __name__ == '__main__':
    # trans_data()
    select_data()


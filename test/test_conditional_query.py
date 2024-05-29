import os
import time
import shutil
import subprocess
import random

import pytest


class TestConditionalQuery:
    DB = "TestConditionalQueryDB"
    SERVER = "./rmdb"
    CLIENT = "./rmdb_client"
    TABLENAME = "TestConditionalQueryTable"
    _TABLENAME = "TestConditionalQueryTable"

    @classmethod
    def setup_class(cls):
        DEBUG = False
        if cls.DB in os.listdir():  # 删掉残留的数据库
            shutil.rmtree(cls.DB)

        if DEBUG:
            # 假服务器，调试用
            cls.server = subprocess.Popen(['ls'])
            time.sleep(8)  # 请在这8s内用gdb启动服务器开始调试
        else:
            cls.server = subprocess.Popen([cls.SERVER, cls.DB])  # 启动服务器
            time.sleep(3)  # 等待服务器启动完毕

    @classmethod
    def teardown_class(cls):
        cls.server.kill()

    @classmethod
    def setup_method(cls):  # 每次测试都启动一个新的客户端
        cls.client = subprocess.Popen([cls.CLIENT], stdin=subprocess.PIPE, preexec_fn=os.setsid)


    @classmethod
    @pytest.mark.parametrize('_round', range(100))
    def test_insert(cls, _round):
        cls.TABLENAME = cls._TABLENAME + str(_round)
        column_types, column_names, mock = cls.create_and_insert()
        for i in range(50):     # 随机更新部分记录
            cls.random_update(column_types, column_names, mock)
        cls.client.stdin.write(f"select * from {cls.TABLENAME};\n".encode())
        cls.client.stdin.close()
        time.sleep(1)
        with open(f"{cls.DB}/output.txt", "w") as f:    # 清空输出，避免上轮输出的结果影响这轮测试
            f.close()
        with open(f"{cls.DB}/output.txt", "rt") as f:
            lines = f.readlines()
        for row_no, line in enumerate(lines[1:]):
            line = line.strip('|\n').split('|')
            for col_no, col in enumerate(column_types):
                lhs, rhs = line[col_no].strip(), mock[row_no][col_no]
                if col == 'int':
                    assert lhs == rhs
                elif col == 'float':
                    assert float(lhs) - float(rhs) < 1e-4
                elif col.startswith('char('):
                    assert lhs == rhs.strip("'")
                else:
                    assert False, '不支持的类型'

    @classmethod
    def random_update(cls, column_types, column_names, mock):
        # 随机选取一行进行更新
        row_no = random.randint(0, len(mock) - 1)
        # 随机选取一列作为where条件，避免浮点数直接比较
        # 生成表时已经保证了不会出现所有列都是浮点类型的情况
        where_col_type = 'float'
        while where_col_type == 'float':
            where_col_no = random.randint(0, len(mock[0]) - 1)
            where_col_type = column_types[where_col_no]
        where_rhs = mock[row_no][where_col_no]
        mock[row_no] = list(cls.generate_random_data(column_types))
        set_clause = ','.join([f"{column_names[i]} = {mock[row_no][i]}" for i in range(len(column_types))])
        where_clause = f"{column_names[where_col_no]} = {where_rhs}"
        sql = f"update {cls.TABLENAME} set {set_clause} where {where_clause};\n"
        cls.client.stdin.write(sql.encode())

    @classmethod
    def create_and_insert(cls):
        count = random.randint(1, 10)
        # 生成随机类型，不要全是浮点类型的
        column_types = ['float']
        while all(map(lambda x: x == 'float', column_types)):
            column_types = [cls.get_random_column_type() for _ in range(count)]
        # 生成表名，命名规则为xn，x为类型名首字母，n为列序号
        column_names = [f"{column_types[j][0]}{j}" for j in range(len(column_types))]
        columns = [names + " " + types for names, types in zip(column_names, column_types)]
        sql = f"create table {cls.TABLENAME} ({','.join(columns)});\n"
        print(sql)
        # 组装sql，通过client发送
        cls.client.stdin.write(sql.encode())
        mock = []
        for i in range(random.randint(1, 100)):
            row = list(cls.generate_random_data(column_types))
            mock.append(row)
            sql = f"insert into {cls.TABLENAME} values({",".join(row)});\n"
            print(sql)
            cls.client.stdin.write(sql.encode())
        return column_types, column_names, mock

    @classmethod
    def generate_random_data(cls, column_types):
        for col in column_types:
            if col == 'int':
                yield str(random.randint(-2 ** 31 - 1, 2 ** 31))
            elif col == 'float':
                yield str(random.random() * 2 * 30)
            else:
                # 'char(23323213)，取出长度
                # 生成不超过范围的随机字符串
                max_len = int(col[5:-1])
                length = random.randint(1, max_len)
                string = []
                string = random.choices("1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz", k=length)
                yield "'" + ''.join(string) + "'"

    @classmethod
    def get_random_column_type(cls):
        columns = ["int", "char", "float"]
        choose = random.choice(columns)
        if choose == "char":
            choose += f"({random.randint(1, 100)})"
        return choose

    @classmethod
    def test_fail(cls):
        cls.server.kill()  # 在最后一个，保证测试失败后正确关闭服务器

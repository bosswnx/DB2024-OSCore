import os
import random
import subprocess
import time
import shutil


# 测试创建表的功能
class TestClassDemoInstance:
    DB = "TestClassDemoInstanceDB"
    SERVER = "./rmdb"
    CLIENT = "./rmdb_client"

    @classmethod
    def setup_class(cls):
        if cls.DB in os.listdir():  # 删掉残留的数据库
            shutil.rmtree(cls.DB)
        cls.server = subprocess.Popen([cls.SERVER, cls.DB])  # 启动服务器
        time.sleep(3)  # 等待服务器启动完毕

    @classmethod
    def teardown_class(cls):
        cls.server.kill()

    @classmethod
    def setup_method(cls):  # 每次测试都启动一个新的客户端
        cls.client = subprocess.Popen([cls.CLIENT], stdin=subprocess.PIPE, preexec_fn=os.setsid)

    @classmethod
    def test_create_many_table(cls):
        # 测试随机创建100个表
        ROUND = 100
        column_names_list = []
        for i in range(ROUND):
            count = random.randint(1, 10)
            # 生成随机类型
            column_types = [cls.get_random_column_type() for _ in range(count)]
            # 生成表名，命名规则为xn，x为类型名首字母，n为列序号
            column_names = [f"{column_types[j][0]}{j}" for j in range(len(column_types))]
            column_names_list.append(column_names)
            columns = [names + " " + types for names, types in zip(column_names, column_types)]
            # 表名序号前面补零直到满5位
            sql = f"create table test_table{i:05} ({','.join(columns)});\n"
            # 组装sql，通过client发送
            cls.client.stdin.write(sql.encode())
        cls.client.stdin.write("show tables;\n".encode())
        cls.client.stdin.close()
        time.sleep(5)
        with open(f"{cls.DB}/output.txt", "rt") as f:
            output = f.readlines()
        assert len(output) == ROUND + 1
        output.pop(0)
        output.sort()
        for i in range(ROUND):
            assert output[i].strip(' |\n') == f"test_table{i:05}"

        # 检查创建的表的每个字段是否匹配
        cls.client = subprocess.Popen([cls.CLIENT], stdin=subprocess.PIPE, preexec_fn=os.setsid)
        with open(f"{cls.DB}/output.txt", "wb") as f:
            f.close()
        for i in range(ROUND):
            cls.client.stdin.write(f"select * from test_table{i:05};\n".encode())
        cls.client.stdin.close()
        time.sleep(1)
        with open(f"{cls.DB}/output.txt", "rt") as f:
            output = f.readlines()
        print(output)
        for i, line in enumerate(output):
            line = line.strip().replace(' ', '').strip('|')  # 去掉头尾的`|`
            for index, col in enumerate(line.split('|')):
                assert col == column_names_list[i][index]

    @classmethod
    def test_fail(cls):
        cls.server.kill()  # 在最后一个，保证测试失败后正确关闭服务器

    @classmethod
    def get_random_column_type(cls):
        columns = ["int", "char", "float"]
        choose = random.choice(columns)
        if choose == "char":
            choose += f"({random.randint(1, 100)})"
        return choose

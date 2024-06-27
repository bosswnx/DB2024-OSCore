"""
用于测试归并连接时生成10k x 10k 规模的数据，输出到merge_join_test.sql，包含创建左右表和插入总共20k记录的sql语句
"""
import random


def generate_random_data(col):
    if col == 'int':
        return str(random.randint(-2 ** 31 - 1, 2 ** 31))
    elif col == 'float':
        return str(random.random() * 2 * 30)
    else:
        # 'char(23323213)，取出长度
        # 生成不超过范围的随机字符串
        max_len = int(col[5:-1])
        length = random.randint(1, max_len)
        string = []
        string = random.choices("1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz", k=length)
        return "'" + ''.join(string) + "'"


def generate_sql(left_data, right_data):
    for data in left_data:
        yield f"insert into left_table values ({','.join(data)});\n"
    for data in right_data:
        yield f"insert into right_table values ({','.join(data)});\n"


DDL = """
create table left_table (i_1 int, i_2 int, f_3 float, s_4 char(128), s_5 char(128), f_6 float);
create table right_table (i_1 int, f_2 float, s_3 char(128), s_4 char(128));
"""
MERGE_SQL = """
SET enable_nestloop=false;
SET enable_sortmerge=true;
select * from left_table, right_table where left_table.i_1 = right_table.i_1 order by left_table.i_1;
"""

def main():
    left_cols = [
        "i_1", "i_2", "f_3", "s_4", "s_5", "f_6"
    ]
    right_cols = [
        "i_1", "f_2", "s_3", "s_4"
    ]

    left_data = []
    for i in range(10000):
        left_data.append([
            str(i),
            generate_random_data("int"),
            generate_random_data("float"),
            generate_random_data("char(128)"),
            generate_random_data("char(128)"),
            generate_random_data("float"),
        ])
    random.shuffle(left_data)
    right_data = []
    for i in range(10000):
        right_data.append([
            str(i),
            generate_random_data("float"),
            generate_random_data("char(128)"),
            generate_random_data("char(128)"),
        ])
    random.shuffle(right_data)
    with open("merge_join_test.sql", "wt") as f:
        f.write(DDL)
        f.write('\n')
        for sql in generate_sql(left_data, right_data):
            f.write(sql)


if __name__ == '__main__':
    main()

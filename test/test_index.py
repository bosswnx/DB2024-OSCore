import uuid

strings = "insert into warehouse values ({}, '{}');"
ret = ""

LEN = 500
for i in range(LEN):
    ret += strings.format(i, str(uuid.uuid4().hex[:8])) + "\n"

with open("test_insert.sql", "w") as f:
    f.write(ret)


'''
create table a(id int, name char(8), value float);
create index a(id,name,value);
'''
strings = "insert into a values ({}, '{}', {});"
ret = ""
for i in range(LEN):
    ret += strings.format(i, str(uuid.uuid4().hex[:8]), float(i)+0.5) + "\n"


with open("test_insert2.sql", "w") as f:
    f.write(ret)

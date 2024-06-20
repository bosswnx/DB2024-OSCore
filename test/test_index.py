import uuid

strings = "insert into warehouse values ({}, '{}');"
ret = ""

LEN = 500
for i in range(LEN):
    ret += strings.format(i, str(uuid.uuid4().hex[:8])) + "\n"

with open("test_insert.sql", "w") as f:
    f.write(ret)




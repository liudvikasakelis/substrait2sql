import ibis
from ibis import _

from common import run_test, sqlite_con

table1 = sqlite_con.table("table1")
table2 = sqlite_con.table("table2")

def transformation(table1, table2):
    return (table1
              .join(table2, table1.id == table2.table1_id, how='left'))

print(run_test(transformation, table1, table2))

import ibis
from ibis import _

from common import run_test, sqlite_con

table1 = sqlite_con.table("table1")

def transformation(table):
    return(table.filter(_.id < 4))

print(run_test(transformation, table1))

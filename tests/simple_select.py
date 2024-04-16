import ibis 
from ibis import _

from common import run_test, sqlite_con

## Test logic

table1 = sqlite_con.table("table1")

def transformation(x):
    return(x.select(_.id, _.ech))

print(run_test(transformation, table1))

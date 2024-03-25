import ibis 
from ibis import _
import subprocess

sqlite_con = ibis.sqlite.connect("test-data.sqlite")

table1 = sqlite_con.table("table1")
table2 = sqlite_con.table("table")

table1_unbound = ibis.table(
    [("id", "float"), ("table2_id", "float"), ("ech", "string")],
    "table1",
)
table2_unbound = ibis.table(
    [("id", "float"), ("table1_id", "float"), ("ech", "string")],
    "table2",
)

def transformation(x):
    return(x.select(_.id, _.ech))

reference_result = transformation(table1).execute()

from ibis_substrait.compiler.core import SubstraitCompiler

compiler = SubstraitCompiler()
protobuf_bytes = compiler.compile(transformation(table1_unbound)).SerializeToString()

roundtrip_query = compiled_query = subprocess.run(
    "./substrait-compiler.R",
    input=protobuf_bytes,
    stdout=subprocess.PIPE)\
    .stdout\
    .decode("utf-8")

# with open("plan3.bin", "wb") as f:
#     f.write(protobuf_bytes.SerializeToString())


sqlite_con.sql(roundtrip_query).execute()



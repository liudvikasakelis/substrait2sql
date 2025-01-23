import ibis
from ibis import _
import subprocess
from ibis_substrait.compiler.core import SubstraitCompiler

sqlite_con = ibis.sqlite.connect("test-data.sqlite")

def unboundify(table):
    return(ibis.table(table.schema(), table.get_name()))

def equal_datasets(t1, t2):
    return(
        (t1.columns == t2.columns) &
        (t1.count().execute() == t2.count().execute()) &
        (t1.join(t2, t1.columns).count().execute() == t1.count().execute())
    )

def run_test(transformation, *tables):
    reference_result = transformation(*tables)

    unbound_tables = [unboundify(table) for table in tables]
    substrait_compiler = SubstraitCompiler()
    protobuf_bytes = substrait_compiler.compile(transformation(*unbound_tables))\
                                       .SerializeToString()
    compiler_output = subprocess.run(
        "./substrait-compiler.R",
        input=protobuf_bytes,
        stdout=subprocess.PIPE
    ).stdout.decode("utf-8")
    test_result = sqlite_con.sql(compiler_output)

    return(equal_datasets(reference_result, test_result))


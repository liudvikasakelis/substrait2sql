## What this is 

This package is an exploration into translating Substrait to (some) SQL. Such a tool would simplify creating quick-and-dirty bridges from Substrait producers to any SQL engine. I see this mostly as a learning experience. 

## What Substrait is

Is [https://substrait.io/](this). TL;DR it's kinda SQL, but not human-readable. There are several packages that generate it, but not very many consumers (DuckDB is perhaps the most famous native substrait consumer).

## What works 
  * **Very** limited subset of SQL 
  * A command line substrait compiler 
  * A "scaffold" for testing 
	* Test cases = Ibis queries. To get reference output, they are executed (by Ibis) against test SQLite database, and return a table. Test outputs are computed by running Ibis Substrait compiler on the queries, compiling substrait to SQL using this compiler, running that SQL directly on SQLite database. Both outputs expected to be equal. 
	  I'm using Ibis substrait compiler because it's the best to my knowledge (I had apparent errors with Substrait-R). Testing only a single implementation, and only one SQL dialect leaves more to be desired, but it's really appropriate for the state of the compiler. 
  

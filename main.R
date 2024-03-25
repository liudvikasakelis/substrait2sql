library(RProtoBuf)
# library(devtools)
library(substrait)
library(dbplyr)
library(dplyr)
library(RJSONIO)
library("RSQLite")
library("tibble")
library("DBI")


## install.packages("RProtoBuf") 
## install.packages("RJSONIO") 
## install_github("voltrondata/substrait-r")

table1 <- tribble(
  ~id, ~table2_id, ~ech,
    1,          6,   "a",
    2,          5,   "b",
    3,          4,   "a",
    4,          3,   "b",
    5,          2,   "a",
    6,          1,   "b"
)

table2 <- tribble(
  ~id, ~table1_id, ~ech,
    1,          6,   "c",
    2,          5,   "d",
    3,          4,   "e",
    4,          3,   "e",
    5,          2,   "d",
    6,          1,   "c"
)
  
sqlite.db <- dbConnect(RSQLite::SQLite(), "test-data.sqlite")

dbWriteTable(sqlite.db, "table1", table1)
dbWriteTable(sqlite.db, "table2", table2)

readProtoFiles2(dir = "substrait",
                protoPath = "~/p/substrait-io/substrait/proto")


ls("RProtoBuf:DescriptorPool")

p1 <- mtcars %>% 
  duckdb_substrait_compiler() %>%
  select(mpg, wt) 

p2 <- mtcars %>% 
  duckdb_substrait_compiler() %>%
  mutate(mpg_plus_one = mpg + 1) %>% 
  select(mpg, wt, mpg_plus_one)

raw.protobuf.substrait <- p1$plan()[1][[1]]

raw.protobuf.substrait <- p2$plan()[1][[1]]

parsed.from.raw <- RProtoBuf::read(substrait.Plan, raw.protobuf.substrait)

parsed.3 <- RProtoBuf::read(substrait.Plan, file("plan3.bin", open="rb"))

Rel <- parsed.from.raw$relations[[1]]$root$input

message2sql(Rel) %>% cat

Rel$project
 %>% message2sql

Rel

parsed.from.raw$relations[[1]]$root %>% message2sql %>% cat

message2sql(parsed.from.raw) %>%
  cat

parsed.from.raw

parsed.3 %>% message2sql %>% cat

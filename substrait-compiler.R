#!/usr/bin/env Rscript

## Reads binary input from STDIN

f <- file("stdin", "rb")
# open(f)

binary_buffer <- readBin(f, raw(), 1e6)
## while(length(line <- readLines(f,n=1)) > 0) {
##   write(line, stderr())
##   # process line
## }

close(f)

source("message2sql.R")

cat(buffer2sql(binary_buffer))


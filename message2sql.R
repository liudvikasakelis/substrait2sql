library("stringr")
library("RProtoBuf")

## Utility functions

AS <- function(inp, out) {
  paste(inp, "AS", out) |>
    paste(collapse=", ")
}

id.to.colname <- function(x) {
  paste("_c_", x, sep="")
}

rename <- function(n, renames) {
  ## Apply renames of the form `c(old_name = "new_name")`
  c(
    renames[intersect(n, names(renames))],
    setdiff(n, names(renames))
  )
}

return.first.match <- function(message, field.list) {
  for (field in field.list) {
    if (message$has(field)) {return(message[[field]])}
  }
}

get.output.names <- function(message) {
  if (message@type == "substrait.NamedStruct") {
    message$names
  } else if (message@type == "substrait.ReadRel") {
    if (message$has("projection")) {
      ## TODO
      ## What is projection ;(
    } else {
      get.output.names(message$base_schema)
    }
  } else if (message@type == "substrait.ProjectRel") {
    message$common$emit$output_mapping %>% id.to.colname
  } else if (message@type == "substrait.FilterRel") {
    ## FIXME
    0:(length(get.output.names(message$input))-1) %>% id.to.colname
  } else if (message@type == "substrait.RelRoot") {
    message$names
  } else if (message@type == "substrait.Rel") {
    return.first.match(
      message,
      c("read", "filter", "project")
    ) %>% get.output.names
  } else {
    stop(message@type, " not defined")
  }
}

retrieve.functions <- function(uris, extensions) {
  ## Creates a named list of all external functions defined in Plan
  ## so they can be accessed by their numerical indices ("anchors"):
  ## function.list[["2"]](1, 2) => "1 + 2"

  function.aliases <- c(
    "lt" = "<",
    "gt" = ">"
  )

  infix.functions <- c(
    "<",
    ">",
    "+",
    "-",
    "/",
    "*"
  )

  prefix.builder <- function(fname) {
    function(...) {
      paste0(fname, "(", paste(..., sep=", "), ")")
    }
  }
  infix.builder <- function(fname) {
    function(a, b) {
      paste("(", a, fname, b, ")")
    }
  }

  function.names <- sapply(extensions, function(x) {x$extension_function$name}) |>
    str_split_i(":", 1) |>   # Removes everyting after first `:`
    rename(function.aliases)

  function.anchors <- sapply(extensions, function(x) {x$extension_function$function_anchor})

  functions <- function.names |>
    lapply(function(x) {if (x %in% infix.functions)
                          infix.builder(x)
                        else
                          prefix.builder(x)})

  names(functions) <- as.character(function.anchors)
  functions
}

## Functions that convert Substrait nodes ("messages") to SQL
## Name of list element is the message "type" that it expects as its argument

sql.emitters <- list(
  substrait.Expression = function(x) {
    if (x$has("selection")) {
      message2sql(x$selection)
    } else if (x$has("scalar_function")) {
      message2sql(x$scalar_function)
    } else if (x$has("literal")) {
      message2sql(x$literal)
    }
  },

  substrait.Expression.FieldReference = function(x) {
    x$direct_reference$struct_field$field |>
      id.to.colname()
  },

  substrait.Expression.Literal = function(x) {
    numeric_types <- c("i8", "i16", "i32", "i64", "fp32", "fp64")
    for (field in numeric_types) {
      if (x$has(field)) {return(x[[field]])}
    }
  },

  substrait.Expression.ScalarFunction = function(x) {
    ## function.call <- parsed.from.raw$relations[[1]]$root$input$project$input$project$expressions[[12]]$scalar_function
    f <- dynGet("plan.functions")[[as.character(x$function_reference)]]
    args <- lapply(x$arguments, message2sql)
    do.call(f, args)
  },

  substrait.FunctionArgument =  function(x) {
    message2sql(x$value)
  },

  substrait.FilterRel =  function(x) {
    paste0(
      "SELECT * FROM (",
      message2sql(x$input),
      ") WHERE (",
      message2sql(x$condition),
      ")"
    )
  },

  substrait.NamedStruct = function(x) {
    get.output.names(x) |>
      paste(collapse=", ")
    ## indices <- seq(from=0, length.out=length(x$names)) |> id.to.colname()
    ## x$names |> AS(indices)
  },

  substrait.Plan =  function(x) {
    plan.functions <- retrieve.functions(x$extension_uris, x$extensions)
    message2sql(x$relations[[1]]$root)
  },

  substrait.ProjectRel =  function(x) {
    inputs <- lapply(
      x$expressions,
      message2sql
    ) |> unlist()
    outputs <- x$common$emit$output_mapping |> id.to.colname()
    columns.to.aliases <- inputs |> AS(outputs)
    paste(
      "SELECT",
      columns.to.aliases,
      "\nFROM\n",
      "(",
      message2sql(x$input),
      ")"
    )
  },

  substrait.ReadRel = function(x) {
    ## FIXME: needs aliases
    inputs <- get.output.names(x$base_schema)
    outputs <- 0:(length(inputs)-1) %>% id.to.colname
    paste(
      "SELECT",
      inputs %>% AS(outputs),
      "FROM",
      message2sql(x$named_table)
    )
  },

  substrait.ReadRel.NamedTable =  function(x) {
    as.character(x$names[1L])
  },

  substrait.Rel = function(x) {
    if (x$has("read")) {
      message2sql(x$read)
    } else if (x$has("project")) {
      message2sql(x$project)
    } else if (x$has("filter")) {
      message2sql(x$filter)
    }
  },

  substrait.RelRoot = function(x) {
    ## This is one of not many functions that has output with named columns

    input.names <- get.output.names(x$input)
    outputs <- get.output.names(x)

    paste(
      "SELECT",
      input.names |> AS(outputs),
      "FROM",
      "(",
      message2sql(x$input),
      ")"
    )
  }

)

readProtoFiles2(dir = "substrait",
                protoPath = "~/p/substrait-io/substrait/proto")

message2sql <- function(message) {
  stopifnot(inherits(message, "Message"))
  stopifnot(message@type %in% names(sql.emitters))

  emitter <- sql.emitters[[message@type]]
  emitter(message)
}

buffer2sql <- function(buffer) {
  RProtoBuf::read(substrait.Plan, buffer) |> message2sql()
}

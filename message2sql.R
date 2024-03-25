library(RProtoBuf)

## Utility functions

AS <- function(inp, out) {
  paste(inp, "AS", out) |> 
    paste(collapse=", ")
}

id.to.colname <- function(x) {
  paste("_c_", x, sep="")
}

retrieve.functions <- function(uris, extensions) {
  ## Creates a named list of all external functions defined in Plan
  ## so they can be accessed by their names (which are numerical indices):
  ## function.list[["2"]](1, 2) => "1 + 2"
  
  infix.functions <- c("+", "-", "/", "*")

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
    
  function.names <- lapply(extensions, function(x) {x$extension_function$name})
  function.anchors <- sapply(extensions, function(x) {x$extension_function$function_anchor})

  functions <- function.names |>
    lapply(function(x) {if (x %in% infix.functions) infix.builder(x) else prefix.builder(x)})
    
  names(functions) <- as.character(function.anchors)
  functions
}

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
    x$fp64 # FIXME
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

  substrait.NamedStruct = function(x) {
    indices <- seq(from=0, length.out=length(x$names)) |> id.to.colname()
    x$names |> AS(indices)
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
    paste(
      "SELECT",
      message2sql(x$base_schema),
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
    }
  },

  substrait.RelRoot = function(x) {
    inputs <- x$input$project$common$emit$output_mapping # FIXME
    outputs <- x$names
    if (is.numeric(outputs)) {
      outputs <- id.to.colname(outputs)
    } 
    columns.to.aliases <- id.to.colname(inputs) |> AS(outputs)
    paste(
      "SELECT",
      columns.to.aliases,
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

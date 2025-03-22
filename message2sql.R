library("stringr")
library("RProtoBuf")

##

partial.translate <- function(full.plan, segment) {
  ## For debugging use, to evaluate a small part of plan
  plan.functions <- retrieve.functions(full.plan$extension_uris, full.plan$extensions)
  substrait2sql.list(segment)
}

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

return.first.match <- function(S, field.list) {
  for (field in field.list) {
    if (field %in% names(S)) {return(S[[field]])}
  }
}

tree.walker <- function(tree, target.node.id, f) {
  ## Returns a @tree where nodes with $.type matching @target.node.id get function
  ## @f applied to them.
  ## In the cases where target type node has children of same type, children will not be
  ## considered by tree.walker. In such cases incorporate tree.walker into @f
  if (is.list(tree)) {
    if (".type" %in% names(tree) && tree$.type == target.node.id) {
      f(tree)
    } else { ## Tree nodes that aren't targeted, but perhaps their children are
      lapply(tree, tree.walker, target.node.id=target.node.id, f=f)
    }
  } else { ## Things that aren't lists are always leaves and are returned as-is
    tree
  }
}

get.output.names <- function(message) {
  ## if (is.null(message$.type)) {
  ##   print('x')
  ##   print(message)
  ## }
  if (message$.type == "substrait.NamedStruct") {
    message$names
  } else if (message$.type == "substrait.ReadRel") {
    if ("projection" %in% names(message)) {
      ## TODO
      ## What is projection ;(
    } else {
      get.output.names(message$base_schema)
    }
  } else if (message$.type == "substrait.ProjectRel") {
    message$common$emit$output_mapping |> id.to.colname()
  } else if (message$.type == "substrait.FilterRel") {
    ## FIXME
    0:(length(get.output.names(message$input))-1) |> id.to.colname()
  } else if (message$.type == "substrait.RelRoot") {
    message$names
  } else if (message$.type == "substrait.Rel") {
    return.first.match(
      message,
      c("read", "filter", "project", "join")
    ) |> get.output.names()
  } else if (message$.type == "substrait.JoinRel") {
    ## FIXME
    0:(length(get.output.names(message$left)) +
         length(get.output.names(message$right)) -
         1) |> id.to.colname()
  } else {
    stop(message$.type, " not defined")
  }
}

retrieve.functions <- function(uris, extensions) {
  ## Creates a named list of all external functions defined in Plan
  ## so they can be accessed by their numerical indices ("anchors"):
  ## function.list[["2"]](1, 2) => "1 + 2"

  function.aliases <- c(
    "lt" = "<",
    "gt" = ">",
    "equal" = "="
  )

  infix.functions <- c(
    "=",
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
  custom.fieldByName = function(x) {
    x$fullname
  },

  substrait.Expression = function(x) {
    if ("selection" %in% names(x)) {
      substrait2sql.list(x$selection)
    } else if ("scalar_function" %in% names(x)) {
      substrait2sql.list(x$scalar_function)
    } else if ("literal" %in% names(x)) {
      substrait2sql.list(x$literal)
    }
  },

  substrait.Expression.FieldReference = function(x) {
    x$direct_reference$struct_field$field |>
      id.to.colname()
  },

  substrait.Expression.Literal = function(x) {
    numeric_types <- c("i8", "i16", "i32", "i64", "fp32", "fp64")
    for (field in numeric_types) {
      if (field %in% names(x)) {return(x[[field]])}
    }
  },

  substrait.Expression.ScalarFunction = function(x) {
    f <- dynGet("plan.functions")[[as.character(x$function_reference)]]
    args <- lapply(x$arguments, substrait2sql.list)
    do.call(f, args)
  },

  substrait.FunctionArgument =  function(x) {
    substrait2sql.list(x$value)
  },

  substrait.FilterRel =  function(x) {
    paste0(
      "SELECT * FROM (",
      substrait2sql.list(x$input),
      ") WHERE (",
      substrait2sql.list(x$condition),
      ")"
    )
  },

  substrait.JoinRel = function(x) {
    ## type 0 = unspecifiedc
    join.types <- c("INNER", "OUTER", "LEFT", "RIGHT", "SEMI", "ANTI", "SINGLE")

    left.inputs <- seq(0, along.with=get.output.names(x$left)) |> id.to.colname()
    right.inputs <- seq(0, along.with=get.output.names(x$right)) |> id.to.colname()
    combined.inputs <- data.frame(
      old.name = c(paste0("LHS.", left.inputs),
                   paste0("RHS.", right.inputs)),
      new.index = seq(0, along.with=c(left.inputs, right.inputs))
    )

    expression <- tree.walker(x$expression, "substrait.Expression.FieldReference", function(elem) {
      index <- elem$direct_reference$struct_field$field
      list(
        .type = "custom.fieldByName",
        fullname = combined.inputs$old.name[combined.inputs$new.index==index]
      )
    })

    paste(
      "SELECT",
      combined.inputs$old.name |> AS(combined.inputs$new.index |> id.to.colname()),
      "FROM",
      "(",
      substrait2sql.list(x$left),
      ") AS LHS",
      join.types[x$type],
      "JOIN",
      "(",
      substrait2sql.list(x$right),
      ") AS RHS",
      "ON",
      substrait2sql.list(expression)
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
    substrait2sql.list(x$relations[[1]]$root)
  },

  substrait.ProjectRel =  function(x) {
    inputs <- lapply(
      x$expressions,
      substrait2sql.list
    ) |> unlist()
    outputs <- x$common$emit$output_mapping |> id.to.colname()
    columns.to.aliases <- inputs |> AS(outputs)
    paste(
      "SELECT",
      columns.to.aliases,
      "\nFROM\n",
      "(",
      substrait2sql.list(x$input),
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
      substrait2sql.list(x$named_table)
    )
  },

  substrait.ReadRel.NamedTable =  function(x) {
    as.character(x$names[1L])
  },

  substrait.Rel = function(x) {
    if ("read" %in% names(x)) {
      substrait2sql.list(x$read)
    } else if ("project" %in% names(x)) {
      substrait2sql.list(x$project)
    } else if ("filter" %in% names(x)) {
      substrait2sql.list(x$filter)
    } else if ("join" %in% names(x)) {
      substrait2sql.list(x$join)
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
      substrait2sql.list(x$input),
      ")"
    )
  }
)


readProtoFiles2(dir = "substrait",
                protoPath = "~/p/substrait-io/substrait/proto")


substrait2sql.list <- function(l) {
  stopifnot(l$.type %in% names(sql.emitters))
  emitter <- sql.emitters[[l$.type]]
  emitter(l)
}

substrait2sql.Message <- function(message) {
  stopifnot(inherits(message, "Message"))
  message |> Message2list() |> substrait2sql.list()
}

Message2list <- function(x) {
  if(inherits(x, "Message")) {
    l <- x |> as.list()
    ## I have a suspicion that only the second filter is necessary
    l <- l[union(
      Filter(x$has, names(l)),
      Filter(function(x) {!inherits(l[[x]], "Message")}, names(l))
    )]
    l$.type <- x@type
    l |> Message2list()
  } else if (is.list(x)) {
    lapply(x, Message2list)
  } else {
    x
  }
}


buffer2sql <- function(buffer) {
  RProtoBuf::read(substrait.Plan, buffer) |> substrait2sql.Message()
}

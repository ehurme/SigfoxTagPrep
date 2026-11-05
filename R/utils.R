# Package-wide internal helpers.

# NULL-default operator (base R has it only from 4.4.0).
`%||%` <- function(a, b) if (is.null(a)) b else a

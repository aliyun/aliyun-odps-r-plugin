library("RODPS")
library(assertthat)

# Init rodps config
rodps.init("~/.config/odps_config.ini")

# Upload iris dataset
rodps.table.drop("iris")
names(iris) <- gsub("\\.", "_", names(iris))
rodps.table.write(iris, "iris")

# Show current project
rodps.project.current()

# Run plain SQL
sql_str <- "select species, count(1) from iris group by species"
result <- rodps.sql(sql_str)
assert_that(is.data.frame(result))  # Resulted data frame fetched locally

result <- rodps.sql(sql_str, result.table.limit = 0L)
assert_that(is.character(result))  # Char vector whose first element is the resulted table name
rodps.table.read(result[1])

# Run plain SQL with MCQA
rodps.sql(sql_str, mcqa = TRUE, result.table.limit = 0L)
result_table <- result[1]
assert_that(rodps.table.rows(result_table) == 3)
rodps.table.read(result_table)

# Drop test table
rodps.table.drop("iris")

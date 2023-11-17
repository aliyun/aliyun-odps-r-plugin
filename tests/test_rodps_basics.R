library("RODPS")
library(assertthat)

# Init rodps config
rodps.init("~/.config/odps_config.ini")

# Reset test data
if (rodps.table.exist("iris")) {
  rodps.table.drop("iris")
}

# Upload iris dataset
names(iris) <- gsub("\\.","_",names(iris))
rodps.table.write(iris, 'iris')

# Show current project
rodps.project.current()

# Run plain SQL
sql_str <- "select species, count(1) from iris group by species"
result <- rodps.sql(sql_str)
result_table <- result[1]
assert_that(rodps.table.rows(result_table) == 3)
rodps.table.read(result_table)

# Run plain SQL with MCQA
rodps.sql(sql_str, mcqa = TRUE)
result_table <- result[1]
assert_that(rodps.table.rows(result_table) == 3)
rodps.table.read(result_table)

# Drop test table
rodps.table.drop("iris")
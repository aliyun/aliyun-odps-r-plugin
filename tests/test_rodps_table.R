library("RODPS")
library(assertthat)

# Init rodps config
rodps.init("~/.config/odps_config.ini")

# Reset test data
if (rodps.table.exist("iris")) {
    rodps.table.drop("iris")
}

# Upload iris dataset
names(iris) <- gsub("\\.", "_", names(iris))
rodps.table.write(iris, "iris")

# DDL generation
sqlddl <- rodps.generate.DDL("testiris", iris, "Iris dataset")
sqlddl2 <- rodps.generate.DDL("testiris", iris)
print(sqlddl)
print(sqlddl2)

# Table reading
tbl1 <- rodps.table.read("iris")
head(tbl1)

# Basic table dimensions
rodps.table.head("iris", n = 3)

assert_that(rodps.table.exist("iris"))
assert_that(!rodps.table.exist("iris-non-existed"))

rodps.table.list()
rodps.table.desc("iris")

assert_that(rodps.table.rows("iris") == 150)
assert_that(rodps.table.size("iris") >= 2380)
assert_that(rodps.table.size("iris") <= 2400)

# Table sampling
if (rodps.table.exist("iris_sampled")) {
    rodps.table.drop("iris_sampled")
}
rodps.table.sample.srs("iris", "iris_sampled", 0.5)
rodps.table.rows("iris_sampled")
rodps.table.drop("iris_sampled")

# Drop test table
rodps.table.drop("iris")

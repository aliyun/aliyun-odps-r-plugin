library("RODPS")

# Init rodps config
rodps.init("~/.config/odps_config.ini")

# Upload iris dataset
names(iris) <- gsub("\\.","_",names(iris))
rodps.table.write(iris, 'iris')

# Show current project
rodps.project.current()

# List tables
rodps.table.list()

# Run plain SQL
sql_str <- "select species, count(1) from iris group by species"
rodps.sql(sql_str)

# Run plain SQL with MCQA
rodps.sql(sql_str, interactive = TRUE)

# Table reading
tbl1 <- rodps.table.read("iris")
head(tbl1)

# Basic table dimensions
rodps.table.desc("iris")
tryCatch(rodps.table.partitions("iris"))
rodps.table.rows("iris")
rodps.table.size("iris")

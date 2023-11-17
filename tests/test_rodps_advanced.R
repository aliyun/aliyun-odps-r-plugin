library("RODPS")
library(assertthat)

# Init rodps config
rodps.init("~/.config/odps_config.ini")

# Upload iris dataset
rodps.table.drop("iris")
names(iris) <- gsub("\\.", "_", names(iris))
rodps.table.write(iris, "iris")

# Table histgram
rodps.table.hist(tblname = "iris", colname = "sepal_length", col = rainbow(10), freq = F)

# Rpart prediction
library(rpart)
fit <- rpart(Species ~ ., data = iris)
rodps.table.drop("iris_p")
rodps.predict(fit, srctbl = "iris", tgttbl = "iris_p")
rodps.table.read("iris_p")

# Drop test table
rodps.table.drop("iris")

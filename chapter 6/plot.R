#!/usr/bin/env Rscript
library(ggplot2)
x <- read.csv("nb2.tsv", header=FALSE, sep="\t")
names(x) <- c("id", "class", "class.pred", "sepal.length", "petal.length")

x$class <- factor(x$class)
x$class.pred <- factor(x$class.pred)
x$train.only <- is.na(x$class.pred)
x$missed <- (x$class != x$class.pred & !x$train.only)

ggplot() +
    geom_point(data=x, aes(x=petal.length, y=sepal.length, color=class, shape=missed), size=4) +
    ggtitle("GaussianNB classification of iris dataset.\nx's are training data, triangles are misclassified") +
    ggsave(file="plot1.png", width=10, height=7)

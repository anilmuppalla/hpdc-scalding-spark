#!/usr/bin/env Rscript
library(ggplot2)
x <- read.csv("in.txt", header=FALSE, sep=",")
names(x) <- c("x", "y")

ggplot() +
    geom_point(data=x, aes(x=x, y=y), size=4) +
    geom_abline(intercept=-3.89, slope=1.193, colour="blue") +
    ggtitle("Regression Line for Food Truck Dataset") +
    ggsave(file="LR.png", width=10, height=7)

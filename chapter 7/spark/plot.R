#!/usr/bin/env Rscript
library(ggplot2)
x <- read.csv("in.txt", header=FALSE, sep=",")
names(x) <- c("x", "y")

#1.1930336441895912,-3.8957808783118404
#x$class <- factor(x$class)
#x$classcentroids <- factor(x$centroidsx)
ggplot() +
    geom_point(data=x, aes(x=x, y=y), size=4) +
    geom_abline(intercept=-3.89, slope=1.193, colour="blue") +
    ggtitle("Regression Line for Food Truck Dataset") +
    ggsave(file="plot4.png", width=10, height=7)

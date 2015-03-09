#imports
library(ggplot2)
library(reshape)
setwd("/home/arthur/")

executionTimeData = read.csv("dados_Java_Scala.csv",header=TRUE)

executionTimeData2 <- melt(executionTimeData) 

colnames(executionTimeData2) <- c("Technologies", "ExecutionTimeSeg")

p <- ggplot(executionTimeData2, aes(Technologies, ExecutionTimeSeg))

p <- p + geom_boxplot()

p + scale_y_continuous(labels=function(x)x/1000)

png("boxPlot.png")
p
dev.off()

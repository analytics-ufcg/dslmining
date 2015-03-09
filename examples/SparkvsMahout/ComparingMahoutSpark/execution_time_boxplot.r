#imports
library(ggplot2)
library(reshape)

#In order to run this code, you should set the path to the ComparingMahoutSpark folder.
#setwd("/home/arthur/dslminig/examples/SparkvsMahout/ComparingMahoutSpark")

executionTimeData = read.csv("dados_Java_Scala.csv",header=TRUE)

executionTimeDataWide <- melt(executionTimeData) 

colnames(executionTimeDataWide) <- c("Technologies", "ExecutionTimeSeg")

p <- ggplot(executionTimeDataWide, aes(Technologies, ExecutionTimeSeg))

p <- p + geom_boxplot()

p + scale_y_continuous(labels=function(x)x/1000)

png("boxPlot.png")
p
dev.off()

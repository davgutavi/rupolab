#Rutina para graficar los resultados del clustering sobre el dataset t123_d
require("reshape2")
source("api/carga_paquetes_variables_globales.R")

df123d_p <- read.parquet(patht123d_p)

df <- as.data.frame(df123d_p)



ncol(df)

a<- c(1)
b <- c(5:458)
i<- c (a,b)

data <- df[i]

tmydf <- setNames(data.frame(t(data[,-1])), data[,1])

show(tmydf[1:5])

w <- t(data)

s1 <- w[,1]

#t1<- ggplot(y=w[,1],aes(SOLUTION,TRIQ))+geom_line()

t1<- ggplot(data=tmydf, aes(x=n, y=CAM638341400100)) +
  geom_line(colour="red")
print(t1)

t1<- ggplot(data=tmydf ,aes(n,CAM638341400100))+geom_line()+scale_x_discrete(labels=n)
print(t1)

days <- data.frame(
  day = 1:454 
)

tl<- cbind(days, tmydf)



t1<- ggplot(tl, aes(days)) + 
  geom_line(aes(y = CAM638341400100, colour = "var1"))+
  geom_line(aes(y = CAM641684400100, colour = "var2"))+
  geom_line(aes(y = CAM641684800100, colour = "var3"))+
  geom_line(aes(y = CAM641687700100, colour = "var4"))+
  geom_line(aes(y = CAM643388400101, colour = "var5"))+
  ylim(-10.0, 10.0)+
scale_x_discrete()
print(t1)

n <-row.names(tmydf)

 







sparkR.stop()
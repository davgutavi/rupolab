#Rutina para graficar los registros de lecturas cuarto horarias

library(SparkR)


sparkR.session(master = "local[*]")
root <- "hdfs://192.168.47.247/user/gutierrez/endesa/datasets_parquet/"
pathIrregularidad <- paste0(root,"lecturasIrregularidad_04")
pathAnomalia <- paste0(root,"lecturasAnomalia_04")
irregularidad<- read.parquet(pathIrregularidad)
anomalia<- read.parquet(pathAnomalia)


head(irregularidad,20)

cups<- "ES0031405141896001RR0F"


contrato2<-"110010962376"
contrato3<-"170052497386"
contrato4<- "210013506074"
contrato5<-"130056628331" 

contrato1<- "110010962376"
version<- "002"
dia <- "15"
mes <- "01"
año <- "2015"

source("funciones.R")
cdia<-consumo_dia(contrato,version,dia,mes,año,irregularidad)

cmes<-consumo_mes(contrato1,"002",mes,año,irregularidad)

plot_grid(plotlist = cmes,nrow = 4,ncol =4 )

caño<-consumo_año(contrato1,"002",año,irregularidad)

aux4 <- days_in_month(as.Date("2010-03-17 "))

horas <- dibujar_horas(cups,irregularidad,4)
phoras<-pintar_horas(horas)
plot_grid(plotlist = phoras)

lineas<-dibujar_linea_horaria(cups,irregularidad,4)
plineas<-pintar_lineas(lineas)
plot_grid(plotlist = plineas)

sparkR.stop()
#Carga específica de SparkR en MacOSX
#Consulta campo origen para Maestro Contratos y Clientes

spark_path <- strsplit(system("brew info apache-spark",intern=T)[4],' ')[[1]][1] # Get your spark path
.libPaths(c(file.path(spark_path,"libexec", "R", "lib"), .libPaths())) # Navigate to SparkR folder
library(SparkR) # Load the library
sparkR.session(master = "local[*]", sparkConfig = list(spark.local.dir="/Users/davgutavi/tempSparkR",spark.driver.memory="8g",spark.network.timeout="10000000s",spark.executor.heartbeatInterval="10000000s"))
source("paths.R")

###################################################### Maestro Contratos - Clientes

#PK USUPO t00C: ccontrat, cnumscct, fpsercon, ffinvesu, cupsree, cpuntmed, tpuntmed
#PK ENDESA t00C: origen, cfinca, cptoserv, cderind, cupsree, cemptitu, ccontrat, cnumscct (erronea)

t00C <- read.parquet(path00C)
createOrReplaceTempView(t00C,"MC")
t05 <- read.parquet(path05C)
createOrReplaceTempView(t05C,"C")


df1<-take(t00C,40)
df2<-take(t05C,40)

t05A <- read.parquet("/Users/davgutavi/Desktop/buffer_endesa/parquet/t05a/")
createOrReplaceTempView(t05A,"A")
df1<-take(t05A,40)


q1 <- sql("SELECT DISTINCT origen FROM C") 

q2 <- sql("SELECT DISTINCT origen FROM MC") 

#show(count(contadorClientes))

showDF(q1)
showDF(q2)


sparkR.stop()

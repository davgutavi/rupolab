#Carga de paquetes y variables globales


##Carga SparkR en Mac OSX
spark_path <- strsplit(system("brew info apache-spark",intern=T)[4],' ')[[1]][1] # Get your spark path
.libPaths(c(file.path(spark_path,"libexec", "R", "lib"), .libPaths())) # Navigate to SparkR folder
#library(SparkR) # Load the library
require(SparkR) # Load the library
sparkR.session(master = "local[*]", sparkConfig = list(spark.local.dir="/Users/davgutavi/tempSparkR",spark.driver.memory="8g",spark.network.timeout="10000000s",spark.executor.heartbeatInterval="10000000s"))
##Carga SparkR en Ubuntu-Linux
require(SparkR)
sparkR.session(master = "local[*]", sparkConfig = list(spark.local.dir="/mnt/datos/tempSparkR",spark.driver.memory="8g",spark.network.timeout="10000000s",spark.executor.heartbeatInterval="10000000s"))



rootHDFS <- "hdfs://192.168.47.247/user/gutierrez/endesa/"
rootHDDavid <-"/Volumes/david/endesa/"
rootHDDLab <- "/mnt/datos/recursos/ENDESA/"


database_parquet <- paste0(rootHDFS,"database_parquet/")
datasets_parquet <- paste0(rootHDFS,"datasets_parquet/")
joins_parquet <- paste0(rootHDFS,"joins_parquet/")
path00C <- paste0(database_parquet,"TAB00C")
path00E <- paste0(database_parquet,"TAB00E")
path01 <- paste0(database_parquet,"TAB01")
path02 <- paste0(database_parquet,"TAB02")
path05A <- paste0(database_parquet,"TAB05A")
path05B <- paste0(database_parquet,"TAB05B")
path05C <- paste0(database_parquet,"TAB05C")
path06 <- paste0(database_parquet,"TAB06")
path08 <- paste0(database_parquet,"TAB08")
path15A <- paste0(database_parquet,"TAB15A")
path15B <- paste0(database_parquet,"TAB15B")
path15C <- paste0(database_parquet,"TAB15C")
path16 <- paste0(database_parquet,"TAB16")
path24 <- paste0(database_parquet,"TAB24")


database_hdd_david <- paste0(rootHDDavid,"database_parquet/")
datasets_hdd_david <- paste0(rootHDDavid,"datasets_parquet/")
joins_hdd_david <- paste0(rootHDDavid,"joins_parquet/")
path00C_hdd_david <- paste0(database_hdd_david,"TAB00C")
path00E_hdd_david <- paste0(database_hdd_david,"TAB00E")
path01_hdd_david <- paste0(database_hdd_david,"TAB01")
path02_hdd_david <- paste0(database_hdd_david,"TAB02")
#path05A_hdd_david <- paste0(database_hdd_david,"TAB05A")
#path05B_hdd_david <- paste0(database_hdd_david,"TAB05B")
#path05C_hdd_david <- paste0(database_hdd_david,"TAB05C")
path06_hdd_david <- paste0(database_hdd_david,"TAB06")
path08_hdd_david <- paste0(database_hdd_david,"TAB08")
path15A_hdd_david <- paste0(database_hdd_david,"TAB15A")
path15B_hdd_david <- paste0(database_hdd_david,"TAB15B")
path15C_hdd_david <- paste0(database_hdd_david,"TAB15C")
path16_hdd_david <- paste0(database_hdd_david,"TAB16")
path24_hdd_david <- paste0(database_hdd_david,"TAB24")







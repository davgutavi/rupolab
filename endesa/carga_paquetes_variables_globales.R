#Carga de paquetes y variables globales
require(ggplot2)
require(dplyr)
require(reshape2)
require(ggrepel)

os <- "MACOSX"
#os <- "LINUX"


if (os == "MACOSX"){
  
  ##Carga SparkR en Mac OSX
  
  #spark_path <- strsplit(system("brew info apache-spark",intern=T)[4],' ')[[1]][1] # Get your spark path
  spark_path <- "/usr/local/Cellar/apache-spark/2.2.0"
  .libPaths(c(file.path(spark_path,"libexec", "R", "lib"), .libPaths())) # Navigate to SparkR folder
  require(SparkR) # Load the library
  sparkR.session(master = "local[*]")
  
  if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
    Sys.setenv(SPARK_HOME = "/Users/davgutavi/Desktop/spark-2.2.1-bin-hadoop2.7")
  }
  
  Sys.setenv(SPARK_HOME = "/Users/davgutavi/Desktop/spark-2.2.1-bin-hadoop2.7")
  Sys.setenv(JAVA_HOME = "/Library/Java/JavaVirtualMachines/jdk1.8.0_152.jdk/Contents/Home")
  library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
  sparkR.session(master = "local[*]", sparkConfig = list(spark.driver.memory = "2g"))
  
}else{
  
  require(SparkR)
  sparkR.session(master = "local[*]", 
                 sparkConfig = list(spark.local.dir="/mnt/datos/tempSparkR",
                                    spark.driver.memory="8g",
                                    spark.network.timeout="10000000s",
                                    spark.executor.heartbeatInterval="10000000s"))
  
}

rootHDFSdatasets <- "hdfs://192.168.47.247/user/datos/endesa/datasets/"
rrootHDDavid <-"/Volumes/david/"
rootHDDLab <- "/mnt/datos/"
rootLocalDatasets <-"/Users/davgutavi/NAS_DAVGUTAVI/endesa/investigacion_paper/datasets/"
rootLocalClasificacion <-"/Users/davgutavi/Desktop/endesa/clasificacion/"
rootLocalClustering <-"/Users/davgutavi/NAS_DAVGUTAVI/endesa/investigacion_paper/clustering/"


#*******************************************CLUSTERING EXPERIMETS

l_path_454d_raw_umr_kmeans<-paste0(rootLocalClustering,"lc_454d_raw_umr")
l_path_454d_raw_con_kmeans<-paste0(rootLocalClustering,"lc_454d_raw_con")
l_path_454d_max_umr_kmeans<-paste0(rootLocalClustering,"lc_454d_max_umr")
l_path_454d_max_con_kmeans<-paste0(rootLocalClustering,"lc_454d_max_con")
l_path_454d_slo_umr_kmeans<-paste0(rootLocalClustering,"lc_454d_slo_umr")
l_path_454d_slo_con_kmeans<-paste0(rootLocalClustering,"lc_454d_slo_con")

l_path_364d_slo_umr_kmeans<-paste0(rootLocalClustering,"lc_364d_raw_umr")
l_path_364d_slo_con_kmeans<-paste0(rootLocalClustering,"lc_364d_raw_con")
l_path_364d_raw_umr_kmeans<-paste0(rootLocalClustering,"lc_364d_max_umr")
l_path_364d_raw_con_kmeans<-paste0(rootLocalClustering,"lc_364d_max_con")
l_path_364d_max_umr_kmeans<-paste0(rootLocalClustering,"lc_364d_slo_umr")
l_path_364d_max_con_kmeans<-paste0(rootLocalClustering,"lc_364d_slo_con")

#*******************************************CLASIFICATION EXPERIMETS

clasificacion_01 <- paste0(rootLocalClasificacion,"t123_364d_pen_02")
clasificacion_02 <- paste0(rootLocalClasificacion,"t123_364d_pen_04")
clasificacion_03 <- paste0(rootLocalClasificacion,"t123_364d_pen_05")
clasificacion_04 <- paste0(rootLocalClasificacion,"t123_364d_pen_10")

clasificacion_364d_con <- paste0(rootLocalClasificacion,"s_364d_con_slo")
clasificacion_364d_nrl <- paste0(rootLocalClasificacion,"s_364d_nrl_slo")
clasificacion_454d_con <- paste0(rootLocalClasificacion,"s_454d_con_slo")
clasificacion_454d_nrl <- paste0(rootLocalClasificacion,"s_454d_nrl_slo")

#*******************************************DATASETS ROOTS LOCAL

l_path_454d_raw_umr<- paste0(rootLocalDatasets,"454d_raw_umr/454d_raw_umr")
l_path_454d_raw_con<- paste0(rootLocalDatasets,"454d_raw_con/454d_raw_con")
l_path_454d_max_umr<- paste0(rootLocalDatasets,"454d_max_umr/454d_max_umr")
l_path_454d_max_con<- paste0(rootLocalDatasets,"454d_max_con/454d_max_con")
l_path_454d_slo_umr<- paste0(rootLocalDatasets,"454d_slo_umr/454d_slo_umr")
l_path_454d_slo_con<- paste0(rootLocalDatasets,"454d_slo_con/454d_slo_con")

l_path_364d_raw_umr<- paste0(rootLocalDatasets,"364d_raw_umr/364d_raw_umr")
l_path_364d_raw_con<- paste0(rootLocalDatasets,"364d_raw_con/364d_raw_con")
l_path_364d_raw_umr<- paste0(rootLocalDatasets,"364d_max_umr/364d_max_umr")
l_path_364d_raw_con<- paste0(rootLocalDatasets,"364d_max_con/364d_max_con")
l_path_364d_slo_umr<- paste0(rootLocalDatasets,"364d_slo_umr/364d_slo_umr")
l_path_364d_slo_con<- paste0(rootLocalDatasets,"364d_slo_con/364d_slo_con")

#*******************************************DATASETS ROOTS LOCAL

r_path_454d_raw_umr<- paste0(rootHDFSdatasets,"454d_raw_umr/454d_raw_umr")
r_path_454d_raw_con<- paste0(rootHDFSdatasets,"454d_raw_con/454d_raw_con")
r_path_454d_max_umr<- paste0(rootHDFSdatasets,"454d_max_umr/454d_max_umr")
r_path_454d_max_con<- paste0(rootHDFSdatasets,"454d_max_con/454d_max_con")
r_path_454d_slo_umr<- paste0(rootHDFSdatasets,"454d_slo_umr/454d_slo_umr")
r_path_454d_slo_con<- paste0(rootHDFSdatasets,"454d_slo_con/454d_slo_con")

r_path_364d_raw_umr<- paste0(rootHDFSdatasets,"364d_raw_umr/364d_raw_umr")
r_path_364d_raw_con<- paste0(rootHDFSdatasets,"364d_raw_con/364d_raw_con")
r_path_364d_raw_umr<- paste0(rootHDFSdatasets,"364d_max_umr/364d_max_umr")
r_path_364d_raw_con<- paste0(rootHDFSdatasets,"364d_max_con/364d_max_con")
r_path_364d_slo_umr<- paste0(rootHDFSdatasets,"364d_slo_umr/364d_slo_umr")
r_path_364d_slo_con<- paste0(rootHDFSdatasets,"364d_slo_con/364d_slo_con")


#******************************************OLD
# database_parquet <- paste0(rootHDFS,"database_parquet/")
# datasets_parquet <- paste0(rootHDFS,"datasets_parquet/")
# joins_parquet <- paste0(rootHDFS,"joins_parquet/")
# path00C <- paste0(database_parquet,"TAB00C")
# path00E <- paste0(database_parquet,"TAB00E")
# path01 <- paste0(database_parquet,"TAB01")
# path02 <- paste0(database_parquet,"TAB02")
# path05A <- paste0(database_parquet,"TAB05A")
# path05B <- paste0(database_parquet,"TAB05B")
# path05C <- paste0(database_parquet,"TAB05C")
# path06 <- paste0(database_parquet,"TAB06")
# path08 <- paste0(database_parquet,"TAB08")
# path15A <- paste0(database_parquet,"TAB15A")
# path15B <- paste0(database_parquet,"TAB15B")
# path15C <- paste0(database_parquet,"TAB15C")
# path16 <- paste0(database_parquet,"TAB16")
# path24 <- paste0(database_parquet,"TAB24")
# 
# database_hdd_david <- paste0(rootHDDavid,"database_parquet/")
# datasets_hdd_david <- paste0(rootHDDavid,"datasets_parquet/")
# joins_hdd_david <- paste0(rootHDDavid,"joins_parquet/")
# path00C_hdd_david <- paste0(database_hdd_david,"TAB00C")
# path00E_hdd_david <- paste0(database_hdd_david,"TAB00E")
# path01_hdd_david <- paste0(database_hdd_david,"TAB01")
# path02_hdd_david <- paste0(database_hdd_david,"TAB02")
# #path05A_hdd_david <- paste0(database_hdd_david,"TAB05A")
# #path05B_hdd_david <- paste0(database_hdd_david,"TAB05B")
# #path05C_hdd_david <- paste0(database_hdd_david,"TAB05C")
# path06_hdd_david <- paste0(database_hdd_david,"TAB06")
# path08_hdd_david <- paste0(database_hdd_david,"TAB08")
# path15A_hdd_david <- paste0(database_hdd_david,"TAB15A")
# path15B_hdd_david <- paste0(database_hdd_david,"TAB15B")
# path15C_hdd_david <- paste0(database_hdd_david,"TAB15C")
# path16_hdd_david <- paste0(database_hdd_david,"TAB16")
# path24_hdd_david <- paste0(database_hdd_david,"TAB24")
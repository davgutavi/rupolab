os <- "MACOSX"
#os <- "LINUX"

if (os == "MACOSX"){
  
  ##Carga SparkR en Mac OSX
  Sys.setenv(SPARK_HOME = "/Users/davgutavi/Spark/spark-2.2.1-bin-hadoop2.7")
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

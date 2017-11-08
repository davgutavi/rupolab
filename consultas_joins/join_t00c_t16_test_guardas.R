#Join Maestro Contratos - Expedientes: test de guardas

library(SparkR)
source("paths.R")
sparkR.session(master = "local[*]", sparkConfig = list(spark.local.dir="/mnt/datos/tempSparkR",spark.driver.memory="8g",spark.network.timeout="10000000s",spark.executor.heartbeatInterval="10000000s"))
t00C <- read.parquet(path00C)
t16 <- read.parquet(path16)  

createOrReplaceTempView(t00C,"MC")
createOrReplaceTempView(t16,"E")

mce1 <- sql("
SELECT MC.origen, MC.cptocred, MC.cfinca, MC.cptoserv, MC.cderind, MC.cupsree, MC.ccounips,MC.cupsree2, MC.cpuntmed, MC.tpuntmed, MC.vparsist, MC.cemptitu,MC.ccontrat, 
       MC.cnumscct, MC.fpsercon, MC.ffinvesu, MC.contrext,
       E.csecexpe, E.fapexpd, E.finifran, E.ffinfran, E.anomalia, E.irregularidad, E.venacord, E.vennofai, E.torigexp, E.texpedie,E.expclass, E.testexpe, 
       E.fnormali, E.cplan, E.ccampa, E.cempresa, E.fciexped, E.cexpeind    
FROM MC JOIN E
ON MC.origen=E.origen AND MC.cfinca=E.cfinca AND MC.cptoserv=E.cptoserv AND MC.cderind=E.cderind
          ")

show(paste0("count mce1 sin guardas = ",count(mce1)))


mce2 <- sql("
SELECT MC.origen, MC.cptocred, MC.cfinca, MC.cptoserv, MC.cderind, MC.cupsree, MC.ccounips,MC.cupsree2, MC.cpuntmed, MC.tpuntmed, MC.vparsist, MC.cemptitu,MC.ccontrat, 
       MC.cnumscct, MC.fpsercon, MC.ffinvesu, MC.contrext,
       E.csecexpe, E.fapexpd, E.finifran, E.ffinfran, E.anomalia, E.irregularidad, E.venacord, E.vennofai, E.torigexp, E.texpedie,E.expclass, E.testexpe, 
       E.fnormali, E.cplan, E.ccampa, E.cempresa, E.fciexped, E.cexpeind    
FROM MC JOIN E
ON MC.origen=E.origen AND MC.cfinca=E.cfinca AND MC.cptoserv=E.cptoserv AND MC.cderind=E.cderind AND MC.fpsercon<E.fapexpd AND E.fapexpd <= MC.ffinvesu
          ")

show(paste0("count mce2 con guardas básicas = ",count(mce2)))

sparkR.stop()
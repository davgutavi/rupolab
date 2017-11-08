#Join Maestro Contratos - Clientes

library(SparkR)
source("paths.R")
sparkR.session(master = "local[*]", sparkConfig = list(spark.local.dir="/mnt/datos/tempSparkR",spark.driver.memory="8g",spark.network.timeout="10000000s",spark.executor.heartbeatInterval="10000000s"))



###################################################### Maestro Contratos - Clientes

#PK USUPO t00E: ccontrat, cnumscct, fpsercon, ffinvesu, cupsree, cpuntmed, tpuntmed
#PK ENDESA t00C: origen, cfinca, cptoserv, cderind, cupsree, cemptitu, ccontrat, cnumscct (erronea)

t00C <- read.parquet(path00C)
createOrReplaceTempView(t00C,"MC")
t05C <- read.parquet(path05C)
createOrReplaceTempView(t05C,"C")

contadorClientes <- sql("SELECT MC.origen, MC.cptocred, MC.cfinca, MC.cptoserv, MC.cderind, MC.cupsree, MC.ccounips,MC.cupsree2, MC.cpuntmed, MC.tpuntmed, MC.vparsist, MC.cemptitu,MC.ccontrat, MC.cnumscct, MC.fpsercon, 
            MC.ffinvesu,E.csecexpe, E.fapexpd, E.finifran, E.ffinfran, E.anomalia, E.irregularidad, E.venacord, E.vennofai, E.torigexp, E.texpedie,E.expclass, E.testexpe, 
            E.fnormali, E.cplan, E.ccampa, E.cempresa, E.fciexped    
            FROM MC JOIN E
            ON MC.origen=E.origen AND MC.cfinca=E.cfinca AND MC.cptoserv=E.cptoserv AND MC.cderind=E.cderind AND E.fapexpd >= MC.fpsercon AND E.fapexpd <= MC.ffinvesu")


sparkR.stop()
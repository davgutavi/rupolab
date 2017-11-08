#Prueba cardinalidades Maestro Contratos, Clientes, Geolocalización

library(SparkR)
source("paths.R")
#sparkR.session(master = "local[*]", sparkConfig = list(spark.local.dir="/mnt/datos/tempSparkR",spark.network.timeout="10000000s",spark.executor.heartbeatInterval="10000000s"))
sparkR.session(master = "local[*]", sparkConfig = list(spark.local.dir="/mnt/datos/tempSparkR",spark.driver.memory="8g"))


t00C <- read.parquet(path00C)
createOrReplaceTempView(t00C,"MC")
t05C <- read.parquet(path05C)
createOrReplaceTempView(t05C,"C")
persist(t05C,"MEMORY_AND_DISK")
t05B <- read.parquet(path05B)
createOrReplaceTempView(t05B,"G")

mcc <- sql("SELECT  MC.cupsree, MC.cfinca, MC.ccontrat, MC.cnumscct, MC.fpsercon, MC.ffinvesu,C.fechamov, C.ccliente, C.cnifdnic, C.dapersoc, C.dnombcli
            FROM MC JOIN C
            ON MC.origen = C.origen AND MC.cemptitu = C.cemptitu AND MC.ccontrat = C.ccontrat AND MC.cnumscct = C.cnumscct
")
createOrReplaceTempView(mcc,"MCC")
persist(mcc,"MEMORY_AND_DISK")
df <- take(mcc,100)
count(mcc)


showDF(sql("SELECT * FROM C WHERE ccliente = '100403491' "),35,FALSE)




########################################################################## cupsree 1--1..n ccontrat 
dfc <- take(t05C,200)

showDF(sql("SELECT  cupsree, COUNT (DISTINCT ccontrat) as cont_ccontrat FROM MC GROUP BY cupsree ORDER BY cont_ccontrat DESC "),10,FALSE)

ES0031406251886001HL0FES0031406251886001HL0F


########################################################################## ccontrat 1--1..n  cupsree


showDF(sql("SELECT  ccontrat, COUNT (DISTINCT cupsree) as cont_cupsree FROM MC GROUP BY ccontrat ORDER BY cont_cupsree DESC "),10,FALSE)

showDF(sql("SELECT DISTINCT ccontrat, cupsree FROM MC WHERE ccontrat = '140051595300' "),35,FALSE)


########################################################################## cupsree 1--1..n ccontrat,cnumscct

showDF(sql("SELECT  cupsree, COUNT (DISTINCT ccontrat,cnumscct) as cont_ccontrat_cnumscct FROM MC GROUP BY cupsree ORDER BY cont_ccontrat_cnumscct DESC "),10,FALSE)

showDF(sql("SELECT DISTINCT cupsree, ccontrat, cnumscct FROM MC WHERE cupsree = 'ES0031405494292006RB0F' "),35,FALSE)


########################################################################## ccontrat,cnumscct 1--1..n  cupsree


showDF(sql("SELECT  ccontrat,cnumscct, COUNT (DISTINCT cupsree) as cont_cupsree FROM MC GROUP BY ccontrat,cnumscct ORDER BY cont_cupsree DESC "),10,FALSE)

showDF(sql("SELECT DISTINCT ccontrat,cnumscct, cupsree FROM MC WHERE ccontrat = '180049322910' AND cnumscct = '003' "),35,FALSE)



########################################################################## cupsree 1--1..n  ccliente


showDF(sql("SELECT  cupsree, COUNT (DISTINCT ccliente) as cont_ccliente FROM MCC GROUP BY cupsree ORDER BY cont_ccliente ASC "),10,FALSE)

showDF(sql("SELECT DISTINCT cupsree, ccliente FROM MCC WHERE cupsree = 'ES0031405890835021HA0F' "),35,FALSE)

########################################################################## ccliente  1--1..n  cupsree
ES0031406251886001HL0F

showDF(sql("SELECT  ccliente, COUNT (DISTINCT cupsree) as cont_cupsree FROM MCC GROUP BY ccliente ORDER BY cont_cupsree DESC "),30,FALSE)

showDF(sql("SELECT DISTINCT ccliente, cupsree FROM MCC WHERE ccliente = '100403491' "),20,FALSE)


########################################################################## cupsree 1--1..n  persona


showDF(sql("SELECT  cupsree, COUNT (DISTINCT cnifdnic) as cont_cnifdnic FROM MCC GROUP BY cupsree ORDER BY cont_cnifdnic DESC "),10,FALSE)

showDF(sql("SELECT DISTINCT cupsree, cnifdnic,dapersoc,dnombcli  FROM MCC WHERE cupsree = 'ES0031405890835021HA0F' "),35,FALSE)

########################################################################## persona  1--1..n  cupsree


showDF(sql("SELECT  cnifdnic, COUNT (DISTINCT cupsree) as cont_cupsree FROM MCC GROUP BY cnifdnic ORDER BY cont_cupsree DESC "),30,FALSE)

showDF(sql("SELECT DISTINCT cnifdnic,dapersoc,dnombcli, cupsree FROM MCC WHERE cnifdnic = 'P0801900B' "),20,FALSE)


########################################################################## cupsree 1--1  cfinca


showDF(sql("SELECT  cupsree, COUNT (DISTINCT cfinca) as cont_cfinca FROM MC GROUP BY cupsree ORDER BY cont_cfinca DESC "),10,FALSE)

showDF(sql("SELECT DISTINCT cupsree, cfinca  FROM MC WHERE cupsree = 'ES0031405402652001NX0F' "),35,FALSE)

########################################################################## cfinca  1--1..n  cupsree


showDF(sql("SELECT  cfinca, COUNT (DISTINCT cupsree) as cont_cupsree FROM MC GROUP BY cfinca ORDER BY cont_cupsree DESC "),30,FALSE)

showDF(sql("SELECT DISTINCT cfinca, cupsree FROM MCC WHERE cfinca = '5693907' "),20,FALSE)



########################################################################## persona  1--1..n  ccliente


showDF(sql("SELECT  cnifdnic, COUNT (DISTINCT ccliente) as cont_ccliente FROM C GROUP BY cnifdnic ORDER BY cont_ccliente DESC"),30,FALSE)

showDF(sql("SELECT DISTINCT cnifdnic,dapersoc,dnombcli, ccliente FROM C  WHERE cnifdnic = 'A48265169' "),20,FALSE)


########################################################################## ccliente  1--1  persona


showDF(sql("SELECT  ccliente, COUNT (DISTINCT cnifdnic) as cont_cnifdnic FROM C GROUP BY ccliente ORDER BY cont_cnifdnic DESC"),30,FALSE)

showDF(sql("SELECT DISTINCT ccliente,cnifdnic,dapersoc,dnombcli FROM C  WHERE ccliente = '143493987' "),20,FALSE)


########################################################################## ccliente  1--1..n  ccontrat


showDF(sql("SELECT  ccliente, COUNT (DISTINCT ccontrat) as cont_ccontrat FROM MCC GROUP BY ccliente ORDER BY cont_ccontrat DESC"),30,FALSE)

showDF(sql("SELECT DISTINCT ccliente, ccontrat FROM MCC  WHERE ccliente = '100403491' "),20,FALSE)


########################################################################## ccontrat  1--1..n  ccliente


showDF(sql("SELECT  ccontrat, COUNT (DISTINCT ccliente) as cont_ccliente FROM MCC GROUP BY ccontrat ORDER BY cont_ccliente DESC"),30,FALSE)

showDF(sql("SELECT DISTINCT ccontrat, ccliente FROM MCC  WHERE ccontrat = '160051922518' "),20,FALSE)


########################################################################## ccliente  1--1..n  ccontrat,cnumscct


showDF(sql("SELECT  ccliente, COUNT (DISTINCT ccontrat,cnumscct) as cont_ccontrat FROM MCC GROUP BY ccliente ORDER BY cont_ccontrat DESC"),30,FALSE)

showDF(sql("SELECT DISTINCT ccliente, ccontrat,cnumscct FROM MCC  WHERE ccliente = '100403491' ORDER BY ccontrat, cnumscct ASC"),20,FALSE)


########################################################################## ccontrat,cnumscct  1--1..n  ccliente


showDF(sql("SELECT  ccontrat,cnumscct, COUNT (DISTINCT ccliente) as cont_ccliente FROM MCC GROUP BY ccontrat,cnumscct ORDER BY cont_ccliente DESC"),30,FALSE)

showDF(sql("SELECT DISTINCT ccontrat,cnumscct, ccliente,cnifdnic,dapersoc,dnombcli FROM MCC  WHERE ccontrat = '110011422920' AND cnumscct = '002' "),20,FALSE)



sparkR.stop()
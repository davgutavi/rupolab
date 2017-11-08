#COnstrucción tabla Irregularidad

library(SparkR)
source("paths.R")
sparkR.session(master = "local[*]", sparkConfig = list(spark.local.dir="/mnt/datos/tempSparkR",spark.driver.memory="8g",spark.network.timeout="10000000s",spark.executor.heartbeatInterval="10000000s"))

t00C <- read.parquet(path00C)
createOrReplaceTempView(t00C,"MC")
t16 <- read.parquet(path16)
createOrReplaceTempView(t16,"E")
t00E <- read.parquet(path00E)
createOrReplaceTempView(t00E,"MA")
t01 <- read.parquet(path01)
createOrReplaceTempView(t01,"CC")

mce <- sql("
          SELECT MC.origen, MC.cptocred, MC.cfinca, MC.cptoserv, MC.cderind, MC.cupsree, MC.ccounips,MC.cupsree2, MC.cpuntmed, MC.tpuntmed, MC.vparsist, MC.cemptitu,MC.ccontrat, MC.cnumscct, MC.fpsercon, MC.ffinvesu, 
                 E.csecexpe, E.fapexpd, E.finifran, E.ffinfran, E.anomalia, E.irregularidad, E.venacord, E.vennofai, E.torigexp, E.texpedie,E.expclass, E.testexpe, 
                 E.fnormali, E.cplan, E.ccampa, E.cempresa, E.fciexped    
                 FROM MC JOIN E
                 ON MC.origen=E.origen AND MC.cfinca=E.cfinca AND MC.cptoserv=E.cptoserv AND MC.cderind=E.cderind AND E.fapexpd >= MC.fpsercon AND E.fapexpd <= MC.ffinvesu AND irregularidad = 'S'
          ")

createOrReplaceTempView(mce,"MCE")
persist(mce,"MEMORY_AND_DISK")
count(mce)

mcema <- sql("SELECT MCE.origen, MCE.cptocred, MCE.cfinca, MCE.cptoserv, MCE.cderind, MCE.cupsree,MCE.ccounips,MCE.cupsree2, MCE.cpuntmed, MCE.tpuntmed, MCE.vparsist, MCE.cemptitu,
                     MCE.ccontrat, MCE.cnumscct, MCE.fpsercon, MCE.ffinvesu,MCE.csecexpe, MCE.fapexpd, MCE.finifran, MCE.ffinfran, MCE.anomalia, MCE.irregularidad,
                     MCE.venacord, MCE.vennofai, MCE.torigexp, MCE.texpedie,MCE.expclass, MCE.testexpe,MCE.fnormali, MCE.cplan, MCE.ccampa, MCE.cempresa, MCE.fciexped,
                     MA.csecptom, MA.fvigorpm, MA.fbajapm,MA.caparmed
              FROM MCE JOIN MA
              ON MCE.origen = MA.origen AND MCE.cupsree2 = MA.cupsree2 AND MCE.cpuntmed = MA.cpuntmed
             ")

createOrReplaceTempView(mcema,"MCEMA")
persist(mce,"MEMORY_AND_DISK")


mcemacc_diff = sql("SELECT MCEMA.origen, MCEMA.cptocred, MCEMA.cfinca, MCEMA.cptoserv,MCEMA.cderind, MCEMA.cupsree,MCEMA.ccounips,MCEMA.cupsree2,MCEMA.cpuntmed, MCEMA.tpuntmed, MCEMA.vparsist, MCEMA.cemptitu,
                   MCEMA.ccontrat, MCEMA.cnumscct, MCEMA.fpsercon, MCEMA.ffinvesu,MCEMA.csecexpe, MCEMA.fapexpd, MCEMA.finifran, MCEMA.ffinfran,MCEMA.anomalia, MCEMA.irregularidad,MCEMA.venacord, MCEMA.vennofai,
                   MCEMA.torigexp, MCEMA.texpedie,MCEMA.expclass, MCEMA.testexpe,MCEMA.fnormali, MCEMA.cplan, MCEMA.ccampa, MCEMA.cempresa,MCEMA.fciexped,MCEMA.csecptom, MCEMA.fvigorpm, MCEMA.fbajapm,MCEMA.caparmed,
                   CC.flectreg, datediff ( flectreg , fapexpd ) AS dif,
                   CC.testcaco, CC.obiscode, CC.vsecccar,
                   CC.hora_01, CC.1q_consumo_01, CC.2q_consumo_01, CC.3q_consumo_01, CC.4q_consumo_01,CC.substatus_01,CC.testmenn_01,CC.testmecnn_01,
                   CC.hora_02, CC.1q_consumo_02, CC.2q_consumo_02, CC.3q_consumo_02, CC.4q_consumo_02,CC.substatus_02,CC.testmenn_02,CC.testmecnn_02,
                   CC.hora_03, CC.1q_consumo_03, CC.2q_consumo_03, CC.3q_consumo_03, CC.4q_consumo_03,CC.substatus_03,CC.testmenn_03,CC.testmecnn_03,
                   CC.hora_04, CC.1q_consumo_04, CC.2q_consumo_04, CC.3q_consumo_04, CC.4q_consumo_04,CC.substatus_04,CC.testmenn_04,CC.testmecnn_04,
                   CC.hora_05, CC.1q_consumo_05, CC.2q_consumo_05, CC.3q_consumo_05, CC.4q_consumo_05,CC.substatus_05,CC.testmenn_05,CC.testmecnn_05,
                   CC.hora_06, CC.1q_consumo_06, CC.2q_consumo_06, CC.3q_consumo_06, CC.4q_consumo_06,CC.substatus_06,CC.testmenn_06,CC.testmecnn_06,
                   CC.hora_07, CC.1q_consumo_07, CC.2q_consumo_07, CC.3q_consumo_07, CC.4q_consumo_07,CC.substatus_07,CC.testmenn_07,CC.testmecnn_07,
                   CC.hora_08, CC.1q_consumo_08, CC.2q_consumo_08, CC.3q_consumo_08, CC.4q_consumo_08,CC.substatus_08,CC.testmenn_08,CC.testmecnn_08,
                   CC.hora_09, CC.1q_consumo_09, CC.2q_consumo_09, CC.3q_consumo_09, CC.4q_consumo_09,CC.substatus_09,CC.testmenn_09,CC.testmecnn_09,
                   CC.hora_10, CC.1q_consumo_10, CC.2q_consumo_10, CC.3q_consumo_10, CC.4q_consumo_10,CC.substatus_10,CC.testmenn_10,CC.testmecnn_10,
                   CC.hora_11, CC.1q_consumo_11, CC.2q_consumo_11, CC.3q_consumo_11, CC.4q_consumo_11,CC.substatus_11,CC.testmenn_11,CC.testmecnn_11,
                   CC.hora_12, CC.1q_consumo_12, CC.2q_consumo_12, CC.3q_consumo_12, CC.4q_consumo_12,CC.substatus_12,CC.testmenn_12,CC.testmecnn_12,
                   CC.hora_13, CC.1q_consumo_13, CC.2q_consumo_13, CC.3q_consumo_13, CC.4q_consumo_13,CC.substatus_13,CC.testmenn_13,CC.testmecnn_13,
                   CC.hora_14, CC.1q_consumo_14, CC.2q_consumo_14, CC.3q_consumo_14, CC.4q_consumo_14,CC.substatus_14,CC.testmenn_14,CC.testmecnn_14,
                   CC.hora_15, CC.1q_consumo_15, CC.2q_consumo_15, CC.3q_consumo_15, CC.4q_consumo_15, CC.substatus_15, CC.testmenn_15, CC.testmecnn_15,
                   CC.hora_16, CC.1q_consumo_16, CC.2q_consumo_16, CC.3q_consumo_16, CC.4q_consumo_16, CC.substatus_16, CC.testmenn_16, CC.testmecnn_16,
                   CC.hora_17, CC.1q_consumo_17, CC.2q_consumo_17, CC.3q_consumo_17, CC.4q_consumo_17, CC.substatus_17, CC.testmenn_17, CC.testmecnn_17,
                   CC.hora_18, CC.1q_consumo_18, CC.2q_consumo_18, CC.3q_consumo_18, CC.4q_consumo_18, CC.substatus_18, CC.testmenn_18, CC.testmecnn_18,
                   CC.hora_19, CC.1q_consumo_19, CC.2q_consumo_19, CC.3q_consumo_19, CC.4q_consumo_19, CC.substatus_19, CC.testmenn_19, CC.testmecnn_19,
                   CC.hora_20, CC.1q_consumo_20, CC.2q_consumo_20, CC.3q_consumo_20, CC.4q_consumo_20, CC.substatus_20, CC.testmenn_20, CC.testmecnn_20,
                   CC.hora_21, CC.1q_consumo_21, CC.2q_consumo_21, CC.3q_consumo_21, CC.4q_consumo_21, CC.substatus_21, CC.testmenn_21, CC.testmecnn_21,
                   CC.hora_22, CC.1q_consumo_22, CC.2q_consumo_22, CC.3q_consumo_22, CC.4q_consumo_22, CC.substatus_22, CC.testmenn_22, CC.testmecnn_22,
                   CC.hora_23, CC.1q_consumo_23, CC.2q_consumo_23, CC.3q_consumo_23, CC.4q_consumo_23, CC.substatus_23, CC.testmenn_23, CC.testmecnn_23,
                   CC.hora_24, CC.1q_consumo_24, CC.2q_consumo_24, CC.3q_consumo_24, CC.4q_consumo_24, CC.substatus_24, CC.testmenn_24, CC.testmecnn_24,
                   CC.hora_25, CC.1q_consumo_25, CC.2q_consumo_25, CC.3q_consumo_25, CC.4q_consumo_25, CC.substatus_25, CC.testmenn_25, CC.testmecnn_25
                   FROM MCEMA JOIN CC
                   ON MCEMA.origen = CC.origen AND MCEMA.cpuntmed = CC.cpuntmed AND CC.obiscode = 'A' AND CC.testcaco = 'R'")





persist(mcemacc_diff,"MEMORY_AND_DISK")
createOrReplaceTempView(mcemacc_diff,"MCEMACC")

df5<-take(where(mcemacc_diff,"cupsree = 'ES0031405183802001XR0F'"),40)

showDF(where(mcemacc_diff,"cupsree = 'ES0031405002817001HT0F'"),10,FALSE)


#lrdates_aux <- sql("SELECT DISTINCT cupsree, add_months(flectreg,-6) AS leftdate, flectreg AS rightdate, dif, fapexpd, ffinfran FROM MCEMACC 
#           WHERE dif<=0 AND flectreg <= ffinfran AND finifran <= add_months(flectreg,-6)  AND 
#            (cupsree,dif) IN (SELECT cupsree, max(dif) as maximo FROM MCEMACC GROUP BY cupsree)")
#df1<- take(lrdates_aux,100)

#lrdates_aux <- sql("SELECT DISTINCT cupsree, add_months(flectreg,-6) AS leftdate, flectreg AS rightdate, dif, fapexpd, ffinfran FROM MCEMACC 
#           WHERE dif<=0 AND  flectreg >= add_months(fapexpd,-2)  AND 
#            (cupsree,dif) IN (SELECT cupsree, max(dif) as maximo FROM MCEMACC GROUP BY cupsree)")
#df2<- take(lrdates_aux,100)

lrdates_aux1 <- sql("SELECT DISTINCT cupsree, add_months(flectreg,-6) AS leftdate, flectreg AS rightdate, dif, fapexpd, ffinfran FROM MCEMACC 
           WHERE dif<=0 AND (cupsree,dif) IN (SELECT cupsree, max(dif) as maximo FROM MCEMACC GROUP BY cupsree)")
df4<- take(lrdates_aux1,100)

v2 <- sql("SELECT cupsree, max(dif) as maximo FROM MCEMACC GROUP BY cupsree")

showDF(v2,100,FALSE)

sparkR.stop()

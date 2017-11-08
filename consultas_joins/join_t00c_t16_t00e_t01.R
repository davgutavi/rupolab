#Joins Maestro contratos, expedientes, Maestro Aparatos y Curvas de carga

library(SparkR)
source("paths.R")
sparkR.session(master = "local[*]", sparkConfig = list(spark.local.dir="/mnt/datos/tempSparkR"))


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
                 ON MC.origen=E.origen AND MC.cfinca=E.cfinca AND MC.cptoserv=E.cptoserv AND MC.cderind=E.cderind AND E.fapexpd >= MC.fpsercon
          ")

createOrReplaceTempView(mce,"MCE")

mcema <- sql("
          SELECT MCE.origen, MCE.cptocred, MCE.cfinca, MCE.cptoserv, MCE.cderind, MCE.cupsree,MCE.ccounips,MCE.cupsree2, MCE.cpuntmed, MCE.tpuntmed, MCE.vparsist, MCE.cemptitu,
                 MCE.ccontrat, MCE.cnumscct, MCE.fpsercon, MCE.ffinvesu,MCE.csecexpe, MCE.fapexpd, MCE.finifran, MCE.ffinfran, MCE.anomalia, MCE.irregularidad, 
                 MCE.venacord, MCE.vennofai, MCE.torigexp, MCE.texpedie,MCE.expclass, MCE.testexpe,MCE.fnormali, MCE.cplan, MCE.ccampa, MCE.cempresa, MCE.fciexped,
                 MA.csecptom, MA.fvigorpm, MA.fbajapm,MA.caparmed
                 FROM MCE JOIN MA
                 ON MCE.origen = MA.origen AND MCE.cupsree2 = MA.cupsree2 AND MCE.cpuntmed = MA.cpuntmed
          ")
createOrReplaceTempView(mcema,"MCEMA")

mcemacc <- sql("
          SELECT MCEMA.origen, MCEMA.cptocred, MCEMA.cfinca, MCEMA.cptoserv,MCEMA.cderind, MCEMA.cupsree,MCEMA.ccounips,MCEMA.cupsree2,MCEMA.cpuntmed, MCEMA.tpuntmed, MCEMA.vparsist, MCEMA.cemptitu,
          MCEMA.ccontrat, MCEMA.cnumscct, MCEMA.fpsercon, MCEMA.ffinvesu,MCEMA.csecexpe, MCEMA.fapexpd, MCEMA.finifran, MCEMA.ffinfran,MCEMA.anomalia, MCEMA.irregularidad,MCEMA.venacord, MCEMA.vennofai, 
          MCEMA.torigexp, MCEMA.texpedie,MCEMA.expclass, MCEMA.testexpe,MCEMA.fnormali, MCEMA.cplan, MCEMA.ccampa, MCEMA.cempresa,MCEMA.fciexped,MCEMA.csecptom, MCEMA.fvigorpm, MCEMA.fbajapm,MCEMA.caparmed,
          CC.flectreg, CC.testcaco, CC.obiscode, CC.vsecccar,
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
          ON MCEMA.origen = CC.origen AND MCEMA.cpuntmed = CC.cpuntmed AND CC.obiscode = 'A' AND CC.testcaco = 'R'
          ")

createOrReplaceTempView(mcemacc,"MCEMACC")

li<- sql("
          SELECT 
          MCEMACC.cupsree, MCEMACC.ccontrat, MCEMACC.cnumscct, MCEMACC.fpsercon, MCEMACC.ffinvesu, MCEMACC.fapexpd, MCEMACC.fciexped,
          MCEMACC.flectreg, MCEMACC.testcaco, MCEMACC.obiscode, MCEMACC.vsecccar,
          MCEMACC.hora_01, MCEMACC.1q_consumo_01, MCEMACC.2q_consumo_01, MCEMACC.3q_consumo_01, MCEMACC.4q_consumo_01,MCEMACC.substatus_01,MCEMACC.testmenn_01,MCEMACC.testmecnn_01,
          MCEMACC.hora_02, MCEMACC.1q_consumo_02, MCEMACC.2q_consumo_02, MCEMACC.3q_consumo_02, MCEMACC.4q_consumo_02,MCEMACC.substatus_02,MCEMACC.testmenn_02,MCEMACC.testmecnn_02,
          MCEMACC.hora_03, MCEMACC.1q_consumo_03, MCEMACC.2q_consumo_03, MCEMACC.3q_consumo_03, MCEMACC.4q_consumo_03,MCEMACC.substatus_03,MCEMACC.testmenn_03,MCEMACC.testmecnn_03,
          MCEMACC.hora_04, MCEMACC.1q_consumo_04, MCEMACC.2q_consumo_04, MCEMACC.3q_consumo_04, MCEMACC.4q_consumo_04,MCEMACC.substatus_04,MCEMACC.testmenn_04,MCEMACC.testmecnn_04,
          MCEMACC.hora_05, MCEMACC.1q_consumo_05, MCEMACC.2q_consumo_05, MCEMACC.3q_consumo_05, MCEMACC.4q_consumo_05,MCEMACC.substatus_05,MCEMACC.testmenn_05,MCEMACC.testmecnn_05,
          MCEMACC.hora_06, MCEMACC.1q_consumo_06, MCEMACC.2q_consumo_06, MCEMACC.3q_consumo_06, MCEMACC.4q_consumo_06,MCEMACC.substatus_06,MCEMACC.testmenn_06,MCEMACC.testmecnn_06,
          MCEMACC.hora_07, MCEMACC.1q_consumo_07, MCEMACC.2q_consumo_07, MCEMACC.3q_consumo_07, MCEMACC.4q_consumo_07,MCEMACC.substatus_07,MCEMACC.testmenn_07,MCEMACC.testmecnn_07,
          MCEMACC.hora_08, MCEMACC.1q_consumo_08, MCEMACC.2q_consumo_08, MCEMACC.3q_consumo_08, MCEMACC.4q_consumo_08,MCEMACC.substatus_08,MCEMACC.testmenn_08,MCEMACC.testmecnn_08,
          MCEMACC.hora_09, MCEMACC.1q_consumo_09, MCEMACC.2q_consumo_09, MCEMACC.3q_consumo_09, MCEMACC.4q_consumo_09,MCEMACC.substatus_09,MCEMACC.testmenn_09,MCEMACC.testmecnn_09,
          MCEMACC.hora_10, MCEMACC.1q_consumo_10, MCEMACC.2q_consumo_10, MCEMACC.3q_consumo_10, MCEMACC.4q_consumo_10,MCEMACC.substatus_10,MCEMACC.testmenn_10,MCEMACC.testmecnn_10,
          MCEMACC.hora_11, MCEMACC.1q_consumo_11, MCEMACC.2q_consumo_11, MCEMACC.3q_consumo_11, MCEMACC.4q_consumo_11,MCEMACC.substatus_11,MCEMACC.testmenn_11,MCEMACC.testmecnn_11,
          MCEMACC.hora_12, MCEMACC.1q_consumo_12, MCEMACC.2q_consumo_12, MCEMACC.3q_consumo_12, MCEMACC.4q_consumo_12,MCEMACC.substatus_12,MCEMACC.testmenn_12,MCEMACC.testmecnn_12,
          MCEMACC.hora_13, MCEMACC.1q_consumo_13, MCEMACC.2q_consumo_13, MCEMACC.3q_consumo_13, MCEMACC.4q_consumo_13,MCEMACC.substatus_13,MCEMACC.testmenn_13,MCEMACC.testmecnn_13,
          MCEMACC.hora_14, MCEMACC.1q_consumo_14, MCEMACC.2q_consumo_14, MCEMACC.3q_consumo_14, MCEMACC.4q_consumo_14,MCEMACC.substatus_14,MCEMACC.testmenn_14,MCEMACC.testmecnn_14,
          MCEMACC.hora_15, MCEMACC.1q_consumo_15, MCEMACC.2q_consumo_15, MCEMACC.3q_consumo_15, MCEMACC.4q_consumo_15, MCEMACC.substatus_15, MCEMACC.testmenn_15, MCEMACC.testmecnn_15,
          MCEMACC.hora_16, MCEMACC.1q_consumo_16, MCEMACC.2q_consumo_16, MCEMACC.3q_consumo_16, MCEMACC.4q_consumo_16, MCEMACC.substatus_16, MCEMACC.testmenn_16, MCEMACC.testmecnn_16,
          MCEMACC.hora_17, MCEMACC.1q_consumo_17, MCEMACC.2q_consumo_17, MCEMACC.3q_consumo_17, MCEMACC.4q_consumo_17, MCEMACC.substatus_17, MCEMACC.testmenn_17, MCEMACC.testmecnn_17,
          MCEMACC.hora_18, MCEMACC.1q_consumo_18, MCEMACC.2q_consumo_18, MCEMACC.3q_consumo_18, MCEMACC.4q_consumo_18, MCEMACC.substatus_18, MCEMACC.testmenn_18, MCEMACC.testmecnn_18,
          MCEMACC.hora_19, MCEMACC.1q_consumo_19, MCEMACC.2q_consumo_19, MCEMACC.3q_consumo_19, MCEMACC.4q_consumo_19, MCEMACC.substatus_19, MCEMACC.testmenn_19, MCEMACC.testmecnn_19,
          MCEMACC.hora_20, MCEMACC.1q_consumo_20, MCEMACC.2q_consumo_20, MCEMACC.3q_consumo_20, MCEMACC.4q_consumo_20, MCEMACC.substatus_20, MCEMACC.testmenn_20, MCEMACC.testmecnn_20,
          MCEMACC.hora_21, MCEMACC.1q_consumo_21, MCEMACC.2q_consumo_21, MCEMACC.3q_consumo_21, MCEMACC.4q_consumo_21, MCEMACC.substatus_21, MCEMACC.testmenn_21, MCEMACC.testmecnn_21,
          MCEMACC.hora_22, MCEMACC.1q_consumo_22, MCEMACC.2q_consumo_22, MCEMACC.3q_consumo_22, MCEMACC.4q_consumo_22, MCEMACC.substatus_22, MCEMACC.testmenn_22, MCEMACC.testmecnn_22,
          MCEMACC.hora_23, MCEMACC.1q_consumo_23, MCEMACC.2q_consumo_23, MCEMACC.3q_consumo_23, MCEMACC.4q_consumo_23, MCEMACC.substatus_23, MCEMACC.testmenn_23, MCEMACC.testmecnn_23,
          MCEMACC.hora_24, MCEMACC.1q_consumo_24, MCEMACC.2q_consumo_24, MCEMACC.3q_consumo_24, MCEMACC.4q_consumo_24, MCEMACC.substatus_24, MCEMACC.testmenn_24, MCEMACC.testmecnn_24,
          MCEMACC.hora_25, MCEMACC.1q_consumo_25, MCEMACC.2q_consumo_25, MCEMACC.3q_consumo_25, MCEMACC.4q_consumo_25, MCEMACC.substatus_25, MCEMACC.testmenn_25, MCEMACC.testmecnn_25
          FROM MCEMACC WHERE MCEMACC.irregularidad='S' AND  MCEMACC.flectreg BETWEEN add_months(MCEMACC.fapexpd,-6) AND MCEMACC.fapexpd
          ")

dfi <- take(li,100)  


la<- sql("
           SELECT 
          MCEMACC.cupsree, MCEMACC.ccontrat, MCEMACC.cnumscct, MCEMACC.fpsercon, MCEMACC.ffinvesu, MCEMACC.fapexpd, MCEMACC.fciexped,
          MCEMACC.flectreg, MCEMACC.testcaco, MCEMACC.obiscode, MCEMACC.vsecccar,
          MCEMACC.hora_01, MCEMACC.1q_consumo_01, MCEMACC.2q_consumo_01, MCEMACC.3q_consumo_01, MCEMACC.4q_consumo_01,MCEMACC.substatus_01,MCEMACC.testmenn_01,MCEMACC.testmecnn_01,
          MCEMACC.hora_02, MCEMACC.1q_consumo_02, MCEMACC.2q_consumo_02, MCEMACC.3q_consumo_02, MCEMACC.4q_consumo_02,MCEMACC.substatus_02,MCEMACC.testmenn_02,MCEMACC.testmecnn_02,
          MCEMACC.hora_03, MCEMACC.1q_consumo_03, MCEMACC.2q_consumo_03, MCEMACC.3q_consumo_03, MCEMACC.4q_consumo_03,MCEMACC.substatus_03,MCEMACC.testmenn_03,MCEMACC.testmecnn_03,
          MCEMACC.hora_04, MCEMACC.1q_consumo_04, MCEMACC.2q_consumo_04, MCEMACC.3q_consumo_04, MCEMACC.4q_consumo_04,MCEMACC.substatus_04,MCEMACC.testmenn_04,MCEMACC.testmecnn_04,
          MCEMACC.hora_05, MCEMACC.1q_consumo_05, MCEMACC.2q_consumo_05, MCEMACC.3q_consumo_05, MCEMACC.4q_consumo_05,MCEMACC.substatus_05,MCEMACC.testmenn_05,MCEMACC.testmecnn_05,
          MCEMACC.hora_06, MCEMACC.1q_consumo_06, MCEMACC.2q_consumo_06, MCEMACC.3q_consumo_06, MCEMACC.4q_consumo_06,MCEMACC.substatus_06,MCEMACC.testmenn_06,MCEMACC.testmecnn_06,
          MCEMACC.hora_07, MCEMACC.1q_consumo_07, MCEMACC.2q_consumo_07, MCEMACC.3q_consumo_07, MCEMACC.4q_consumo_07,MCEMACC.substatus_07,MCEMACC.testmenn_07,MCEMACC.testmecnn_07,
          MCEMACC.hora_08, MCEMACC.1q_consumo_08, MCEMACC.2q_consumo_08, MCEMACC.3q_consumo_08, MCEMACC.4q_consumo_08,MCEMACC.substatus_08,MCEMACC.testmenn_08,MCEMACC.testmecnn_08,
          MCEMACC.hora_09, MCEMACC.1q_consumo_09, MCEMACC.2q_consumo_09, MCEMACC.3q_consumo_09, MCEMACC.4q_consumo_09,MCEMACC.substatus_09,MCEMACC.testmenn_09,MCEMACC.testmecnn_09,
          MCEMACC.hora_10, MCEMACC.1q_consumo_10, MCEMACC.2q_consumo_10, MCEMACC.3q_consumo_10, MCEMACC.4q_consumo_10,MCEMACC.substatus_10,MCEMACC.testmenn_10,MCEMACC.testmecnn_10,
          MCEMACC.hora_11, MCEMACC.1q_consumo_11, MCEMACC.2q_consumo_11, MCEMACC.3q_consumo_11, MCEMACC.4q_consumo_11,MCEMACC.substatus_11,MCEMACC.testmenn_11,MCEMACC.testmecnn_11,
          MCEMACC.hora_12, MCEMACC.1q_consumo_12, MCEMACC.2q_consumo_12, MCEMACC.3q_consumo_12, MCEMACC.4q_consumo_12,MCEMACC.substatus_12,MCEMACC.testmenn_12,MCEMACC.testmecnn_12,
          MCEMACC.hora_13, MCEMACC.1q_consumo_13, MCEMACC.2q_consumo_13, MCEMACC.3q_consumo_13, MCEMACC.4q_consumo_13,MCEMACC.substatus_13,MCEMACC.testmenn_13,MCEMACC.testmecnn_13,
          MCEMACC.hora_14, MCEMACC.1q_consumo_14, MCEMACC.2q_consumo_14, MCEMACC.3q_consumo_14, MCEMACC.4q_consumo_14,MCEMACC.substatus_14,MCEMACC.testmenn_14,MCEMACC.testmecnn_14,
          MCEMACC.hora_15, MCEMACC.1q_consumo_15, MCEMACC.2q_consumo_15, MCEMACC.3q_consumo_15, MCEMACC.4q_consumo_15, MCEMACC.substatus_15, MCEMACC.testmenn_15, MCEMACC.testmecnn_15,
          MCEMACC.hora_16, MCEMACC.1q_consumo_16, MCEMACC.2q_consumo_16, MCEMACC.3q_consumo_16, MCEMACC.4q_consumo_16, MCEMACC.substatus_16, MCEMACC.testmenn_16, MCEMACC.testmecnn_16,
          MCEMACC.hora_17, MCEMACC.1q_consumo_17, MCEMACC.2q_consumo_17, MCEMACC.3q_consumo_17, MCEMACC.4q_consumo_17, MCEMACC.substatus_17, MCEMACC.testmenn_17, MCEMACC.testmecnn_17,
          MCEMACC.hora_18, MCEMACC.1q_consumo_18, MCEMACC.2q_consumo_18, MCEMACC.3q_consumo_18, MCEMACC.4q_consumo_18, MCEMACC.substatus_18, MCEMACC.testmenn_18, MCEMACC.testmecnn_18,
          MCEMACC.hora_19, MCEMACC.1q_consumo_19, MCEMACC.2q_consumo_19, MCEMACC.3q_consumo_19, MCEMACC.4q_consumo_19, MCEMACC.substatus_19, MCEMACC.testmenn_19, MCEMACC.testmecnn_19,
          MCEMACC.hora_20, MCEMACC.1q_consumo_20, MCEMACC.2q_consumo_20, MCEMACC.3q_consumo_20, MCEMACC.4q_consumo_20, MCEMACC.substatus_20, MCEMACC.testmenn_20, MCEMACC.testmecnn_20,
          MCEMACC.hora_21, MCEMACC.1q_consumo_21, MCEMACC.2q_consumo_21, MCEMACC.3q_consumo_21, MCEMACC.4q_consumo_21, MCEMACC.substatus_21, MCEMACC.testmenn_21, MCEMACC.testmecnn_21,
          MCEMACC.hora_22, MCEMACC.1q_consumo_22, MCEMACC.2q_consumo_22, MCEMACC.3q_consumo_22, MCEMACC.4q_consumo_22, MCEMACC.substatus_22, MCEMACC.testmenn_22, MCEMACC.testmecnn_22,
          MCEMACC.hora_23, MCEMACC.1q_consumo_23, MCEMACC.2q_consumo_23, MCEMACC.3q_consumo_23, MCEMACC.4q_consumo_23, MCEMACC.substatus_23, MCEMACC.testmenn_23, MCEMACC.testmecnn_23,
          MCEMACC.hora_24, MCEMACC.1q_consumo_24, MCEMACC.2q_consumo_24, MCEMACC.3q_consumo_24, MCEMACC.4q_consumo_24, MCEMACC.substatus_24, MCEMACC.testmenn_24, MCEMACC.testmecnn_24,
          MCEMACC.hora_25, MCEMACC.1q_consumo_25, MCEMACC.2q_consumo_25, MCEMACC.3q_consumo_25, MCEMACC.4q_consumo_25, MCEMACC.substatus_25, MCEMACC.testmenn_25, MCEMACC.testmecnn_25
          FROM MCEMACC WHERE MCEMACC.anomalia='S' AND  MCEMACC.flectreg BETWEEN add_months(MCEMACC.fapexpd,-6) AND MCEMACC.fapexpd
          ")

dfa <- take(la,100)




















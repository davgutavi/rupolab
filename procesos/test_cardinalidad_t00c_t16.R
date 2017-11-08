#Test cardinalidad Maestro Contratos, Expedientes

library(SparkR)
source("paths.R")
#sparkR.session(master = "local[*]", sparkConfig = list(spark.local.dir="/mnt/datos/tempSparkR",spark.network.timeout="10000000s",spark.executor.heartbeatInterval="10000000s"))
sparkR.session(master = "local[*]", sparkConfig = list(spark.local.dir="/mnt/datos/tempSparkR",spark.driver.memory="8g"))





t00C <- read.parquet(path00C)
createOrReplaceTempView(t00C,"MC")
t16 <- read.parquet(path16)
createOrReplaceTempView(t16,"E")

mce<-sql("SELECT MC.origen, MC.cptocred, MC.cfinca, MC.cptoserv, MC.cderind, MC.cupsree, MC.ccounips,MC.cupsree2, MC.cpuntmed, MC.tpuntmed, MC.vparsist, 
                 MC.cemptitu,MC.ccontrat, MC.cnumscct, MC.fpsercon, MC.ffinvesu,
                 E.csecexpe, E.fapexpd, E.finifran, E.ffinfran, E.anomalia, E.irregularidad, E.venacord, E.vennofai, E.torigexp, E.texpedie,E.expclass, 
                 E.testexpe, E.fnormali, E.cplan, E.ccampa, E.cempresa, E.fciexped
                 FROM MC JOIN E
                 ON MC.origen=E.origen AND MC.cfinca=E.cfinca AND MC.cptoserv=E.cptoserv AND MC.cderind=E.cderind AND E.fapexpd >= MC.fpsercon AND E.fapexpd <= MC.ffinvesu")

createOrReplaceTempView(mce,"MCE")


##########################################################################################Cardinalidad TAB_16 - TAB_00C



showDF(sql("SELECT origen, cfinca, cptoserv, cderind, csecexpe, count(DISTINCT *) AS contadorregistros
           FROM MCE
           GROUP BY origen, cfinca, cptoserv, cderind, csecexpe
           ORDER BY contadorregistros DESC    
          "),10,FALSE)


p01<-sql("SELECT DISTINCT * FROM MCE WHERE origen = 'F' AND cfinca = '6288715' AND cptoserv= '001' AND cderind = '001' AND csecexpe = '003'")
df01<-take(p01,10)

##########################################################################################Cardinalidad  TAB_00C - TAB_16 

showDF(sql("SELECT ccontrat,cnumscct, fpsercon, ffinvesu, cupsree, cpuntmed, tpuntmed, count(DISTINCT *) AS contadorregistros
           FROM MCE
           GROUP BY ccontrat,cnumscct, fpsercon, ffinvesu, cupsree, cpuntmed, tpuntmed
           ORDER BY contadorregistros DESC    
          "),10,FALSE)






########################################################################## expediente 1--1 cupsree 

showDF(sql("SELECT origen,cemptitu, cfinca, cptoserv, cderind,csecexpe,fapexpd,finifran,ffinfran,anomalia,irregularidad, venacord, vennofai, torigexp, texpedie,expclass, 
                   testexpe, fnormali, cplan, ccampa, cempresa, fciexped, COUNT(DISTINCT cupsree) AS cont_cupsree FROM MCE 
           GROUP BY origen,cemptitu, cfinca, cptoserv, cderind,csecexpe,fapexpd,finifran,ffinfran,anomalia,irregularidad, venacord, vennofai, torigexp, texpedie,expclass, 
                   testexpe, fnormali, cplan, ccampa, cempresa, fciexped
           ORDER BY cont_cupsree DESC
          "),10,FALSE)

showDF(sql("SELECT DISTINCT cupsree, origen, cemptitu, cfinca, cptoserv, cderind,csecexpe,fapexpd,finifran,ffinfran,anomalia,irregularidad, venacord, vennofai, torigexp, texpedie,expclass, 
                   testexpe, fnormali, cplan, ccampa, cempresa, fciexped FROM MCE WHERE
                   origen='F'AND cemptitu='00040' AND cfinca='5422294'AND cptoserv='007'AND cderind='001'AND csecexpe='003'AND fapexpd='2010-05-05' AND finifran='0002-11-30' AND ffinfran='0002-11-30' AND anomalia='N'AND
                   irregularidad = 'S'AND venacord=32000.0 AND vennofai= 0.0 AND torigexp='R'AND texpedie='412' AND expclass=4 AND 
                   testexpe='014' AND fnormali='2010-05-05'AND cplan='0000'AND ccampa='000'AND cempresa='00040'AND fciexped='2010-08-31'"),35,FALSE)


########################################################################## cupsree 1 -- 0..n expediente


showDF(sql("SELECT cupsree , COUNT(DISTINCT origen,cemptitu, cfinca, cptoserv, cderind,csecexpe,fapexpd,finifran,ffinfran,anomalia,irregularidad, venacord, vennofai, torigexp, texpedie,expclass, 
                   testexpe, fnormali, cplan, ccampa, cempresa, fciexped) AS cont_expediente FROM MCE 
           GROUP BY cupsree
           ORDER BY cont_expediente ASC
          "),400,FALSE)


showDF(sql("SELECT DISTINCT cupsree, origen,cemptitu, cfinca, cptoserv, cderind,csecexpe,fapexpd,finifran,ffinfran,anomalia,irregularidad, venacord, vennofai, torigexp, texpedie,expclass, 
                   testexpe, fnormali, cplan, ccampa, cempresa, fciexped FROM MCE WHERE cupsree = 'ES0031406251886001HL0F' 
           ORDER BY csecexpe ASC"),35,FALSE)



















#Construcción de tabla de periodos de lectura

library(SparkR)
source("paths.R")
sparkR.session(master = "local[*]", sparkConfig = list(spark.local.dir="/mnt/datos/tempSparkR"))


t00C <- read.parquet(path00C)
createOrReplaceTempView(t00C,"MC")

t16 <- read.parquet(path16)
createOrReplaceTempView(t16,"E")

mce <- sql("
          SELECT MC.origen, MC.cptocred, MC.cfinca, MC.cptoserv, MC.cderind, MC.cupsree, MC.ccounips,MC.cupsree2, MC.cpuntmed, MC.tpuntmed, MC.vparsist, MC.cemptitu,MC.ccontrat, MC.cnumscct, MC.fpsercon, MC.ffinvesu, 
                 E.csecexpe, E.fapexpd, E.finifran, E.ffinfran, E.anomalia, E.irregularidad, E.venacord, E.vennofai, E.torigexp, E.texpedie,E.expclass, E.testexpe, 
                 E.fnormali, E.cplan, E.ccampa, E.cempresa, E.fciexped    
                 FROM MC JOIN E
                 ON MC.origen=E.origen AND MC.cfinca=E.cfinca AND MC.cptoserv=E.cptoserv AND MC.cderind=E.cderind AND E.fapexpd >= MC.fpsercon
          ")



sparkR.session(master = "local[*]", sparkConfig = list(spark.local.dir="/mnt/datos/tempSparkR",spark.driver.memory="8g"))
t01 <- sql("SELECT cupsree,fapexpd,flectreg , datediff ( flectreg , fapexpd ) AS dif FROM I  ")
createOrReplaceTempView(t01,"t01")
showDF(t01,10,FALSE)
count(t01)

t02_aux <- sql("SELECT cupsree,add_months(flectreg,-6) AS regl, flectreg AS regr, dif, fapexpd FROM t01 
           WHERE (cupsree,dif) IN (SELECT cupsree, max(dif) as maximo FROM t01 GROUP BY cupsree)")

t02 <- dropDuplicates(t02_aux)

createOrReplaceTempView(t02,"t02")
showDF(t02,10,FALSE)
count(t02)

t03 <- sql("SELECT cupsree, COUNT(DISTINCT regl, regr, dif, fapexpd ) AS count FROM t02 GROUP BY cupsree HAVING count > 1 ORDER BY count DESC")
showDF(t03,100,FALSE)

t04 <- sql("SELECT * FROM t02 WHERE cupsree = 'ES0031406071805001RA0F'")
showDF(t04,10,FALSE)

cache(t04)

sparkR.stop()



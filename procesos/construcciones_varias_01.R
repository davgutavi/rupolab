#Proceso para probar el número de registros de t16
#Eliminación de duplicados de MCE
#Construcción LR dates
#Pruebas sobre t16








library(SparkR)
source("paths.R")
sparkR.session(master = "local[*]", sparkConfig = list(spark.local.dir="/mnt/datos/tempSparkR",spark.driver.memory="8g",spark.network.timeout="10000000s",spark.executor.heartbeatInterval="10000000s"))

t00C <- read.parquet(path00C)
createOrReplaceTempView(t00C,"MC")
t16 <- read.parquet(path16)
createOrReplaceTempView(t16,"E")
t01 <- read.parquet(path01)
createOrReplaceTempView(t01,"CC")

####################################################################################################################################################################################################

#Proceso para probar el número de registros de t16
count(t16) 

#Prueba de clave primaria de t16
t1 <- sql("SELECT DISTINCT origen, cfinca, cptoserv, cderind, csecexpe FROM E")
count(t1)
#Join MC - E
mce <- sql("SELECT MC.origen, MC.cptocred, MC.cfinca, MC.cptoserv, MC.cderind, MC.cupsree, MC.ccounips,MC.cupsree2, MC.cpuntmed, MC.tpuntmed, MC.vparsist, MC.cemptitu,MC.ccontrat, MC.cnumscct, MC.fpsercon, 
            MC.ffinvesu,E.csecexpe, E.fapexpd, E.finifran, E.ffinfran, E.anomalia, E.irregularidad, E.venacord, E.vennofai, E.torigexp, E.texpedie,E.expclass, E.testexpe, 
            E.fnormali, E.cplan, E.ccampa, E.cempresa, E.fciexped    
            FROM MC JOIN E
            ON MC.origen=E.origen AND MC.cfinca=E.cfinca AND MC.cptoserv=E.cptoserv AND MC.cderind=E.cderind AND E.fapexpd >= MC.fpsercon AND E.fapexpd <= MC.ffinvesu")
createOrReplaceTempView(mce,"MCE")
persist(mce,"MEMORY_AND_DISK")
count(mce)

p1 <- sql("SELECT * FROM  MCE WHERE fpsercon <> '0002-11-30' ")
showDF(p1,20)
View(take(p1,20))



#Probando la relación 1:1 de cups con expediente
t2<-sql("SELECT cupsree, COUNT (DISTINCT *) FROM MCE GROUP BY cupsree")
View(take(t2,200))

t3<-sql("SELECT DISTINCT origen, cptocred, cfinca, cptoserv, cderind, cupsree, ccounips, cupsree2, cpuntmed, tpuntmed, vparsist, cemptitu, fpsercon, 
            ffinvesu,csecexpe,fapexpd, finifran, ffinfran, anomalia, irregularidad, venacord,vennofai, torigexp, texpedie,expclass, testexpe, 
            fnormali, cplan, ccampa, cempresa, fciexped  FROM MCE WHERE cupsree = 'ES0031405404423014DT0F'")
View(take(t3,200))

#Mostrar registros con fpsercon = 2-11-30 y ffinvesu = 9999-12-31
t4<- sql("SELECT cupsree FROM MCE WHERE fpsercon = '0002-11-30' AND ffinvesu = '9999-12-31' ")
count(t4)
View(take(t4,200))

#Obtener los registros que tienen fechas finitas y que están relacionados con registros con fechas infinitas
t5<- sql("SELECT * FROM MCE WHERE cupsree IN
         (SELECT cupsree FROM MCE WHERE fpsercon = '0002-11-30' AND ffinvesu = '9999-12-31') ")
count(t5)
View(take(t5,200))

####################################################################################################################################################################################################

#Eliminación de duplicados de MCE

#1.- 2.- Eliminar duplicados por fapexpd >=fpsercon, fapexpd <= ffinvesu y por eliminación del secuencial de contrato del join
mce_axu <- sql("SELECT DISTINCT MC.origen, MC.cptocred, MC.cfinca, MC.cptoserv, MC.cderind, MC.cupsree, MC.ccounips,MC.cupsree2, MC.cpuntmed, MC.tpuntmed, MC.vparsist, MC.cemptitu,MC.ccontrat, MC.fpsercon, 
            MC.ffinvesu,E.csecexpe, E.fapexpd, E.finifran, E.ffinfran, E.anomalia, E.irregularidad, E.venacord, E.vennofai, E.torigexp, E.texpedie,E.expclass, E.testexpe, 
            E.fnormali, E.cplan, E.ccampa, E.cempresa, E.fciexped    
            FROM MC JOIN E
            ON MC.origen=E.origen AND MC.cfinca=E.cfinca AND MC.cptoserv=E.cptoserv AND MC.cderind=E.cderind AND E.fapexpd >= MC.fpsercon AND E.fapexpd <= MC.ffinvesu")
createOrReplaceTempView(mce_axu,"MCE_aux")
persist(mce_axu,"MEMORY_AND_DISK")
#count(mce)

#3.- Eliminar duplicados por fechas infinitas

mce<- sql("SELECT * FROM MCE_aux WHERE fpsercon <> '0002-11-30' OR ffinvesu <>  '9999-12-31' ")
createOrReplaceTempView(mce,"MCE")
persist(mce,"MEMORY_AND_DISK")
count(t5)
View(take(t5,200))

#u1 <- sql("SELECT COUNT (DISTINCT origen, cfinca, cptoserv, cderind, csecexpe) AS contador, cupsree FROM MCECC GROUP BY cupsree ")
#View(take(u1,100))
#count(u1)

#Enlazar con curvas de cargas


mcecc = sql("SELECT MCE.origen, MCE.cptocred, MCE.cfinca, MCE.cptoserv, MCE.cderind, MCE.cupsree, MCE.ccounips,MCE.cupsree2, MCE.cpuntmed, MCE.tpuntmed, MCE.vparsist, MCE.cemptitu,MCE.ccontrat, MCE.fpsercon, 
                    MCE.ffinvesu,MCE.csecexpe, MCE.fapexpd, MCE.finifran, MCE.ffinfran, MCE.anomalia, MCE.irregularidad, MCE.venacord, MCE.vennofai, MCE.torigexp, MCE.texpedie,MCE.expclass, MCE.testexpe, 
                   MCE.fnormali, MCE.cplan, MCE.ccampa, MCE.cempresa, MCE.fciexped,   
                   CC.flectreg, datediff(flectreg,fapexpd) AS diferencia,
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
                   CC.hora_15, CC.1q_consumo_15, CC.2q_consumo_15, CC.3q_consumo_15, CC.4q_consumo_15,CC.substatus_15, CC.testmenn_15, CC.testmecnn_15,
                   CC.hora_16, CC.1q_consumo_16, CC.2q_consumo_16, CC.3q_consumo_16, CC.4q_consumo_16,CC.substatus_16, CC.testmenn_16, CC.testmecnn_16,
                   CC.hora_17, CC.1q_consumo_17, CC.2q_consumo_17, CC.3q_consumo_17, CC.4q_consumo_17,CC.substatus_17, CC.testmenn_17, CC.testmecnn_17,
                   CC.hora_18, CC.1q_consumo_18, CC.2q_consumo_18, CC.3q_consumo_18, CC.4q_consumo_18,CC.substatus_18, CC.testmenn_18, CC.testmecnn_18,
                   CC.hora_19, CC.1q_consumo_19, CC.2q_consumo_19, CC.3q_consumo_19, CC.4q_consumo_19,CC.substatus_19, CC.testmenn_19, CC.testmecnn_19,
                   CC.hora_20, CC.1q_consumo_20, CC.2q_consumo_20, CC.3q_consumo_20, CC.4q_consumo_20,CC.substatus_20, CC.testmenn_20, CC.testmecnn_20,
                   CC.hora_21, CC.1q_consumo_21, CC.2q_consumo_21, CC.3q_consumo_21, CC.4q_consumo_21,CC.substatus_21, CC.testmenn_21, CC.testmecnn_21,
                   CC.hora_22, CC.1q_consumo_22, CC.2q_consumo_22, CC.3q_consumo_22, CC.4q_consumo_22,CC.substatus_22, CC.testmenn_22, CC.testmecnn_22,
                   CC.hora_23, CC.1q_consumo_23, CC.2q_consumo_23, CC.3q_consumo_23, CC.4q_consumo_23,CC.substatus_23, CC.testmenn_23, CC.testmecnn_23,
                   CC.hora_24, CC.1q_consumo_24, CC.2q_consumo_24, CC.3q_consumo_24, CC.4q_consumo_24,CC.substatus_24, CC.testmenn_24, CC.testmecnn_24,
                   CC.hora_25, CC.1q_consumo_25, CC.2q_consumo_25, CC.3q_consumo_25, CC.4q_consumo_25,CC.substatus_25, CC.testmenn_25, CC.testmecnn_25
                   FROM MCE JOIN CC
                   ON MCE.origen = CC.origen AND MCE.cpuntmed = CC.cpuntmed AND CC.obiscode = 'A' AND CC.testcaco = 'R'")
createOrReplaceTempView(mcecc,"MCECC")
#count(mcecc)
persist(mcecc,"MEMORY_AND_DISK")

# Construir LRDATES
#aux1 <- sql("SELECT cupsree,fapexpd, fciexped, origen, cfinca, cptoserv, cderind, csecexpe, max(diferencia) as maximo FROM MCECC 
#            GROUP BY cupsree,fapexpd, fciexped, origen, cfinca, cptoserv, cderind, csecexpe")
#count(aux1)
#createOrReplaceTempView(aux1,"AUX1")
#persist(aux1,"MEMORY_AND_DISK")
#df1<- take(aux1,100)
#aux2<- sql("SELECT DISTINCT cupsree FROM MCECC")
#count(aux2)
#aux3 <- sql("SELECT COUNT (DISTINCT cupsree) contador,fapexpd, fciexped, origen, cfinca, cptoserv, cderind, csecexpe FROM MCECC 
#            GROUP BY fapexpd, fciexped, origen, cfinca, cptoserv, cderind, csecexpe")
#showDF(aux3,20)
#count(aux3)



lrdates <- sql("SELECT DISTINCT cupsree, add_months(flectreg,-6) AS ldate, flectreg AS rdate, diferencia, fapexpd  FROM MCECC 
                WHERE diferencia<=0 AND add_months(fapexpd,-3)>flectreg AND (cupsree,diferencia) IN (SELECT cupsree, max(diferencia) as maximo FROM MCECC GROUP BY cupsree)")
persist(lrdates,"MEMORY_AND_DISK")
createOrReplaceTempView(lrdates,"LRDATES")
#count(lrdates)
#df4<- take(lrdates,210)

# JOIN mcecc - lrdates


mcecclr = sql("SELECT MCECC.origen, MCECC.cptocred, MCECC.cfinca, MCECC.cptoserv, MCECC.cderind, MCECC.cupsree, MCECC.ccounips,MCECC.cupsree2, MCECC.cpuntmed, MCECC.tpuntmed, MCECC.vparsist, MCECC.cemptitu,
                    MCECC.ccontrat, MCECC.fpsercon, MCECC.ffinvesu,MCECC.csecexpe, MCECC.fapexpd, MCECC.finifran, MCECC.ffinfran, MCECC.anomalia, MCECC.irregularidad, MCECC.venacord, MCECC.vennofai, 
                    MCECC.torigexp, MCECC.texpedie,MCECC.expclass, MCECC.testexpe, 
                   MCECC.fnormali, MCECC.cplan, MCECC.ccampa, MCECC.cempresa, MCECC.fciexped,   
                   MCECC.flectreg, LRDATES.ldate,LRDATES.rdate, LRDATES.diferencia,
                   MCECC.testcaco, MCECC.obiscode, MCECC.vsecccar, 
                   MCECC.hora_01, MCECC.1q_consumo_01, MCECC.2q_consumo_01, MCECC.3q_consumo_01, MCECC.4q_consumo_01,MCECC.substatus_01,MCECC.testmenn_01,MCECC.testmecnn_01,
                   MCECC.hora_02, MCECC.1q_consumo_02, MCECC.2q_consumo_02, MCECC.3q_consumo_02, MCECC.4q_consumo_02,MCECC.substatus_02,MCECC.testmenn_02,MCECC.testmecnn_02,
                   MCECC.hora_03, MCECC.1q_consumo_03, MCECC.2q_consumo_03, MCECC.3q_consumo_03, MCECC.4q_consumo_03,MCECC.substatus_03,MCECC.testmenn_03,MCECC.testmecnn_03,
                   MCECC.hora_04, MCECC.1q_consumo_04, MCECC.2q_consumo_04, MCECC.3q_consumo_04, MCECC.4q_consumo_04,MCECC.substatus_04,MCECC.testmenn_04,MCECC.testmecnn_04,
                   MCECC.hora_05, MCECC.1q_consumo_05, MCECC.2q_consumo_05, MCECC.3q_consumo_05, MCECC.4q_consumo_05,MCECC.substatus_05,MCECC.testmenn_05,MCECC.testmecnn_05,
                   MCECC.hora_06, MCECC.1q_consumo_06, MCECC.2q_consumo_06, MCECC.3q_consumo_06, MCECC.4q_consumo_06,MCECC.substatus_06,MCECC.testmenn_06,MCECC.testmecnn_06,
                   MCECC.hora_07, MCECC.1q_consumo_07, MCECC.2q_consumo_07, MCECC.3q_consumo_07, MCECC.4q_consumo_07,MCECC.substatus_07,MCECC.testmenn_07,MCECC.testmecnn_07,
                   MCECC.hora_08, MCECC.1q_consumo_08, MCECC.2q_consumo_08, MCECC.3q_consumo_08, MCECC.4q_consumo_08,MCECC.substatus_08,MCECC.testmenn_08,MCECC.testmecnn_08,
                   MCECC.hora_09, MCECC.1q_consumo_09, MCECC.2q_consumo_09, MCECC.3q_consumo_09, MCECC.4q_consumo_09,MCECC.substatus_09,MCECC.testmenn_09,MCECC.testmecnn_09,
                   MCECC.hora_10, MCECC.1q_consumo_10, MCECC.2q_consumo_10, MCECC.3q_consumo_10, MCECC.4q_consumo_10,MCECC.substatus_10,MCECC.testmenn_10,MCECC.testmecnn_10,
                   MCECC.hora_11, MCECC.1q_consumo_11, MCECC.2q_consumo_11, MCECC.3q_consumo_11, MCECC.4q_consumo_11,MCECC.substatus_11,MCECC.testmenn_11,MCECC.testmecnn_11,
                   MCECC.hora_12, MCECC.1q_consumo_12, MCECC.2q_consumo_12, MCECC.3q_consumo_12, MCECC.4q_consumo_12,MCECC.substatus_12,MCECC.testmenn_12,MCECC.testmecnn_12,
                   MCECC.hora_13, MCECC.1q_consumo_13, MCECC.2q_consumo_13, MCECC.3q_consumo_13, MCECC.4q_consumo_13,MCECC.substatus_13,MCECC.testmenn_13,MCECC.testmecnn_13,
                   MCECC.hora_14, MCECC.1q_consumo_14, MCECC.2q_consumo_14, MCECC.3q_consumo_14, MCECC.4q_consumo_14,MCECC.substatus_14,MCECC.testmenn_14,MCECC.testmecnn_14,
                   MCECC.hora_15, MCECC.1q_consumo_15, MCECC.2q_consumo_15, MCECC.3q_consumo_15, MCECC.4q_consumo_15,MCECC.substatus_15, MCECC.testmenn_15, MCECC.testmecnn_15,
                   MCECC.hora_16, MCECC.1q_consumo_16, MCECC.2q_consumo_16, MCECC.3q_consumo_16, MCECC.4q_consumo_16,MCECC.substatus_16, MCECC.testmenn_16, MCECC.testmecnn_16,
                   MCECC.hora_17, MCECC.1q_consumo_17, MCECC.2q_consumo_17, MCECC.3q_consumo_17, MCECC.4q_consumo_17,MCECC.substatus_17, MCECC.testmenn_17, MCECC.testmecnn_17,
                   MCECC.hora_18, MCECC.1q_consumo_18, MCECC.2q_consumo_18, MCECC.3q_consumo_18, MCECC.4q_consumo_18,MCECC.substatus_18, MCECC.testmenn_18, MCECC.testmecnn_18,
                   MCECC.hora_19, MCECC.1q_consumo_19, MCECC.2q_consumo_19, MCECC.3q_consumo_19, MCECC.4q_consumo_19,MCECC.substatus_19, MCECC.testmenn_19, MCECC.testmecnn_19,
                   MCECC.hora_20, MCECC.1q_consumo_20, MCECC.2q_consumo_20, MCECC.3q_consumo_20, MCECC.4q_consumo_20,MCECC.substatus_20, MCECC.testmenn_20, MCECC.testmecnn_20,
                   MCECC.hora_21, MCECC.1q_consumo_21, MCECC.2q_consumo_21, MCECC.3q_consumo_21, MCECC.4q_consumo_21,MCECC.substatus_21, MCECC.testmenn_21, MCECC.testmecnn_21,
                   MCECC.hora_22, MCECC.1q_consumo_22, MCECC.2q_consumo_22, MCECC.3q_consumo_22, MCECC.4q_consumo_22,MCECC.substatus_22, MCECC.testmenn_22, MCECC.testmecnn_22,
                   MCECC.hora_23, MCECC.1q_consumo_23, MCECC.2q_consumo_23, MCECC.3q_consumo_23, MCECC.4q_consumo_23,MCECC.substatus_23, MCECC.testmenn_23, MCECC.testmecnn_23,
                   MCECC.hora_24, MCECC.1q_consumo_24, MCECC.2q_consumo_24, MCECC.3q_consumo_24, MCECC.4q_consumo_24,MCECC.substatus_24, MCECC.testmenn_24, MCECC.testmecnn_24,
                   MCECC.hora_25, MCECC.1q_consumo_25, MCECC.2q_consumo_25, MCECC.3q_consumo_25, MCECC.4q_consumo_25,MCECC.substatus_25, MCECC.testmenn_25, MCECC.testmecnn_25
              FROM MCECC JOIN LRDATES
              ON MCECC.cupsree = LRDATES.cupsree AND MCECC.fapexpd = LRDATES.fapexpd AND MCECC.flectreg BETWEEN LRDATES.ldate AND LRDATES.rdate")
persist(mcecclr,"MEMORY_AND_DISK")
createOrReplaceTempView(mcecclr,"MCECCLR")
count(mcecclr)



cirr <- sql("SELECT * FROM MCECCLR WHERE irregularidad = 'S'")
count(cirr)

cano <- sql("SELECT * FROM MCECCLR WHERE anomalia = 'S'")
count (cano)




####################################################################################################################################################################################################



##Expedientes con cups asignado. 122306 registros (total de expedientes = 252435)
cups <- sql(" SELECT DISTINCT origen, cptocred, cfinca, cptoserv, cderind, cupsree, ccounips,cupsree2, cpuntmed, tpuntmed, vparsist, cemptitu,fpsercon, ffinvesu, 
                 csecexpe, fapexpd, finifran, ffinfran, anomalia, irregularidad, venacord, vennofai, torigexp, texpedie,expclass, testexpe, 
                 fnormali, cplan, ccampa, cempresa, fciexped  FROM MCE ")
createOrReplaceTempView(cups,"CUPS")
persist(cups,"MEMORY_AND_DISK")
View(take(cups,200))
count(cups)


##Expedientes que no tienen ningún cups asignado. 130129 registros (total de expedientes = 252435). 122306 asignados + 130129 sin asignar = 252435 en total
mceno <- sql("
          SELECT MC.origen, MC.cptocred, MC.cfinca, MC.cptoserv, MC.cderind, MC.cupsree, MC.ccounips,MC.cupsree2, MC.cpuntmed, MC.tpuntmed, MC.vparsist, MC.cemptitu,MC.ccontrat, MC.cnumscct, MC.fpsercon, MC.ffinvesu, 
                 E.csecexpe, E.fapexpd, E.finifran, E.ffinfran, E.anomalia, E.irregularidad, E.venacord, E.vennofai, E.torigexp, E.texpedie,E.expclass, E.testexpe, 
                 E.fnormali, E.cplan, E.ccampa, E.cempresa, E.fciexped    
                 FROM E LEFT JOIN MC
                 ON MC.origen=E.origen AND MC.cfinca=E.cfinca AND MC.cptoserv=E.cptoserv AND MC.cderind=E.cderind AND E.fapexpd >= MC.fpsercon AND E.fapexpd <=ffinvesu
                 WHERE MC.origen IS NULL AND MC.cfinca IS NULL AND MC.cptoserv IS NULL AND MC.cderind IS NULL AND MC.fpsercon IS NULL AND MC.ffinvesu IS NULL
          ")

createOrReplaceTempView(mceno,"MCENO")
persist(mceno,"MEMORY_AND_DISK")
count(mceno)
View(take(mceno,20))

sinasignar <- sql("SELECT DISTINCT * FROM MCENO")
count(sinasignar)



showDF(sql("SELECT * FROM MCE1 WHERE cupsree=  'ES0031405000007001CW0F' "),20,FALSE)


sparkR.stop()

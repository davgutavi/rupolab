#Joins varios 01

library(SparkR)
source("paths.R")
sparkR.session(master = "local[*]", sparkConfig = list(spark.local.dir="/mnt/datos/tempSparkR"))




conexp <- sql("
          SELECT MaestroContratos.origen, MaestroContratos.cptocred, MaestroContratos.cfinca, MaestroContratos.cptoserv, MaestroContratos.cderind, MaestroContratos.cupsree,
          MaestroContratos.ccounips,MaestroContratos.cupsree2, MaestroContratos.cpuntmed, MaestroContratos.tpuntmed, MaestroContratos.vparsist, MaestroContratos.cemptitu,
          MaestroContratos.ccontrat, MaestroContratos.cnumscct, MaestroContratos.fpsercon, MaestroContratos.ffinvesu,
          Expedientes.csecexpe, Expedientes.fapexpd, Expedientes.finifran, Expedientes.ffinfran, Expedientes.anomalia, Expedientes.irregularidad, 
          Expedientes.venacord, Expedientes.vennofai, Expedientes.torigexp, Expedientes.texpedie,Expedientes.expclass, Expedientes.testexpe, 
          Expedientes.fnormali, Expedientes.cplan, Expedientes.ccampa, Expedientes.cempresa, Expedientes.fciexped    
          FROM MaestroContratos JOIN Expedientes
          ON MaestroContratos.origen=Expedientes.origen AND MaestroContratos.cfinca=Expedientes.cfinca AND
          MaestroContratos.cptoserv=Expedientes.cptoserv AND MaestroContratos.cderind=Expedientes.cderind 
          ")
# Expedientes.fapexpd >= MaestroContratos.fpsercon AND Expedientes.fapexpd <= MaestroContratos.ffinvesu

createOrReplaceTempView(conexp,"MaestroContratosExpedientes")

conexpapa <- sql("
          SELECT MaestroContratosExpedientes.origen, MaestroContratosExpedientes.cptocred, MaestroContratosExpedientes.cfinca, MaestroContratosExpedientes.cptoserv, MaestroContratosExpedientes.cderind, MaestroContratosExpedientes.cupsree,
          MaestroContratosExpedientes.ccounips,MaestroContratosExpedientes.cupsree2, MaestroContratosExpedientes.cpuntmed, MaestroContratosExpedientes.tpuntmed, MaestroContratosExpedientes.vparsist, MaestroContratosExpedientes.cemptitu,
          MaestroContratosExpedientes.ccontrat, MaestroContratosExpedientes.cnumscct, MaestroContratosExpedientes.fpsercon, MaestroContratosExpedientes.ffinvesu,
          MaestroContratosExpedientes.csecexpe, MaestroContratosExpedientes.fapexpd, MaestroContratosExpedientes.finifran, MaestroContratosExpedientes.ffinfran, MaestroContratosExpedientes.anomalia, MaestroContratosExpedientes.irregularidad, 
          MaestroContratosExpedientes.venacord, MaestroContratosExpedientes.vennofai, MaestroContratosExpedientes.torigexp, MaestroContratosExpedientes.texpedie,MaestroContratosExpedientes.expclass, MaestroContratosExpedientes.testexpe, 
          MaestroContratosExpedientes.fnormali, MaestroContratosExpedientes.cplan, MaestroContratosExpedientes.ccampa, MaestroContratosExpedientes.cempresa, MaestroContratosExpedientes.fciexped,
          MaestroAparatos.csecptom, MaestroAparatos.fvigorpm, MaestroAparatos.fbajapm,MaestroAparatos.caparmed
          FROM MaestroContratosExpedientes JOIN MaestroAparatos
          ON MaestroContratosExpedientes.origen = MaestroAparatos.origen AND MaestroContratosExpedientes.cupsree2 = MaestroAparatos.cupsree2 AND MaestroContratosExpedientes.cpuntmed = MaestroAparatos.cpuntmed
          ")
createOrReplaceTempView(conexpapa,"MaestroContratosExpedientesMaestroAparatos")

conexpapacur<- sql("
          SELECT MaestroContratosExpedientesMaestroAparatos.origen, MaestroContratosExpedientesMaestroAparatos.cptocred, MaestroContratosExpedientesMaestroAparatos.cfinca, MaestroContratosExpedientesMaestroAparatos.cptoserv, 
          MaestroContratosExpedientesMaestroAparatos.cderind, MaestroContratosExpedientesMaestroAparatos.cupsree,MaestroContratosExpedientesMaestroAparatos.ccounips,MaestroContratosExpedientesMaestroAparatos.cupsree2, 
          MaestroContratosExpedientesMaestroAparatos.cpuntmed, MaestroContratosExpedientesMaestroAparatos.tpuntmed, MaestroContratosExpedientesMaestroAparatos.vparsist, MaestroContratosExpedientesMaestroAparatos.cemptitu,
          MaestroContratosExpedientesMaestroAparatos.ccontrat, MaestroContratosExpedientesMaestroAparatos.cnumscct, MaestroContratosExpedientesMaestroAparatos.fpsercon, MaestroContratosExpedientesMaestroAparatos.ffinvesu,
          MaestroContratosExpedientesMaestroAparatos.csecexpe, MaestroContratosExpedientesMaestroAparatos.fapexpd, MaestroContratosExpedientesMaestroAparatos.finifran, MaestroContratosExpedientesMaestroAparatos.ffinfran, 
          MaestroContratosExpedientesMaestroAparatos.anomalia, MaestroContratosExpedientesMaestroAparatos.irregularidad,MaestroContratosExpedientesMaestroAparatos.venacord, MaestroContratosExpedientesMaestroAparatos.vennofai, 
          MaestroContratosExpedientesMaestroAparatos.torigexp, MaestroContratosExpedientesMaestroAparatos.texpedie,MaestroContratosExpedientesMaestroAparatos.expclass, MaestroContratosExpedientesMaestroAparatos.testexpe, 
          MaestroContratosExpedientesMaestroAparatos.fnormali, MaestroContratosExpedientesMaestroAparatos.cplan, MaestroContratosExpedientesMaestroAparatos.ccampa, MaestroContratosExpedientesMaestroAparatos.cempresa, 
          MaestroContratosExpedientesMaestroAparatos.fciexped,MaestroContratosExpedientesMaestroAparatos.csecptom, MaestroContratosExpedientesMaestroAparatos.fvigorpm, MaestroContratosExpedientesMaestroAparatos.fbajapm,
          MaestroContratosExpedientesMaestroAparatos.caparmed,
          CurvasCarga.flectreg, CurvasCarga.testcaco, CurvasCarga.obiscode, CurvasCarga.vsecccar,
          CurvasCarga.hora_01, CurvasCarga.1q_consumo_01, CurvasCarga.2q_consumo_01, CurvasCarga.3q_consumo_01, CurvasCarga.4q_consumo_01,CurvasCarga.substatus_01,CurvasCarga.testmenn_01,CurvasCarga.testmecnn_01,
          CurvasCarga.hora_02, CurvasCarga.1q_consumo_02, CurvasCarga.2q_consumo_02, CurvasCarga.3q_consumo_02, CurvasCarga.4q_consumo_02,CurvasCarga.substatus_02,CurvasCarga.testmenn_02,CurvasCarga.testmecnn_02,
          CurvasCarga.hora_03, CurvasCarga.1q_consumo_03, CurvasCarga.2q_consumo_03, CurvasCarga.3q_consumo_03, CurvasCarga.4q_consumo_03,CurvasCarga.substatus_03,CurvasCarga.testmenn_03,CurvasCarga.testmecnn_03,
          CurvasCarga.hora_04, CurvasCarga.1q_consumo_04, CurvasCarga.2q_consumo_04, CurvasCarga.3q_consumo_04, CurvasCarga.4q_consumo_04,CurvasCarga.substatus_04,CurvasCarga.testmenn_04,CurvasCarga.testmecnn_04,
          CurvasCarga.hora_05, CurvasCarga.1q_consumo_05, CurvasCarga.2q_consumo_05, CurvasCarga.3q_consumo_05, CurvasCarga.4q_consumo_05,CurvasCarga.substatus_05,CurvasCarga.testmenn_05,CurvasCarga.testmecnn_05,
          CurvasCarga.hora_06, CurvasCarga.1q_consumo_06, CurvasCarga.2q_consumo_06, CurvasCarga.3q_consumo_06, CurvasCarga.4q_consumo_06,CurvasCarga.substatus_06,CurvasCarga.testmenn_06,CurvasCarga.testmecnn_06,
          CurvasCarga.hora_07, CurvasCarga.1q_consumo_07, CurvasCarga.2q_consumo_07, CurvasCarga.3q_consumo_07, CurvasCarga.4q_consumo_07,CurvasCarga.substatus_07,CurvasCarga.testmenn_07,CurvasCarga.testmecnn_07,
          CurvasCarga.hora_08, CurvasCarga.1q_consumo_08, CurvasCarga.2q_consumo_08, CurvasCarga.3q_consumo_08, CurvasCarga.4q_consumo_08,CurvasCarga.substatus_08,CurvasCarga.testmenn_08,CurvasCarga.testmecnn_08,
          CurvasCarga.hora_09, CurvasCarga.1q_consumo_09, CurvasCarga.2q_consumo_09, CurvasCarga.3q_consumo_09, CurvasCarga.4q_consumo_09,CurvasCarga.substatus_09,CurvasCarga.testmenn_09,CurvasCarga.testmecnn_09,
          CurvasCarga.hora_10, CurvasCarga.1q_consumo_10, CurvasCarga.2q_consumo_10, CurvasCarga.3q_consumo_10, CurvasCarga.4q_consumo_10,CurvasCarga.substatus_10,CurvasCarga.testmenn_10,CurvasCarga.testmecnn_10,
          CurvasCarga.hora_11, CurvasCarga.1q_consumo_11, CurvasCarga.2q_consumo_11, CurvasCarga.3q_consumo_11, CurvasCarga.4q_consumo_11,CurvasCarga.substatus_11,CurvasCarga.testmenn_11,CurvasCarga.testmecnn_11,
          CurvasCarga.hora_12, CurvasCarga.1q_consumo_12, CurvasCarga.2q_consumo_12, CurvasCarga.3q_consumo_12, CurvasCarga.4q_consumo_12,CurvasCarga.substatus_12,CurvasCarga.testmenn_12,CurvasCarga.testmecnn_12,
          CurvasCarga.hora_13, CurvasCarga.1q_consumo_13, CurvasCarga.2q_consumo_13, CurvasCarga.3q_consumo_13, CurvasCarga.4q_consumo_13,CurvasCarga.substatus_13,CurvasCarga.testmenn_13,CurvasCarga.testmecnn_13,
          CurvasCarga.hora_14, CurvasCarga.1q_consumo_14, CurvasCarga.2q_consumo_14, CurvasCarga.3q_consumo_14, CurvasCarga.4q_consumo_14,CurvasCarga.substatus_14,CurvasCarga.testmenn_14,CurvasCarga.testmecnn_14,
          CurvasCarga.hora_15, CurvasCarga.1q_consumo_15, CurvasCarga.2q_consumo_15, CurvasCarga.3q_consumo_15, CurvasCarga.4q_consumo_15, CurvasCarga.substatus_15, CurvasCarga.testmenn_15, CurvasCarga.testmecnn_15,
          CurvasCarga.hora_16, CurvasCarga.1q_consumo_16, CurvasCarga.2q_consumo_16, CurvasCarga.3q_consumo_16, CurvasCarga.4q_consumo_16, CurvasCarga.substatus_16, CurvasCarga.testmenn_16, CurvasCarga.testmecnn_16,
          CurvasCarga.hora_17, CurvasCarga.1q_consumo_17, CurvasCarga.2q_consumo_17, CurvasCarga.3q_consumo_17, CurvasCarga.4q_consumo_17, CurvasCarga.substatus_17, CurvasCarga.testmenn_17, CurvasCarga.testmecnn_17,
          CurvasCarga.hora_18, CurvasCarga.1q_consumo_18, CurvasCarga.2q_consumo_18, CurvasCarga.3q_consumo_18, CurvasCarga.4q_consumo_18, CurvasCarga.substatus_18, CurvasCarga.testmenn_18, CurvasCarga.testmecnn_18,
          CurvasCarga.hora_19, CurvasCarga.1q_consumo_19, CurvasCarga.2q_consumo_19, CurvasCarga.3q_consumo_19, CurvasCarga.4q_consumo_19, CurvasCarga.substatus_19, CurvasCarga.testmenn_19, CurvasCarga.testmecnn_19,
          CurvasCarga.hora_20, CurvasCarga.1q_consumo_20, CurvasCarga.2q_consumo_20, CurvasCarga.3q_consumo_20, CurvasCarga.4q_consumo_20, CurvasCarga.substatus_20, CurvasCarga.testmenn_20, CurvasCarga.testmecnn_20,
          CurvasCarga.hora_21, CurvasCarga.1q_consumo_21, CurvasCarga.2q_consumo_21, CurvasCarga.3q_consumo_21, CurvasCarga.4q_consumo_21, CurvasCarga.substatus_21, CurvasCarga.testmenn_21, CurvasCarga.testmecnn_21,
          CurvasCarga.hora_22, CurvasCarga.1q_consumo_22, CurvasCarga.2q_consumo_22, CurvasCarga.3q_consumo_22, CurvasCarga.4q_consumo_22, CurvasCarga.substatus_22, CurvasCarga.testmenn_22, CurvasCarga.testmecnn_22,
          CurvasCarga.hora_23, CurvasCarga.1q_consumo_23, CurvasCarga.2q_consumo_23, CurvasCarga.3q_consumo_23, CurvasCarga.4q_consumo_23, CurvasCarga.substatus_23, CurvasCarga.testmenn_23, CurvasCarga.testmecnn_23,
          CurvasCarga.hora_24, CurvasCarga.1q_consumo_24, CurvasCarga.2q_consumo_24, CurvasCarga.3q_consumo_24, CurvasCarga.4q_consumo_24, CurvasCarga.substatus_24, CurvasCarga.testmenn_24, CurvasCarga.testmecnn_24,
          CurvasCarga.hora_25, CurvasCarga.1q_consumo_25, CurvasCarga.2q_consumo_25, CurvasCarga.3q_consumo_25, CurvasCarga.4q_consumo_25, CurvasCarga.substatus_25, CurvasCarga.testmenn_25, CurvasCarga.testmecnn_25
          FROM MaestroContratosExpedientesMaestroAparatos JOIN CurvasCarga
          ON MaestroContratosExpedientesMaestroAparatos.origen = CurvasCarga.origen AND MaestroContratosExpedientesMaestroAparatos.cpuntmed = CurvasCarga.cpuntmed
          AND CurvasCarga.obiscode = 'A' AND CurvasCarga.testcaco = 'R' AND 
          CurvasCarga.flectreg >= MaestroContratosExpedientesMaestroAparatos.fpsercon AND CurvasCarga.flectreg <= MaestroContratosExpedientesMaestroAparatos.ffinvesu
          ")

createOrReplaceTempView(conexpapacur,"MaestroContratosExpedientesMaestroAparatosCurvasCarga")


conirregularidad<- sql("
          SELECT MaestroContratos.origen, MaestroContratos.cptocred, MaestroContratos.cfinca, MaestroContratos.cptoserv, MaestroContratos.cderind, MaestroContratos.cupsree,
          MaestroContratos.ccounips,MaestroContratos.cupsree2, MaestroContratos.cpuntmed, MaestroContratos.tpuntmed, MaestroContratos.vparsist, MaestroContratos.cemptitu,
          MaestroContratos.ccontrat, MaestroContratos.cnumscct, MaestroContratos.fpsercon, MaestroContratos.ffinvesu,
          Expedientes.csecexpe, Expedientes.fapexpd, Expedientes.finifran, Expedientes.ffinfran, Expedientes.anomalia, Expedientes.irregularidad, 
          Expedientes.venacord, Expedientes.vennofai, Expedientes.torigexp, Expedientes.texpedie,Expedientes.expclass, Expedientes.testexpe, 
          Expedientes.fnormali, Expedientes.cplan, Expedientes.ccampa, Expedientes.cempresa, Expedientes.fciexped    
          FROM MaestroContratos JOIN Expedientes
          ON MaestroContratos.origen=Expedientes.origen AND MaestroContratos.cfinca=Expedientes.cfinca AND
          MaestroContratos.cptoserv=Expedientes.cptoserv AND MaestroContratos.cderind=Expedientes.cderind AND
          Expedientes.fapexpd >= MaestroContratos.fpsercon AND Expedientes.fapexpd <= MaestroContratos.ffinvesu AND irregularidad = 'S'
          ")
#persist(conirregularidad,"DISK_ONLY")

conanomalia<- sql("
          SELECT MaestroContratos.origen, MaestroContratos.cptocred, MaestroContratos.cfinca, MaestroContratos.cptoserv, MaestroContratos.cderind, MaestroContratos.cupsree,
          MaestroContratos.ccounips,MaestroContratos.cupsree2, MaestroContratos.cpuntmed, MaestroContratos.tpuntmed, MaestroContratos.vparsist, MaestroContratos.cemptitu,
          MaestroContratos.ccontrat, MaestroContratos.cnumscct, MaestroContratos.fpsercon, MaestroContratos.ffinvesu,
          Expedientes.csecexpe, Expedientes.fapexpd, Expedientes.finifran, Expedientes.ffinfran, Expedientes.anomalia, Expedientes.irregularidad, 
          Expedientes.venacord, Expedientes.vennofai, Expedientes.torigexp, Expedientes.texpedie,Expedientes.expclass, Expedientes.testexpe, 
          Expedientes.fnormali, Expedientes.cplan, Expedientes.ccampa, Expedientes.cempresa, Expedientes.fciexped    
          FROM MaestroContratos JOIN Expedientes
          ON MaestroContratos.origen=Expedientes.origen AND MaestroContratos.cfinca=Expedientes.cfinca AND
          MaestroContratos.cptoserv=Expedientes.cptoserv AND MaestroContratos.cderind=Expedientes.cderind AND
          Expedientes.fapexpd <= MaestroContratos.fpsercon AND Expedientes.fapexpd <= MaestroContratos.ffinvesu AND anomalia = 'S'
          ")

#todos los expedientes relacionados con ccontrat = 180000836140
t01<-  sql("
          SELECT * FROM MaestroContratosExpedientes WHERE ccontrat = 210016945200 AND irregularidad = 'S'  
          ")

t02<-  sql("
            SELECT  * FROM MaestroContratosExpedientes WHERE ccontrat = 210016945200   
          ")

df3<-take(t02,100)



t03<-  sql("
            SELECT  * FROM MaestroContratos WHERE ccontrat = 180000836140   
          ")

df4<-take(t03,20)


#DISTINCT
#persist(conanomalia,"DISK_ONLY")

head(conanomalia)
count(conanomalia)

head(conirregularidad)
count(conirregularidad)

df1<-take(conirregularidad,40)



df<-take(conexp,30)










sparkR.stop()


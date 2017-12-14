source("api/system_initialization.R")
source("api/clasificacion.R")
#######################################################################################################
#######################################################################################################

#**Carga de resultados clasificación
experiment<-loadClasificationResults(clasificacion_454d_nrl_slo)

rtest<-experiment[[1]]
rcamp<-experiment[[2]]
study<-experiment[[3]]

#NOTA: probar con experimentos no transformados
#**Obtener los conjuntos de confusión

ct <- getConfusionSets(rtest)
cc <- getConfusionSets(rcamp)

ct_legal <- ct[[1]]
NROW(ct_legal)
ct_fraud <- ct[[2]]
NROW(ct_fraud)
ct_tn    <- ct[[3]]
NROW(ct_tn)
ct_fn    <- ct[[4]]
NROW(ct_fn)
ct_fp    <- ct[[5]]
NROW(ct_fp)
ct_tp    <- ct[[6]]
NROW(ct_tp)

#**Obtener los conjuntos de confusión de un experimento realizando conversión previa de datos transformados a datos originales
dataset<-read.df(l_path_454d_slo_umr)

ct <- getConfusionSetsFromSlopes(dataset,rtest)
cc <- getConfusionSetsFromSlopes(dataset,rcamp)

ct_legal <- ct[[1]]
NROW(ct_legal)
ct_fraud <- ct[[2]]
NROW(ct_fraud)
ct_tn    <- ct[[3]]
NROW(ct_tn)
ct_fn    <- ct[[4]]
NROW(ct_fn)
ct_fp    <- ct[[5]]
NROW(ct_fp)
ct_tp    <- ct[[6]]
NROW(ct_tp)

#**Graficar la matriz de confusión
lcm <- confusionMatrixPieChart(study)

plot(lcm[[1]])
plot(lcm[[2]])

#**Graficar las medidas basadas en la matriz de confusión
lms <- measuresBarChart(study)

plot(lms[[1]])
plot(lms[[2]])

#**Graficar un porcentaje de las curvas de consumo de los resultados
g1<-getGraphCurves(ct_fn,numero_de_series = 4)
print(g1)

#NOTA: probar mezclando experimentos transformados y no transformados
#**Unir experimentos para un posterior análisis comparativo
e1<-loadClasificationResults(clasificacion_364d_con_slo)
e2<-loadClasificationResults(clasificacion_364d_nrl_slo)
e3<-loadClasificationResults(clasificacion_454d_con_slo)
e4<-loadClasificationResults(clasificacion_454d_nrl_slo)

experiments<-joinExperiments(list(e1,e2,e3,e4))

test<-experiments[[1]]
field<-experiments[[2]]

#**Graficar la matriz de confusión y las medidas de validación para comparación de experimentos
gr2<-experimentsCfMatrixBarChart(c("tn","fn","fp","tp"),test)
print(gr2)

gr3<-experimentsCfMatrixBarChart(c("tpr","tnr","ppv","npv","fnr","fpr","fdr","for","acc","f1","mcc","bm","mk"),test)
print(gr3)

gr4<-experimentsCfMatrixBarChart(c("tn","fn","fp","tp"),field)
print(gr4)

gr5<-experimentsCfMatrixBarChart(c("tpr","tnr","ppv","npv","fnr","fpr","fdr","for","acc","f1","mcc","bm","mk"),field)
print(gr5)

#**Obtener el código Latex de la matriz de confusión y las medidas de validación para comparación de experimentos
tl <- experimentsCfMatrixLatex(c("tn","fn","fp","tp","acc","ppv"),test)
tl

#######################################################################################################
sparkR.stop()
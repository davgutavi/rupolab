#Rutina para graficar los resultados del clustering sobre el dataset t123_d

# Para un experimento de clasificación:
# - Matriz de confusión: pie chart
# - Medidas de validación: barplot
# - Curvas: conversor pendiente-consumo + linechart

# Comparación de experimentos:
# - Matriz de confusión: radar, linechart
# - Medidas de validación: radar, linechart

# Clustering:
# - Centroides (sólo valores reales y max): linechart 
# - Caracterización sobre el total: multi piechart
# - Caracterización sobre el cluster: multi piechart

# Para documentación de comparación de modelos:
# - LATEX: Matriz de confusión: filas: variables, columnas: modelos
# - LATEX: Medidas de validación: filas: variables, columnas: modelos

source("api/carga_paquetes_variables_globales.R")
source("api/graficas_dataset_t123_d.R")

#######################################################################################################










#######################################################################################################
#**********************Graficar la matriz de confusión y las medidas de validación para comparación de experimentos
e1<-loadClasificationResults(clasificacion_364d_con)
e2<-loadClasificationResults(clasificacion_364d_nrl)
e3<-loadClasificationResults(clasificacion_454d_con)
e4<-loadClasificationResults(clasificacion_454d_nrl)

experiments<-joinExperiments(list(e1,e2,e3,e4))

test<-experiments[[1]]
field<-experiments[[2]]

gr2<-experimentsCfMatrixBarChart(c("tn","fn","fp","tp"),test)
print(gr2)

gr3<-experimentsCfMatrixBarChart(c("tpr","tnr","ppv","npv","fnr","fpr","fdr","for","acc","f1","mcc","bm","mk"),test)
print(gr3)
#**********************Graficar un porcentaje de las curvas de consumo de los resultados
g1<-getGraphCurves(ct_fp,percentage = 0.3)
print(g1)

#**********************Transformar los resultados de modelos de pendientes en valores de consumo
dataset<-read.df(path_t123_364d_con)

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
#**********************Graficar las medidas basadas en la matriz de confusión
lms <- measuresBarChart(study)

plot(lms[[1]])
plot(lms[[2]])
#**********************Graficar la matriz de confusión
lcm <- confusionMatrixPieChart(study)

plot(lcm[[1]])
plot(lcm[[2]])
#**********************Carga de resultados clasificación
experiment<-loadClasificationResults(clasificacion_364d_con)

rtest<-experiment[[1]]
rcamp<-experiment[[2]]
study<-experiment[[3]]

#######################################################################################################
sparkR.stop()
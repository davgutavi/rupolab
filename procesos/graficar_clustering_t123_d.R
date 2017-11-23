#Rutina para graficar los resultados del clustering sobre el dataset t123_d
#require("reshape2")
#require(ggrepel)
# t123_454d_all <- read.parquet(path_t123_454d_all)
# t123_454d_con <- read.parquet(path_t123_454d_con)
# t123_454d_nrl <- read.parquet(path_t123_454d_nrl)
# t123_364d_all <- read.parquet(path_t123_364d_all)
# t123_364d_con <- read.parquet(path_t123_364d_con)
# t123_364d_nrl <- read.parquet(path_t123_364d_nrl)
# t123_454d_all_slo <- read.parquet(path_t123_454d_all_slo)
# t123_454d_con_slo <- read.parquet(path_t123_454d_con_slo)
# t123_454d_nrl_slo <- read.parquet(path_t123_454d_nrl_slo)
# t123_364d_all_slo <- read.parquet(path_t123_364d_all_slo)
# t123_364d_con_slo <- read.parquet(path_t123_364d_con_slo)
# t123_364d_nrl_slo <- read.parquet(path_t123_364d_nrl_slo)

source("api/carga_paquetes_variables_globales.R")
source("api/graficas_dataset_t123_d.R")


#Paths datasets completos
path364d <-"/Users/davgutavi/Desktop/endesa_pendientes_fourier/datasets/t123_364d"
path454d <-"/Users/davgutavi/Desktop/endesa_pendientes_fourier/datasets/t123_454d"

#Entradas clustering
clu_exp_path <- "/Users/davgutavi/Desktop/endesa_pendientes_fourier/clustering/t123_364d_pen_01"

#Entradas clasificación
cla_exp_path <-"/Users/davgutavi/Desktop/endesa_pendientes_fourier/clasificacion/t123_364d_pen_10"
rcampPath <-paste0(cla_exp_path,"/t123_364d_pen_10_rcamp")
rtestPath <-paste0(cla_exp_path,"/t123_364d_pen_10_rtest")

#**********************Carga de resultados
l<-loadClasificationResults(cla_exp_path)

rtest<-l[[1]]
rcamp<-l[[2]]
study<-l[[3]]

#**********************Graficar la matriz de confusión
lcm <- confusionMatrixPieChart(study)

plot(lcm[[1]])
plot(lcm[[2]])

#**********************Graficar las medidas basadas en la matriz de confusión
lms <- measuresBarChart(study)

plot(lms[[1]])
plot(lms[[2]])

#**********************Transformar los resultados de modelos de pendientes en valores de consumo
dataset<-read.df(path364d)
rt <- read.df(rcampPath)
rc <- read.df(rtestPath)

ct <- getConfusionSetsFromSlopes(dataset,rt)
cc <- getConfusionSetsFromSlopes(dataset,rc)

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

#**********************Graficar un porcentaje de las curvas de consumo de los resultados

g1<-getGraphCurves(ct_fp,percentage = 0.1)
print(g1)

#######################################################################################################


sparkR.stop()
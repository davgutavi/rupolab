rootHDFSdatasets <- "hdfs://192.168.47.247/user/datos/endesa/datasets/"
rootLocalDatasets <-"/Users/davgutavi/NAS_PROYECTOS/smartfd/investigacion/datasets/"
rootLocalClasificacion <-"/Users/davgutavi/NAS_DAVGUTAVI/endesa/investigacion_paper/clasificacion/"
rootLocalClustering <-"/Users/davgutavi/NAS_DAVGUTAVI/endesa/investigacion_paper/clustering/"

#*******************************************CLUSTERING EXPERIMETS LOCAL

l_path_454d_raw_umr_kmeans<-paste0(rootLocalClustering,"lc_454d_raw_umr")
l_path_454d_raw_con_kmeans<-paste0(rootLocalClustering,"lc_454d_raw_con")
l_path_454d_max_umr_kmeans<-paste0(rootLocalClustering,"lc_454d_max_umr")
l_path_454d_max_con_kmeans<-paste0(rootLocalClustering,"lc_454d_max_con")
l_path_454d_slo_umr_kmeans<-paste0(rootLocalClustering,"lc_454d_slo_umr")
l_path_454d_slo_con_kmeans<-paste0(rootLocalClustering,"lc_454d_slo_con")

l_path_364d_slo_umr_kmeans<-paste0(rootLocalClustering,"lc_364d_raw_umr")
l_path_364d_slo_con_kmeans<-paste0(rootLocalClustering,"lc_364d_raw_con")
l_path_364d_raw_umr_kmeans<-paste0(rootLocalClustering,"lc_364d_max_umr")
l_path_364d_raw_con_kmeans<-paste0(rootLocalClustering,"lc_364d_max_con")
l_path_364d_max_umr_kmeans<-paste0(rootLocalClustering,"lc_364d_slo_umr")
l_path_364d_max_con_kmeans<-paste0(rootLocalClustering,"lc_364d_slo_con")

#*******************************************CLASIFICATION EXPERIMETS LOCAL

clasificacion_364d_con_slo <- paste0(rootLocalClasificacion,"s_364d_con_slo")
clasificacion_364d_nrl_slo <- paste0(rootLocalClasificacion,"s_364d_nrl_slo")
clasificacion_454d_con_slo <- paste0(rootLocalClasificacion,"s_454d_con_slo")
clasificacion_454d_nrl_slo <- paste0(rootLocalClasificacion,"s_454d_nrl_slo")

#*******************************************DATASETS ROOTS LOCAL

l_path_454d_raw_umr<- paste0(rootLocalDatasets,"454d_raw_umr/454d_raw_umr")
l_path_454d_raw_con<- paste0(rootLocalDatasets,"454d_raw_con/454d_raw_con")
l_path_454d_max_umr<- paste0(rootLocalDatasets,"454d_max_umr/454d_max_umr")
l_path_454d_max_con<- paste0(rootLocalDatasets,"454d_max_con/454d_max_con")
l_path_454d_slo_umr<- paste0(rootLocalDatasets,"454d_slo_umr/454d_slo_umr")
l_path_454d_slo_con<- paste0(rootLocalDatasets,"454d_slo_con/454d_slo_con")

l_path_364d_raw_umr<- paste0(rootLocalDatasets,"364d_raw_umr/364d_raw_umr")
l_path_364d_raw_con<- paste0(rootLocalDatasets,"364d_raw_con/364d_raw_con")
l_path_364d_raw_umr<- paste0(rootLocalDatasets,"364d_max_umr/364d_max_umr")
l_path_364d_raw_con<- paste0(rootLocalDatasets,"364d_max_con/364d_max_con")
l_path_364d_slo_umr<- paste0(rootLocalDatasets,"364d_slo_umr/364d_slo_umr")
l_path_364d_slo_con<- paste0(rootLocalDatasets,"364d_slo_con/364d_slo_con")

#*******************************************DATASETS ROOTS HDFS

r_path_454d_raw_umr<- paste0(rootHDFSdatasets,"454d_raw_umr/454d_raw_umr")
r_path_454d_raw_con<- paste0(rootHDFSdatasets,"454d_raw_con/454d_raw_con")
r_path_454d_max_umr<- paste0(rootHDFSdatasets,"454d_max_umr/454d_max_umr")
r_path_454d_max_con<- paste0(rootHDFSdatasets,"454d_max_con/454d_max_con")
r_path_454d_slo_umr<- paste0(rootHDFSdatasets,"454d_slo_umr/454d_slo_umr")
r_path_454d_slo_con<- paste0(rootHDFSdatasets,"454d_slo_con/454d_slo_con")

r_path_364d_raw_umr<- paste0(rootHDFSdatasets,"364d_raw_umr/364d_raw_umr")
r_path_364d_raw_con<- paste0(rootHDFSdatasets,"364d_raw_con/364d_raw_con")
r_path_364d_raw_umr<- paste0(rootHDFSdatasets,"364d_max_umr/364d_max_umr")
r_path_364d_raw_con<- paste0(rootHDFSdatasets,"364d_max_con/364d_max_con")
r_path_364d_slo_umr<- paste0(rootHDFSdatasets,"364d_slo_umr/364d_slo_umr")
r_path_364d_slo_con<- paste0(rootHDFSdatasets,"364d_slo_con/364d_slo_con")
source("api/paths_datasets_clustering_clasificacion.R")

#**********************Carga de resultados de un experimento completo de clustering (cada experimento tiene varios modelos)
models <- loadClusteringModels(l_path_454d_raw_umr_kmeans)

c1<-models[[1]]
c1cen<-c1[[1]]
c1clu<-c1[[2]]
c1std<-c1[[3]]
c2<-models[[2]]
c2cen<-c2[[1]]
c2clu<-c2[[2]]
c2std<-c2[[3]]
c3<-models[[3]]
c3cen<-c3[[1]]
c3clu<-c3[[2]]
c3std<-c3[[3]]
c4<-models[[4]]
c4cen<-c4[[1]]
c4clu<-c4[[2]]
c4std<-c4[[3]]
c5<-models[[5]]
c5cen<-c5[[1]]
c5clu<-c5[[2]]
c5std<-c5[[3]]
c6<-models[[6]]
c6cen<-c6[[1]]
c6clu<-c6[[2]]
c6std<-c6[[3]]
c7<-models[[7]]
c7cen<-c7[[1]]
c7clu<-c7[[2]]
c7std<-c7[[3]]
c8<-models[[8]]
c8cen<-c8[[1]]
c8clu<-c8[[2]]
c8std<-c8[[3]]

#**********************Graficar los mapas de calor de un estudio de un modelo de clustering
gr <- getClusteringHeatMaps(c8std)

plot(gr[[1]])
plot(gr[[2]])
plot(gr[[3]])

#**********************Graficar los mapas de calor de todos los estudios de todos los modelos de un experimento 
hmps<-getClusteringExperimentHeatMaps(models)

c1hms<-hmps[[1]]
c1g1<-c1hms[[1]]
c1g2<-c1hms[[2]]
c1g3<-c1hms[[3]]
c2hms<-hmps[[2]]
c2g1<-c2hms[[1]]
c2g2<-c2hms[[2]]
c2g3<-c2hms[[3]]
c3hms<-hmps[[3]]
c3g1<-c3hms[[1]]
c3g2<-c3hms[[2]]
c3g3<-c3hms[[3]]
c4hms<-hmps[[4]]
c4g1<-c4hms[[1]]
c4g2<-c4hms[[2]]
c4g3<-c4hms[[3]]
c5hms<-hmps[[5]]
c5g1<-c5hms[[1]]
c5g2<-c5hms[[2]]
c5g3<-c5hms[[3]]
c6hms<-hmps[[6]]
c6g1<-c6hms[[1]]
c6g2<-c6hms[[2]]
c6g3<-c6hms[[3]]
c7hms<-hmps[[7]]
c7g1<-c7hms[[1]]
c7g2<-c7hms[[2]]
c7g3<-c7hms[[3]]
c8hms<-hmps[[8]]
c8g1<-c8hms[[1]]
c8g2<-c8hms[[2]]
c8g3<-c8hms[[3]]

plot(c1g1)
plot(c1g2)
plot(c1g3)

plot(c8g1)
plot(c8g2)
plot(c8g3)

#**********************Graficar los centroides de un modelo de clustering
cn<-getCentroidsGraph(c4cen)
print(cn)
cn<-getCentroidsGraph(c4cen,percentage = 0.8)
print(cn)
cn<-getCentroidsGraph(c4cen,centroids = c(1,3,5,7))
print(cn)

#**********************Graficar los centroides de todos los modelos de un experimento 
centroids<- getExperimentCentroidsGraphs(models)
cent1<-centroids[[1]]
cent2<-centroids[[2]]
cent3<-centroids[[3]]
cent4<-centroids[[4]]
cent5<-centroids[[5]]
cent6<-centroids[[6]]
cent7<-centroids[[7]]
cent8<-centroids[[8]]


#######################################################################################################
sparkR.stop()
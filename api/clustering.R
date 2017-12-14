
loadClusteringModels<- function(experimentPath){
  
  filePaths <- dir(experimentPath,full.names = T)
  
  #print(filePaths)
  
  nmodels<- length(filePaths)/3
  
  models<- list()
  
  for(i in 1:nmodels){
    
    models[[i]]<-loadClusteringModelByIndex(i,filePaths)
    # print(paste0("model ",i," loaded"))
  }
  
  return(models)
  
}

loadClusteringModelByIndex <-function(clusteringModelIndex,modelPathsList){
  
  nmodels<- length(modelPathsList)/3
  
  if(clusteringModelIndex>nmodels){
    return("Error")
  }
  
  else{
 
    cenFileIndex <- clusteringModelIndex
    cluFileIndex <- nmodels + cenFileIndex
    stdFileIndex <- nmodels*2 + cenFileIndex
    
    # print(paste0(cenFileIndex,"-",cluFileIndex,"-",stdFileIndex))
    
    cenPath <- modelPathsList[[cenFileIndex]]
    cluPath <- modelPathsList[[cluFileIndex]]
    stdPath <- modelPathsList[[stdFileIndex]]
    
    #print(paste0(cenPath,"-",cluPath,"-",stdPath))
    
    cen<-read.csv(cenPath,sep=";",header=F)
    #clu<-as.data.frame(read.parquet(cluPath))
    clu<-read.parquet(cluPath)
    std<-read.csv(stdPath,header=T,sep=";")
    
    return(list(cen=cen,clu=clu,std=std))
    
  }
}  

getClusteringModelNumber<-function (rootPath){
  
  ln <- strsplit(rootPath,"_",fixed=T)[[1]]
  
  r <- ln[length(ln)-1]
  
  return(r)
  
}




getClusteringExperimentHeatMaps<-function(models){
  
  hmps <- list(getClusteringHeatMaps(models[[1]][[3]]))
  
  for(i in 2:length(models)){
    
    model<-models[[i]]
    
    hmps[[i]]<- getClusteringHeatMaps(model[[3]])
    
    
  }
  
  return(hmps)
  
}


getClusteringHeatMaps <- function(cluStudy){
  
  blues <- brewer.pal(9, "Blues")
  
  
  el<-c()
  for (e in cluStudy[,1]){
    print(e)
    el<-c(el,paste0("cluster #",e))
  }
  
  cluStudy[,1]<-el
  
  # print(cluStudy[,1])
  
  names(cluStudy)[2]<-"T"
  names(cluStudy)[3]<-"%T"
  names(cluStudy)[4]<-"F"
  names(cluStudy)[5]<-"L"
  names(cluStudy)[6]<-"%LC"
  names(cluStudy)[7]<-"%FC"
  names(cluStudy)[8]<-"%LT"
  names(cluStudy)[9]<-"%FT"
  
  melted_1 <- melt(cluStudy[,c(1,4,5,2)],"cluster")
  melted_2 <- melt(cluStudy[,c(1,8,9,3)],"cluster")
  melted_3 <- melt(cluStudy[,c(1,6,7)],"cluster")
  
  rl<-list(getHeatMapGraph(melted_1,cluStudy$cluster,blues),
           getHeatMapGraph(melted_2,cluStudy$cluster,blues),
           getHeatMapGraph(melted_3,cluStudy$cluster,blues))
  
  return(rl)
  
}

getHeatMapGraph <- function(melted,labels,pallete){
  
  # print(melted$cluster)
  
  grp<- ggplot(melted, aes(x=variable, y=cluster)) +
    geom_tile(aes(fill = value),colour="black") + 
    geom_text(aes(label = value),size=2) +
    scale_fill_gradientn(colours =pallete[1:5] )+
    scale_y_discrete(limits= rev(labels))+
    theme(axis.title.x=element_blank(),
          axis.ticks.x =element_blank(),
          axis.title.y=element_blank(),
          axis.ticks.y =element_blank(),
          panel.background = element_blank(),
          text = element_text(size=7),
          legend.position="none")
  
  return(grp)
  
}


getExperimentCentroidsGraphs<-function(models){
  
  cen <- list(getCentroidsGraph(models[[1]][[1]]))
  
  for(i in 2:length(models)){
    
    model<-models[[i]]
    
    cen[[i]]<- getCentroidsGraph(model[[1]])
    
  }
  
  return(cen)
  
}


getCentroidsGraph<- function(centroidData, percentage = 1.0, centroids=c()){
  
  if (length(centroids!=0)){
    sel<- centroidData[centroids,]
  }
  else{
    sel<-dplyr::sample_frac(centroidData, percentage)
  }
  
  n<- tdfWn(sel)
  
  ndf<-melt(n,"measure")
  
  cols <- c()
  for (e in 1:ndays){
    cols<- c(cols,e)
  }
  
  l1<-1
  l3<-ceiling(ndays/2)
  l2<-ceiling(l3/2)
  l4<-l3+l2
  l5<-ndays
  
  lim<- c(l1,l2,l3,l4,l5)
  
  
  
  g<-ggplot(ndf,aes(x=measure,y=value,group=variable,color=variable) )+ 
    geom_line()+
    labs(x="days",y="compsumption")+
    scale_x_discrete(limits=lim)+
        theme(axis.text=element_text(size=8),
             axis.title=element_text(size=8,face="bold"),
            legend.position = "none")
  
  return(g)
  
}
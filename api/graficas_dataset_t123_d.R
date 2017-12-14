#API para graficar toda la información relativa al dataset t123_d

loadClasificationResults <- function(path){
  
  filePaths <- dir(path)
  
  l <- list()
  
  for (p in filePaths){
    
    ln <- strsplit(p,"_")[[1]]
    
    name <- ln[length(ln)]
    #print(name)
    
    switch(name, 
           
           rtest={
             
             rtestPath = paste0(path,"/",p)
             print(rtestPath)
             sdf2 <- read.parquet(rtestPath)
             l[[1]]<-sdf2    
           },
           rcamp={
             rcampPath = paste0(path,"/",p)
             print(rcampPath)
             sdf1 <- read.parquet(rcampPath)
             l[[2]]<-sdf1    
           },
           study={
             studyPath = paste0(path,"/",p)
             print(studyPath)
             sdf <- read.df(studyPath,header = TRUE,source="csv", sep = ";")
             df<-as.data.frame(sdf)
             aux<-fromStringToNumberDF(df[,2:18])
             r<-cbind(df[1],aux)
             colnames(r)[1]<-"tipo"
             l[[3]]<-r
           }
    )
    
  }
  
  return(l)
  
}


measuresBarChart <- function(study){
  
  measures<- c("tpr","tnr","ppv","npv","fnr","fpr","dfr","for","acc","f1","mcc","bmmk")
  
  l <- list()
  
  test<-study[1,c(1,c(6:17))]
  st<- tdf(test,measures)
  
  field<-study[2,c(1,c(6:17))]
  sf<- tdf(field,measures)
  
  l[[1]]<- ggplot(st, aes(measure, test))+
    labs(x="",y="") +
    geom_col()
  
  
  l[[2]]<- ggplot(sf, aes(measure, campo))+
    labs(x="",y="") +
    geom_col()
  
  return (l)
  
}


confusionMatrixPieChart <- function(study){
  
  l <- list()
  
  measures <- c("tn", "fn","fp","tp")
  
  test<-study[1,1:5]
  st<- tdf(test,measures)
  
  field<-study[2,1:5]
  sf<- tdf(field,measures)
  
  l[[1]]<-ggplot(data = st, aes(x = "", y = test, fill = measure)) +
    geom_bar(width = 1, stat = "identity")+
    scale_fill_brewer("") +
    geom_text(aes(label = test), position = position_stack(vjust = 0.5)) +
    theme_void()+
    coord_polar(theta = "y") 
  
  
  l[[2]]<-ggplot(data = sf, aes(x = "", y = campo, fill = measure)) +
    geom_bar(width = 1, stat = "identity")+
    scale_fill_brewer("") +
    geom_text(aes(label = campo), position = position_stack(vjust = 0.5)) +
    theme_void()+
    coord_polar(theta = "y")
  
  return (l)
  
}

tdf <- function(df,names){
  
  series <- setNames(data.frame(t(df[,-1])), df[,1])
  measure <- data.frame(measure = names)
  s<- cbind(measure, series)
  return(s)

  }


getConfusionSetsFromSlopes<- function (sourceDataset, solutionDataset){
  
  dataset <-drop(sourceDataset,c("ccodpost","cenae","nle"))
  
  solution<-withColumnRenamed(solutionDataset,"cpuntmed","result")
  
  
  aux1 <- SparkR::select(solution,c("result","prediction"))
  
  aux2 <- join(dataset,aux1,dataset$cpuntmed==aux1$result)
  
  aux3 <- drop(aux2,"result")
  
  f<-as.data.frame(aux3)
  
  legal <- f[f$prediction==0,]
  fraud <- f[f$prediction==1,]
  tn <- f[f$prediction==0&f$label==0,]
  fn <- f[f$prediction==0&f$label==1,]
  fp <- f[f$prediction==1&f$label==0,]
  tp <- f[f$prediction==1&f$label==1,]
  
  drops <- c("label","prediction")
  rlegal<-legal[ , !(names(legal) %in% drops)]
  rfraud<-fraud[ , !(names(fraud) %in% drops)]
  rtn<-tn[ , !(names(tn) %in% drops)]
  rfn<-fn[ , !(names(fn) %in% drops)]
  rfp<-fp[ , !(names(fp) %in% drops)]
  rtp<-tp[ , !(names(tp) %in% drops)]
  
  r<-list(rlegal,rfraud,rtn,rfn,rfp,rtp)
  
  return(r)
  
}



getGraphCurves<-function (consumValues, percentage = 1.0){
    ndays <- NCOL(consumValues)-1
  
    l1<-1
    l3<-ceiling(ndays/2)
    l2<- ceiling(l3/2)
    l4<-l3+l2
    l5<-ndays
    
    lim<- c(l1,l2,l3,l4,l5)
          
          sam<- sample_frac(consumValues, percentage)
          n <- tdf(sam,c(1:ndays))
          
          
          cols <- c()
          for (e in 1:ndays){
            cols<- c(cols,e)
          }
          names(cols)<-cols
          
          ndf<-melt(n,"measure")
          
          g<-  ggplot(ndf,aes(x=measure,y=value,group=variable,color=variable) )+ 
            geom_line()+
            theme(legend.position="none")+
            labs(x="days",y="compsumption")+
            scale_x_discrete(labels=cols,limits=lim)
          
          return(g)
}



experimentsCfMatrixBarChart<-function(variables,values){
  
  df<-values[,c("tipo",variables)]
  
  a <- tdf(df,variables)
  
  g <- melt(a,"measure")
  
  gr<-ggplot(g, aes(x=variable,y=value,fill=variable)) + 
    geom_bar( stat="identity") +    
    theme(legend.position="none")+
    labs(x="value",y="experiments")+
    facet_wrap(~measure,scales = "free")
  
  return(gr)

}


joinExperiments <- function(experiments){
 
  t <- (experiments[[1]])[[3]]
  t[1,1]<-paste0("test_",1)
  t[2,1]<-paste0("field_",1)
  
  test  <- t[1,]
  field <- t[2,]
  
   
  for(i in 2:length(experiments)){
    
    s <- (experiments[[i]])[[3]]
    
    s[1,1]<-paste0("test_",i)
    s[2,1]<-paste0("field_",i)
    
    test<-rbind.data.frame(test,s[1,])
    field<- rbind.data.frame(field,s[2,])
    
    i<- i+1
  }
  
  l<-list(test,field)
  
  return(l)
  
}


fromStringToNumberDF<-function(df){
  
  
  a <- data.frame(as.numeric(df[,1]))
  colnames(a)[1]<-names(df)[1]
  
  for (i in 2:ncol(df)){
    
   
     a<-cbind(a,as.numeric(df[,i]))
     colnames(a)[i]<-names(df)[i]
     
  }
  
  return(a)
  
  
}


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



















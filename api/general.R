tdf <- function(df,names){
  
  series <- setNames(data.frame(t(df[,-1])), df[,1])
  measure <- data.frame(measure = names)
  s<- cbind(measure, series)
  return(s)
  
}

tdfWn <- function(df){
  
  aux<- cbind(centroid = c(1:NROW(df)), df)
  nnames<-NCOL(aux)-1
  names <- 1:nnames
  n<- tdf(aux,names)
  return(n)
  
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
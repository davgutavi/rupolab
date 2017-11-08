#API gráficas registros de consumo cuarto horario

require(ggplot2)
require(cowplot)
require(lubridate)

consumo_dia <- function(contrato,version,dia,mes,año,dataset,modo="lineas"){
  
  fechai<-as.Date(paste0(año,"-",mes,"-",dia))
  show(fechai)
  filtrado <- filter(dataset,dataset$ccontrat==contrato&dataset$cnumscct==version& dataset$flectreg==fechai)
  plots <- construir_graficos(filtrado,modo)
  print(plot_grid(plotlist = plots))
  return(plots)
}

consumo_mes <- function(contrato,version,mes,año,dataset,modo="lineas"){
  
  fechai<-as.Date(paste0(año,"-",mes,"-","01"))
  fechad<-as.Date(paste0(año,"-",mes,"-",as.character(days_in_month(fechai))))
  show(paste0("[",fechai,",",fechad,"]"))
  filtrado <- filter(dataset,dataset$ccontrat==contrato&dataset$cnumscct==version& dataset$flectreg>=fechai &dataset$flectreg<=fechad  )
  plots <- construir_graficos(filtrado,modo)
  print(plot_grid(plotlist = plots))
  return(plots)
}


consumo_año <- function(contrato,version,año,dataset,modo="lineas"){
  
  fechai<-as.Date(paste0(año,"-01-01"))
  fechad<-as.Date(paste0(año,"-12-31"))
  show(paste0("[",fechai,",",fechad,"]"))
  filtrado <- filter(dataset,dataset$ccontrat==contrato&dataset$cnumscct==version& dataset$flectreg>=fechai &dataset$flectreg<=fechad  )
  plots <- construir_graficos(filtrado,modo)
  #print(plot_grid(plotlist = plots))
  return(plots)
}


construir_graficos <- function(filtrado,modo){
  
  auxdf <- as.data.frame(filtrado)
  
  df <- auxdf[order(auxdf$flectreg),]
  
  show(head(df))
  
  show(paste0("registros = ",nrow(df)))
  
  lecturas <- list()
  
  for (i in 1:nrow(df)){
      
    if(modo=="horas"){lecturas[[i]] <-contruye_horas(df[i,])}
    else if (modo=="lineas"){lecturas[[i]] <-contruye_linea_horaria(df[i,])}
  }
  
  
  
  plots <- list()
  if(modo=="horas"){
    plots<-pintar_horas(lecturas)
    
  }
  else if (modo=="lineas"){
    plots<-pintar_lineas(lecturas)
   
  }
  
  
  return(plots)
  
}


contruye_horas <- function (l){

  horas <- data.frame(cuarto=numeric(4),h1=numeric(4),h2=numeric(4),h3=numeric(4),h4=numeric(4),h5=numeric(4),h6=numeric(4),h7=numeric(4),h8=numeric(4),h9=numeric(4),h10=numeric(4),
                      h11=numeric(4),h12=numeric(4),h13=numeric(4),h14=numeric(4),h15=numeric(4),h16=numeric(4),h17=numeric(4),h18=numeric(4),h19=numeric(4),h20=numeric(4),
                                        h21=numeric(4),h22=numeric(4),h23=numeric(4),h24=numeric(4),h25=numeric(4))

  registro <- l[,"flectreg"]
  
  names(horas) = c("cuarto","h1","h2","h3","h4","h5","h6","h7","h8","h9","h10","h11","h12","h13","h14","h15","h16","h17","h18","h19","h20","h21","h22","h23","h24","h25")

  horas["cuarto"] <- c(0.25,0.5,0.75,1)
  horas["h1"]  <- c(l[,"1q_consumo_01"],l[,"2q_consumo_01"],l[,"3q_consumo_01"],l[,"4q_consumo_01"])
  horas["h2"]  <- c(l[,"1q_consumo_02"],l[,"2q_consumo_02"],l[,"3q_consumo_02"],l[,"4q_consumo_02"])
  horas["h3"]  <- c(l[,"1q_consumo_03"],l[,"2q_consumo_03"],l[,"3q_consumo_03"],l[,"4q_consumo_03"])
  horas["h4"]  <- c(l[,"1q_consumo_04"],l[,"2q_consumo_04"],l[,"3q_consumo_04"],l[,"4q_consumo_04"])
  horas["h5"]  <- c(l[,"1q_consumo_05"],l[,"2q_consumo_05"],l[,"3q_consumo_05"],l[,"4q_consumo_05"])
  horas["h6"]  <- c(l[,"1q_consumo_06"],l[,"2q_consumo_06"],l[,"3q_consumo_06"],l[,"4q_consumo_06"])
  horas["h7"]  <- c(l[,"1q_consumo_07"],l[,"2q_consumo_07"],l[,"3q_consumo_07"],l[,"4q_consumo_07"])
  horas["h8"]  <- c(l[,"1q_consumo_08"],l[,"2q_consumo_08"],l[,"3q_consumo_08"],l[,"4q_consumo_08"])
  horas["h9"]  <- c(l[,"1q_consumo_09"],l[,"2q_consumo_09"],l[,"3q_consumo_09"],l[,"4q_consumo_09"])
  horas["h10"] <- c(l[,"1q_consumo_10"],l[,"2q_consumo_10"],l[,"3q_consumo_10"],l[,"4q_consumo_10"])
  horas["h11"] <- c(l[,"1q_consumo_11"],l[,"2q_consumo_11"],l[,"3q_consumo_11"],l[,"4q_consumo_11"])
  horas["h12"] <- c(l[,"1q_consumo_12"],l[,"2q_consumo_12"],l[,"3q_consumo_12"],l[,"4q_consumo_12"])
  horas["h13"] <- c(l[,"1q_consumo_13"],l[,"2q_consumo_13"],l[,"3q_consumo_13"],l[,"4q_consumo_13"])
  horas["h14"] <- c(l[,"1q_consumo_14"],l[,"2q_consumo_14"],l[,"3q_consumo_14"],l[,"4q_consumo_14"])
  horas["h15"] <- c(l[,"1q_consumo_15"],l[,"2q_consumo_15"],l[,"3q_consumo_15"],l[,"4q_consumo_15"])
  horas["h16"] <- c(l[,"1q_consumo_16"],l[,"2q_consumo_16"],l[,"3q_consumo_16"],l[,"4q_consumo_16"])
  horas["h17"] <- c(l[,"1q_consumo_17"],l[,"2q_consumo_17"],l[,"3q_consumo_17"],l[,"4q_consumo_17"])
  horas["h18"] <- c(l[,"1q_consumo_18"],l[,"2q_consumo_18"],l[,"3q_consumo_18"],l[,"4q_consumo_18"])
  horas["h19"] <- c(l[,"1q_consumo_19"],l[,"2q_consumo_19"],l[,"3q_consumo_19"],l[,"4q_consumo_19"])
  horas["h20"] <- c(l[,"1q_consumo_20"],l[,"2q_consumo_20"],l[,"3q_consumo_20"],l[,"4q_consumo_20"])
  horas["h21"] <- c(l[,"1q_consumo_21"],l[,"2q_consumo_21"],l[,"3q_consumo_21"],l[,"4q_consumo_21"])
  horas["h22"] <- c(l[,"1q_consumo_22"],l[,"2q_consumo_22"],l[,"3q_consumo_22"],l[,"4q_consumo_22"])
  horas["h23"] <- c(l[,"1q_consumo_23"],l[,"2q_consumo_23"],l[,"3q_consumo_23"],l[,"4q_consumo_23"])
  horas["h24"] <- c(l[,"1q_consumo_24"],l[,"2q_consumo_24"],l[,"3q_consumo_24"],l[,"4q_consumo_24"])
  horas["h25"] <- c(l[,"1q_consumo_25"],l[,"2q_consumo_25"],l[,"3q_consumo_25"],l[,"4q_consumo_25"])
  
  lectura <- list(registro = registro, horas = horas)
  
  return(lectura)
  
}

contruye_linea_horaria <- function (l){
  
  linea <- data.frame(horario=numeric(100),consumo=numeric(100))
  
  names(linea) = c("horario","consumo")
  
  registro <- l[,"flectreg"]
  
  linea["horario"] <- c(1,1.25,1.5,1.75,
                        2,2.25,2.5,2.75,
                        3,3.25,3.5,3.75,
                        4,4.25,4.5,4.75,
                        5,5.25,5.5,5.75,
                        6,6.25,6.5,6.75,
                        7,7.25,7.5,7.75,
                        8,8.25,8.5,8.75,
                        9,9.25,9.5,9.75,
                        10,10.25,10.5,10.75,
                        11,11.25,11.5,11.75,
                        12,12.25,12.5,12.75,
                        13,13.25,13.5,13.75,
                        14,14.25,14.5,14.75,
                        15,15.25,15.5,15.75,
                        16,16.25,16.5,16.75,
                        17,17.25,17.5,17.75,
                        18,18.25,18.5,18.75,
                        19,19.25,19.5,19.75,
                        20,20.25,20.5,20.75,
                        21,21.25,21.5,21.75,
                        22,22.25,22.5,22.75,
                        23,23.25,23.5,23.75,
                        24,24.25,24.5,24.75,
                        25,25.25,25.5,25.75)
 
  linea["consumo"] <- c(l[,"1q_consumo_01"],l[,"2q_consumo_01"],l[,"3q_consumo_01"],l[,"4q_consumo_01"],
                        l[,"1q_consumo_02"],l[,"2q_consumo_02"],l[,"3q_consumo_02"],l[,"4q_consumo_02"],
                        l[,"1q_consumo_03"],l[,"2q_consumo_03"],l[,"3q_consumo_03"],l[,"4q_consumo_03"],
                        l[,"1q_consumo_04"],l[,"2q_consumo_04"],l[,"3q_consumo_04"],l[,"4q_consumo_04"],
                        l[,"1q_consumo_05"],l[,"2q_consumo_05"],l[,"3q_consumo_05"],l[,"4q_consumo_05"],
                        l[,"1q_consumo_06"],l[,"2q_consumo_06"],l[,"3q_consumo_06"],l[,"4q_consumo_06"],
                        l[,"1q_consumo_07"],l[,"2q_consumo_07"],l[,"3q_consumo_07"],l[,"4q_consumo_07"],
                        l[,"1q_consumo_08"],l[,"2q_consumo_08"],l[,"3q_consumo_08"],l[,"4q_consumo_08"],
                        l[,"1q_consumo_09"],l[,"2q_consumo_09"],l[,"3q_consumo_09"],l[,"4q_consumo_09"],
                        l[,"1q_consumo_10"],l[,"2q_consumo_10"],l[,"3q_consumo_10"],l[,"4q_consumo_10"],
                        l[,"1q_consumo_11"],l[,"2q_consumo_11"],l[,"3q_consumo_11"],l[,"4q_consumo_11"],
                        l[,"1q_consumo_12"],l[,"2q_consumo_12"],l[,"3q_consumo_12"],l[,"4q_consumo_12"],
                        l[,"1q_consumo_13"],l[,"2q_consumo_13"],l[,"3q_consumo_13"],l[,"4q_consumo_13"],
                        l[,"1q_consumo_14"],l[,"2q_consumo_14"],l[,"3q_consumo_14"],l[,"4q_consumo_14"],
                        l[,"1q_consumo_15"],l[,"2q_consumo_15"],l[,"3q_consumo_15"],l[,"4q_consumo_15"],
                        l[,"1q_consumo_16"],l[,"2q_consumo_16"],l[,"3q_consumo_16"],l[,"4q_consumo_16"],
                        l[,"1q_consumo_17"],l[,"2q_consumo_17"],l[,"3q_consumo_17"],l[,"4q_consumo_17"],
                        l[,"1q_consumo_18"],l[,"2q_consumo_18"],l[,"3q_consumo_18"],l[,"4q_consumo_18"],
                        l[,"1q_consumo_19"],l[,"2q_consumo_19"],l[,"3q_consumo_19"],l[,"4q_consumo_19"],
                        l[,"1q_consumo_20"],l[,"2q_consumo_20"],l[,"3q_consumo_20"],l[,"4q_consumo_20"],
                        l[,"1q_consumo_21"],l[,"2q_consumo_21"],l[,"3q_consumo_21"],l[,"4q_consumo_21"],
                        l[,"1q_consumo_22"],l[,"2q_consumo_22"],l[,"3q_consumo_22"],l[,"4q_consumo_22"],
                        l[,"1q_consumo_23"],l[,"2q_consumo_23"],l[,"3q_consumo_23"],l[,"4q_consumo_23"],
                        l[,"1q_consumo_24"],l[,"2q_consumo_24"],l[,"3q_consumo_24"],l[,"4q_consumo_24"],
                        l[,"1q_consumo_25"],l[,"2q_consumo_25"],l[,"3q_consumo_25"],l[,"4q_consumo_25"])
  
  lectura <- list(registro = registro,linea = linea)
  
  return(lectura)
  
}


 pintar_horas<- function (lectura){
   
    plots <- list()
   
   for (i in 1:length(lectura)){
     
     fecha <- lectura[[i]]$registro
     
     horas <- lectura[[i]]$horas
     
     p<-ggplot(horas,aes(cuarto))+ xlab("cuarto horario") + ylab("consumo") + labs(colour = "horas")+ggtitle(as.character(fecha))+
       geom_line(aes(y = h1, colour = "h01"))+
       geom_line(aes(y = h2, colour = "h02"))+
       geom_line(aes(y = h3, colour = "h03"))+
       geom_line(aes(y = h4, colour = "h04"))+
       geom_line(aes(y = h5, colour = "h05"))+
       geom_line(aes(y = h6, colour = "h06"))+
       geom_line(aes(y = h7, colour = "h07"))+
       geom_line(aes(y = h8, colour = "h08"))+
       geom_line(aes(y = h9, colour = "h09"))+
       geom_line(aes(y = h10, colour = "h10"))+
       geom_line(aes(y = h11, colour = "h11"))+
       geom_line(aes(y = h12, colour = "h12"))+
       geom_line(aes(y = h13, colour = "h13"))+
       geom_line(aes(y = h14, colour = "h14"))+
       geom_line(aes(y = h15, colour = "h15"))+
       geom_line(aes(y = h16, colour = "h16"))+
       geom_line(aes(y = h17, colour = "h17"))+
       geom_line(aes(y = h18, colour = "h18"))+
       geom_line(aes(y = h19, colour = "h19"))+
       geom_line(aes(y = h20, colour = "h20"))+
       geom_line(aes(y = h21, colour = "h21"))+
       geom_line(aes(y = h22, colour = "h22"))+
       geom_line(aes(y = h23, colour = "h23"))+
       geom_line(aes(y = h24, colour = "h24"))+
       geom_line(aes(y = h25, colour = "h25"))
     
     #print(p)
     
     plots[[i]] <- p
     
     
     
   }
   
  
   
   return(plots)
   
   
 }

 
 pintar_lineas<- function (lectura){
   
   plots <- list()
   
   for (i in 1:length(lectura)){
     
     fecha <- lectura[[i]]$registro
     
     linea <- lectura[[i]]$linea
     
     p<-ggplot(linea,aes(horario))+ xlab("horas") + ylab("consumo") + labs(colour = "lineas")+ggtitle(as.character(fecha))+

       geom_line(aes(y = consumo, colour = "consumo"))
     
     plots[[i]]<-p
     
   }
   
   return (plots)
   
 }
 













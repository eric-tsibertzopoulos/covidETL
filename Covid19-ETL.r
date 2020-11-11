print(">>>> Start...")
library(sparklyr) # Interface to Apache Spark
library(dplyr)    # Data Refinery/wrangling
library(ggplot2)  # Data Visualization
library(rvest)    # Web Scraping

configuration <- spark_config()
configuration$`sparklyr.jars.default` <- c("d://spark-3.0.0//jars//delta-core_2.12-0.7.0.jar")

sc <- spark_connect(master="local", version="3.0.0", 
app_name = "COVID-19_ETL", 
config=configuration)
print("Connected to Spark...")

url<-"https://www.worldometers.info/coronavirus/"
data <- url %>%
read_html() %>%
html_nodes("#main_table_countries_today") %>%
html_table()
print("Downloaded Web Data...")

df <- as.data.frame(data[1], stringsAsFactors = F)

df<-as.data.frame(data[1], stringsAsFactors = F)
df<-df[-(1:8),]
df<-subset(df, df$Country.Other != "Total:")
df<-df[,-c(1,17,18,19)]

colnames(df)<-c("Country",
"TotalCases","NewCases","TotalDeaths","NewDeaths",
"TotalRecovered","NewRecovered","ActiveCases",
"Critical","TotalCases1Mpop", "Deaths1Mpop",
"TotalTests","Tests1Mpop","Population","Continent")

df$TotalCases<-as.numeric(gsub(",", "", df$TotalCases))
df$NewCases<-as.numeric(gsub(",", "", df$NewCases))
df$TotalDeaths<-as.numeric(gsub(",", "", df$TotalDeaths))
df$NewDeaths<-as.numeric(gsub(",", "", df$NewDeaths))
df$TotalRecovered<-as.numeric(gsub(",", "", df$TotalRecovered))
df$NewRecovered<-as.numeric(gsub(",", "", df$NewRecovered))
df$ActiveCases<-as.numeric(gsub(",", "", df$ActiveCases))
df$Critical<-as.numeric(gsub(",", "", df$Critical))
df$TotalCases1Mpop<-as.numeric(gsub(",", "", df$TotalCases1Mpop))
df$Deaths1Mpop<-as.numeric(gsub(",", "", df$Deaths1Mpop))
df$TotalTests<-as.numeric(gsub(",", "", df$TotalTests))
df$Tests1Mpop<-as.numeric(gsub(",", "", df$Tests1Mpop))
df$Population<-as.numeric(gsub(",", "", df$Population))
df$date<-Sys.Date()
print("Refined Data Set...")

covid_df<-copy_to(sc, df, "covid19", overwrite=T)
spark_write_delta(
covid_df, 
"d://delta//covid19", 
mode ="overwrite", 
options = list(), 
partition_by ="Country" )
print("Persisted Data in delta-lake...")

df2<-spark_read_delta(
sc, 
"d://delta//covid19", 
"covid19", 
overwrite=T)

nc<-df2 %>% filter(Country=="USA") %>% summarise(newCases = sum(NewCases,na.rm=T)) %>% collect()
nd<-df2 %>% filter(Country=="USA") %>% summarise(newDeaths = sum(NewDeaths,na.rm=T)) %>% collect()
ac<-df2 %>% filter(Country=="USA") %>% summarise(activeCases = sum(ActiveCases,na.rm=T)) %>% collect() 
cc<-df2 %>% filter(Country=="USA") %>% summarise(critical = sum(Critical,na.rm=T)) %>% collect()

print(paste('USA Stats ,', Sys.Date(),', New Cases: ', nc, ', New Deaths: ', nd, ', Critical: ', cc, ', Active Cases: ', ac, sep="" ))

library(mailR)
send.mail(from = "xxxxxx@somewhere.com",
          to = "yyyyyy@somewhere.com",
          subject = "Covid-19 ETL R-Script Automation",
          body = paste('USA Stats ,', Sys.Date(),', New Cases: ', nc, ', New Deaths: ', nd, ', Critical: ', cc, ', Active Cases: ', ac, sep="" ),
          smtp = list(host.name = "smtp.xxxx.xxx", port = 465, user.name = "xxxxxx@somewhere.com", passwd = "xxx", ssl = TRUE), 
          authenticate = TRUE,
          send = TRUE)
print(">>>> Success !")

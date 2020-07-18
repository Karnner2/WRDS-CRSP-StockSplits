####################################
#   This code produces Stock Splits
#   From the CRSP DSE files 
#   Note specific treatment and 
#   selection of share codes 
#   adjust to your own needs
#   Use of this script contains no 
#   warrants.
####################################
#   J.T. Fluharty-Jaidee
#   last edit: 07/13/2020
####################################
library(tidyverse)
library(readr)
library(stringr)
library(withr)
library(RPostgres)
library(sqldf)
library(lubridate)
library(MASS)
library(fractional)
####### Log into WRDS and set up the direct connection
closeAllConnections()
wrds <- dbConnect(Postgres(),
                  host='wrds-pgdata.wharton.upenn.edu',
                  port=9737,
                  dbname='wrds',
                  sslmode='require',
                  user='',
                  password='') ### note update User and password for WRDS 

############### Not required: Pull schemas if you need to find variable names 
##### CRSP DSF schema 
# res <- dbSendQuery(wrds, "select column_name
#                    from information_schema.columns 
#                      where table_schema='crsp'             
#                      and table_name='dsf'
#                      order by column_name")
# data <- dbFetch(res, n=-1)
# dbClearResult(res)
# data
# 
###### COMPUSTAT Funda schema 
# res <- dbSendQuery(wrds, "select column_name
#                    from information_schema.columns 
#                      where table_schema='comp'             
#                      and table_name='funda'
#                      order by column_name")
# data <- dbFetch(res, n=-1)
# dbClearResult(res)
# data


### Note: DISTCD is the distribution code first digit of 5 indicates splits or stock dividends, second digit 
####### represents a secondary payment method
####### 0 - unknown
####### Code	    Meaning
####### 0	unknown, not yet coded
####### 1	unspecified or not applicable
####### 2	cash, United States dollars
####### 3	cash, foreign currency converted to US dollars
####### 4	cash, Canadian dollars (now obsolete, converted to US dollars)
####### 5	same issue of common stock
####### 6	units including same issue of common stock
####### 7	an issue of a different common stock which is on the file
####### 8	other property
##############
####### Remaining digits 3 and 4 represent tax treatment of the distribution, CRSP notes that they do not 
####### validate that these are correct so all with in the range are collected.
##############
####### SHRCD represents the type of company shares, 10 to 18 captures all equities and funds, true single-stock equity is 10 and 11 only,
####### filtered later in case others are needed.
##############
crsp.down.call <- dbSendQuery(wrds, "SELECT  a.permco, a.permno,  a.date, a.cusip, a.dclrdt, a.event, a.paydt, a.rcrddt, 
                                        a.distcd, a.divamt, a.facpr, a.facshr, b.cfacpr, b.cfacshr,b.shrout
                                      FROM crsp.dse a join crsp.dsf b
                                      ON a.permno=b.permno
                                      AND a.date=b.date
                                      WHERE a.date between '2000-01-01' 
                                      AND '2020-12-31'
                                      AND a.shrcd between 10 AND 18
                                      AND a.distcd between 5000 AND 5199  OR 
                                          a.distcd between 5500 AND 5699
                                      ")
crsp.down <- dbFetch(crsp.down.call, n=-1)
dbClearResult(crsp.down.call)
head(crsp.down)

##### The above does not include the names or tickers, match back to the DSE.names set for those. 
crsp.down.call.names <- dbSendQuery(wrds, "SELECT  a.permco, a.permno, a.comnam, a.date, a.shrcd, 
                                          a.ticker, a.tsymbol, a.nameendt
                                      FROM crsp.dse a 
                                      WHERE a.nameendt between '1995-01-01' 
                                      AND '2020-12-31'
                                      AND a.shrcd between 10 AND 18
                                      ")
crsp.down.names <- dbFetch(crsp.down.call.names, n=-1)
dbClearResult(crsp.down.call.names)
head(crsp.down.names)


#### Get the list of all names in the sequence, since the names file is constructed as a "from-to" condensed list, you need to expand it
########## to match to the split-event date. 
namesmaster<-crsp.down.names %>% split(., sort(as.numeric(rownames(.)))) %>% 
                                map(~complete(data=.x, date = seq.Date(as.Date(.x$date, format="%Y-%m-%d"),as.Date(.x$nameendt, format="%Y-%m-%d"),by="day"))) %>%
                                map(~fill(data=.x,permco,permno,comnam,shrcd,ticker,tsymbol)) %>% bind_rows() %>% distinct(permco,date,.keep_all = TRUE) %>% dplyr::select(-nameendt)

totalSplit<-merge(crsp.down,namesmaster,by.x=c("permno","date"),by.y=c("permno","date"),all.x=TRUE)
saveRDS(totalSplit,"/scratch/wvu/totalSplit1.rds")

##### following research to the right, splits are distributions which have facpr==facshr, a facpr of 0 would likely be cash distribution, so for splits/reverses select not 0.)
##### Mergers are negative facpr so restrict to >0. 
totalSplit<-totalSplit %>% filter(.,shrcd==10 | shrcd==11 & facpr>0 & facpr==facshr) ### See Lin, Singh, Yu (2009, JFE),p.477, and Minnick and Raman (2013, FM) for specific treatment.
totalSplit$facpr2<-totalSplit$facpr+1                                                ### you need to add one to get the correct factor
totalSplit$SplitRatioTop<-totalSplit$facpr2 %>% fractional %>% numerators()          ### note Lin et al. and Minnick have larger samples because they match to Compustat
totalSplit$SplitRatioBottom<-totalSplit$facpr2 %>% fractional %>% denominators()
###### requested ratio style, however, use facpr for calculations. 
totalSplit$RatioReport<-paste0(totalSplit$SplitRatioBottom,":",totalSplit$SplitRatioTop)
print(paste0("There are: ", n_distinct(totalSplit$permco.x)," unique firms."))
print(paste0("There are: ", n_distinct(totalSplit)," unique split events."))
saveRDS(totalSplit,"/scratch/wvu/totalSplit2.rds")

###### Compustat CIK Name Pull, if you need to match to the CIKs from COMPUSTAT, note this may 
########## reduce the sample significantly as CRSP-COMPUSTAT do not link perfectly. 
########## see the CCM linking below. 
comp.cik.merge.call <- dbSendQuery(wrds, "SELECT cusip, cik, datadate, fyear, gvkey
                                    FROM comp.funda
                                    WHERE datadate between '1995-01-01' 
                                    AND '2020-12-31'
                                    AND datafmt = 'STD'
                                    AND consol = 'C'
                                    AND indfmt ='INDL'
                                    AND popsrc = 'D'
                                    ")
comp.cik.merge <- dbFetch(comp.cik.merge.call, n=-1)
dbClearResult(comp.cik.merge.call)
head(comp.cik.merge)



# ####### Collect the CCM_LINKTABLE, schema 
# res <- dbSendQuery(wrds, "select column_name
#                    from information_schema.columns 
#                      where table_schema='crsp'             
#                      and table_name='ccmxpf_linktable'
#                      order by column_name")
# data <- dbFetch(res, n=-1)
# dbClearResult(res)
# head(data)

#### Create the CCM_LINKTABLE WITH A SQL MERGE 
res <- dbSendQuery(wrds,"select GVKEY, LPERMNO, LINKDT, LINKENDDT, LINKTYPE, LINKPRIM
                    from crsp.ccmxpf_lnkhist")
data.ccmlink <- dbFetch(res, n = -1)
dbClearResult(res)
head(data.ccmlink)

data.ccm <-  data.ccmlink %>%
  # use only primary links (from WRDS Merged Compustat/CRSP examples)
  filter(linktype %in% c("LU", "LC", "LS")) %>%
  filter(linkprim %in% c("P", "C", "J")) %>%
  merge(comp.cik.merge, by="gvkey") %>% # inner join, keep only if permno exists
  mutate(datadate = as.Date(datadate), 
         permno = as.factor(lpermno),
         linkdt = as.Date(linkdt),
         linkenddt = as.Date(linkenddt),
         linktype = factor(linktype, levels=c("LC", "LU", "LS")),
         linkprim = factor(linkprim, levels=c("P", "C", "J"))) %>%
  # remove compustat fiscal ends that do not fall within linked period; linkenddt=NA (from .E) means ongoing  
  filter(datadate >= linkdt & (datadate <= linkenddt | is.na(linkenddt))) %>%
  # prioritize linktype, linkprim based on order of preference/primary if duplicate
  arrange(datadate, permno, linktype, linkprim) %>%
  distinct(datadate, permno, .keep_all = TRUE)

data.ccm.clean<-data.ccm %>% dplyr::select(gvkey, linkdt, linkenddt,cusip,cik,permno)
data.ccm.clean$linkenddt<-as.Date(ifelse(is.na(data.ccm.clean$linkenddt),as.Date(Sys.Date(),"%Y-%m-%d"),as.Date(data.ccm.clean$linkenddt,"%Y-%m-%d")),origin='1970-01-01')

link.file.master<-data.ccm.clean %>% split(., sort(as.numeric(rownames(.)))) %>% 
  map(~complete(data=.x, linkdt = seq.Date(as.Date(.x$linkdt, format="%Y-%m-%d"),as.Date(.x$linkenddt, format="%Y-%m-%d"),by="day"))) %>%
  map(~fill(data=.x,gvkey,cusip,cik,permno)) %>% bind_rows() %>% distinct(permno,gvkey,linkdt,.keep_all = TRUE) %>% dplyr::select(-linkenddt)


######### Merge To Link File
head(totalSplit)
head(link.file.master)
finalOut<-merge(totalSplit,link.file.master,
                  by.x=c("permno","date"),
                  by.y=c("permno","linkdt"),
                  all.x = TRUE)
head(finalOut)

##### Out the files 
saveRDS(finalOut,"/scratch/wvu/SplitFinal.rds")
write.csv(finalOut,"/scratch/wvu/SplitFinal.csv",row.names = FALSE)

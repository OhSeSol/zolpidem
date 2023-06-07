# 필요한 패키지 설치하기 -------------------------------------------------------------------
rm(list=ls())
wants <- c("DatabaseConnector", "survival", "SqlRender", "RPostgreSQL", 
           "lubridate", "RDocumentation", "tidyverse", "lubridate", "readxl")

has <- wants %in% rownames(installed.packages())

if(any(!has)) install.packages(wants[!has])

invisible(lapply(wants, library, character.only = TRUE))

rm(has, wants)


# DB 접속 세팅 ----------------------------------------------------------------

mydbtype= "postgresql"      # db type: "postgresql", "oracle", "pdw", "redshift", "impala", "netezza", "bigquery", "redshift", "sqlite", "mssql", "sql server"
mydbname = "postgres"       # database name
myschemaname = "cdm"        # schema name
myhost = "172.23.100.123"   # db host
myport = 1335               # db port
myuserid = "cdmreader"      # db id
mypassword = "cdmRead2r!"   #password
myvocabschemaname="cdm"     # OMOP @A vocabulary schema name


drv = dbDriver("PostgreSQL") # "MySQL", "PostgreSQL", "RSQLite"
con = dbConnect(drv,
                dbname= mydbname,
                host= myhost,
                port= myport,
                user= myuserid,
                password= mypassword)

target_drug =  read.csv("data/target_drug.csv")
antipsychotics = read.csv("data/antipsychotics.csv")
Zolpidem = target_drug %>% filter(grepl('zolpidem', target_drug$concept_name, ignore.case = TRUE)) %>% select(concept_id) %>% pull
Comparator = target_drug %>% filter(!grepl('zolpidem', target_drug$concept_name, ignore.case = TRUE)) %>% select(concept_id) %>% pull
S_ben = target_drug %>% filter(grepl('etizolam|mexazolam|triazolam', target_drug$concept_name, ignore.case = TRUE)) %>% select(concept_id) %>% pull 
I_ben = target_drug %>% filter(grepl('alprazolam|bromazepam|clotiazepam|lorazepam', target_drug$concept_name, ignore.case = TRUE)) %>% select(concept_id) %>% pull
L_ben = target_drug %>% filter(grepl('clonazepam|clobazam|clorazepate|diazepam|flunitrazepam', target_drug$concept_name, ignore.case = TRUE)) %>% select(concept_id) %>% pull
S_anti = target_drug %>% filter(grepl('amitriptyline|imipramine|mirtazapine|nortriptyline|trazodone', target_drug$concept_name, ignore.case = TRUE)) %>% select(concept_id) %>% pull 
acb1_code =  read.csv("data/acb_1_code.csv")[,1]
acb2_code =  read.csv("data/acb_2_code.csv")[,1]
acb3_code =  read.csv("data/acb_3_code.csv")[,1]
All = c(Zolpidem,Comparator)

# 검사기록에 관한 concept_id는 병원에서 사용하는 코드로 수정 부탁드립니다.
WBC = c(3010813)
Hb = c(3000963)
Hct = c(3009542)
BUN  = c(3013682)
Cr = c(3016723)
GFR = c(46236952)
AST = c(3042781)
ALT = c(3006923)
Albumin = c(3024561)
Weight = c(3013762)
Height = c(3015514)

##### 1. Data extracting ######################################################################################################################################## 
sql = "SELECT 
	visit.person_id,
	person.gender_concept_id,
	EXTRACT(YEAR from visit_start_date)-person.year_of_birth AS age_at_hospitalization,
	visit_occurrence_id AS target_visit_id,
	visit_start_date AS target_start_date,
	visit_end_date AS target_end_date,
	(visit_end_date-visit_start_date+1) AS target_hospitalization_days,
	specialty_source_value AS department
	
FROM(
	SELECT * 
	FROM @A.visit_occurrence
	WHERE visit_occurrence_id = ANY (
		SELECT DISTINCT(visit_occurrence_id)
		FROM @A.drug_exposure
		WHERE drug_concept_id IN (@B)
	)	
	AND visit_concept_id = 9201			--inpatient
) visit

LEFT JOIN @A.person person
ON visit.person_id = person.person_id
	
LEFT JOIN @A.provider provider
ON visit.provider_id=provider.provider_id

WHERE specialty_source_value NOT IN('NP','CHP','PMCNP','C81','061','037','062')	--Non-psych
"

target = dbGetQuery(con, render(translate(sql, targetDialect = mydbtype), 
                                                   A= myschemaname,
                                                   B= target_drug[,1]))      

dbWriteTable(conn=con, name="cte_nonpsych_inpatient", value=target, overwrite = TRUE)

##### exclusion criteria 1
sql = "SELECT DISTINCT target_visit_id
FROM cte_nonpsych_inpatient  target	

INNER JOIN(
	SELECT *
	FROM @A.drug_exposure
	WHERE  drug_concept_id IN (@B)
) drug
ON target.person_id = drug.person_id			--Join drug table
	
WHERE drug_exposure_start_date BETWEEN target_start_date-182 AND target_start_date
"

cte1 = dbGetQuery(con, render(translate(sql, targetDialect = mydbtype), 
                                 A= myschemaname,
                                 B= antipsychotics[,1])) %>% pull

##### exclusion criteria 2
sql = "SELECT DISTINCT target_visit_id
FROM cte_nonpsych_inpatient  target	
	
INNER JOIN(
	SELECT *
	FROM @A.condition_occurrence
	WHERE ext_cond_source_value_kcd LIKE 'F05%'
	OR ext_cond_source_value_kcd LIKE 'F134'
	OR ext_cond_source_value_kcd LIKE 'F194'
	OR ext_cond_source_value_kcd LIKE 'F23%'
)co
ON target.person_id = co.person_id			--Join condition table
	
WHERE condition_start_date BETWEEN target_start_date-182 AND target_start_date
"

cte2 = dbGetQuery(con, render(translate(sql, targetDialect = mydbtype), 
                               A= myschemaname)) %>% pull


target = target %>% filter(!target_visit_id %in% cte1 & !target_visit_id %in% cte2)

dbRemoveTable(conn = con,name = "cte_nonpsych_inpatient")
dbWriteTable(conn=con, name="cte_nonpsych_inpatient", value=target)

##### drug
sql = "SELECT DISTINCT
	target.person_id,gender_concept_id, age_at_hospitalization, 	--person info
	target.target_visit_id, target.target_start_date, target.target_end_date,	--target hospitalization info
	visit.visit_occurrence_id, visit_concept_id, visit_start_date, visit_end_date,  --visit info 
	drug.drug_exposure_id, drug.drug_concept_id, drug_exposure_start_date, drug_exposure_end_date, drug_source_value,		 -- drug info 
	target_hospitalization_days, department

FROM(
	SELECT * 
	FROM cte_nonpsych_inpatient
) target

LEFT JOIN(
	SELECT * 
	FROM @A.visit_occurrence
	WHERE person_id = ANY(SELECT person_id FROM cte_nonpsych_inpatient)
) visit
ON target.person_id = visit.person_id					--Join all the target patients' visits

INNER JOIN(
	SELECT *
	FROM @A.drug_exposure
	WHERE  drug_concept_id IN (@B)
) drug
ON visit.visit_occurrence_id = drug.visit_occurrence_id			--Join drug data

WHERE  (drug_exposure_start_date BETWEEN target_start_date AND target_end_date) 

ORDER BY person_id, target_visit_id, drug_exposure_start_date
"

drug1 = dbGetQuery(con, render(translate(sql, targetDialect = mydbtype), 
                               A= myschemaname,
                               B= target_drug[,1]))


sql = "SELECT DISTINCT
	target.person_id,gender_concept_id, age_at_hospitalization, 	--person info
	target.target_visit_id, target.target_start_date, target.target_end_date,	--target hospitalization info
	visit.visit_occurrence_id, visit_concept_id, visit_start_date, visit_end_date,  --visit info 
	drug.drug_exposure_id, drug.drug_concept_id, drug_exposure_start_date, drug_exposure_end_date, drug_source_value,		 -- drug info 
	target_hospitalization_days, department

FROM(
	SELECT * 
	FROM cte_nonpsych_inpatient
) target

LEFT JOIN(
	SELECT * 
	FROM @A.visit_occurrence
	WHERE person_id = ANY(SELECT person_id FROM cte_nonpsych_inpatient)
) visit
ON target.person_id = visit.person_id					--Join all the target patients' visits

INNER JOIN(
	SELECT *
	FROM @A.drug_exposure
	WHERE  drug_concept_id IN (@B)
) drug
ON visit.visit_occurrence_id = drug.visit_occurrence_id			--Join drug data

WHERE  (drug_exposure_start_date BETWEEN target_start_date AND target_end_date) 

ORDER BY person_id, target_visit_id, drug_exposure_start_date
"

drug2 = dbGetQuery(con, render(translate(sql, targetDialect = mydbtype), 
                              A= myschemaname,
                              B= antipsychotics[,1]))

drug = rbind(drug1,drug2)

sql = "SELECT DISTINCT
	target.person_id, gender_concept_id, age_at_hospitalization,	--person info
	target_visit_id, target.target_start_date, target.target_end_date,		--target hospitalization info
	visit.visit_occurrence_id, visit_concept_id, visit_start_date, visit_end_date,		--visit info
	condition_concept_id, condition_start_date, condition_end_date,	--condition info
	condition_source_value,	-- condition source info
	target_hospitalization_days, department

FROM(
	SELECT * 
	FROM cte_nonpsych_inpatient
) target

LEFT JOIN(
	SELECT * 
	FROM @A.visit_occurrence
	WHERE person_id = ANY(SELECT person_id FROM cte_nonpsych_inpatient)
) visit
ON target.person_id = visit.person_id 			--Join all the target patients` visits

INNER JOIN(
	SELECT *
	FROM @A.condition_occurrence
	WHERE  ext_cond_source_value_kcd LIKE 'F05%'
	OR ext_cond_source_value_kcd LIKE 'F134'
	OR ext_cond_source_value_kcd LIKE 'F194'
	OR ext_cond_source_value_kcd LIKE 'F23%'
) co
ON visit.visit_occurrence_id = co.visit_occurrence_id		--Join 'F059'data

WHERE (condition_start_date BETWEEN target_start_date AND target_end_date) AND (condition_start_date BETWEEN visit_start_date AND visit_end_date)

ORDER BY person_id,target_visit_id, condition_start_date
"

F059 = dbGetQuery(con, render(translate(sql, targetDialect = mydbtype), 
                               A= myschemaname))
f059 = F059 %>% select(condition_concept_id) %>% distinct %>% pull

drug = drug  %>% filter(age_at_hospitalization>=18)
F059 = F059  %>% filter(age_at_hospitalization>=18)

##### cohort
inpatient_B1<-drug  %>% select(person_id, target_visit_id,target_start_date,target_end_date,department)
inpatient_B2<-F059 %>% select(person_id, target_visit_id,target_start_date,target_end_date,department)
inpatient_B<-rbind(inpatient_B1, inpatient_B2) %>% arrange(person_id,target_start_date,department)

#person_id with more than 1hospitalization
person<-inpatient_B %>% group_by(person_id) %>% 
  distinct(person_id,target_visit_id,target_start_date,target_end_date) %>%
  count %>% filter(n>1) %>% pull(person_id)   

#mydata: hospitalization episode information
mydata<-inpatient_B %>% filter(person_id %in% person) %>%
  group_by(person_id) %>% 
  distinct(person_id,target_visit_id,target_start_date,target_end_date,department) %>%
  arrange(person_id,target_start_date)

# function that finds consecutive hospitalizations
myfunc<-function(p){
  temp<-mydata %>% filter(person_id %in% p) %>% arrange(target_start_date)
  n<-nrow(temp)
  visit_id<-c()
  for(i in 1:(n-1)){
    if(temp[i,'target_end_date']>=temp[i+1,'target_start_date']){
      visit_id<-c(visit_id,temp[i,'target_visit_id'],temp[i+1,'target_visit_id'])
    }
  }
  return(unique(visit_id))
}

# contains target_visit_id that need to be grouped into one episode
consec<-map(person,myfunc) %>% discard(is.null)   

# regrouping WITHIN the person_id 
num<-length(consec)
delete<-c()
for(i in 1:num){
  aa<-unlist(consec[[i]])
  
  if(length(aa)==2) next
  
  #When there are more than 3 target_visit_id in one person_id: look again and do the groupings properly
  else{
    visit_id<-c()
    temp<-mydata %>% filter(target_visit_id %in% aa) %>% arrange(target_start_date)
    
    k<-1
    for(j in 1:(length(aa)-1)){
      if(temp[j,'target_end_date'] >= temp[j+1,'target_start_date']){
        visit_id<-c(visit_id,temp[j,'target_visit_id'],temp[j+1,'target_visit_id'])
      }
      
      else{
        if(k==1) {
          delete<-c(delete,i)
          k<-k+1
        }
        consec[[length(consec)+1]]<-unique(visit_id)
        visit_id<-c()
      }
      
      if(temp[j,'target_end_date'] >= temp[j+1,'target_start_date'] & j==(length(aa)-1) & k!=1){
        consec[[length(consec)+1]]<-unique(visit_id)
      }
    }
  }
}

if(length(delete)!=0) consec[[delete]]<-NULL

##-----Make consecutive hospitalizations into one episode(modify data)
# change `taret_vistit_id`, `target_start_date`, `target_end_date`, `target_hospitalization_days`, `department`
for(i in 1:length(consec)){
  aa<-unlist(consec[[i]])
  start<-mydata[mydata$target_visit_id==aa[1],'target_start_date']
  end<-mydata[mydata$target_visit_id==aa[length(aa)],'target_end_date']
  department<-mydata[mydata$target_visit_id==aa[1],'department']
  
  #modify `drug` dataset
  drug[drug$target_visit_id %in% aa,'target_visit_id']<-aa[1]
  drug[drug$target_visit_id %in% aa,'target_start_date']<-start
  drug[drug$target_visit_id %in% aa,'target_end_date']<-end
  drug[drug$target_visit_id %in% aa,'target_hospitalization_days']<-as.numeric(end-start+1)
  drug[drug$target_visit_id %in% aa,'department']<-department
  
  #modify `F059` dataset
  F059[F059$target_visit_id %in% aa,'target_visit_id']<-aa[1]
  F059[F059$target_visit_id %in% aa,'target_start_date']<- start
  F059[F059$target_visit_id %in% aa,'target_end_date']<- end
  F059[F059$target_visit_id %in% aa,'target_hospitalization_days']<- as.numeric(end-start+1)
  F059[F059$target_visit_id %in% aa,'department']<- department
}

drug<-drug %>% arrange(person_id, target_visit_id, drug_exposure_start_date)
F059<-F059 %>% arrange(person_id, target_visit_id, condition_start_date)

rm(inpatient_B1,inpatient_B2,inpatient_B)
drug = drug %>% filter(target_hospitalization_days<=30)
F059 = F059 %>% filter(target_hospitalization_days<=30)
##########---------- Find Delirium Occurrence Episodes ----------##########
##--1) No delirium prior to ZPD transcription
first_ZPD<-drug %>% group_by(target_visit_id) %>% 
  filter(drug_concept_id%in% All) %>% 
  filter(drug_exposure_start_date==min(drug_exposure_start_date)) %>%
  distinct(target_visit_id,.keep_all = T)


# Record `target_visit_id`s that do not have Antipsychotics prior to ZPD
# x: target_visit_id, y: index_date(first ZPD)
prior_ZPD<-function(x,y){
  prior_ZPD_data<-drug %>% filter(target_visit_id==x & drug_exposure_start_date<=y)
  prior_antipsychotics<-prior_ZPD_data %>% 
    filter(drug_concept_id %in% antipsychotics[,1])    
  return(ifelse(nrow(prior_antipsychotics)==0,x,0))
}

visits<-first_ZPD %>% pull(target_visit_id)
index_date<-first_ZPD %>% pull(drug_exposure_start_date)
prior_anti_no<-map2_dbl(visits,index_date,prior_ZPD)
prior_anti_no<-prior_anti_no[!(prior_anti_no %in% 0)]   #Remove 0s


# Record `target_visit_id`s that have F059 prior to ZPD
# x: target_visit_id, y: index_date(first ZPD)
prior_F059_func<-function(x,y){
  prior_ZPD_F059<-F059 %>% filter(target_visit_id==x & condition_start_date<=y)
  return(ifelse(nrow(prior_ZPD_F059)!=0,x,0))
}

condition_visit<-F059 %>% distinct(target_visit_id) %>% pull %>% sort
index_date<-first_ZPD %>% filter(target_visit_id %in% condition_visit) %>% arrange(target_visit_id) %>% pull(drug_exposure_start_date)
prior_F059_yes<-map2_dbl(condition_visit,index_date,prior_F059_func)  # F059 prior to ZPD
prior_F059_yes<-prior_F059_yes[!prior_F059_yes %in% 0]

B_index<-prior_anti_no[!prior_anti_no %in% prior_F059_yes]    #Remove `target_visit_id` that has F059 prior to first ZPD
drug_B<-drug %>% filter(target_visit_id %in% B_index)
F059_B<-F059 %>% filter(target_visit_id %in% B_index)

##--2) Developed delirium after ZPD prescription(F059 OR Antipsychotics)
b<-drug_B %>% filter(drug_concept_id %in% antipsychotics[,1])  %>%   
  distinct(target_visit_id) %>% pull()

bb<-F059_B %>% distinct(target_visit_id) %>% pull()

# Final `target_visit_id` 
delirium_OR<-unique(c(b,bb))

#--Aggregate `drug` and `F059` table
#--OR condition
inpatient_B1<-drug %>% filter(target_visit_id %in% delirium_OR)  %>% 
  rename(omop_concept_id=drug_concept_id,start_date=drug_exposure_start_date, end_date=drug_exposure_end_date, source_value=drug_source_value) %>%
  select(-drug_exposure_id)

inpatient_B2<-F059 %>% filter(target_visit_id %in% delirium_OR)  %>% 
  rename(omop_concept_id=condition_concept_id,start_date=condition_start_date, end_date=condition_end_date, source_value=condition_source_value) 

inpatient_OR<-rbind(inpatient_B1, inpatient_B2) %>% arrange(person_id, target_visit_id, start_date)

delirium_OR_df<-inpatient_OR  %>% 
  distinct(target_visit_id,visit_occurrence_id,gender_concept_id,age_at_hospitalization,department,target_hospitalization_days)


rm(index_date)


#####---------- Case-Control Matching ----------#####
case_OR<-inpatient_OR %>%  
  distinct(target_visit_id,gender_concept_id,age_at_hospitalization,department,target_hospitalization_days)

control.candidate<-drug %>% filter(!target_visit_id %in% delirium_OR) %>% 
  distinct(target_visit_id,gender_concept_id,age_at_hospitalization,department,target_hospitalization_days)

#Case-Control Matching(OR condition)
set.seed(10)
case_OR<-case_OR[sample(nrow(case_OR)),]
control.candidate<-control.candidate[sample(nrow(control.candidate)),]
control_OR<-NULL

num<-nrow(case_OR)
count<-NULL
remove<-NULL
for(i in 1:num){
  gender<-case_OR[[i,'gender_concept_id']]
  age<-case_OR[[i,'age_at_hospitalization']]
  dept<-case_OR[[i,'department']]
  hospitalization_days<-case_OR[[i,'target_hospitalization_days']]
  
  #matching 
  candidate<-control.candidate %>% filter(gender_concept_id == gender & department == dept) %>% 
    filter(age-5<=age_at_hospitalization & age_at_hospitalization<=age+5) %>% 
    mutate(age_diff=abs(age_at_hospitalization-age),days_diff=abs(target_hospitalization_days-hospitalization_days)) %>% 
    arrange(age_diff,days_diff)
  
  if(nrow(candidate)>=4){
    control_OR<-rbind(control_OR,candidate[1:4,])
    visit_id<-candidate[1:4,] %>% pull(target_visit_id)
    control.candidate<-control.candidate %>% filter(!target_visit_id %in% visit_id)
  }
  
  else{
    remove<-c(remove,case_OR[i,] %>% pull(target_visit_id))
  }
}

case_OR<-case_OR %>% filter(!target_visit_id %in% remove)

control_OR = control_OR[1:5]

case_control_OR = data.frame()

for (i in 1:nrow(case_OR)){
  case_control_OR = rbind(case_control_OR,case_OR[i,])
  case_control_OR = rbind(case_control_OR,control_OR[(4*(i-1))+1,])
  case_control_OR = rbind(case_control_OR,control_OR[(4*(i-1))+2,])
  case_control_OR = rbind(case_control_OR,control_OR[(4*(i-1))+3,])
  case_control_OR = rbind(case_control_OR,control_OR[(4*(i-1))+4,])
}
case_control_OR['delirium'] = rep(c(1,0,0,0,0),nrow(case_OR))


case_control_OR['set'] = rep(c(1:(nrow(case_control_OR)/5)),each= 5)

case_control_OR = left_join(case_control_OR,first_ZPD, by = c('target_visit_id'='target_visit_id'))

case_control_OR = case_control_OR %>% select(person_id,
                                             
                                             gender_concept_id=gender_concept_id.x,
                                             
                                             age_at_hospitalization=age_at_hospitalization.x,
                                             
                                             target_visit_id,
                                             
                                             target_start_date,
                                             
                                             target_end_date,
                                             
                                             target_hospitalization_days=target_hospitalization_days.x,
                                             
                                             department=department.x,
                                             
                                             drug=drug_source_value,
                                             
                                             drug_exposure_start_date,
                                             
                                             delirium,
                                             
                                             set)



#index_date - case군의 첫 투약일부터 섬망 발생일까지

index_date = tibble(indexdt = as.Date(character())); index_date



for (i in 1:nrow(case_control_OR)){
  
  a = (((inpatient_OR %>% filter(target_visit_id==as.integer(case_control_OR[i,'target_visit_id']) & (omop_concept_id %in% antipsychotics[,1]|omop_concept_id %in% f059 )))[1,'start_date']))  %>% as_date()
  
  if (is.na(a)){
    
    index_date[i,1] = min( (case_control_OR[i,'drug_exposure_start_date']  %>% as_date() + days(b)), (case_control_OR[i,'target_end_date']%>% as_date()) ) 
    
  }
  
  else{
    
    b=as.integer(a - as.Date(case_control_OR[i,'drug_exposure_start_date'] %>% as_date()))
    
    
    
    index_date[i,1] = a
  }
}

case_control_OR['index_date'] = index_date





#index date 3/7일전 각 약물 처방 여부

drug = rbind(drug1,drug2)

ZPD3 = function(table){
  for (i in 1:nrow(table)){
    a=drug %>% filter(target_visit_id==as.integer(table[i,'target_visit_id']) &
                        between(drug_exposure_start_date,(table[i,'index_date']  %>% as_date()) - days(3), table[i,'index_date']  %>% as_date())) %>% filter(drug_concept_id %in% Zolpidem) %>% count
    if (a != 0){
      table[i,'zpd_3']=1
    }
    else{
      table[i,'zpd_3']=0
    }
  }
  return(table)
}



ZPD7 = function(table){
  for (i in 1:nrow(table)){
    a=drug %>% filter(target_visit_id==as.integer(table[i,'target_visit_id']) &
                        between(drug_exposure_start_date,(table[i,'index_date'] %>% as_date()) - days(7), table[i,'index_date']  %>% as_date())) %>% filter(drug_concept_id %in% Zolpidem) %>% count
    if (a != 0){
      table[i,'zpd_7']=1
    }
    else{
      table[i,'zpd_7']=0
    }
  }
  return(table)
}



S_ben3 = function(table){
  for (i in 1:nrow(table)){
    a=drug %>% filter(target_visit_id==as.integer(table[i,'target_visit_id']) &
                        between(drug_exposure_start_date,(table[i,'index_date']  %>% as_date()) - days(3), table[i,'index_date'] %>% as_date())) %>% filter(drug_concept_id %in% S_ben) %>% count
    if (a != 0){
      table[i,'short_ben_3']=1
    }
    else{
      table[i,'short_ben_3']=0
    }
  }
  return(table)
}


S_ben7 = function(table){
  for (i in 1:nrow(table)){
    a=drug %>% filter(target_visit_id==as.integer(table[i,'target_visit_id']) &
                        between(drug_exposure_start_date,(table[i,'index_date'] %>% as_date()) - days(7), table[i,'index_date'] %>% as_date())) %>% filter(drug_concept_id %in% S_ben) %>% count
    if (a != 0){
      table[i,'short_ben_7']=1
    }
    else{
      table[i,'short_ben_7']=0
    }
  }
  return(table)
}



I_ben3 = function(table){
  for (i in 1:nrow(table)){
    a=drug %>% filter(target_visit_id==as.integer(table[i,'target_visit_id']) &
                        between(drug_exposure_start_date,(table[i,'index_date'] %>% as_date()) - days(3), table[i,'index_date'] %>% as_date())) %>% filter(drug_concept_id %in% I_ben) %>% count
    if (a != 0){
      table[i,'intermediate_ben_3']=1
    }
    else{
      table[i,'intermediate_ben_3']=0
    }
  }
  return(table)
}



I_ben7 = function(table){
  for (i in 1:nrow(table)){
    a=drug %>% filter(target_visit_id==as.integer(table[i,'target_visit_id']) &
                        between(drug_exposure_start_date,(table[i,'index_date'] %>% as_date()) - days(7), table[i,'index_date'] %>% as_date())) %>% filter(drug_concept_id %in% I_ben) %>% count
    if (a != 0){
      table[i,'intermediate_ben_7']=1
    }
    else{
      table[i,'intermediate_ben_7']=0
    }
  }
  return(table)
}



L_ben3 = function(table){
  for (i in 1:nrow(table)){
    a=drug %>% filter(target_visit_id==as.integer(table[i,'target_visit_id']) &
                        between(drug_exposure_start_date,(table[i,'index_date'] %>% as_date()) - days(3), table[i,'index_date'] %>% as_date())) %>% filter(drug_concept_id %in% L_ben) %>% count
    if (a != 0){
      table[i,'long_ben_3']=1
    }
    else{
      table[i,'long_ben_3']=0
    }
  }
  return(table)
}



L_ben7 = function(table){
  for (i in 1:nrow(table)){
    a=drug %>% filter(target_visit_id==as.integer(table[i,'target_visit_id']) &
                        between(drug_exposure_start_date,(table[i,'index_date'] %>% as_date()) - days(7), table[i,'index_date'] %>% as_date())) %>% filter(drug_concept_id %in% L_ben) %>% count
    if (a != 0){
      table[i,'long_ben_7']=1
    }
    else{
      table[i,'long_ben_7']=0
    }
  }
  return(table)
}



S_anti3 = function(table){
  for (i in 1:nrow(table)){
    a=drug %>% filter(target_visit_id==as.integer(table[i,'target_visit_id']) &
                        between(drug_exposure_start_date,(table[i,'index_date'] %>% as_date()) - days(3), table[i,'index_date'] %>% as_date())) %>% filter(drug_concept_id %in% S_anti) %>% count
    if (a != 0){
      table[i,'s_anti_3']=1
    }
    else{
      table[i,'s_anti_3']=0
    }
  }
  return(table)
}



S_anti7 = function(table){
  for (i in 1:nrow(table)){
    a=drug %>% filter(target_visit_id==as.integer(table[i,'target_visit_id']) &
                        between(drug_exposure_start_date,(table[i,'index_date'] %>% as_date()) - days(7), table[i,'index_date'] %>% as_date())) %>% filter(drug_concept_id %in% S_anti) %>% count
    if (a != 0){
      table[i,'s_anti_7']=1
    }
    else{
      table[i,'s_anti_7']=0
    }
  }
  return(table)
}


case_control_OR = ZPD3(case_control_OR)

case_control_OR = ZPD7(case_control_OR)

case_control_OR = S_ben3(case_control_OR)

case_control_OR = S_ben7(case_control_OR)

case_control_OR = I_ben3(case_control_OR)

case_control_OR = I_ben7(case_control_OR)

case_control_OR = L_ben3(case_control_OR)

case_control_OR = L_ben7(case_control_OR)

case_control_OR = S_anti3(case_control_OR)

case_control_OR = S_anti7(case_control_OR)


dbWriteTable(conn=con, name="case_control_or", value=case_control_OR, row.names = FALSE)

##### opoid
opioid_code = read.csv('data/opioid.csv')[,1]

sql = "SELECT target.person_id, target.target_visit_id, target.index_date, target.target_start_date, target.target_end_date, drug.drug_exposure_start_date as opioid_exposure_date
FROM case_control_or target
LEFT JOIN(SELECT person_id, drug_exposure_start_date
          FROM @A.drug_exposure
          WHERE drug_concept_id IN (@B)) drug
ON target.person_id = drug.person_id
"
opioid = dbGetQuery(con, render(translate(sql, targetDialect = mydbtype), 
                              A= myschemaname,
                              B= opioid_code))

opioid['opioid_3'] = ifelse((opioid$opioid_exposure_date %>% as.Date >= opioid$index_date %>% as.Date - days(3))&(opioid$opioid_exposure_date %>% as.Date <= opioid$index_date %>% as.Date),1,0)
opioid['opioid_7'] = ifelse((opioid$opioid_exposure_date %>% as.Date >= opioid$index_date %>% as.Date - days(7))&(opioid$opioid_exposure_date %>% as.Date <= opioid$index_date %>% as.Date),1,0)

case_control_OR = case_control_OR %>% left_join(opioid %>% filter(opioid_3==1) %>% select(target_visit_id, opioid_3) %>% distinct, by='target_visit_id') 
case_control_OR['opioid_3'] = ifelse(is.na(case_control_OR['opioid_3']), 0, 1)
case_control_OR = case_control_OR %>% left_join(opioid %>% filter(opioid_7==1) %>% select(target_visit_id, opioid_7) %>% distinct, by='target_visit_id') 
case_control_OR['opioid_7'] = ifelse(is.na(case_control_OR['opioid_7']), 0, 1)



##### dementia medication
dementia_code = read.csv('data/dementia_med.csv')[,1]

sql = "SELECT target.person_id, target.target_visit_id, target.index_date, target.target_start_date, target.target_end_date, drug.drug_exposure_start_date as dementia_exposure_date
FROM case_control_or target
LEFT JOIN(SELECT person_id, drug_exposure_start_date
          FROM @A.drug_exposure
          WHERE drug_concept_id IN (@B)) drug
ON target.person_id = drug.person_id
"

dementia_med = dbGetQuery(con, render(translate(sql, targetDialect = mydbtype), 
                                A= myschemaname,
                                B= dementia_code))

dementia_med['dementia_med_3'] = ifelse((dementia_med$dementia_exposure_date %>% as.Date >= dementia_med$index_date %>% as.Date - days(3))&(dementia_med$dementia_exposure_date %>% as.Date <= dementia_med$index_date %>% as.Date),1,0)
dementia_med['dementia_med_7'] = ifelse((dementia_med$dementia_exposure_date %>% as.Date >= dementia_med$index_date %>% as.Date - days(7))&(dementia_med$dementia_exposure_date %>% as.Date <= dementia_med$index_date %>% as.Date),1,0)

case_control_OR = case_control_OR %>% left_join(dementia_med %>% filter(dementia_med_3==1) %>% select(target_visit_id, dementia_med_3) %>% distinct, by='target_visit_id') 
case_control_OR['dementia_med_3'] = ifelse(is.na(case_control_OR['dementia_med_3']), 0, 1)
case_control_OR = case_control_OR %>% left_join(dementia_med %>% filter(dementia_med_7==1) %>% select(target_visit_id, dementia_med_7) %>% distinct, by='target_visit_id') 
case_control_OR['dementia_med_7'] = ifelse(is.na(case_control_OR['dementia_med_7']), 0, 1)

##### WBC
sql = "SELECT target.person_id, gender_concept_id, age_at_hospitalization, 	--person info
	target_visit_id, target.target_start_date, target.target_end_date, target.drug_exposure_start_date,		--target hospitalization info
	measurement.value_as_number as WBC, measurement.measurement_date as WBC_date
FROM case_control_or target
LEFT JOIN(SELECT person_id, measurement_concept_id, value_as_number, measurement_date
          FROM @A.measurement
          WHERE measurement_concept_id IN (@B)) measurement
ON target.person_id = measurement.person_id
WHERE (measurement.measurement_date BETWEEN target_start_date AND target_end_date)
ORDER BY person_id
"

wbc = dbGetQuery(con, render(translate(sql, targetDialect = mydbtype), 
                                      A= myschemaname,
                                      B= WBC))
wbc$diff = abs(wbc$drug_exposure_start_date-wbc$wbc_date)
wbc$wbc = as.numeric(wbc$wbc)
wbc$wbc = ifelse(wbc$wbc<4, 1, ifelse(wbc$wbc>10, 2, 0))
wbc=wbc %>% select(target_visit_id, wbc, diff)
wbc = left_join(case_control_OR, wbc, by=('target_visit_id'='target_visit_id'))
target=wbc %>% select(target_visit_id) %>% distinct() %>% pull
temp=data.frame()
for (i in target){
  a=(wbc %>% filter(target_visit_id==i) %>% arrange(diff))[1,]
  temp = rbind(temp,a)
}
temp = temp %>% select(-diff)
temp$wbc = ifelse(is.na(temp$wbc), 0, temp$wbc)
case_control_OR = temp
case_control_OR$wbc = as.factor(case_control_OR$wbc)


##### Hb
sql = "SELECT target.person_id, gender_concept_id, age_at_hospitalization, 	--person info
	target_visit_id, target.target_start_date, target.target_end_date, target.drug_exposure_start_date,		--target hospitalization info
	measurement.value_as_number as Hb, measurement.measurement_date as Hb_date
FROM case_control_or target
LEFT JOIN(SELECT person_id, measurement_concept_id, value_as_number, measurement_date
          FROM @A.measurement
          WHERE measurement_concept_id IN (@B)) measurement
ON target.person_id = measurement.person_id
WHERE (measurement.measurement_date BETWEEN target_start_date AND target_end_date)
ORDER BY person_id
"

hb = dbGetQuery(con, render(translate(sql, targetDialect = mydbtype), 
                             A= myschemaname,
                             B= Hb))
hb$diff = abs(hb$drug_exposure_start_date-hb$hb_date)
hb$hb = as.numeric(hb$hb)
hb$hb = ifelse((hb$hb<13.0 & hb$gender_concept_id==8507), 1, ifelse((hb$hb<12.0 & hb$gender_concept_id==8532), 1, ifelse((hb$hb>17.0 & hb$gender_concept_id==8507), 2, ifelse((hb$hb>16.0 & hb$gender_concept_id==8532), 2, 0))))
hb=hb %>% select(target_visit_id, hb, diff)
hb = left_join(case_control_OR, hb, by=('target_visit_id'='target_visit_id'))
target=hb %>% select(target_visit_id) %>% distinct() %>% pull
temp=data.frame()
for (i in target){
  a=(hb %>% filter(target_visit_id==i) %>% arrange(diff))[1,]
  temp = rbind(temp,a)
}
temp = temp %>% select(-diff)
temp$hb = ifelse(is.na(temp$hb), 0, temp$hb)
case_control_OR = temp
case_control_OR$hb = as.factor(case_control_OR$hb)


##### Hct
sql = "SELECT target.person_id, gender_concept_id, age_at_hospitalization, 	--person info
	target_visit_id, target.target_start_date, target.target_end_date, target.drug_exposure_start_date,		--target hospitalization info
	measurement.value_as_number as Hct, measurement.measurement_date as Hct_date
FROM case_control_or target
LEFT JOIN(SELECT person_id, measurement_concept_id, value_as_number, measurement_date
          FROM @A.measurement
          WHERE measurement_concept_id IN (@B)) measurement
ON target.person_id = measurement.person_id
WHERE (measurement.measurement_date BETWEEN target_start_date AND target_end_date)
ORDER BY person_id
"

hct = dbGetQuery(con, render(translate(sql, targetDialect = mydbtype), 
                            A= myschemaname,
                            B= Hct))
hct$diff = abs(hct$drug_exposure_start_date-hct$hct_date)
hct$hct = as.numeric(hct$hct)
hct$hct = ifelse((hct$hct<39 & hct$gender_concept_id==8507), 1, ifelse((hct$hct<36 & hct$gender_concept_id==8532), 1, ifelse(hct$hct>52 & hct$gender_concept_id==8507, 2, ifelse((hct$hct>48 & hct$gender_concept_id==8532), 2, 0))))
hct=hct %>% select(target_visit_id, hct, diff)
hct = left_join(case_control_OR, hct, by=('target_visit_id'='target_visit_id'))
target=hct %>% select(target_visit_id) %>% distinct() %>% pull
temp=data.frame()
for (i in target){
  a=(hct %>% filter(target_visit_id==i) %>% arrange(diff))[1,]
  temp = rbind(temp,a)
}
temp = temp %>% select(-diff)
temp$hct = ifelse(is.na(temp$hct), 0, temp$hct)
case_control_OR = temp
case_control_OR$hct = as.factor(case_control_OR$hct)


##### BUN
sql = "SELECT target.person_id, gender_concept_id, age_at_hospitalization, 	--person info
	target_visit_id, target.target_start_date, target.target_end_date, target.drug_exposure_start_date,		--target hospitalization info
	measurement.value_as_number as BUN, measurement.measurement_date as BUN_date
FROM case_control_or target
LEFT JOIN(SELECT person_id, measurement_concept_id, value_as_number, measurement_date
          FROM @A.measurement
          WHERE measurement_concept_id IN (@B)) measurement
ON target.person_id = measurement.person_id
WHERE (measurement.measurement_date BETWEEN target_start_date AND target_end_date)
ORDER BY person_id
"

bun = dbGetQuery(con, render(translate(sql, targetDialect = mydbtype), 
                             A= myschemaname,
                             B= BUN))
bun$diff = abs(bun$drug_exposure_start_date-bun$bun_date)
bun$bun = as.numeric(bun$bun)
bun$bun = ifelse(bun$bun<10, 1, ifelse(bun$bun>26, 2, 0))
bun=bun %>% select(target_visit_id, bun, diff)
bun = left_join(case_control_OR, bun, by=('target_visit_id'='target_visit_id'))
target=bun %>% select(target_visit_id) %>% distinct() %>% pull
temp=data.frame()
for (i in target){
  a=(bun %>% filter(target_visit_id==i) %>% arrange(diff))[1,]
  temp = rbind(temp,a)
}
temp = temp %>% select(-diff)
temp$bun = ifelse(is.na(temp$bun), 0, temp$bun)
case_control_OR = temp
case_control_OR$bun = as.factor(case_control_OR$bun)


##### Cr
sql = "SELECT target.person_id, gender_concept_id, age_at_hospitalization, 	--person info
	target_visit_id, target.target_start_date, target.target_end_date, target.drug_exposure_start_date,		--target hospitalization info
	measurement.value_as_number as Cr, measurement.measurement_date as Cr_date
FROM case_control_or target
LEFT JOIN(SELECT person_id, measurement_concept_id, value_as_number, measurement_date
          FROM @A.measurement
          WHERE measurement_concept_id IN (@B)) measurement
ON target.person_id = measurement.person_id
WHERE (measurement.measurement_date BETWEEN target_start_date AND target_end_date)
ORDER BY person_id
"

cr = dbGetQuery(con, render(translate(sql, targetDialect = mydbtype), 
                             A= myschemaname,
                             B= Cr))
cr$diff = abs(cr$drug_exposure_start_date-cr$cr_date)
cr$cr = as.numeric(cr$cr)
cr$cr = ifelse(cr$cr<0.7, 1, ifelse(cr$cr>1.4, 2, 0))
cr=cr %>% select(target_visit_id, cr, diff)
cr = left_join(case_control_OR, cr, by=('target_visit_id'='target_visit_id'))
target=cr %>% select(target_visit_id) %>% distinct() %>% pull
temp=data.frame()
for (i in target){
  a=(cr %>% filter(target_visit_id==i) %>% arrange(diff))[1,]
  temp = rbind(temp,a)
}
temp = temp %>% select(-diff)
temp$cr = ifelse(is.na(temp$cr), 0, temp$cr)
case_control_OR = temp
case_control_OR$cr = as.factor(case_control_OR$cr)


##### GFR 
sql = "SELECT target.person_id, gender_concept_id, age_at_hospitalization, 	--person info
	target_visit_id, target.target_start_date, target.target_end_date, target.drug_exposure_start_date,		--target hospitalization info
	measurement.value_as_number as GFR, measurement.measurement_date as GFR_date
FROM case_control_or target
LEFT JOIN(SELECT person_id, measurement_concept_id, value_as_number, measurement_date
          FROM @A.measurement
          WHERE measurement_concept_id IN (@B)) measurement
ON target.person_id = measurement.person_id
WHERE (measurement.measurement_date BETWEEN target_start_date AND target_end_date)
ORDER BY person_id
"

gfr = dbGetQuery(con, render(translate(sql, targetDialect = mydbtype), 
                            A= myschemaname,
                            B= GFR))
gfr$diff = abs(gfr$drug_exposure_start_date-gfr$gfr_date)
gfr$gfr = as.numeric(gfr$gfr)
gfr$gfr = ifelse(gfr$gfr<90, 1, 0)
gfr=gfr %>% select(target_visit_id, gfr, diff)
gfr = left_join(case_control_OR, gfr, by=('target_visit_id'='target_visit_id'))
target=gfr %>% select(target_visit_id) %>% distinct() %>% pull
temp=data.frame()
for (i in target){
  a=(gfr %>% filter(target_visit_id==i) %>% arrange(diff))[1,]
  temp = rbind(temp,a)
}
temp = temp %>% select(-diff)
temp$gfr = ifelse(is.na(temp$gfr), 0, temp$gfr)
case_control_OR = temp
case_control_OR$gfr = as.factor(case_control_OR$gfr)

##### AST
sql = "SELECT target.person_id, gender_concept_id, age_at_hospitalization, 	--person info
	target_visit_id, target.target_start_date, target.target_end_date, target.drug_exposure_start_date,		--target hospitalization info
	measurement.value_as_number as AST, measurement.measurement_date as AST_date
FROM case_control_or target
LEFT JOIN(SELECT person_id, measurement_concept_id, value_as_number, measurement_date
          FROM @A.measurement
          WHERE measurement_concept_id IN (@B)) measurement
ON target.person_id = measurement.person_id
WHERE (measurement.measurement_date BETWEEN target_start_date AND target_end_date)
ORDER BY person_id
"

ast = dbGetQuery(con, render(translate(sql, targetDialect = mydbtype), 
                             A= myschemaname,
                             B= AST))
ast$diff = abs(ast$drug_exposure_start_date-ast$ast_date)
ast$ast = as.numeric(ast$ast)
ast$ast = ifelse(ast$ast>40, 1, 0)
ast=ast %>% select(target_visit_id, ast, diff)
ast = left_join(case_control_OR, ast, by=('target_visit_id'='target_visit_id'))
target=ast %>% select(target_visit_id) %>% distinct() %>% pull
temp=data.frame()
for (i in target){
  a=(ast %>% filter(target_visit_id==i) %>% arrange(diff))[1,]
  temp = rbind(temp,a)
}
temp = temp %>% select(-diff)
temp$ast = ifelse(is.na(temp$ast), 0, temp$ast)
case_control_OR = temp
case_control_OR$ast = as.factor(case_control_OR$ast)

##### ALT 
sql = "SELECT target.person_id, gender_concept_id, age_at_hospitalization, 	--person info
	target_visit_id, target.target_start_date, target.target_end_date, target.drug_exposure_start_date,		--target hospitalization info
	measurement.value_as_number as ALT, measurement.measurement_date as ALT_date
FROM case_control_or target
LEFT JOIN(SELECT person_id, measurement_concept_id, value_as_number, measurement_date
          FROM @A.measurement
          WHERE measurement_concept_id IN (@B)) measurement
ON target.person_id = measurement.person_id
WHERE (measurement.measurement_date BETWEEN target_start_date AND target_end_date)
ORDER BY person_id
"

alt = dbGetQuery(con, render(translate(sql, targetDialect = mydbtype), 
                             A= myschemaname,
                             B= ALT))
alt$diff = abs(alt$drug_exposure_start_date-alt$alt_date)
alt$alt = as.numeric(alt$alt)
alt$alt = ifelse(alt$alt>40, 1, 0)
alt=alt %>% select(target_visit_id, alt, diff)
alt = left_join(case_control_OR, alt, by=('target_visit_id'='target_visit_id'))
target=alt %>% select(target_visit_id) %>% distinct() %>% pull
temp=data.frame()
for (i in target){
  a=(alt %>% filter(target_visit_id==i) %>% arrange(diff))[1,]
  temp = rbind(temp,a)
}
temp = temp %>% select(-diff)
temp$alt = ifelse(is.na(temp$alt), 0, temp$alt)
case_control_OR = temp
case_control_OR$alt = as.factor(case_control_OR$alt)

##### Albumin
sql = "SELECT target.person_id, gender_concept_id, age_at_hospitalization, 	--person info
	target_visit_id, target.target_start_date, target.target_end_date, target.drug_exposure_start_date,		--target hospitalization info
	measurement.value_as_number as Albumin, measurement.measurement_date as Albumin_date
FROM case_control_or target
LEFT JOIN(SELECT person_id, measurement_concept_id, value_as_number, measurement_date
          FROM @A.measurement
          WHERE measurement_concept_id IN (@B)) measurement
ON target.person_id = measurement.person_id
WHERE (measurement.measurement_date BETWEEN target_start_date AND target_end_date)
ORDER BY person_id
"

albumin = dbGetQuery(con, render(translate(sql, targetDialect = mydbtype), 
                             A= myschemaname,
                             B= Albumin))
albumin$diff = abs(albumin$drug_exposure_start_date-albumin$albumin_date)
albumin$albumin = as.numeric(albumin$albumin)
albumin$albumin = ifelse(albumin$albumin<3.3, 1, 0)
albumin=albumin %>% select(target_visit_id, albumin, diff)
albumin = left_join(case_control_OR, albumin, by=('target_visit_id'='target_visit_id'))
target=albumin %>% select(target_visit_id) %>% distinct() %>% pull
temp=data.frame()
for (i in target){
  a=(albumin %>% filter(target_visit_id==i) %>% arrange(diff))[1,]
  temp = rbind(temp,a)
}
temp = temp %>% select(-diff)
temp$albumin = ifelse(is.na(temp$albumin), 0, temp$albumin)
case_control_OR = temp
case_control_OR$albumin = as.factor(case_control_OR$albumin)

##### BMI
sql = "SELECT DISTINCT target.person_id, gender_concept_id, age_at_hospitalization,	--person info
	target_visit_id, target.target_start_date, target.target_end_date, target.drug_exposure_start_date,		--target hospitalization info
	measurement.measurement_date as weight_date, measurement2.measurement_date as height_date,
	measurement.value_as_number as weight, measurement2.value_as_number as height
FROM case_control_or target
LEFT JOIN(SELECT person_id, measurement_concept_id, value_as_number, measurement_date
          FROM @A.measurement
          WHERE measurement_concept_id IN (@B)) measurement
ON target.person_id = measurement.person_id
LEFT JOIN(SELECT person_id, measurement_concept_id, value_as_number, measurement_date
          FROM @A.measurement
          WHERE measurement_concept_id IN (@C)) measurement2
ON target.person_id = measurement2.person_id
WHERE (measurement.measurement_date BETWEEN target_start_date AND target_end_date)
AND (measurement2.measurement_date BETWEEN target_start_date AND target_end_date)
ORDER BY person_id"

bmi = dbGetQuery(con, render(translate(sql, targetDialect = mydbtype), 
                                 A= myschemaname,
                                 B= Weight,
                                 C= Height))
bmi$diff = abs(bmi$drug_exposure_start_date-bmi$weight_date) + abs(bmi$drug_exposure_start_date-bmi$height_date)
bmi$weight = as.numeric(bmi$weight)
bmi$height = as.numeric(bmi$height)
bmi$weight = ifelse(is.na(bmi$weight),0,bmi$weight)
bmi$height = ifelse(is.na(bmi$height),0,bmi$height)
bmi$weight = ifelse(bmi$weight=='NULL',0,bmi$weight)
bmi$height = ifelse(bmi$height=='NULL',0,bmi$height)
for (i in 1:nrow(bmi)){
  bmi[i,'bmi'] = as.numeric(bmi[i,'weight'])/(as.numeric(bmi[i,'height'])/100)^2
}
bmi['bmi'] = round(bmi['bmi'], 1)
bmi = bmi %>% filter(between(bmi, 8, 43))   
bmi['bmi'] = ifelse(bmi$bmi<18.5, 1, ifelse(bmi$bmi>23, 2, 0))
bmi = bmi %>% select('target_visit_id','bmi','diff')

bmi= left_join(case_control_OR,bmi, by = c('target_visit_id'='target_visit_id'))

target=bmi %>% select(target_visit_id) %>% distinct() %>% pull
temp=data.frame()

for (i in target){
  a=(bmi %>% filter(target_visit_id==i) %>% arrange(diff))[1,]
  temp = rbind(temp,a)
}

for (i in 1:nrow(temp)){
  if (is.na(temp[i,'bmi'])){
    temp[i,'bmi']=0
  }
  else
    temp[i,'bmi']=temp[i,'bmi']
}

temp = temp %>% select(-diff)
case_control_OR = temp
case_control_OR$bmi = as.factor(case_control_OR$bmi)


##### Affective disorder
sql = "SELECT DISTINCT target.person_id, gender_concept_id, age_at_hospitalization,	--person info
target_visit_id, target.target_start_date, target.target_end_date,			--target hospitalization info
condition_start_date

FROM case_control_or target

LEFT JOIN(
  SELECT *
    
    FROM @A.condition_occurrence
  
  WHERE ext_cond_source_value_kcd LIKE 'F3%'
)co

ON target.person_id = co.person_id			--Join condition table

WHERE condition_start_date BETWEEN target_start_date-182 AND target_start_date


ORDER BY person_id,target_visit_id
"

affective_disorder = dbGetQuery(con, render(translate(sql, targetDialect = mydbtype), 
                                            A= myschemaname))

##### Dementia
sql = "SELECT DISTINCT target.person_id, gender_concept_id, age_at_hospitalization,	--person info
target_visit_id, target.target_start_date, target.target_end_date,			--target hospitalization info
condition_start_date

FROM case_control_or target

LEFT JOIN(
	SELECT *

	FROM @A.condition_occurrence

	WHERE ext_cond_source_value_kcd LIKE 'F00%'
	OR ext_cond_source_value_kcd LIKE 'F01%'
	OR ext_cond_source_value_kcd LIKE 'F02%'
	OR ext_cond_source_value_kcd LIKE 'F03%'
	OR ext_cond_source_value_kcd LIKE 'F04%'
	OR ext_cond_source_value_kcd LIKE 'G30%'
)co

ON target.person_id = co.person_id			--Join condition table

WHERE condition_start_date BETWEEN target_start_date-182 AND target_start_date


ORDER BY person_id,target_visit_id
"
dementia = dbGetQuery(con, render(translate(sql, targetDialect = mydbtype), 
                                  A= myschemaname))

##### Diabetes
sql = "SELECT DISTINCT target.person_id, gender_concept_id, age_at_hospitalization,	--person info
target_visit_id, target.target_start_date, target.target_end_date,			--target hospitalization info
condition_start_date

FROM case_control_or target

LEFT JOIN(
	SELECT *

	FROM @A.condition_occurrence

	WHERE ext_cond_source_value_kcd LIKE 'E10%'
	OR ext_cond_source_value_kcd LIKE 'E11%'
	OR ext_cond_source_value_kcd LIKE 'E12%'
	OR ext_cond_source_value_kcd LIKE 'E13%'
	OR ext_cond_source_value_kcd LIKE 'E14%'
)co

ON target.person_id = co.person_id			--Join condition table

WHERE condition_start_date BETWEEN target_start_date-182 AND target_start_date


ORDER BY person_id,target_visit_id
"
diabetes = dbGetQuery(con, render(translate(sql, targetDialect = mydbtype), 
                                  A= myschemaname))

##### Hypertension
sql = "SELECT DISTINCT target.person_id, gender_concept_id, age_at_hospitalization,	--person info
target_visit_id, target.target_start_date, target.target_end_date,			--target hospitalization info
condition_start_date

FROM case_control_or target

LEFT JOIN(
	SELECT *

	FROM @A.condition_occurrence

	WHERE ext_cond_source_value_kcd LIKE 'I10%'
	OR ext_cond_source_value_kcd LIKE 'I11%'
	OR ext_cond_source_value_kcd LIKE 'I12%'
	OR ext_cond_source_value_kcd LIKE 'I13%'
	OR ext_cond_source_value_kcd LIKE 'I14%'
	OR ext_cond_source_value_kcd LIKE 'I15%'
)co

ON target.person_id = co.person_id			--Join condition table

WHERE condition_start_date BETWEEN target_start_date-182 AND target_start_date


ORDER BY person_id,target_visit_id
"
hypertension = dbGetQuery(con, render(translate(sql, targetDialect = mydbtype), 
                                      A= myschemaname))

##### Parkinson
sql = "SELECT DISTINCT target.person_id, gender_concept_id, age_at_hospitalization,	--person info
target_visit_id, target.target_start_date, target.target_end_date,			--target hospitalization info
condition_start_date

FROM case_control_or target

LEFT JOIN(
	SELECT *

	FROM @A.condition_occurrence

	WHERE ext_cond_source_value_kcd LIKE 'G20%'
	OR ext_cond_source_value_kcd LIKE 'G21%'
	OR ext_cond_source_value_kcd LIKE 'G22%'
	OR ext_cond_source_value_kcd LIKE 'G23%'
	OR ext_cond_source_value_kcd LIKE 'G24%'
	OR ext_cond_source_value_kcd LIKE 'G25%'
	OR ext_cond_source_value_kcd LIKE 'G26%'
)co

ON target.person_id = co.person_id			--Join condition table

WHERE condition_start_date BETWEEN target_start_date-182 AND target_start_date


ORDER BY person_id,target_visit_id
"
parkinson = dbGetQuery(con, render(translate(sql, targetDialect = mydbtype), 
                                   A= myschemaname))

##### Psychotic disorder
sql = "SELECT DISTINCT target.person_id, gender_concept_id, age_at_hospitalization,	--person info
target_visit_id, target.target_start_date, target.target_end_date,			--target hospitalization info
condition_start_date

FROM case_control_or target

LEFT JOIN(
	SELECT *

	FROM @A.condition_occurrence

	WHERE ext_cond_source_value_kcd LIKE 'F2%'
)co

ON target.person_id = co.person_id			--Join condition table

WHERE condition_start_date BETWEEN target_start_date-182 AND target_start_date


ORDER BY person_id,target_visit_id
"
psychotic_disorder = dbGetQuery(con, render(translate(sql, targetDialect = mydbtype), 
                                            A= myschemaname))

##### Stroke
sql = "SELECT DISTINCT target.person_id, gender_concept_id, age_at_hospitalization,	--person info
target_visit_id, target.target_start_date, target.target_end_date,			--target hospitalization info
condition_start_date

FROM case_control_or target

LEFT JOIN(
	SELECT *

	FROM @A.condition_occurrence

	WHERE ext_cond_source_value_kcd LIKE 'I6%'
)co

ON target.person_id = co.person_id			--Join condition table

WHERE condition_start_date BETWEEN target_start_date-182 AND target_start_date


ORDER BY person_id,target_visit_id
"
stroke = dbGetQuery(con, render(translate(sql, targetDialect = mydbtype), 
                                A= myschemaname))

##### Vascular disease
sql = "SELECT DISTINCT target.person_id, gender_concept_id, age_at_hospitalization,	--person info
target_visit_id, target.target_start_date, target.target_end_date,			--target hospitalization info
condition_start_date

FROM case_control_or target

LEFT JOIN(
	SELECT *

	FROM @A.condition_occurrence

	WHERE ext_cond_source_value_kcd LIKE 'I7%'
)co

ON target.person_id = co.person_id			--Join condition table

WHERE condition_start_date BETWEEN target_start_date-182 AND target_start_date


ORDER BY person_id,target_visit_id
"
vascular_disease = dbGetQuery(con, render(translate(sql, targetDialect = mydbtype), 
                                          A= myschemaname))

condition = function(table, name){
  table[name] = 1
  table = table %>% select(target_visit_id, name) %>% distinct()
  temp = left_join(case_control_OR, table, by='target_visit_id')
  temp[name] = ifelse(is.na(temp[name]), 0, 1)
  return(temp)
}

case_control_OR = condition(affective_disorder,'affective_disorder')
case_control_OR = condition(dementia,'dementia')
case_control_OR = condition(diabetes,'diabetes')
case_control_OR = condition(hypertension,'hypertension')
case_control_OR = condition(parkinson,'parkinson')
case_control_OR = condition(psychotic_disorder,'psychotic_disorder')
case_control_OR = condition(stroke,'stroke')
case_control_OR = condition(vascular_disease,'vascular_disease')


##### CCI Score1
sql = "SELECT DISTINCT target.person_id, gender_concept_id, age_at_hospitalization,	--person info
	target_visit_id, target.target_start_date, target.target_end_date,			--target hospitalization info
	condition_start_date, ext_cond_source_value_kcd



FROM case_control_or target


LEFT JOIN(
	SELECT *

	FROM @A.condition_occurrence

	WHERE ext_cond_source_value_kcd LIKE 'I21%'
	OR ext_cond_source_value_kcd LIKE 'I22%'
	OR ext_cond_source_value_kcd LIKE 'I43%'
	OR ext_cond_source_value_kcd LIKE 'I50%'
	OR ext_cond_source_value_kcd LIKE 'I70%'
	OR ext_cond_source_value_kcd LIKE 'I71%'
	OR ext_cond_source_value_kcd LIKE 'G45%'
	OR ext_cond_source_value_kcd LIKE 'G46%'
	OR ext_cond_source_value_kcd LIKE 'I6%'
	OR ext_cond_source_value_kcd LIKE 'F00%'
	OR ext_cond_source_value_kcd LIKE 'F03%'
	OR ext_cond_source_value_kcd LIKE 'G30%'
	OR ext_cond_source_value_kcd LIKE 'J40%'
	OR ext_cond_source_value_kcd LIKE 'J41%'
	OR ext_cond_source_value_kcd LIKE 'J42%'
	OR ext_cond_source_value_kcd LIKE 'J43%'
	OR ext_cond_source_value_kcd LIKE 'J44%'
	OR ext_cond_source_value_kcd LIKE 'J45%'
	OR ext_cond_source_value_kcd LIKE 'J46%'
	OR ext_cond_source_value_kcd LIKE 'J47%'
	OR ext_cond_source_value_kcd LIKE 'J60%'
	OR ext_cond_source_value_kcd LIKE 'J61%'
	OR ext_cond_source_value_kcd LIKE 'J62%'
	OR ext_cond_source_value_kcd LIKE 'J63%'
	OR ext_cond_source_value_kcd LIKE 'J64%'
	OR ext_cond_source_value_kcd LIKE 'J65%'
	OR ext_cond_source_value_kcd LIKE 'J66%'
	OR ext_cond_source_value_kcd LIKE 'J67%'
	OR ext_cond_source_value_kcd LIKE 'M05%'
	OR ext_cond_source_value_kcd LIKE 'M06%'
	OR ext_cond_source_value_kcd LIKE 'M32%'
	OR ext_cond_source_value_kcd LIKE 'M33%'
	OR ext_cond_source_value_kcd LIKE 'M34%'
	OR ext_cond_source_value_kcd LIKE 'K25%'
	OR ext_cond_source_value_kcd LIKE 'K26%'
	OR ext_cond_source_value_kcd LIKE 'K27%'
	OR ext_cond_source_value_kcd LIKE 'K28%'
	OR ext_cond_source_value_kcd LIKE 'B18%'
	OR ext_cond_source_value_kcd LIKE 'K73%'
	OR ext_cond_source_value_kcd LIKE 'K74%'
	OR ext_cond_source_value_kcd IN ('I252', 'I110', 'I130', 'I099', 'I132', 'I255', 'I420', 'I425', 'I426', 'I427', 'I428', 'I429', 'P290',
									 'I731', 'I738', 'I739', 'I771', 'I790', 'I792', 'K551', 'K558', 'K559', 'Z958', 'Z959', 'H340', 'F051', 
									 'G311', 'I278', 'I279', 'J684', 'J701', 'J703', 'M315', 'M351', 'M353', 'M360', 'K700', 'K701', 'K702', 
									 'K703', 'K709', 'K713', 'K714', 'K715', 'K717', 'K760', 'K762', 'K763', 'K764', 'K768', 'K769', 'Z944', 
									 'E100', 'E101', 'E106', 'E108', 'E109', 'E110', 'E111', 'E116', 'E118', 'E119', 'E120', 'E121', 'E121', 
									 'E126', 'E128', 'E129', 'E130', 'E131', 'E136', 'E138', 'E139', 'E140', 'E141', 'E146', 'E148', 'E149')
)co

ON target.person_id = co.person_id			--Join condition table

WHERE condition_start_date BETWEEN target_start_date-182 AND target_start_date


ORDER BY person_id,target_visit_id
"

cci1 = dbGetQuery(con, render(translate(sql, targetDialect = mydbtype), 
                                          A= myschemaname))

##### CCI Score2
sql = "SELECT DISTINCT target.person_id, gender_concept_id, age_at_hospitalization,	--person info
	target_visit_id, target.target_start_date, target.target_end_date,			--target hospitalization info
	condition_start_date, ext_cond_source_value_kcd

FROM case_control_or target

LEFT JOIN(
	SELECT *

	FROM @A.condition_occurrence

	WHERE ext_cond_source_value_kcd LIKE 'G81%'
	OR ext_cond_source_value_kcd LIKE 'G82%'
	OR ext_cond_source_value_kcd LIKE 'N18%'
	OR ext_cond_source_value_kcd LIKE 'N19%'
	OR ext_cond_source_value_kcd LIKE 'C0%'
	OR ext_cond_source_value_kcd LIKE 'C1%'
	OR ext_cond_source_value_kcd LIKE 'C21%'
	OR ext_cond_source_value_kcd LIKE 'C22%'
	OR ext_cond_source_value_kcd LIKE 'C23%'
	OR ext_cond_source_value_kcd LIKE 'C24%'
	OR ext_cond_source_value_kcd LIKE 'C25%'
	OR ext_cond_source_value_kcd LIKE 'C26%'
	OR ext_cond_source_value_kcd LIKE 'C30%'
	OR ext_cond_source_value_kcd LIKE 'C31%'
	OR ext_cond_source_value_kcd LIKE 'C32%'
	OR ext_cond_source_value_kcd LIKE 'C33%'
	OR ext_cond_source_value_kcd LIKE 'C34%'
	OR ext_cond_source_value_kcd LIKE 'C37%'
	OR ext_cond_source_value_kcd LIKE 'C38%'
	OR ext_cond_source_value_kcd LIKE 'C39%'
	OR ext_cond_source_value_kcd LIKE 'C40%'
	OR ext_cond_source_value_kcd LIKE 'C41%'
	OR ext_cond_source_value_kcd LIKE 'C43%'
	OR ext_cond_source_value_kcd LIKE 'C45%'
	OR ext_cond_source_value_kcd LIKE 'C46%'
	OR ext_cond_source_value_kcd LIKE 'C47%'
	OR ext_cond_source_value_kcd LIKE 'C48%'
	OR ext_cond_source_value_kcd LIKE 'C49%'
	OR ext_cond_source_value_kcd LIKE 'C50%'
	OR ext_cond_source_value_kcd LIKE 'C51%'
	OR ext_cond_source_value_kcd LIKE 'C52%'
	OR ext_cond_source_value_kcd LIKE 'C53%'
	OR ext_cond_source_value_kcd LIKE 'C54%'
	OR ext_cond_source_value_kcd LIKE 'C55%'
	OR ext_cond_source_value_kcd LIKE 'C56%'
	OR ext_cond_source_value_kcd LIKE 'C57%'
	OR ext_cond_source_value_kcd LIKE 'C58%'
	OR ext_cond_source_value_kcd LIKE 'C6%'
	OR ext_cond_source_value_kcd LIKE 'C70%'
	OR ext_cond_source_value_kcd LIKE 'C71%'
	OR ext_cond_source_value_kcd LIKE 'C72%'
	OR ext_cond_source_value_kcd LIKE 'C73%'
	OR ext_cond_source_value_kcd LIKE 'C74%'
	OR ext_cond_source_value_kcd LIKE 'C75%'
	OR ext_cond_source_value_kcd LIKE 'C76%'
	OR ext_cond_source_value_kcd LIKE 'C81%'
	OR ext_cond_source_value_kcd LIKE 'C82%'
	OR ext_cond_source_value_kcd LIKE 'C83%'
	OR ext_cond_source_value_kcd LIKE 'C84%'
	OR ext_cond_source_value_kcd LIKE 'C85%'
	OR ext_cond_source_value_kcd LIKE 'C88%'
	OR ext_cond_source_value_kcd LIKE 'C90%'
	OR ext_cond_source_value_kcd LIKE 'C91%'
	OR ext_cond_source_value_kcd LIKE 'C92%'
	OR ext_cond_source_value_kcd LIKE 'C93%'
	OR ext_cond_source_value_kcd LIKE 'C94%'
	OR ext_cond_source_value_kcd LIKE 'C95%'
	OR ext_cond_source_value_kcd LIKE 'C96%'
	OR ext_cond_source_value_kcd LIKE 'C97%'
	OR ext_cond_source_value_kcd IN ('E102', 'E103', 'E104', 'E105', 'E107', 'E112', 'E115', 'E117', 'E122', 'E123', 'E124', 'E125', 'E127', 'E132', 
									 'E133', 'E134', 'E135', 'E137', 'E142', 'E143', 'E144', 'E145', 'E147', 'G041', 'G114', 'G801', 'G802', 'G830', 
									 'G831', 'G832', 'G833', 'G834', 'G839', 'I120', 'I131', 'I120', 'N032', 'N033', 'N034', 'N035', 'N036', 'N037', 
									 'N032', 'N052', 'N053', 'N054', 'N055', 'N056', 'N057', 'N250', 'Z490', 'Z491', 'Z492', 'Z940', 'Z992')
)co

ON target.person_id = co.person_id			--Join condition table

WHERE condition_start_date BETWEEN target_start_date-182 AND target_start_date


ORDER BY person_id,target_visit_id
"
cci2 = dbGetQuery(con, render(translate(sql, targetDialect = mydbtype), 
                              A= myschemaname))

##### CCI Score3
sql = "SELECT DISTINCT target.person_id, gender_concept_id, age_at_hospitalization,	--person info
	target_visit_id, target.target_start_date, target.target_end_date,			--target hospitalization info
	condition_start_date, ext_cond_source_value_kcd

FROM case_control_or target

LEFT JOIN(
	SELECT *

	FROM @A.condition_occurrence

	WHERE ext_cond_source_value_kcd IN ('I850', 'I859', 'I864', 'I982', 'K704', 'K711', 'K721', 'K729', 'K765', 'K766', 'K767')
)co


ON target.person_id = co.person_id			--Join condition table

WHERE condition_start_date BETWEEN target_start_date-182 AND target_start_date


ORDER BY person_id,target_visit_id
"
cci3 = dbGetQuery(con, render(translate(sql, targetDialect = mydbtype), 
                              A= myschemaname))


##### CCI Score6
sql = "SELECT DISTINCT target.person_id, gender_concept_id, age_at_hospitalization,	--person info
	target_visit_id, target.target_start_date, target.target_end_date,			--target hospitalization info
	condition_start_date, ext_cond_source_value_kcd

FROM case_control_or target

LEFT JOIN(
	SELECT *

	FROM @A.condition_occurrence

	WHERE ext_cond_source_value_kcd LIKE 'C77%'
	OR ext_cond_source_value_kcd LIKE 'C78%'
	OR ext_cond_source_value_kcd LIKE 'C79%'
	OR ext_cond_source_value_kcd LIKE 'C80%'
	OR ext_cond_source_value_kcd LIKE 'B20%'
	OR ext_cond_source_value_kcd LIKE 'B21%'
	OR ext_cond_source_value_kcd LIKE 'B22%'
	OR ext_cond_source_value_kcd LIKE 'B24%'
)co


ON target.person_id = co.person_id			--Join condition table

WHERE condition_start_date BETWEEN target_start_date-182 AND target_start_date


ORDER BY person_id,target_visit_id
"
cci6 = dbGetQuery(con, render(translate(sql, targetDialect = mydbtype), 
                              A= myschemaname))

target = cci1 %>% select(target_visit_id) %>% distinct() %>% pull
cci_score_1 = data.frame(target_visit_id=numeric(), score=numeric())
for (i in target){
  s = cci1 %>% filter(target_visit_id==i) %>% select(ext_cond_source_value_kcd) %>% distinct %>% count
  cci_score_1 = rbind(cci_score_1, data.frame(target_visit_id=i, score=s))
}
cci_score_1 = cci_score_1 %>% rename(cci_score_1 = n)
case_control_OR = left_join(case_control_OR, cci_score_1, by=('target_visit_id'))


target = cci2 %>% select(target_visit_id) %>% distinct() %>% pull
cci_score_2 = data.frame(target_visit_id=numeric(), score=numeric())
for (i in target){
  s = (cci2 %>% filter(target_visit_id==i) %>% select(ext_cond_source_value_kcd) %>% distinct %>% count)*2
  cci_score_2 = rbind(cci_score_2, data.frame(target_visit_id=i, score=s))
}
cci_score_2 = cci_score_2 %>% rename(cci_score_2 = n)
case_control_OR = left_join(case_control_OR, cci_score_2, by=('target_visit_id'))

target = cci3 %>% select(target_visit_id) %>% distinct() %>% pull
cci_score_3 = data.frame(target_visit_id=numeric(), score=numeric())
for (i in target){
  s = (cci3 %>% filter(target_visit_id==i) %>% select(ext_cond_source_value_kcd) %>% distinct %>% count)*3
  cci_score_3 = rbind(cci_score_3, data.frame(target_visit_id=i, score=s))
}
cci_score_3 = cci_score_3 %>% rename(cci_score_3 = n)
case_control_OR = left_join(case_control_OR, cci_score_3, by=('target_visit_id'))

target = cci6 %>% select(target_visit_id) %>% distinct() %>% pull
cci_score_6 = data.frame(target_visit_id=numeric(), score=numeric())
for (i in target){
  s = (cci6 %>% filter(target_visit_id==i) %>% select(ext_cond_source_value_kcd) %>% distinct %>% count)*6
  cci_score_6 = rbind(cci_score_6, data.frame(target_visit_id=i, score=s))
}
cci_score_6 = cci_score_6 %>% rename(cci_score_6 = n)
case_control_OR = left_join(case_control_OR, cci_score_6, by=('target_visit_id'))

case_control_OR$cci_score_1 = ifelse(is.na(case_control_OR$cci_score_1),0,case_control_OR$cci_score_1)
case_control_OR$cci_score_2 = ifelse(is.na(case_control_OR$cci_score_2),0,case_control_OR$cci_score_2)
case_control_OR$cci_score_3 = ifelse(is.na(case_control_OR$cci_score_3),0,case_control_OR$cci_score_3)
case_control_OR$cci_score_6 = ifelse(is.na(case_control_OR$cci_score_6),0,case_control_OR$cci_score_6)

case_control_OR$cci_score = case_control_OR$cci_score_1+case_control_OR$cci_score_2+case_control_OR$cci_score_3+case_control_OR$cci_score_6
case_control_OR = case_control_OR %>% select(-c(cci_score_1, cci_score_2, cci_score_3, cci_score_6))


##### ACB score1
sql = "select target.person_id, target.target_visit_id, target.target_start_date, target.target_end_date,	--target hospitalization info
drug.drug_exposure_start_date as acb_score_1, drug_source_value


FROM case_control_or target

LEFT JOIN(
	SELECT *

	FROM @A.drug_exposure

	WHERE drug_concept_id IN (@B) 
)drug


ON target.person_id = drug.person_id			

WHERE (drug.drug_exposure_start_date <= target_end_date)
 

ORDER BY person_id,target_visit_id
"

acb1 = dbGetQuery(con, render(translate(sql, targetDialect = mydbtype), 
                              A= myschemaname,
                              B= acb1_code))

temp = case_control_OR %>% select(target_visit_id, index_date)
acb1 = left_join(acb1, temp, by = c('target_visit_id' = 'target_visit_id'))
acb1 = acb1 %>% filter(!is.na(index_date))
target = acb1 %>% select(target_visit_id) %>% distinct() %>% pull
acb1_score = data.frame(target_visit_id=numeric(), score=numeric())
for (i in target){
  s = acb1 %>% filter(target_visit_id==i) %>% filter((acb_score_1 %>% as.Date >= index_date %>% as.Date - days(182))&(acb_score_1 %>% as.Date <= index_date %>% as.Date)) %>% select(drug_source_value) %>% distinct %>% count
  acb1_score = rbind(acb1_score, data.frame(target_visit_id=i, score=s))
}


##### ACB score2
sql = "select target.person_id, target.target_visit_id, target.target_start_date, target.target_end_date,	--target hospitalization info
drug.drug_exposure_start_date as acb_score_2, drug_source_value


FROM case_control_or target

LEFT JOIN(
	SELECT *

	FROM @A.drug_exposure

	WHERE drug_concept_id IN (@B) 
)drug


ON target.person_id = drug.person_id			

WHERE (drug.drug_exposure_start_date <= target_end_date)
 

ORDER BY person_id,target_visit_id
"
acb2 = dbGetQuery(con, render(translate(sql, targetDialect = mydbtype), 
                              A= myschemaname,
                              B= acb2_code))

acb2 = left_join(acb2, temp, by = c('target_visit_id' = 'target_visit_id'))
acb2 = acb2 %>% filter(!is.na(index_date))
target = acb2 %>% select(target_visit_id) %>% distinct() %>% pull
acb2_score = data.frame(target_visit_id=numeric(), score=numeric())
for (i in target){
  s = ( acb2 %>% filter(target_visit_id==i) %>% filter((acb_score_2 %>% as.Date >= index_date %>% as.Date - days(182))&(acb_score_2 %>% as.Date <= index_date %>% as.Date)) %>% select(drug_source_value) %>% distinct %>% count)*2
  acb2_score = rbind(acb2_score, data.frame(target_visit_id=i, score=s))
}


##### ACB score3
sql = "select target.person_id, target.target_visit_id, target.target_start_date, target.target_end_date,	--target hospitalization info
drug.drug_exposure_start_date as acb_score_3, drug_source_value


FROM case_control_or target

LEFT JOIN(
	SELECT *

	FROM @A.drug_exposure

	WHERE drug_concept_id IN (@B) 
)drug


ON target.person_id = drug.person_id			

WHERE (drug.drug_exposure_start_date <= target_end_date)
 

ORDER BY person_id,target_visit_id
"
acb3 = dbGetQuery(con, render(translate(sql, targetDialect = mydbtype), 
                              A= myschemaname,
                              B= acb3_code))

acb3 = left_join(acb3, temp, by = c('target_visit_id' = 'target_visit_id'))
acb3 = acb3 %>% filter(!is.na(index_date))
target = acb3 %>% select(target_visit_id) %>% distinct() %>% pull
acb3_score = data.frame(target_visit_id=numeric(), score=numeric())
for (i in target){
  s = ( acb3 %>% filter(target_visit_id==i) %>% filter((acb_score_3 %>% as.Date >= index_date %>% as.Date - days(182))&(acb_score_3 %>% as.Date <= index_date %>% as.Date)) %>% select(drug_source_value) %>% distinct %>% count)*2
  acb3_score = rbind(acb3_score, data.frame(target_visit_id=i, score=s))
}


acb1_score = acb1_score %>% rename(score1=n)
acb2_score = acb2_score %>% rename(score2=n)
acb3_score = acb3_score %>% rename(score3=n)

case_control_OR = left_join(case_control_OR, acb1_score, by=c('target_visit_id'))
case_control_OR = left_join(case_control_OR, acb2_score, by=c('target_visit_id'))
case_control_OR = left_join(case_control_OR, acb3_score, by=c('target_visit_id'))

case_control_OR$score1 = ifelse(is.na(case_control_OR$score1), 0, case_control_OR$score1)
case_control_OR$score2 = ifelse(is.na(case_control_OR$score2), 0, case_control_OR$score2)
case_control_OR$score3 = ifelse(is.na(case_control_OR$score3), 0, case_control_OR$score3)

case_control_OR['acb_score'] = case_control_OR['score1']+case_control_OR['score2']+case_control_OR['score3']

case_control_OR = case_control_OR %>% select(-c(score1, score2, score3))


##### 2. Analysis ############################################################################################################################################# 
#count N
countn <- list()
countn$gender_concept_id<-table(case_control_OR$gender_concept_id,case_control_OR$delirium)
countn$age_at_hospitalization<-table(case_control_OR$age_at_hospitalization,case_control_OR$delirium)
countn$zpd_3<-table(case_control_OR$zpd_3,case_control_OR$delirium)
countn$zpd_7<-table(case_control_OR$zpd_7,case_control_OR$delirium)
countn$short_ben_3<-table(case_control_OR$short_ben_3,case_control_OR$delirium)
countn$short_ben_7<-table(case_control_OR$short_ben_7,case_control_OR$delirium)
countn$intermediate_ben_3<-table(case_control_OR$intermediate_ben_3,case_control_OR$delirium)
countn$intermediate_ben_7<-table(case_control_OR$intermediate_ben_7,case_control_OR$delirium)
countn$long_ben_3<-table(case_control_OR$long_ben_3,case_control_OR$delirium)
countn$long_ben_7<-table(case_control_OR$long_ben_7,case_control_OR$delirium)
countn$s_anti_3<-table(case_control_OR$s_anti_3,case_control_OR$delirium)
countn$s_anti_7<-table(case_control_OR$s_anti_7,case_control_OR$delirium)
countn$opioid_3<-table(case_control_OR$opioid_3,case_control_OR$delirium)
countn$opioid_7<-table(case_control_OR$opioid_7,case_control_OR$delirium)
countn$dementia_med_3<-table(case_control_OR$dementia_med_3,case_control_OR$delirium)
countn$dementia_med_7<-table(case_control_OR$dementia_med_7,case_control_OR$delirium)
countn$bmi<-table(case_control_OR$bmi,case_control_OR$delirium)
countn$wbc<-table(case_control_OR$wbc,case_control_OR$delirium)
countn$hb<-table(case_control_OR$hb,case_control_OR$delirium)
countn$hct<-table(case_control_OR$hct,case_control_OR$delirium)
countn$bun<-table(case_control_OR$bun,case_control_OR$delirium)
countn$cr<-table(case_control_OR$cr,case_control_OR$delirium)
countn$gfr<-table(case_control_OR$gfr,case_control_OR$delirium)
countn$ast<-table(case_control_OR$ast,case_control_OR$delirium)
countn$alt<-table(case_control_OR$alt,case_control_OR$delirium)
countn$albumin<-table(case_control_OR$albumin,case_control_OR$delirium)
countn$affective_disorder<-table(case_control_OR$affective_disorder,case_control_OR$delirium)
countn$dementia<-table(case_control_OR$dementia,case_control_OR$delirium)
countn$diabetes<-table(case_control_OR$diabetes,case_control_OR$delirium)
countn$hypertension<-table(case_control_OR$hypertension,case_control_OR$delirium)
countn$parkinson<-table(case_control_OR$parkinson,case_control_OR$delirium)
countn$psychotic_disorder<-table(case_control_OR$psychotic_disorder,case_control_OR$delirium)
countn$stroke<-table(case_control_OR$stroke,case_control_OR$delirium)
countn$vascular_disease<-table(case_control_OR$vascular_disease,case_control_OR$delirium)
countn$acb_score<-table(case_control_OR$acb_score,case_control_OR$delirium)
countn$cci_score<-table(case_control_OR$cci_score,case_control_OR$delirium)

write_rds(countn, 'count.rds')

write.csv(table(case_control_OR$target_hospitalization_days,case_control_OR$delirium),'hospitalization_day.csv')

#as.factor
case_control_OR$bmi = as.factor(case_control_OR$bmi)
case_control_OR$wbc = as.factor(case_control_OR$wbc)
case_control_OR$hb = as.factor(case_control_OR$hb)
case_control_OR$hct = as.factor(case_control_OR$hct)
case_control_OR$bun = as.factor(case_control_OR$bun)
case_control_OR$cr = as.factor(case_control_OR$cr)

#3_days
#univariate
#데이터에 없는 변수는 지워주세요
variable_3days = c('zpd_3', 
                   'short_ben_3', 
                   'intermediate_ben_3', 
                   'long_ben_3', 
                   's_anti_3', 
                   'bmi', 
                   'wbc', 
                   'hb', 
                   'hct', 
                   'bun', 
                   'cr', 
                   'gfr', 
                   'ast', 
                   'alt', 
                   'albumin', 
                   'affective_disorder', 
                   'dementia', 
                   'diabetes', 
                   'hypertension', 
                   'parkinson', 
                   'psychotic_disorder', 
                   'stroke', 
                   'vascular_disease', 
                   'acb_score', 
                   'cci_score', 
                   'opioid_3', 
                   'dementia_med_3', 
                   'target_hospitalization_days',
                   'procedure')
full_model = c()


crude_OR_table <- data.frame()

for(var in variable_3days){
  if (var %in% c('bmi', 'wbc', 'hb', 'hct', 'bun', 'cr')){
    for (i in 1:2){
      fmla <- as.formula(paste0("delirium ~ ",var, " +strata(set)"))
      
      # fit Conditinal logistic model
      fit <- clogit(fmla, case_control_OR)
      
      ## capture summary stats
      OR <- summary(fit)$conf.int[i,1]
      lower_95 <- summary(fit)$conf.int[i,3]
      upper_95 <- summary(fit)$conf.int[i,4]
      p.value <- summary(fit)$coefficients[i,5]
      
      # get coefficents of fit
      coef_fit <- summary(fit)$coefficients[i,]
      conf_cfit <- summary(fit)$conf.int[i,]
      
      # create temporary data frame
      df <- data.frame(var = row.names(summary(fit)$coefficients)[i],
                       OR =  summary(fit)$coefficients[i,2],
                       lower_95 = summary(fit)$conf.int[i,3],
                       upper_95 = summary(fit)$conf.int[i,4],
                       p.value = summary(fit)$coefficients[i,5],
                       stringsAsFactors = F)
      crude_OR_table <- rbind(crude_OR_table, df)
      
      if (p.value < 0.1){
        full_model = append(full_model, var)
      }
    }
  }
  else{fmla <- as.formula(paste0("delirium ~ ",var, " +strata(set)"))
  
  # fit Conditinal logistic model
  fit <- clogit(fmla, case_control_OR)
  
  ## capture summary stats
  OR <- summary(fit)$conf.int[1]
  lower_95 <- summary(fit)$conf.int[3]
  upper_95 <- summary(fit)$conf.int[4]
  p.value <- summary(fit)$coefficients[5]
  
  # get coefficents of fit
  coef_fit <- summary(fit)$coefficients
  conf_cfit <- summary(fit)$conf.int
  
  # create temporary data frame
  df <- data.frame(var = row.names(summary(fit)$coefficients)[1],
                   OR =  summary(fit)$coefficients[2],
                   lower_95 = summary(fit)$conf.int[3],
                   upper_95 = summary(fit)$conf.int[4],
                   p.value = summary(fit)$coefficients[5],
                   stringsAsFactors = F)
  crude_OR_table <- rbind(crude_OR_table, df)
  if (p.value < 0.1){
    full_model = append(full_model, var)
  }
  }
}
full_model=unique(full_model)

write_csv(crude_OR_table,'output/univariate(3days).csv', row.names = FALSE)


#multivariate(full)
fmla <- as.formula(paste0("delirium~", paste(full_model, collapse = "+"), " +strata(set)"))
fit <- clogit(fmla, case_control_OR)
OR <- summary(fit)$coefficients[,2]
lower_95 <- summary(fit)$conf.int[,3]
upper_95 <- summary(fit)$conf.int[,4]
p.value <- summary(fit)$coefficients[,5]
coef_fit <- summary(fit)$coefficients
conf_cfit <- summary(fit)$conf.int

adjusted_OR_table <- data.frame(var = row.names(summary(fit)$coefficients),
                                OR =  summary(fit)$coefficients[,2],
                                lower_95 = summary(fit)$conf.int[,3],
                                upper_95 = summary(fit)$conf.int[,4],
                                p.value = summary(fit)$coefficients[,5],
                                stringsAsFactors = F)

write_csv(adjusted_OR_table,'output/multivariate(3days).csv', row.names = FALSE)


#multivariate(reduced)
reduced <- stepAIC(fit, direction = 'backward')
reduced$call[2] #실행 후 콘솔창 결과에서 변수부분(ex:zpd_3~target_hospitalization_days전까지)만 복사해서 아래 코드에 붙여넣기 부탁드립니다.
fmla <- as.formula(paste0("delirium~", '#이 부분에 붙여넣기 해주세요#', "+zpd_3+short_ben_3+intermediate_ben_3+long_ben_3+s_anti_3 +strata(set)"))
fit <- clogit(fmla, case_control_OR)
OR <- summary(reduced)$coefficients
lower_95 <- summary(fit)$conf.int[,3]
upper_95 <- summary(fit)$conf.int[,4]
p.value <- summary(fit)$coefficients[,5]
coef_fit <- summary(fit)$coefficients
conf_cfit <- summary(fit)$conf.int

adjusted_OR_table <- data.frame(var = row.names(summary(fit)$coefficients),
                                OR =  summary(fit)$coefficients[,2],
                                lower_95 = summary(fit)$conf.int[,3],
                                upper_95 = summary(fit)$conf.int[,4],
                                p.value = summary(fit)$coefficients[,5],
                                stringsAsFactors = F)

write_csv(adjusted_OR_table,'output/reduced(3days).csv', row.names = FALSE)



#7_days
#univariate
#데이터에 없는 변수는 지워주세요
variable_7days = c('zpd_7',
                   'short_ben_7', 
                   'intermediate_ben_7', 
                   'long_ben_7', 
                   's_anti_7', 
                   'bmi', 
                   'wbc',
                   'hb', 
                   'hct', 
                   'bun', 
                   'cr', 
                   'gfr', 
                   'ast', 
                   'alt', 
                   'albumin', 
                   'affective_disorder', 
                   'dementia', 
                   'diabetes', 
                   'hypertension', 
                   'parkinson', 
                   'psychotic_disorder', 
                   'stroke', 
                   'vascular_disease', 
                   'acb_score', 
                   'cci_score',
                   'opioid_7', 
                   'dementia_med_7', 
                   'target_hospitalization_days')
full_model = c()


crude_OR_table <- data.frame()

for(var in variable_7days){
  if (var %in% c('bmi', 'wbc', 'hb', 'hct', 'bun', 'cr')){
    for (i in 1:2){
      fmla <- as.formula(paste0("delirium ~ ",var, " +strata(set)"))
      
      # fit Conditinal logistic model
      fit <- clogit(fmla, case_control_OR)
      
      ## capture summary stats
      OR <- summary(fit)$conf.int[i,1]
      lower_95 <- summary(fit)$conf.int[i,3]
      upper_95 <- summary(fit)$conf.int[i,4]
      p.value <- summary(fit)$coefficients[i,5]
      
      # get coefficents of fit
      coef_fit <- summary(fit)$coefficients[i,]
      conf_cfit <- summary(fit)$conf.int[i,]
      
      # create temporary data frame
      df <- data.frame(var = row.names(summary(fit)$coefficients)[i],
                       OR =  summary(fit)$coefficients[i,2],
                       lower_95 = summary(fit)$conf.int[i,3],
                       upper_95 = summary(fit)$conf.int[i,4],
                       p.value = summary(fit)$coefficients[i,5],
                       stringsAsFactors = F)
      crude_OR_table <- rbind(crude_OR_table, df)
      
      if (p.value < 0.1){
        full_model = append(full_model, var)
      }
    }
  }
  else{fmla <- as.formula(paste0("delirium ~ ",var, " +strata(set)"))
  
  # fit Conditinal logistic model
  fit <- clogit(fmla, case_control_OR)
  
  ## capture summary stats
  OR <- summary(fit)$conf.int[1]
  lower_95 <- summary(fit)$conf.int[3]
  upper_95 <- summary(fit)$conf.int[4]
  p.value <- summary(fit)$coefficients[5]
  
  # get coefficents of fit
  coef_fit <- summary(fit)$coefficients
  conf_cfit <- summary(fit)$conf.int
  
  # create temporary data frame
  df <- data.frame(var = row.names(summary(fit)$coefficients)[1],
                   OR =  summary(fit)$coefficients[2],
                   lower_95 = summary(fit)$conf.int[3],
                   upper_95 = summary(fit)$conf.int[4],
                   p.value = summary(fit)$coefficients[5],
                   stringsAsFactors = F)
  crude_OR_table <- rbind(crude_OR_table, df)
  if (p.value < 0.1){
    full_model = append(full_model, var)
  }
  }
}
full_model=unique(full_model)

write_csv(crude_OR_table,'output/univariate(7days).csv', row.names = FALSE)

#multivariate(full)
fmla <- as.formula(paste0("delirium~", paste(full_model, collapse = "+"), " +strata(set)"))
fit <- clogit(fmla, case_control_OR)
OR <- summary(fit)$coefficients[,2]
lower_95 <- summary(fit)$conf.int[,3]
upper_95 <- summary(fit)$conf.int[,4]
p.value <- summary(fit)$coefficients[,5]
coef_fit <- summary(fit)$coefficients
conf_cfit <- summary(fit)$conf.int

adjusted_OR_table <- data.frame(var = row.names(summary(fit)$coefficients),
                                OR =  summary(fit)$coefficients[,2],
                                lower_95 = summary(fit)$conf.int[,3],
                                upper_95 = summary(fit)$conf.int[,4],
                                p.value = summary(fit)$coefficients[,5],
                                stringsAsFactors = F)

write_csv(adjusted_OR_table,'output/multivariate(7days).csv', row.names = FALSE)


#multivariate(reduced)
reduced <- stepAIC(fit, direction = 'backward')
reduced$call[2] #실행 후 콘솔창 결과에서 변수부분(ex:zpd_3~target_hospitalization_days전까지)만 복사해서 아래 코드에 붙여넣기 부탁드립니다.
fmla <- as.formula(paste0("delirium~", '#이 부분에 붙여넣기 해주세요#', "+zpd_7+short_ben_7+intermediate_ben_7+long_ben_7+s_anti_7+strata(set)"))
fit <- clogit(fmla, case_control_OR)
OR <- summary(reduced)$coefficients
lower_95 <- summary(fit)$conf.int[,3]
upper_95 <- summary(fit)$conf.int[,4]
p.value <- summary(fit)$coefficients[,5]
coef_fit <- summary(fit)$coefficients
conf_cfit <- summary(fit)$conf.int

adjusted_OR_table <- data.frame(var = row.names(summary(fit)$coefficients),
                                OR =  summary(fit)$coefficients[,2],
                                lower_95 = summary(fit)$conf.int[,3],
                                upper_95 = summary(fit)$conf.int[,4],
                                p.value = summary(fit)$coefficients[,5],
                                stringsAsFactors = F)

write_csv(adjusted_OR_table,'output/reduced(7days).csv', row.names = FALSE)


#생성되는 파일을 모두 메일로 보내주시면 감사하겠습니다.
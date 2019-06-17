library(RPostgreSQL)
library(dplyr)
library(stringr)
library(tidyr)
library(lubridate)
#library(openxlsx)
library(data.table)
dia<-format(today(),"%d/%m/%Y")
drv <- dbDriver("PostgreSQL")
# creates a connection to the postgres database
# note that "con" will be used later in each connection to the database
con <- dbConnect(drv, dbname = "",
                 host = "", port = ,
                 user = "", password = "******")
#Esta consulta resultará na movimentação dos pacientes que estavão internados na data 'dia'.
query = paste0("select f.nome_especialidade,
e.descricao as UnidadeFuncional,
h.descricao as Tp_movimento_internacao,
b.prontuario,b.nome,b.sexo ,
CAST (dt_nascimento AS VARCHAR (10)),
--CAST (dthr_lancamento AS VARCHAR (10)),
CAST (dthr_internacao AS VARCHAR (10)),
CAST (dthr_alta_medica AS VARCHAR (10)),
CAST (dt_saida_paciente AS VARCHAR (10)),
c.descricao as Mot_Alta,g.* ,
CAST (dthr_internacao AS VARCHAR (19)) as dthr_internacao2,
case when dthr_alta_medica is null then 0
     else 1 end as saida,
case when dthr_internacao::date=now()::date then 1
     else 0 end as internado,
date_part('days',coalesce(a.dt_saida_paciente,CURRENT_TIMESTAMP)-a.dthr_internacao) AS permanencia
from agh.ain_internacoes a join agh.aip_pacientes b on a.pac_codigo = b.codigo
left join agh.ain_tipos_alta_medica c on a.tam_codigo = c.codigo
--left join agh.ain_leitos d on a.lto_lto_id = d.lto_id
join agh.ain_movimentos_internacao g on a.seq = g.int_seq
join agh.ain_tipos_mvto_internacao h on g.tmi_seq = h.seq
join agh.agh_especialidades f on g.esp_seq = f.seq
join agh.agh_unidades_funcionais e on g.unf_seq = e.seq
where  (dthr_internacao::date<='",dia,"' and  (dthr_alta_medica is null or dthr_alta_medica::date='",dia,"'))
and g.dthr_lancamento::date<='",dia,"' 
--where date(dthr_internacao)>='01/01/2019'
--order by prontuario,seq dthr_alta_medica = null  or
order by int_seq, seq")

dados<-  dbGetQuery(con,query)
dbDisconnect(con)
set_utf8 = function(x){
  # Declare UTF-8 encoding on all character strings:
  for (i in 1:ncol(x)){
    if (is.character(x[, i])) Encoding(x[, i]) <- 'UTF-8'
  }
  # Same on column names:
  for (name in colnames(x)){
    Encoding(name) <- 'UTF-8'
  }
  return(x)
}
#Quando puxa o dado de um sistema deve-se codificar com UTF-8.
dados<-set_utf8(dados)

#Seleciona a última movimentação do paciente que corresponde ao local que este se encontra em 'dia'.
base<- dados %>% 
  group_by(int_seq) %>% 
  mutate(ultima=max(seq),permanencia=ifelse(saida==1,NA,permanencia))%>%
  filter(ultima==seq)
#Protegendo o anonimato das pessoas.
base$nome<-"aaaaa"
rm(dados)
rm(dia)



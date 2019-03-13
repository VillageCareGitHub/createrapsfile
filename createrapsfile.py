import os
import psycopg2
import configparser
import datetime
import time
import pyodbc
import pandas as pd


# Initializing Configuration
initConfig = configparser.ConfigParser()
initConfig.read("AWS_List.config")

# Creating RAPS File and writing Headers

# *Contract ID Headers for MAP and DSNP
header1='AAASH9967'
header2='BBB0000001H2168\n'


# *Formating Current Date
infodate=datetime.date.today()
infodate=str(infodate)
infodate=infodate.replace("-","")

# *Getting FileID
fileid=infodate+"00"

# *Getting File Indicator Either PROD or TEST
fileind='PROD'

# *Getting Diagnosis Type
diatype='ICD10'

# *Getting Full Header
fullheader=header1+fileid+infodate+fileind+diatype

#print(fullheader)

# Filename for RAPSFILE
rapsfilename="RAPSFILE_SUPPLEMENTAL_"+infodate+".txt"

# Opening File for creation and adding Headers
with open(rapsfilename,"w",encoding='utf-8') as rapsfile:
    rapsfile.write(fullheader+"\n")
    rapsfile.write(header2)


# Opening SQL Server and Query
driver='ODBC Driver 13 for SQL Server'
server=initConfig.get('profile prod', 'sqlserver')
port=initConfig.get('profile prod', 'sqlserverport')
database=initConfig.get('profile prod', 'sqlserverdb')
uid=initConfig.get('profile prod', 'sqlserveruser')
pwd=initConfig.get('profile prod', 'sqlserverpwd')


connstr = f"""Driver={driver};
Server={server},{port};
Database={database};
UID={uid};
PWD={pwd};
MARS_Connection=yes;
APP=RAPS FILE Supplemental Python Script
"""

conn=pyodbc.connect(connstr)
cur=conn.cursor()


SQL=""" select distinct rapsmember.hicn,

rapsmember.cin,
rapsmember.membno,


clcos.date_birth,

min(date_from) minfromdate,

max(date_from) maxtodate

 

from (select distinct hicn,[member id],fn.membno,fn.cin

from fn352 fn left join cmsrapfile rap on fn.HICN=rap.[member id]

where ((cast(fn.effdat as date) BETWEEN '01/01/2018' AND '12/31/2018') 
OR (cast(fn.expdat as date) BETWEEN '01/01/2018' AND '12/31/2018') 
OR (cast(fn.effdat as date)  <= '01/01/2018' AND (cast(fn.expdat as date) >= '12/31/2018' OR cast(fn.expdat as date) IS NULL)))

and not([RECORD-ID] in ('AAA','BBB','YYY','ZZZ'))

and fn.lob!='VCMMCD') rapsmember left join claimcos clcos on rapsmember.cin=clcos.cin

                                                       left join (select distinct cin,dx1 dx

from claimcos clcos

where clcos.lob in ('VCMMAP','VCMDLT','VCMDSN')

and clcos.date_from>='01/01/2018' and clcos.date_from<='12/31/2018'

and dx1!=''

union

select distinct cin,dx2 dx

from claimcos clcos

where clcos.lob in ('VCMMAP','VCMDLT','VCMDSN')

and clcos.date_from>='01/01/2018' and clcos.date_from<='12/31/2018'

and dx2!=''

union

select distinct cin,dx3 dx

from claimcos clcos

where clcos.lob in ('VCMMAP','VCMDLT','VCMDSN')

and clcos.date_from>='01/01/2018' and clcos.date_from<='12/31/2018'

and dx3!='') diag on rapsmember.cin=diag.cin

where clcos.date_from>='01/01/2018' and clcos.date_from<='12/31/2018'

and clcos.lob!='VCMMCD'

group by rapsmember.hicn,

rapsmember.cin,
rapsmember.membno,
clcos.date_birth


order by rapsmember.cin """

def calclength(calcstring):
    if len(calcstring)==7:
        return str(calcstring)
    if len(calcstring)==6:
        return "0"+str(calcstring)
    if len(calcstring)==5:
        return "00"+str(calcstring)
    if len(calcstring)==4:
        return "000"+str(calcstring)
    if len(calcstring)==3:
        return "0000"+str(calcstring)
    if len(calcstring)==2:
        return "00000"+str(calcstring)
    if len(calcstring)==1:
        return "000000"+str(calcstring)
    if len(calcstring)==0:
        return "00000000"

    return "0000000"

def howmanyspaces(maxlength,lengthoffield):    
    newlength=maxlength-lengthoffield
    if newlength<=0:
        newlength=0
        addspace=''
    if newlength>0:
         addspace=' '
    x=0
    while x<newlength:
        if x==0:
            addspace=' '
        if x>0:
            addspace=addspace+' '
        x+=1    

    return addspace


def getdiagnosis(cin,fromdate,todate,providertype):

    # Result holder
    results=""
    
    # Initializing Configuration
    initConfig = configparser.ConfigParser()
    initConfig.read("AWS_List.config")


    # Opening SQL Server and Query
    driver='ODBC Driver 13 for SQL Server'
    server=initConfig.get('profile prod', 'sqlserver')
    port=initConfig.get('profile prod', 'sqlserverport')
    database=initConfig.get('profile prod', 'sqlserverdb')
    uid=initConfig.get('profile prod', 'sqlserveruser')
    pwd=initConfig.get('profile prod', 'sqlserverpwd')


    connstr = f"""Driver={driver};
    Server={server},{port};
    Database={database};
    UID={uid};
    PWD={pwd};
    MARS_Connection=yes;
    APP=RAPS FILE Supplemental Python Script - get diagnosis
    """

    conn=pyodbc.connect(connstr)
    cur=conn.cursor()

    SQL=f""" select distinct cin,dx1 dx

    from claimcos clcos

    where clcos.lob in ('VCMMAP','VCMDLT','VCMDSN')

    and clcos.date_from>='01/01/2018' and clcos.date_from<='12/31/2018'

    and dx1!='' and clcos.cin='{cin}'

    union

    select distinct cin,dx2 dx

    from claimcos clcos

    where clcos.lob in ('VCMMAP','VCMDLT','VCMDSN')

    and clcos.date_from>='01/01/2018' and clcos.date_from<='12/31/2018'

    and dx2!='' and clcos.cin='{cin}'

    union

    select distinct cin,dx3 dx

    from claimcos clcos

    where clcos.lob in ('VCMMAP','VCMDLT','VCMDSN')

    and clcos.date_from>='01/01/2018' and clcos.date_from<='12/31/2018'

    and dx3!='' and clcos.cin='{cin}'"""

    diadata=pd.read_sql(SQL,conn)
    c=0
    for index, row in diadata.iterrows():
        c=c+1
        if c<=10:
            if results=="":
                results=str(providertype)+str(fromdate)+str(todate)+' '+str(row['dx'])+howmanyspaces(7,len(str(row['dx'])))+'      '
            elif results!="":
                results=results+str(providertype)+str(fromdate)+str(todate)+' '+str(row['dx'])+howmanyspaces(7,len(str(row['dx'])))+'      '


    conn.close()
    return results

def getriskassessmentcode(cin):
    # Result holder
    results=""
        
    # Initializing Configuration
    initConfig = configparser.ConfigParser()
    initConfig.read("AWS_List.config")


    # Opening SQL Server and Query
    driver='ODBC Driver 13 for SQL Server'
    server=initConfig.get('profile prod', 'sqlserver')
    port=initConfig.get('profile prod', 'sqlserverport')
    database=initConfig.get('profile prod', 'sqlserverdb')
    uid=initConfig.get('profile prod', 'sqlserveruser')
    pwd=initConfig.get('profile prod', 'sqlserverpwd')


    connstr = f"""Driver={driver};
        Server={server},{port};
        Database={database};
        UID={uid};
        PWD={pwd};
        MARS_Connection=yes;
        APP=RAPS FILE Supplemental Python Script - get diagnosis
        """

    conn=pyodbc.connect(connstr)
    cur=conn.cursor()

    SQL=f""" select distinct cin,dx1 dx

        from claimcos clcos

        where clcos.lob in ('VCMMAP','VCMDLT','VCMDSN')

        and clcos.date_from>='01/01/2018' and clcos.date_from<='12/31/2018'

        and dx1!='' and clcos.cin='{cin}'

        union

        select distinct cin,dx2 dx

        from claimcos clcos

        where clcos.lob in ('VCMMAP','VCMDLT','VCMDSN')

        and clcos.date_from>='01/01/2018' and clcos.date_from<='12/31/2018'

        and dx2!='' and clcos.cin='{cin}'

        union

        select distinct cin,dx3 dx

        from claimcos clcos

        where clcos.lob in ('VCMMAP','VCMDLT','VCMDSN')

        and clcos.date_from>='01/01/2018' and clcos.date_from<='12/31/2018'

        and dx3!='' and clcos.cin='{cin}'"""

    diadata=pd.read_sql(SQL,conn)
    c=0
    for index, row in diadata.iterrows():
        c=c+1
        if c<=10:
            if results=="":
                results='A   '
            elif results!="":
                results=results+'A   '


    conn.close()
    return results



    
    



# Looping through information to form details
rapsdata=pd.read_sql(SQL,conn)
conn.close()

c=0 #Sequence Count for CCC Record
with open(rapsfilename,"a+",encoding='utf-8') as rapsfile:
    for index, row in rapsdata.iterrows():
        c=c+1
        #fmt="%0s%4s"  
        

        seq="CCC"+calclength(str(c))
        dob=str(row['date_birth']).replace('-','')
        dob=dob[:8]
        minfromdate=str(row['minfromdate']).replace('-','')
        minfromdate=minfromdate[:8]
        maxtodate=str(row['maxtodate']).replace('-','')
        maxtodate=maxtodate[:8]

        
        #detailrecord="{0:13}{1:40}{2:25}{3:3}{4:8}{5:3}{6:2}{7:8}{8:8}{9:1}{10:7}{11:6}".format(seq,row['membno'],row['hicn'],'   ',dob,'   ','20',minfromdate,maxtodate,' ',row['PaidClaim_DiagnosisCode'],'      ')
        detailrecord="{0:13}{1:40}{2:25}{3:3}{4:8}{5:3}{6:320}{7:25}{8:4}{9:35}".format(seq,row['membno'],row['hicn'],'   ',dob,'   ',getdiagnosis(row['cin'],minfromdate,maxtodate,'20'),howmanyspaces(25,0),getriskassessmentcode(row['cin']),howmanyspaces(75,len(getriskassessmentcode(row['cin']))))
        rapsfile.write(detailrecord+"\n")
        #fmt=fmt % (seq,row['membno'])
        #print(detailrecord)
        #print(row['PaidClaim_DiagnosisCode'])


# Footer information
#YYY0000001H21680004503
#ZZZSH996701826100070000001
with open(rapsfilename,"a+",encoding='utf-8') as rapsfile:
    footer1="YYY0000001H2168"+calclength(str(c))
    footer2="ZZZSH9967"+fileid+calclength(str("1"))
    rapsfile.write(footer1+"\n")
    rapsfile.write(footer2)

# print(footer1)
# print(footer2)
print("Raps File Created!")
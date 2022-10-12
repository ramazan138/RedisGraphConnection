# from ast import alias
# from unittest import skip
from lib2to3.pytree import convert
from logging import exception
from sqlite3 import Cursor
from unittest import result
from venv import create
import pyodbc
import redis
# cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=localhost;PORT=1433;DATABASE=KURS_;UID=sa;PWD=asdasd;')
import  time

# from das.pyadsd import SA
from datetime import datetime

def ConnectSqlCar(Value):
    conn = pyodbc.connect('DRIVER={SQL Server};SERVER=localhost;PORT=1433;DATABASE=KURS_;UID=sa;PWD=asdasd;')
    cursor = conn.cursor() 
    cursor.execute('SELECT link.*   ,plaka.Create_Date,plaka.IL_,plaka.ILCE_,plaka.KURUM_ADI,plaka.LOKASYON_,plaka.TARIH_,plaka.VERİ_KAYNAĞI\
	 \
    FROM [KURS_].[dbo].[_LinkEnd] as link\
    inner join KURS_.dbo.Pts_Bağlantısı pts on pts.Unique_ID=link.Link_ID \
    inner join ARAÇ_ as arac on arac.Unique_ID=link.Entity_ID1\
    inner join PLAKATESPİT_ as plaka on plaka.Unique_ID=link.Entity_ID2\
    \
  where Entity_ID1 =?',Value)
    return cursor
    




def ConvertDate(Value):
    # print(Value)
    
    now1=datetime(2022, 1, 24, 23, 5, 39)
    try:
        if((Value.__class__)!=now1.__class__ or Value==None):
            return 0

        timestamp = int(datetime.timestamp(Value))
        return timestamp
    except Exception as e:
        print(e)
        return 0



r = redis.StrictRedis(host="127.0.0.1")

SAH="AD%"
AD="AD%"
OL="OL%"
YÖN="YÖN%"
SA="SA%"
SAH="SAH%"
take=10000  
skip1=0
import uuid
import string
conn12 = pyodbc.connect('DRIVER={SQL Server};SERVER=localhost;PORT=1433;DATABASE=KURS_;UID=sa;PWD=asdasd;')
while True:
    
    cursor = conn12.cursor() 
    cursor.execute(' SELECT   link.Entity_ID1   ,link.Entity_ID2  ,link.Link_ID \
    ,a.[Adı_],[Adı_Soyadı] ,[Kimlik_Numarası] ,a.Medeni_Hali,a.AltEntity as gender ,a.Create_Date as  sahisCreateDate ,a.Last_Upd_Date as SahisUpDate,a.Uyruğu_ \
    ,d.Adres_Bilgisi,d.İlçe_Adı,d.İli_ ,d.X_KG_Değeri,d.Y_DB_Değeri,d.Create_Date,d.Last_Upd_Date\
    ,adresB.Kullanım_Amacı ,adresB.Create_Date as linkadresCreate_Date \
        \
    ,sahibi.Tescil_Yapilan_Il,sahibi.Açıklama_ ,sahibi.Bilgi_Kaynağı\
    ,araba.Araç_Marka,araba.Araç_Model,araba.Plakası_,araba.Şase_Numarası,araba.AltEntity ,araba.Motor_Numarası \
    ,organ.Faaliyet_Adi,organ.Faaliyet_Ili \
    ,Yönetici.Görevi_ \
    ,olay.Olay_Türü_Alt ,olay.Olay_Yeri_il ,olay.Olay_Yeri_ilçe  ,olay.Dosya_No,olay.Olay_Türü,olay.Olay_Tarihi \
    ,olaylink.OLAY_NO ,olaylink.Olaydaki_Durum ,olaylink.Olaydaki_Rolü \
    ,telefon.Telefon_Numarası, telefon.Create_Date as TelCreateDate    \
        \
                  ,abone.İrtibat_Numarası,abone.Aktivasyon_Tarihi,abone.Kapanma_Tarihi\
    FROM [KURS_].[dbo].[SAHIS_] \
    as a \
    full join _LinkEnd  as link  on link.Entity_ID1= a.Unique_ID  \
    full Join Adres_ as d on link.Entity_ID2=d.Unique_ID  \
    full join Adres_Bağlantısı  as adresB on adresB.Unique_ID=link.Link_ID \
    full join OLAY_ as olay on olay.Unique_ID=link.Entity_ID2 \
    full join Olay_linki as olaylink on olaylink.Unique_ID=link.Link_ID \
    full join YöneticiOrtak_Bağlantısı as Yönetici on Yönetici.Unique_ID=link.Link_ID \
    full join ORGANIZASYON_ as organ on organ.Unique_ID=link.Entity_ID2 \
    full join ARAÇ_ as araba on araba.Unique_ID=link.Entity_ID2 \
    full join Sahibi_ as sahibi on sahibi.Unique_ID =link.Link_ID \
    full join Telefon_ as telefon on telefon.Unique_ID=link.Entity_ID2\
    full join Abonesi_ as abone on abone.Unique_ID=link.Link_ID\
    where (link.Entity_ID1 like ?  )  \
    order by link.Entity_ID1 OFFSET ? ROWS  FETCH NEXT ? ROWS ONLY ',SAH,skip1,take)


    deneme=""
    deneme1=""
    alisName="KomLink"
    alisName1="Komisim"
    alisName2="Konumisim"
    # id=1
    GRAPHNAME="Kom2"
    pipe=r.pipeline() 
    sayac=0

    valueDegiskenler={}

    # for i,(row,u) in enumerate(zip(cursor[0],(cursor.description)) ) :
        # [u[0]]=row[i]

    # print(valueDegiskenler)
    # print(valueDegiskenler["Entity_ID1"])    
    # print(cursor.description[0])
    # print(len(cursor.fetchall()))
    # for i in cursor:
    #     print(i[41],i[4])
    SqlDegerler=cursor.fetchall()
    FirstPerson=(SqlDegerler[0][0])
    # for i in range[(1,10):
    #     print(i)
    for rowas in  range(0,int(len(SqlDegerler) )): 
        # print(row[i],u[0])
        # print(row)
        # print(row["Entity_ID1"])
        valueDegiskenler={}   
        
        header1 = [i[0] for i in cursor.description]
        aaa=SqlDegerler[rowas]
        rows1 = [i for i in aaa]
        # print(rows1)
        # rows1.insert(0, header1)
        for row1,u in zip(header1,rows1):
            valueDegiskenler[row1]=u
        sayac+=1
        alisName="KomLink"
        alisName1="Komisim"
        alisName2="KomKonum"
        deneme=""
        deneme1=""
        Entitiy1UniqId=valueDegiskenler["Entity_ID1"]
        Entitiy2UniqId=valueDegiskenler["Entity_ID2"]
        LinkUniqId=valueDegiskenler["Link_ID"] 
        if(FirstPerson==Entitiy1UniqId):
            
        if((str(Entitiy1UniqId).startswith("SAH") and str(Entitiy2UniqId).startswith("OLA") ) and str(LinkUniqId).startswith("OL")):
            # #
            alisName+=uuid.uuid4().hex
            olayTuru=valueDegiskenler["Olay_Türü"] 
            olayNumarasi=valueDegiskenler["Dosya_No"] 
            olayTarihi=  ConvertDate(valueDegiskenler["Olay_Tarihi"])   
            OlayTuruAlt=valueDegiskenler["Olay_Türü_Alt"] 
            OlayYeriIl=valueDegiskenler["Olay_Yeri_il"] 
            OlayYeriIlce=valueDegiskenler["Olay_Yeri_ilçe"] 

            Tc=valueDegiskenler["Kimlik_Numarası"] 
            Adi=valueDegiskenler["Adı_"] 
            SoyAdi=valueDegiskenler["Adı_Soyadı"] 
            Gender=valueDegiskenler["gender"] 
            MedeniHali=valueDegiskenler["Medeni_Hali"]
            sahisCreateDate=ConvertDate(valueDegiskenler["sahisCreateDate"] )
            SahisUpDate=ConvertDate(valueDegiskenler["SahisUpDate"]) 
            Uyruğu_=valueDegiskenler["Uyruğu_"]  
            
            olayno=valueDegiskenler["OLAY_NO"] 
            olayrolu=str(valueDegiskenler["Olaydaki_Rolü"]) .translate(str.maketrans({"'":  r"\'"}))
            olaydakiDurumu=str(valueDegiskenler["Olaydaki_Durum"]) .translate(str.maketrans({"'":  r"\'"}))
            # #
            alisName1+=uuid.uuid4().hex
            deneme+=" merge("+alisName1+":Person{UniqId:'"+Entitiy1UniqId+"'}) "
            deneme+=" on create set "+alisName1+".TC='"+str(Tc)+"',"+alisName1+".Adi='"+str(Adi)+"',"+alisName1+".AdiSoyAdi='"+str(SoyAdi)+"',"+alisName1+".gender='"+str(Gender)+"',"+alisName1+".Medenihali='"+str(MedeniHali)+"' \
                ,"+alisName1+".sahisCreateDate="+str(sahisCreateDate)+","+alisName1+".SahisUpDate="+str(SahisUpDate)+","+alisName1+".uyrugu='"+str(Uyruğu_)+"'"
                         
            # deneme+=" merge ("+alisName1+":Person{TC:'"+str(Tc)+"',Adi:'"+str(Adi)+"',AdiSoyAdi:'"+str(SoyAdi)+"',gender:'"+str(Gender)+"',Medenihali:'"+str(MedeniHali)+"'}) "
            deneme+=" merge("+alisName+":Olay{UniqId:'"+Entitiy2UniqId+"'})"
            deneme+="  on create set "+alisName+".olayNumarasi='"+str(((olayNumarasi)))+"',"+alisName+".OlayTarihi="+str(olayTarihi)+","+alisName+".OlayTarihi1='"+str(valueDegiskenler["Olay_Tarihi"])+"',"+alisName+".OlayTuru='"+str(olayTuru)+"',"+alisName+".OlayTuruAlt='"+str(OlayTuruAlt)+"',"+alisName+".OlayYeriIl='"+str(OlayYeriIl)+"',"+alisName+".OlayYeriIlce='"+str(OlayYeriIlce)+"'    "
            deneme1+="  create ("+alisName1+")-[:SahisOlayi{olaydakiDurumu:'"+str(olaydakiDurumu)+"',olaydakiRolu:'"+str(olayrolu)+"',OLAY_NO:'"+str(olayno)+"'}]->("+alisName+")   "
         

        if((str(Entitiy1UniqId).startswith("SAH") and str(Entitiy2UniqId).startswith("ADR") ) and str(LinkUniqId).startswith("AD")):


            alisName+=uuid.uuid4().hex
            Tc=valueDegiskenler["Kimlik_Numarası"] 
            Adi=valueDegiskenler["Adı_"] 
            SoyAdi=valueDegiskenler["Adı_Soyadı"] 
            Gender=valueDegiskenler["gender"] 
            MedeniHali=valueDegiskenler["Medeni_Hali"] 
            sahisCreateDate=ConvertDate(valueDegiskenler["sahisCreateDate"] )
            SahisUpDate=ConvertDate(valueDegiskenler["SahisUpDate"]) 
            Uyruğu_=valueDegiskenler["Uyruğu_"] 

            AdresBilgisi=str(valueDegiskenler["Adres_Bilgisi"]).translate(str.maketrans({"'":  r"\'"}))
            ilceAdi=valueDegiskenler["İlçe_Adı"] 
            ili=valueDegiskenler["İli_"] 
            X_KG_Değeri=valueDegiskenler["X_KG_Değeri"] 
            Y_DB_Değeri=valueDegiskenler["Y_DB_Değeri"] 
            Create_Date= ConvertDate( valueDegiskenler["Create_Date"]) 
            Last_Upd_Date=ConvertDate(valueDegiskenler["Last_Upd_Date"] )
            linkadresCreate_Date=ConvertDate(valueDegiskenler["linkadresCreate_Date"]) 
            

            KullanimAmaci=valueDegiskenler["Kullanım_Amacı"]

            alisName1+=uuid.uuid4().hex
            deneme+="merge("+alisName1+":Person{UniqId:'"+Entitiy1UniqId+"'})"
            deneme+=" on create set "+alisName1+".TC='"+str(Tc)+"',"+alisName1+".Adi='"+str(Adi)+"',"+alisName1+".AdiSoyAdi='"+str(SoyAdi)+"',"+alisName1+".gender='"+str(Gender)+"',"+alisName1+".Medenihali='"+str(MedeniHali)+"' \
                ,"+alisName1+".sahisCreateDate="+str(sahisCreateDate)+","+alisName1+".SahisUpDate="+str(SahisUpDate)+","+alisName1+".uyrugu='"+str(Uyruğu_)+"'"
                
            deneme+=" merge ("+alisName+":Konum{UniqId:'"+Entitiy2UniqId+"'})  "
            # deneme+=" on create set "+alisName+".AdresBilgisi='"+str(AdresBilgisi)+"',"+alisName+".ilceAdi='"+str(ilceAdi)+"',"+alisName+".ili='"+str(ili)+"',"+alisName+".X='"+str(X_KG_Değeri)+"',"+alisName+".Y='"+str(Y_DB_Değeri)+"'\
            #     ,"+alisName+".createDate="+str(Create_Date)+" ,"+alisName+".createDate1='"+str(valueDegiskenler["Create_Date"])+"',"+alisName+".lastUpDate="+str(Last_Upd_Date)+" "
            # deneme1+=" create ("+alisName1+")-[:SonAdresi{KullanimAmaci:'"+str(KullanimAmaci)+"',createDate:"+str(linkadresCreate_Date)+",createDate1:'"+str(valueDegiskenler["linkadresCreate_Date"] )+"'}]->("+alisName+") "


            deneme+=" on create set "+alisName+".AdresBilgisi='"+str(AdresBilgisi)+"',"+alisName+".ilceAdi='"+str(ilceAdi)+"',"+alisName+".ili='"+str(ili)+"',"+alisName+".X='"+str(X_KG_Değeri)+"',"+alisName+".Y='"+str(Y_DB_Değeri)+"'\
                ,"+alisName+".createDate="+str(Create_Date)+" ,"+alisName+".createDate1='"+str(valueDegiskenler["Create_Date"])+"',"+alisName+".lastUpDate="+str(Last_Upd_Date)+" "
            deneme1+=" create ("+alisName1+")-[:AdresiTumu{KullanimAmaci:'"+str(KullanimAmaci)+"',createDate:"+str(linkadresCreate_Date)+",createDate1:'"+str(valueDegiskenler["linkadresCreate_Date"] )+"'}]->("+alisName+") "

        


        
        if((str(Entitiy1UniqId).startswith("SAH") and str(Entitiy2UniqId).startswith("ARA") ) and str(LinkUniqId).startswith("SA")):
            # ##(row[2])
            # ##(row[1])
            # ##(row[0])
            #
            alisName+=uuid.uuid4().hex
            
            alisName1+=uuid.uuid4().hex
            
            Tc=valueDegiskenler["Kimlik_Numarası"] 
            Adi=valueDegiskenler["Adı_"] 
            SoyAdi=valueDegiskenler["Adı_Soyadı"] 
            Gender=valueDegiskenler["gender"] 
            MedeniHali=valueDegiskenler["Medeni_Hali"]
            sahisCreateDate=ConvertDate(valueDegiskenler["sahisCreateDate"] )
            SahisUpDate=ConvertDate(valueDegiskenler["SahisUpDate"]) 
            Uyruğu_=valueDegiskenler["Uyruğu_"] 
 

            AracMarka=valueDegiskenler["Araç_Marka"] 
            AracModel=valueDegiskenler["Araç_Model"] 
            plakasi=valueDegiskenler["Plakası_"] 
            SaseNumarasi=valueDegiskenler["Şase_Numarası"] 
            Altentity=valueDegiskenler["AltEntity"] 
            Motor_Numarası=valueDegiskenler["Motor_Numarası"] 
            
            
            tescilYapilanİl=valueDegiskenler["Tescil_Yapilan_Il"] 

            deneme+="merge("+alisName1+":Person{UniqId:'"+Entitiy1UniqId+"'})"
            deneme+=" on create set "+alisName1+".TC='"+str(Tc)+"',"+alisName1+".Adi='"+str(Adi)+"',"+alisName1+".AdiSoyAdi='"+str(SoyAdi)+"',"+alisName1+".gender='"+str(Gender)+"',"+alisName1+".Medenihali='"+str(MedeniHali)+"' \
                ,"+alisName1+".sahisCreateDate="+str(sahisCreateDate)+","+alisName1+".SahisUpDate="+str(SahisUpDate)+","+alisName1+".uyrugu='"+str(Uyruğu_)+"'"
                
            if(  str(LinkUniqId).startswith("SAB") ):
                deneme+="merge("+alisName+":Arac{UniqId:'"+Entitiy2UniqId+"'})"
                deneme+=" on create set "+alisName+".AracMarka='"+str(AracMarka)+"', "+alisName+".AracModel='"+str(AracModel)+"',"+alisName+".Altentity='"+str(Altentity)+"',"+alisName+".SaseNumarasi='"+str(SaseNumarasi)+"',"+alisName+".motorNo='"+str(Motor_Numarası)+"'  "
                deneme1+="create ("+alisName1+")-[:SahibiAracBaglantisi{plakasi:'"+str(plakasi)+"',TescilYapilanil:'"+str(tescilYapilanİl)+"'}]->("+alisName+") "
                    
            elif(str(LinkUniqId).startswith("SA") ):
                deneme+="merge("+alisName+":Arac{UniqId:'"+Entitiy2UniqId+"'})"
                deneme+=" on create set "+alisName+".AracMarka='"+str(AracMarka)+"', "+alisName+".AracModel='"+str(AracModel)+"',"+alisName+".Altentity='"+str(Altentity)+"',"+alisName+".SaseNumarasi='"+str(SaseNumarasi)+"' ,"+alisName+".motorNo='"+str(Motor_Numarası)+"'  "
                deneme1+=" create ("+alisName1+")-[:Sahibi{plakasi:'"+str(plakasi)+"',TescilYapilanil:'"+str(tescilYapilanİl)+"'}]->("+alisName+") "
            else:
                print("PASS")


            result2=ConnectSqlCar(Entitiy2UniqId)
            result1=result2.fetchall()

            for PTS in  range(0,int(len(result1) )): 

                valueDegiskenlerPTS={}   
                
                header1 = [i[0] for i in result2.description]
                rows1 = [i for i in result1[PTS]]
                for row1,u in zip(header1,rows1):
                    valueDegiskenlerPTS[row1]=u
                if((valueDegiskenlerPTS["VERİ_KAYNAĞI"]) =="EPDK"):
                    # valueDegiskenlerPTS["Entity_ID2"]
                        
                    deneme+=" Merge("+alisName2+":Konum{UniqId:'"+valueDegiskenlerPTS["Entity_ID2"]+"'}) "
                    deneme+=" on create set "+alisName2+".IL='"+str( valueDegiskenlerPTS["IL_"])+"', "+alisName2+".ILCE='"+str(valueDegiskenlerPTS["ILCE_"])+"'\
                    ,"+alisName2+".LOKASYON='"+str(valueDegiskenlerPTS["LOKASYON_"])+"' , "+alisName2+".TARIH1='"+str(valueDegiskenlerPTS["TARIH_"])+"'\
                    , "+alisName2+".CREATEDATE1='"+str(  ( valueDegiskenlerPTS["Create_Date"]))+"'     "
                    deneme1+= " create("+alisName+")-[:EPDK{plakasi:'"+str(plakasi)+"',TARIH:"+str(ConvertDate( valueDegiskenlerPTS["TARIH_"]))+",CREATEDATE:"+str(ConvertDate(( valueDegiskenlerPTS["Create_Date"])))+"}]->("+alisName2+")  "
                    alisName2="KomKonum"
                    alisName2+=uuid.uuid4().hex
                    
                    # deneme1+="merge("+alisName+")\
                    #     -[:EPDK{plakasi:'"+str(plakasi)+"',TARIH:"+str(ConvertDate( valueDegiskenlerPTS["TARIH_"]))+",CREATEDATE:"+str(ConvertDate(( valueDegiskenlerPTS["Create_Date"])))+"}]->\
                    #     (:Konum{UniqId:'"+valueDegiskenlerPTS["Entity_ID2"]+"',\
                    #     IL:'"+str( valueDegiskenlerPTS["IL_"])+"',\
                    #     ILCE:'"+str(valueDegiskenlerPTS["ILCE_"])+"',\
                    #     LOKASYON:'"+str(valueDegiskenlerPTS["LOKASYON_"])+"',TARIH1:'"+str(valueDegiskenlerPTS["TARIH_"])+"'\
                    #     ,CREATEDATE1:'"+str(  ( valueDegiskenlerPTS["Create_Date"]))+"'})"


                else:
                    deneme+=" Merge("+alisName2+":Konum{UniqId:'"+valueDegiskenlerPTS["Entity_ID2"]+"'}) "
                    deneme+=" on create set "+alisName2+".IL='"+str( valueDegiskenlerPTS["IL_"])+"', "+alisName2+".ILCE='"+str(valueDegiskenlerPTS["ILCE_"])+"'\
                    ,"+alisName2+".LOKASYON='"+str(valueDegiskenlerPTS["LOKASYON_"])+"' , "+alisName2+".TARIH1='"+str(valueDegiskenlerPTS["TARIH_"])+"'\
                    , "+alisName2+".CREATEDATE1='"+str(  ( valueDegiskenlerPTS["Create_Date"]))+"'     "
                    deneme1+= " create("+alisName+")-[:DigerPTS{plakasi:'"+str(plakasi)+"',Veri_Kaynagi:'"+valueDegiskenlerPTS["VERİ_KAYNAĞI"]+"',TARIH:"+str(ConvertDate( valueDegiskenlerPTS["TARIH_"]))+",CREATEDATE:"+str(ConvertDate(( valueDegiskenlerPTS["Create_Date"])))+"}]->("+alisName2+")  "
                    alisName2="KomKonum"
                    alisName2+=uuid.uuid4().hex
                    # deneme1+=" merge("+alisName+")-[:DIGERPTS{plakasi:'"+str(plakasi)+"',TARIH:"+str(ConvertDate( valueDegiskenlerPTS["TARIH_"]))+",CREATEDATE:"+str(ConvertDate(( valueDegiskenlerPTS["Create_Date"])))+"\
                    #     ,VERIKAYNAGI:'"+str(  ( valueDegiskenlerPTS["VERİ_KAYNAĞI"]))+"'}]->(:Konum{UniqId:'"+valueDegiskenlerPTS["Entity_ID2"]+"',IL:'"+str( valueDegiskenlerPTS["IL_"])+"',ILCE:'"+str(valueDegiskenlerPTS["ILCE_"])+"',\
                    #     LOKASYON:'"+str(valueDegiskenlerPTS["LOKASYON_"])+"',TARIH1:'"+str(valueDegiskenlerPTS["TARIH_"])+"'\
                    #       ,CREATEDATE1:'"+str(  ( valueDegiskenlerPTS["Create_Date"]))+"'\
                    #           })  "



                        
        if((str(Entitiy1UniqId).startswith("SAH") and str(Entitiy2UniqId).startswith("ORG") ) and str(LinkUniqId).startswith("YÖN")):

            alisName+=uuid.uuid4().hex
            
            alisName1+=uuid.uuid4().hex
            Tc=valueDegiskenler["Kimlik_Numarası"] 
            Adi=valueDegiskenler["Adı_"] 
            SoyAdi=valueDegiskenler["Adı_Soyadı"] 
            Gender=valueDegiskenler["gender"] 
            MedeniHali=valueDegiskenler["Medeni_Hali"] 
            sahisCreateDate=ConvertDate(valueDegiskenler["sahisCreateDate"] )
            SahisUpDate=ConvertDate(valueDegiskenler["SahisUpDate"]) 
            Uyruğu_=valueDegiskenler["Uyruğu_"] 

            Faaliyetalani=str(valueDegiskenler["Faaliyet_Adi"]).translate(str.maketrans({"'":  r"\'"})) 
            Faaliyetili=valueDegiskenler["Faaliyet_Ili"] 
            gorevi=valueDegiskenler["Görevi_"] 

            deneme+=" merge("+alisName1+":Person{UniqId:'"+Entitiy1UniqId+"'}) "
            deneme+=" on create set "+alisName1+".TC='"+str(Tc)+"',"+alisName1+".Adi='"+str(Adi)+"',"+alisName1+".AdiSoyAdi='"+str(SoyAdi)+"',"+alisName1+".gender='"+str(Gender)+"',"+alisName1+".Medenihali='"+str(MedeniHali)+"' \
                ,"+alisName1+".sahisCreateDate="+str(sahisCreateDate)+","+alisName1+".SahisUpDate="+str(SahisUpDate)+","+alisName1+".uyrugu='"+str(Uyruğu_)+"'"
                 
            deneme+=" merge("+alisName+":Sirket{UniqId:'"+Entitiy2UniqId+"'}) "
            deneme+=" on create set "+alisName+".Faaliyetalani='"+str(Faaliyetalani)+"',"+alisName+".FaaliyetAdi='"+str(Faaliyetalani)+"',"+alisName+".FaaliyetIli='"+str(Faaliyetili)+"'  "
            deneme1+=" create ("+alisName1+")-[:yonetici{gorevi:'"+str(gorevi)+"'}]->("+alisName+")  "  
        if((str(Entitiy1UniqId).startswith("SAH") and str(Entitiy2UniqId).startswith("TEL") ) ):

            # ##(row)
            #
            alisName+=uuid.uuid4().hex
            
            alisName1+=uuid.uuid4().hex
            Tc=valueDegiskenler["Kimlik_Numarası"] 
            Adi=valueDegiskenler["Adı_"] 
            SoyAdi=valueDegiskenler["Adı_Soyadı"] 
            Gender=valueDegiskenler["gender"] 
            MedeniHali=valueDegiskenler["Medeni_Hali"] 
            sahisCreateDate=ConvertDate(valueDegiskenler["sahisCreateDate"] )
            SahisUpDate=ConvertDate(valueDegiskenler["SahisUpDate"]) 
            Uyruğu_=valueDegiskenler["Uyruğu_"] 

            Telefon_Numarası=valueDegiskenler["Telefon_Numarası"] 
            TelCreateDate= ConvertDate( valueDegiskenler["TelCreateDate"]) 
            Bilgi_Kaynağı=valueDegiskenler["Bilgi_Kaynağı"] 





            deneme+=" merge("+alisName1+":Person{UniqId:'"+Entitiy1UniqId+"'}) "
            deneme+=" on create set "+alisName1+".TC='"+str(Tc)+"',"+alisName1+".Adi='"+str(Adi)+"',"+alisName1+".AdiSoyAdi='"+str(SoyAdi)+"',"+alisName1+".gender='"+str(Gender)+"',"+alisName1+".Medenihali='"+str(MedeniHali)+"' \
                    ,"+alisName1+".sahisCreateDate="+str(sahisCreateDate)+","+alisName1+".SahisUpDate="+str(SahisUpDate)+","+alisName1+".uyrugu='"+str(Uyruğu_)+"'"
                    

            if(str(LinkUniqId).startswith("SA")):

                deneme+=" merge("+alisName+":Telefon{UniqId:'"+Entitiy2UniqId+"'}) "
                deneme+=" on create set "+alisName+".Telefon_Numarasi='"+str(Telefon_Numarası)+"',"+alisName+".TelCreateDate="+str(TelCreateDate)+","+alisName+".BilgiKaynagi='"+str(Bilgi_Kaynağı)+"'  "
                deneme1+=" create ("+alisName1+")-[:Sahibi]->("+alisName+")  "  

            if(str(LinkUniqId).startswith("AB")):
                İrtibat_Numarası=valueDegiskenler["İrtibat_Numarası"] 
                Aktivasyon_Tarihi=ConvertDate( valueDegiskenler["Aktivasyon_Tarihi"] )
                Kapanma_Tarihi= ConvertDate( valueDegiskenler["Kapanma_Tarihi"] )
 
                deneme+=" merge("+alisName+":Telefon{UniqId:'"+Entitiy2UniqId+"'}) "
                deneme+=" on create set "+alisName+".Telefon_Numarasi='"+str(Telefon_Numarası)+"',"+alisName+".AktivasyonTarihi="+str(Aktivasyon_Tarihi)+","+alisName+".KapanmaTarihi="+str(Kapanma_Tarihi)+","+alisName+".irtibatNumarasi='"+str(İrtibat_Numarası)+"' \
                    ,"+alisName+".BilgiKaynagi='"+str(Bilgi_Kaynağı)+"' "
                deneme1+=" create ("+alisName1+")-[:Abonesi]->("+alisName+")  "  










        del valueDegiskenler

        all1=deneme+deneme1
        if(all1):
            pipe.execute_command('GRAPH.QUERY', GRAPHNAME,all1 )
        if(sayac>=1000):
            # all1=deneme+deneme1
            sayac=0
            # #(sayac)
            # pipe.execute_command('GRAPH.QUERY', GRAPHNAME,all1 )
            if(True):
                t1=time.time() 
                try:
    
                        
                    pipe.execute()
                
                    time.sleep(0.01)
                except Exception as e:
                    print(e)
                print(time.time()-t1)
                skip1+=1000    

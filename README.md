# Henkilöautokannan keskeiset tiedot

Tämä Python-ohjelma näyttää Suomen henkilöautokannan keskeisiä tietoja.
Tiedot näytetään konsolissa ja kirjoitetaan raporttitiedostoon.

Ohjelma käyttää ajoneuvorekisterin avointa dataa.

Dataa analysoidaan pyspark-kirjastolla.
Pyspark on Python-api Apache Sparkin käyttämiseksi.
Apache Spark on Java-pohjainen datan hallinnan moottori (engine).

Näytettävät tiedot ovat:
- henkilöautojen määrä
- keskimääräinen ensirekisteröintipäivämäärä
- keskimääräinen matkamittarilukema
- käyttöluokat yleisyysjärjestyksessä
- korityypit yleisyysjärjestyksessä
- käyttövoimatyypit yleisyysjärjestyksessä
- sähköhybridiautojen tyypit yleisyysjärjestyksessä
- vaihteistotyypit yleisyysjärjestyksessä
- keskimääräinen ajoneuvon suurin nettoteho
- keskimääräinen painotettu WLTP-arvo (hiilidioksidipäästöt)
- suosituimmat merkit yleisyysjärjestyksessä
- suosituimmat mallit yleisyysjärjestyksessä.

# Ohjelman käyttäminen

Ohjelma käynnistetään komennolla "python passenger_cars_essentials.py".

Ohjelman toimimiseksi tarvitaan seuraavat asennukset/lataukset:
- Python-tulkki, vähintään versio 3.8
- Java-suoritusympäristö, versio 8 - 17
- Apache Spark (pyspark-kirjastoineen)
- tarvittavien ympäristömuuttujien asettaminen
- winutils.exe -moduuli (jos ohjelmaa käytetään Windows-koneella)
- ajoneuvorekisterin avoin data csv-muodossa.

## Apache Spark

- Apache Sparkin voi ladata osoitteesta https://spark.apache.org/downloads.html.
- Valitse "Spark release 3.5.0" ja "Pre-built for Apache Hadoop 3.3 and later".
- Käynnistä lataus klikkaamalla "Download Spark: spark-3.5.0-bin-hadoop3.tgz".
- Pura ladattu tgz-paketti. Luo kansio "c:\apps". Siirrä purettu kansio "spark-3.5.0-bin-hadoop3" kansioon "c:\apps".
- Apache Spark sisältää pyspark-kirjaston.

## Ympäristömuuttujat

- Lisää ympäristömuuttuja: SPARK_HOME = C:\apps\spark-3.5.0-bin-hadoop3.
- Lisää ympäristömuuttuja: HADOOP_HOME = C:\apps\spark-3.5.0-bin-hadoop3.
- Lisää ympäristömuuttuja: PYTHONPATH = C:\apps\spark-3.5.0-bin-hadoop3\python;C:\apps\spark-3.5.0-bin-hadoop3\python\lib\py4j-0.10.9.7-src.zip.
- Lisää ympäristömuuttuja: PYSPARK_PYTHON = [Python-asennuksesi pääkansio, esim. C:\Python311].
- Lisää PATH-ympäristömuuttujaan arvo: C:\apps\spark-3.5.0-bin-hadoop3\bin.

## Winutils

- Jos käytät Windows-konetta, lataa "winutils.exe" osoitteesta https://github.com/steveloughran/winutils/blob/master/hadoop-3.0.0/bin/winutils.exe.
- (Binääritiedoston lataamiseksi klikkaa pientä tekstiä "Raw".)
- Siirrä ladattu winutils.exe kansioon "C:\apps\spark-3.5.0-bin-hadoop3\bin".

## Ajoneuvojen avoin data

- Lataa "Ajoneuvojen avoin data" osoitteesta https://tieto.traficom.fi/fi/tietotraficom/avoin-data?toggle=Ajoneuvojen%20avoin%20data.
- ZIP-pakatun tiedoston koko on 217 Mt. Puretun CSV-muotoisen tiedoston koko on 919 Mt.
- Siirrä ladattu ja purettu tiedosto koneellasi samaan kansioon, johon kopioit tämän repositoryn Python-skriptit!

## Käynnistäminen ja ohjelman toiminta

Käynnistä ohjelma komennolla "python passenger_cars_essentials.py".
On hyvä tietää, että taustalla toimivan Apache Sparkin käynnistyminen sekä csv-datan lukeminen ja käsittely ottavat aikansa.
Ohjelman suorittaminen kestänee koneesta riippuen joistakin minuuteista jopa 15 minuuttiin.

Konsoliin saattaa tulostua varoituksia (WARNING) tai virheitä (ERROR).
Nämä liittyvät Apache Sparkin tai pyspark-kirjaston asetuksiin.
Itse ohjelman pitäisi toimia ja tuottaa haluttu tulos.

## Ohjelman tulosten tulkinta

Tulostaulukoiden tulkintaan annetaan tulosraportissa joitakin ohjeita.
Lisäohjeita tulostaulukoiden koodien tulkintaan on info-kansiosta löytyvässä excel-tiedostossa.

## Lisätietoa

- Pyspark: https://sparkbyexamples.com/pyspark-tutorial/.
- Apache Spark: https://spark.apache.org/.
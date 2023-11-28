# PASSENGER CAR ESSENTIALS IN THE FINNISH VEHICLE REGISTRY
# This script shows the essential characteristics of the Finnish passenger car base
# Using the vehicle registry open data
# See the main function below to see the characteristic values

import os
import sys
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DateType
from pyspark.sql.functions import udf, mean, desc, col
import datetime
import utils, intro

# Initial settings and variables
os.environ['PYSPARK_PYTHON'] = sys.executable
spark = None
cars = None
table_count = 12 # -> The number of characteristic values, which are shown in a table form

# Creates a Spark session, reads vehicle data from the csv and filters passengers cars from the csv
def getCarsData():
    global spark
    global cars

    print('\nKäynnistetään tietojen lukemisen ja analysoimisen taustatoiminnot, tämä voi kestää usean minuutin...\n')
    spark = SparkSession.builder \
        .config('spark.log.level', 'off') \
        .config('spark.ui.showConsoleProgress', 'false') \
        .appName('Passenger Cars Essentials in the Finnish Vehicle Registry') \
        .getOrCreate()

    # Read vehicle registry data from a csv file
    # (Option inferSchema sets column kayttoonottopvm to wrong type (is string, should be date))
    # (But this column is not used in this script)
    os.system('cls')
    print('Luetaan ajoneuvorekisterin pohjadataa, tämä voi kestää usean minuutin...\n')
    df = spark.read \
        .option('delimiter', ';') \
        .option('header', True) \
        .option('inferSchema', True) \
        .csv('Ajoneuvojen_avoin_data_5_21.csv')
    
    # Include passenger cars
    print('Suodatetaan mukaan henkilöautot...')
    cars = df.filter( (df.ajoneuvoluokka == 'M1') | (df.ajoneuvoluokka == 'M1G') )

    os.system('cls')
    intro.waitMessage(table_count)

# Groups and shows a given column's data
def showGrouped(column):
    intro.runInfo(column)
    grouped = cars.groupBy(column).count().withColumnRenamed("count","lkm").orderBy(col('lkm').desc())
    utils.own_show(grouped)

# Calculates and shows a given column's mean value
def showMean(column, unit):
    intro.runInfo(column)
    avg = int((cars.select(mean(column))).collect()[0][0])
    col_name = (f'{column}, keskiarvo ({unit})')
    utils.own_show(spark.createDataFrame([{col_name: avg}]))

# Shows the number of passenger cars in Finland
def show_cars_count():
    column = 'Henkilöautojen lkm'
    intro.runInfo(column)
    cars_count = cars.count()
    utils.own_show(spark.createDataFrame([{column: cars_count}]))

# Shows the average first registration date
def show_avg_first_registration():
    column = 'Ensirekisteröinti, keskiarvo'
    intro.runInfo(column)
    ordinal = udf(lambda date: date.toordinal() if (date is not None) else None, IntegerType())
    avg_first_reg = int((cars.select(mean(ordinal('ensirekisterointipvm')))).collect()[0][0])
    avg_first_reg_date = datetime.date.fromordinal(avg_first_reg)
    utils.own_show(spark.createDataFrame([{column: avg_first_reg_date}]))

# Shows the count of the types of HEV cars (hybrid electric vehicles)
def show_HEV_types():
    column = 'sahkohybridinluokka'
    intro.runInfo(column)
    hev_types = cars.filter(cars.sahkohybridi == True) \
        .groupBy(column).count() \
        .withColumnRenamed("count","lkm").orderBy(col('lkm').desc())
    utils.own_show(hev_types)

def main():
    save_location = intro.confirmRun() # Gets a run confirmation and a save location
    getCarsData() # Creates a Spark session, reads vehicle data from the csv and filters passengers cars from the csv
    show_cars_count() # Shows the number of passenger cars in Finland
    show_avg_first_registration() # Shows the average first registration date
    showMean('matkamittarilukema', 'km') # Shows the average mileage in km
    showGrouped('ajoneuvonkaytto') # Shows the count of the usage types (private, rental etc)
    showGrouped('korityyppi') # Shows the count of the bodywork types
    showGrouped('kayttovoima') # Shows the count of the driving power types
    show_HEV_types() # Shows the count of the types of HEV cars
    showGrouped('vaihteisto') # Shows the count of the transmission types
    showMean('suurinNettoteho', 'kW') # Shows the average net power in kW
    showMean('WLTP2_Co2', 'g/km') # Shows the average CO2 emissions in weighted WLTP in grams/km
    showGrouped('merkkiSelvakielinen') # Shows (the most common) brands
    showGrouped('kaupallinenNimi') # Shows (the most common) models
    utils.writeToFile(save_location) # Writes the result tables to a file
    print(f'\nValmis. Tiedot kirjoitettu tiedostoon {save_location}\n')

main()

# Stops the Spark session
spark.stop()
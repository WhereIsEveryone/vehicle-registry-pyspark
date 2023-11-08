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
import pandas as pd
import datetime

os.environ['PYSPARK_PYTHON'] = sys.executable

spark = SparkSession.builder \
    .appName('Passenger Cars Essentials in the Finnish Vehicle Registry') \
    .getOrCreate()

# Read vehicle registry data from a csv file
# (Option inferSchema sets column kayttoonottopvm to wrong type (is string, should be date))
# (But this column is not used in this script)
df = spark.read \
    .option('delimiter', ';') \
    .option('header', True) \
    .option('inferSchema', True) \
    .csv('./data/Ajoneuvojen_avoin_data_5_21.csv')

# Include passenger cars
cars = df.filter( (df.ajoneuvoluokka == 'M1') | (df.ajoneuvoluokka == 'M1G') )

# Number of rows when showing grouped data
show_items = 20

# Shows a single value with a header in a table
def own_show(header, value):
    length = len(header) if (len(header) >= len(str(value))) else len(str(value))
    length += 4
    def dash():
        for i in range(length):
            end_char = '' if (i < length-1) else '\n' 
            print('-', end=end_char)
    dash()
    print(f'|{header.center(length-2)}|')
    dash()
    print(f'|{str(value).center(length-2)}|')
    dash()

# Groups and shows a given column's data
def showGrouped(column):
    cars.groupBy(column).count().withColumnRenamed("count","lkm").orderBy(col('lkm').desc()).show(show_items)

# Calculates and shows a given column's mean value
def showMean(column, unit):
    avg = int((cars.select(mean(column))).collect()[0][0])
    own_show(f'{column}, keskiarvo ({unit})', avg)

# Explanation in the main function
def show_cars_count():
    own_show('Henkilöautojen lkm', cars.count())

# Explanation in the main function
def show_avg_first_registration():
    ordinal = udf(lambda date: date.toordinal() if (date is not None) else None, IntegerType())
    avg_first_reg = int((cars.select(mean(ordinal('ensirekisterointipvm')))).collect()[0][0])
    own_show('Ensirekisteröinti, keskiarvo', datetime.date.fromordinal(avg_first_reg))

# Explanation in the main function
def show_HEV_types():
    cars.filter(cars.sahkohybridi == True) \
        .groupBy('sahkohybridinluokka').count() \
        .withColumnRenamed("count","lkm").orderBy(col('lkm').desc()) \
        .show()

def main():
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

main()

spark.stop()
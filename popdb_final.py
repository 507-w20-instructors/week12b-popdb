import json
import requests
import csv
import sqlite3

DB_NAME = 'choc_bars.sqlite'

def load_countries():
    base_url = 'https://restcountries.eu/rest/v2/all'
    countries = requests.get(base_url).json()
    # print(json.dumps(countries, indent=2))

    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    insert_sql = '''
        INSERT INTO "Countries"
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    '''

    for c in countries:
        eng_name = c['name']
        alpha2 = c['alpha2Code']
        alpha3 = c['alpha3Code']
        region = c['region']
        subregion = c['subregion']
        population = c['population']
        
        area = c['area']
        cur.execute(insert_sql,
            [ 
                None, alpha2, alpha3, eng_name, 
                region, subregion, population, area
            ]
        )
    conn.commit()
    conn.close()

def load_bars():
    bars_csv_contents = open('flavors_of_cacao_cleaned.csv', 'r')
    bars_csv_reader = csv.reader(bars_csv_contents, delimiter=',')
    next(bars_csv_reader) # skip the first row

    insert_sql = '''
        INSERT INTO "Bars"
        VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''
    country_by_name_sql = '''
        SELECT Id 
        FROM Countries
        WHERE EnglishName = ?
    '''
    '''
        "Id" INTEGER PRIMARY KEY AUTOINCREMENT, 
        "Company" TEXT NOT NULL,
        "SpecificBeanBarName" TEXT NOT NULL, 
        "REF" TEXT NOT NULL,
        "ReviewDate" TEXT NOT NULL,
        "CocoaPercent" REAL NOT NULL,
        "CompanyLocationId" INTEGER NOT NULL, 
        "Rating" REAL NOT NULL, 
        "BeanType" TEXT NOT NULL, 
        "BroadBeanOrigin" INTEGER
    '''
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    for row in bars_csv_reader:
        print(row)
        print('looking up', row[5], row[8])
        cur.execute(country_by_name_sql, [row[5]])
        sell_country = cur.fetchone()
        if (sell_country is not None):
            sell_country = sell_country[0]

        cur.execute(country_by_name_sql, [row[8]])
        source_country = cur.fetchone()
        if (source_country is not None):
            source_country = source_country[0]

        row[5] = sell_country
        row[8] = source_country

        cur.execute(insert_sql, row)
    
    conn.commit()
    conn.close()

def create_db():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    drop_bars_sql = 'DROP TABLE IF EXISTS "Bars"'
    drop_countries_sql = 'DROP TABLE IF EXISTS "Countries"'
    
    create_bars_sql = '''
        CREATE TABLE IF NOT EXISTS "Bars" (
            "Id" INTEGER PRIMARY KEY AUTOINCREMENT, 
            "Company" TEXT NOT NULL,
            "SpecificBeanBarName" TEXT NOT NULL, 
            "REF" TEXT NOT NULL,
            "ReviewDate" TEXT NOT NULL,
            "CocoaPercent" REAL NOT NULL,
            "CompanyLocationId" INTEGER NOT NULL, 
            "Rating" REAL NOT NULL, 
            "BeanType" TEXT NOT NULL, 
            "BroadBeanOrigin" INTEGER 
        )
    '''
    create_countries_sql = '''
        CREATE TABLE IF NOT EXISTS 'Countries'(
            'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
            'Alpha2' TEXT NOT NULL,
            'Alpha3' TEXT NOT NULL,
            'EnglishName' TEXT NOT NULL,
            'Region' TEXT NOT NULL,
            'Subregion' TEXT NOT NULL,
            'Population' INTEGER NOT NULL,
            'Area' REAL 
        )
    '''
    cur.execute(drop_bars_sql)
    cur.execute(drop_countries_sql)
    cur.execute(create_countries_sql)
    cur.execute(create_bars_sql)
    conn.commit()
    conn.close()

create_db()
load_countries()
load_bars()


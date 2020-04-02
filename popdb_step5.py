import csv
import sqlite3
import requests

DB_NAME = 'choc_bars.sqlite'
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
            "CompanyLocationId" INTEGER, 
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

def load_bars():
    file_contents = open('flavors_of_cacao_cleaned.csv', 'r')
    csv_reader = csv.reader(file_contents)
    next(csv_reader)

    select_country_id_sql = '''
        SELECT Id FROM Countries
        WHERE EnglishName = ?
    '''

    insert_bar_sql = '''
        INSERT INTO Bars
        VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''

    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    for row in csv_reader:
        
        # get Id for company location
        cur.execute(select_country_id_sql, [row[5]])
        res = cur.fetchone()
        company_location_id = None
        if res is not None:
            company_location_id = res[0]

        # get Id for origin location
        cur.execute(select_country_id_sql, [row[8]])
        res = cur.fetchone()
        origin_location_id = None
        if res is not None:
            origin_location_id = res[0]

        cur.execute(insert_bar_sql, [
            row[0], # Company
            row[1], # SpecificBeanBarName
            row[2], # REF
            row[3], # ReviewDate
            row[4], # CocoaPercent
            company_location_id, 
            row[6], # Rating
            row[7], # BeanType
            origin_location_id
        ])
    conn.commit()
    conn.close()

def load_countries(): 
    base_url = 'https://restcountries.eu/rest/v2/all'
    countries = requests.get(base_url).json()

    insert_country_sql = '''
        INSERT INTO Countries
        VALUES (NULL, ?, ?, ?, ?, ?, ?, ?)
    '''

    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    for c in countries:
        cur.execute(insert_country_sql,
            [
                c['alpha2Code'],
                c['alpha3Code'],
                c['name'], 
                c['region'],
                c['subregion'],
                c['population'],
                c['area']
            ]
        )
    conn.commit()
    conn.close()

create_db()
load_countries()
load_bars()

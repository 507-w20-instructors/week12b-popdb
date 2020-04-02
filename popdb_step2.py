import sqlite3

def create_db():
    conn = sqlite3.connect('choc_bars.sqlite')
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
            "BroadBeanOrigin" INTEGER NOT NULL
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
            'Area' REAL NOT NULL
        )
    '''
    cur.execute(drop_bars_sql)
    cur.execute(drop_countries_sql)
    cur.execute(create_countries_sql)
    cur.execute(create_bars_sql)
    conn.commit()
    conn.close()

create_db()
import sqlite3
import csv

conn = sqlite3.connect('flightdb.sqlite')
cur = conn.cursor()

cur.execute('''
DROP TABLE IF EXISTS FlightLeg''')

cur.execute('''
CREATE TABLE FlightLeg (
    id INTEGER PRIMARY KEY AUTOINCREMENT, 
    tailNumber TEXT, 
    sourceAirportCode TEXT, 
    destinationAirportCode TEXT, 
    sourceCountryCode TEXT, 
    destinationCountryCode TEXT, 
    departureTimeUtc TEXT, 
    landingTimeUtc TEXT);''')

with open('flightlegs.csv', 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f, delimiter=';')
    to_db = [(i['tailNumber'], 
    i['source_airport_code'], 
    i['source_country_code'], 
    i['destination_airport_code'], 
    i['destination_country_code'], 
    i['departure_time'], 
    i['landing_time']) for i in reader]



cur.executemany('''
INSERT INTO FlightLeg (
    tailNumber, 
    sourceAirportCode, 
    sourceCountryCode, 
    destinationAirportCode, 
    destinationCountryCode, 
    departureTimeUtc, 
    landingTimeUtc) 
    VALUES (?, ?, ?, ?, ?, ?, ?);''', to_db)

conn.commit()


# Adding new columns
# Flight duration in minutes & flight type ("D" for domestic, "I" for international)

cur.executescript('''
ALTER TABLE FlightLeg ADD flightDuration INTEGER;
ALTER TABLE FlightLeg ADD flightType TEXT''')

conn.commit()

cur.executescript('''
UPDATE FlightLeg SET flightDuration = ROUND((JULIANDAY(landingTimeUtc) - JULIANDAY(departureTimeUtc)) * 1440);
UPDATE FlightLeg SET flightType = CASE WHEN sourceCountryCode = destinationCountryCode THEN "D" ELSE "I" END''')

conn.commit()


# Looking for a plane with the most flights

flights = cur.execute('''
SELECT tailNumber, count(tailNumber) FROM FlightLeg GROUP BY tailNumber''').fetchall()
max_flights = max(flights, key=lambda item:item[1])
print(max_flights)


# Looking for a longest and shortest flights, broken down into flight types

longest_flights = cur.execute('''
SELECT tailNumber, departureTimeUtc, flightType, MAX(flightDuration) FROM FlightLeg GROUP BY flightType''').fetchall()
print(longest_flights)

shortest_flights = cur.execute('''
SELECT tailNumber, departureTimeUtc, flightType, MIN(flightDuration) FROM FlightLeg GROUP BY flightType''').fetchall()
print(shortest_flights)


conn.close()


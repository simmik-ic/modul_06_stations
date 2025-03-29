import csv
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, ForeignKey

#funkcja pobiera pierwszy wiersz z pliku csv i zwraca go jako listę
def get_headers(file_name):
    result = None
    with open(file_name, 'r') as file:
        reader = csv.reader(file)
        result = next(reader)
    return result

#funkcja pobiera dane z pliku csv jako listę słowników
def get_data(file_name):
    result = []
    with open(file_name, 'r') as file:
        reader = csv.DictReader(file)           #Wczytujemy dane jako słowniki
        for row in reader:
            result.append(row)                  #Dodajemy każdą linię jako słownik
    return result

#Funkcja tworzy tabelę dynamicznie, bazując na podanej liście nagłówków (zakładamy typ danych string)
#foreign_keys - argument opcjonalny, lista krotek
def create_table(table_name, headers, foreign_keys=None):
    columns = [Column('id', Integer, primary_key=True)]         #utwórz kolumnę 'id' jako klucz główny

    if foreign_keys is not None:                                #jeżeli podano klucze obce
        for column_name, reference in foreign_keys:
            referenced_table, referenced_column = reference.split('.')
            columns.append(Column(column_name, Integer, ForeignKey(f"{referenced_table}.{referenced_column}")))

    for header in headers:
        columns.append(Column(header, String))                  #dodaj kolejne kolumny stringowe
    
    return Table(table_name, meta, *columns)                    #zwróć utworzoną tabelę

#Funkcja wypełnia tabelę danymi, bazując na podanej liście słowników
#wymaga podania silnika, tabeli
def populate_table(engine, table, dict_data):
    batch_size=1000                                 #rozmiar jednej partii danych do wstawienia
    with engine.connect() as conn:
        for i in range(0, len(dict_data), batch_size):
            batch = dict_data[i:i + batch_size]
            conn.execute(table.insert(), batch)     #wstaw całą partię naraz


#------------------------------------------------------------------------------------
#Część wykonywalna

#utwórz połączączenie z bazą danych (lub stwórz nową bazę danych)
engine = create_engine('sqlite:///stations.db')
meta = MetaData()

#wczytaj dane z plików
plik_stations = 'clean_stations.csv'
plik_measure = 'clean_measure.csv'

stations_data = get_data(plik_stations)
stations_headers = get_headers(plik_stations)
measure_data = get_data(plik_measure)
measure_headers = get_headers(plik_measure)

#utwórz tabele
stations_table = create_table('stations', stations_headers)
measure_table = create_table('measure', measure_headers)
meta.create_all(engine)

with engine.connect() as conn:
    conn.execute(stations_table.delete())
    conn.execute(measure_table.delete())

#wypełnij tabele
populate_table(engine, stations_table, stations_data)
populate_table(engine, measure_table, measure_data)


#Sprawdzanie danych z tabeli 'stations' za pomocą SQLAlchemy
with engine.connect() as conn:
    result = conn.execute("SELECT * FROM stations LIMIT 5")
    print(result.fetchall())
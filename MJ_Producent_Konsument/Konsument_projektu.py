from kafka import KafkaConsumer
import json

# Parametry połączenia z brokerem Kafka i tematem
SERVER = "broker:9092"
TOPIC = "samoloty"

# Inicjalizacja konsumenta Kafka
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=SERVER,
    auto_offset_reset='earliest',  # Od którego miejsca w logu Kafka zacząć czytać
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),  # Deserializacja JSON
    group_id="samoloty-grupa"
)


# OD TEGO MIEJSCA ZACZYNA SIĘ "ANALIZA" 

# Flagi kontrolujące, co ma być wypisywane
POKAZUJ_REKORDY = 0        # Jeśli 1, to wypisuje dane pojedynczych rekordów
liczba_lotów = 1           # Jeśli 1, to wypisuje liczbę unikalnych lotów i statystyki

# Zmienne pomocnicze
ostatni_time = None        # Zapamiętuje wartość time z poprzedniego rekordu
unikalne_loty = set()      # Zbiór unikalnych numerów lotów (flight)


print("🎧 Konsument nasłuchuje na dane...")

# Lista zmiennych liczbowych do analizy
zmienne = ['alt_geom', 'gs', 'geom_rate', 'track_rate', 'distance_km']

# Inicjalizacja struktur do obliczania sum, maksimum i minimum dla każdej zmiennej
suma_zmiennych = {zm: 0.0 for zm in zmienne}
max_zmiennych = {zm: float('-inf') for zm in zmienne}
min_zmiennych = {zm: float('inf') for zm in zmienne}
licznik_rekordów = 0  # Liczba rekordów w danej grupie czasowej

# Główna pętla nasłuchiwania wiadomości z Kafki
for message in consumer:
    record = message.value
    flight = record['flight']
    time = record['time']

    # Jeśli wykryto przejście do nowej grupy czasowej
    if liczba_lotów and ostatni_time is not None and time != ostatni_time:
        # Wypisanie liczby unikalnych lotów do tej pory
        print(f"📊 Do time = {ostatni_time} zaobserwowano {len(unikalne_loty)} unikalnych lotów ogółem.")

        # Wypisanie statystyk ogólnych dla zmiennych liczbowych
        if licznik_rekordów > 0:
            print("📈 Statystyki ogólne:")
            for zm in zmienne:
                srednia = suma_zmiennych[zm] / licznik_rekordów
                maksimum = max_zmiennych[zm]
                minimum = min_zmiennych[zm]
                print(f"   • {zm}: średnia = {round(srednia, 2)}, maksimum = {round(maksimum, 2)}, minimum = {round(minimum, 2)}")
            print()

        # Reset statystyk dla nowej grupy czasowej
        suma_zmiennych = {zm: 0.0 for zm in zmienne}
        max_zmiennych = {zm: float('-inf') for zm in zmienne}
        min_zmiennych = {zm: float('inf') for zm in zmienne}
        licznik_rekordów = 0

    # Dodanie numeru lotu do zbioru unikalnych lotów
    unikalne_loty.add(flight)
    ostatni_time = time

    # Aktualizacja statystyk: suma, maksimum i minimum
    for zm in zmienne:
        try:
            wartosc = float(record[zm])  # Konwersja na float (obsługuje też stringi z liczbami)
            suma_zmiennych[zm] += wartosc
            if wartosc > max_zmiennych[zm]:
                max_zmiennych[zm] = wartosc
            if wartosc < min_zmiennych[zm]:
                min_zmiennych[zm] = wartosc
        except (ValueError, TypeError):
            continue  # Jeśli konwersja się nie uda, pomijamy daną wartość

    licznik_rekordów += 1  # Zwiększamy licznik rekordów w grupie aby obliczyć statystyki

    # Jeśli wybrano opcję wyświetlania rekordów, to wypisujemy dane samolotu (UWAGA NA SPAM)
    if POKAZUJ_REKORDY:
        print(f"""
        ✈️ Pozycja samolotu:
            • Numer lotu: {record['flight']}
            • Wysokość geometryczna: {record['alt_geom']} ft
            • Prędkość względem ziemi: {record['gs']} węzłów
            • Kategoria emitera: {record['category']}
            • Pozycja: {record['lat']}, {record['lon']}
            • Czas od ostatniej pozycji: {record['seen_pos']} s
            • Zmiana wysokości: {record['geom_rate']} ft/min
            • Zmiana kursu: {record['track_rate']} °/s
            • Odległość od lotniska: {round(float(record['distance_km']), 1)} km
            • Znacznik czasu: {str(record['time']).zfill(6)}
        """)
    
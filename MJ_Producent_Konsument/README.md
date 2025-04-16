Instrukcja
1) Pobierz pliki z tego folderu
2) Otwórz przez dockera czy tam inaczej środowisko
3) Umieść te 3 pliki w jednym miejscu tego notebooka ze środowiska
4) Odpal terminal i musisz stworzyć topic o nazwie samoloty w cd ~ poprzez: kafka/bin/kafka-topics.sh --create --topic samoloty --bootstrap-server broker:9092
5) Następnie przejdź do lokalizacji tych 3 plików i odpal dwa terminale
6) Na jedynm python Konsument_projektu.py a na drugim python Producent_projektu.py
8) Edytuj kod konsumenta aby zrobić dalszą analizę (ja tam umieściłem jakieś proste zliczanie unikalnych lotów oraz syatystyki ogólne z najświeższej paczki danych)
9) Konsument wysyła jedną paczkę na 5 sekund. W jednej paczce znajdują się wartości zmiennych dla wszystkich lotów które zmierzają do Warszawy i były w naszej bazie danych


10) Ważnym elementem tego kodu konsumenta co napisałem jest fragment:
11) Flagi kontrolujące, co ma być wypisywane
12) POKAZUJ_REKORDY = 0         (Jeśli 1, to wypisuje dane pojedynczych rekordów)
13) statystyki = 0            (Jeśli 1, to wypisuje liczbę unikalnych lotów i statystyki)
14) Chodzi o to żeby wam nie zaspamiło terminala wartościami, użyłem ten kod do sprawdzenia czy wszystko hula (nie hulało, ale udało się naprawić po długiej walce xd)
15) ale jeżeli uważacie że niepotrzebujemy tego w kodzie możecie usunąć kompletnie te rzeczy.

Instrukcja
1) Pobierz pliki z tego folderu
2) Otwórz przez dockera czy tam inaczej środowisko
3) Umieść te 3 pliki w jednym miejscu tego notebooka ze środowiska
4) Odpal terminal i musisz stworzyć topic o nazwie samoloty w cd ~ poprzez: kafka/bin/kafka-topics.sh --create --topic samoloty --bootstrap-server broker:9092
5) Następnie przejdź do lokalizacji tych 3 plików i odpal dwa terminale
6) Na jedynm python Konsument_projektu.py a na drugim python Producent_projektu.py
7) Edytuj kod konsumenta aby zrobić dalszą analizę (ja tam umieściłem jakieś proste zliczanie unikalnych lotów oraz syatystyki ogólne z najświeższej paczki danych)
Konsument wysyła jedną paczkę na 5 sekund. W jednej paczce znajdują się wartości zmiennych dla wszystkich lotów które zmierzają do Warszawy i były w naszej bazie danych

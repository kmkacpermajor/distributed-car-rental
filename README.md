Klient rejestruje się komendą: addc

Klient może wyświetlić listę klas samochodów dostępnych danego dnia komendą: cla

Klient może zarezerwować samochód danej klasy komendą: res

Następnie po przyjściu do firmy, operator wykonuje komendę: rentAll dla klienta, który przyszedł


Klient może również usunąć swoją rezerwację komendą: del



`jmeter -n \
  -t rental-stress.jmx \
  -l bin/jmeter/results.jtl \
  -e -o bin/jmeter/report`
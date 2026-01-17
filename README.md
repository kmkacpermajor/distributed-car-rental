CREATE TABLE rentalLog (
    dateFrom date,
    rentorId UUID,
    dateTo date,
    carClass text,
);

PRIMARY KEY (dateFrom, rentorId);

SELECT * FROM WHERE dateFrom = 'dziś'

CREATE TABLE carRentals ( - zmieniana w dniu wynajmu
    carId
    rentorId Default NULL
)

PRIMARY KEY carId;

SELECT * FROM rentalLog WHERE carClass

CREATE TABLE carHistory (
    carId
    dateFrom
    dateTo
    dateReceived
)
PRIMARY KEY (carId, dateFrom, dateTo);


Wynajmując samochód klient dopisuje swój wynajem do rentalLog
Przychodzi klient do firmy, jeśli jest w rentalLog to wynajmujemy jakiś samochód
- wybieramy samochody typu carClass, jeśli żaden nie jest dostępny dajemy lepszą itd.
- po wyborze samochodu próbujemy zmienić rentorId w carRentals na Id klienta - jeśli się powiedzie to dopisujemy do carHistory


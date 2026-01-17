CREATE KEYSPACE IF NOT EXISTS Test
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

USE Test;

CREATE TABLE rentalLog (
  dateFrom date,
  renterId uuid,
  dateTo date,
  carClass text,
  PRIMARY KEY (dateFrom, renterId)
  );

CREATE TABLE carRentals (
  carId int,
  renterId uuid,
  PRIMARY KEY (carId)
  );

CREATE TABLE carHistory (
  carId int,
  dateFrom date,
  dateTo date,
  dateReceived date,
  renterId uuid,
  PRIMARY KEY (carId, dateFrom, dateTo)
  );

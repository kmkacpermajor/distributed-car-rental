CREATE KEYSPACE IF NOT EXISTS Test
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

USE Test;

CREATE TABLE rentalLog (
    dateFrom date,
    renterId uuid,
    rentalId uuid,
    dateTo date,
    carClass text,
    PRIMARY KEY (dateFrom, renterId, rentalId)
);

CREATE TABLE availableCars (
    date date,
    carClass text,
    count counter,
    PRIMARY KEY ( date, carClass )
);

CREATE TABLE carHistory (
    carId int,
    dateFrom date,
    dateTo date,
    dateReceived date,
    renterId uuid,
    rentalId uuid,
    PRIMARY KEY (carId, dateFrom, dateTo)
);

CREATE TABLE carDetails (
    carId int PRIMARY KEY,
    carName text,
    carClass text,
    licensePlate text
);

CREATE TABLE carClasses (
    carClass text PRIMARY KEY,
    carIdList list<int>
);

CREATE TABLE carRentals (
    carId int,
    rentalId uuid,
    PRIMARY KEY (carId)
);

INSERT INTO carRentals (carId, rentalId) VALUES (0, null);
INSERT INTO carRentals (carId, rentalId) VALUES (1, null);
INSERT INTO carRentals (carId, rentalId) VALUES (2, null);
INSERT INTO carRentals (carId, rentalId) VALUES (3, null);
INSERT INTO carRentals (carId, rentalId) VALUES (4, null);
INSERT INTO carRentals (carId, rentalId) VALUES (5, null);
INSERT INTO carRentals (carId, rentalId) VALUES (6, null);
INSERT INTO carRentals (carId, rentalId) VALUES (7, null);
INSERT INTO carRentals (carId, rentalId) VALUES (8, null);
INSERT INTO carRentals (carId, rentalId) VALUES (9, null);
INSERT INTO carRentals (carId, rentalId) VALUES (10, null);
INSERT INTO carRentals (carId, rentalId) VALUES (11, null);
INSERT INTO carRentals (carId, rentalId) VALUES (12, null);
INSERT INTO carRentals (carId, rentalId) VALUES (13, null);
INSERT INTO carRentals (carId, rentalId) VALUES (14, null);
INSERT INTO carRentals (carId, rentalId) VALUES (15, null);
INSERT INTO carRentals (carId, rentalId) VALUES (16, null);
INSERT INTO carRentals (carId, rentalId) VALUES (17, null);
INSERT INTO carRentals (carId, rentalId) VALUES (18, null);
INSERT INTO carRentals (carId, rentalId) VALUES (19, null);
INSERT INTO carRentals (carId, rentalId) VALUES (20, null);
INSERT INTO carRentals (carId, rentalId) VALUES (21, null);
INSERT INTO carRentals (carId, rentalId) VALUES (22, null);
INSERT INTO carRentals (carId, rentalId) VALUES (23, null);
INSERT INTO carRentals (carId, rentalId) VALUES (24, null);
INSERT INTO carRentals (carId, rentalId) VALUES (25, null);
INSERT INTO carRentals (carId, rentalId) VALUES (26, null);
INSERT INTO carRentals (carId, rentalId) VALUES (27, null);
INSERT INTO carRentals (carId, rentalId) VALUES (28, null);
INSERT INTO carRentals (carId, rentalId) VALUES (29, null);
INSERT INTO carRentals (carId, rentalId) VALUES (30, null);
INSERT INTO carRentals (carId, rentalId) VALUES (31, null);
INSERT INTO carRentals (carId, rentalId) VALUES (32, null);
INSERT INTO carRentals (carId, rentalId) VALUES (33, null);
INSERT INTO carRentals (carId, rentalId) VALUES (34, null);
INSERT INTO carRentals (carId, rentalId) VALUES (35, null);
INSERT INTO carRentals (carId, rentalId) VALUES (36, null);
INSERT INTO carRentals (carId, rentalId) VALUES (37, null);
INSERT INTO carRentals (carId, rentalId) VALUES (38, null);
INSERT INTO carRentals (carId, rentalId) VALUES (39, null);
INSERT INTO carRentals (carId, rentalId) VALUES (40, null);
INSERT INTO carRentals (carId, rentalId) VALUES (41, null);
INSERT INTO carRentals (carId, rentalId) VALUES (42, null);
INSERT INTO carRentals (carId, rentalId) VALUES (43, null);
INSERT INTO carRentals (carId, rentalId) VALUES (44, null);
INSERT INTO carRentals (carId, rentalId) VALUES (45, null);
INSERT INTO carRentals (carId, rentalId) VALUES (46, null);
INSERT INTO carRentals (carId, rentalId) VALUES (47, null);
INSERT INTO carRentals (carId, rentalId) VALUES (48, null);
INSERT INTO carRentals (carId, rentalId) VALUES (49, null);
INSERT INTO carRentals (carId, rentalId) VALUES (50, null);

INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (0, 'Audi A3', 'C', 'PO 1284A');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (1, 'Volkswagen Golf', 'C', 'PO 7721X');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (2, 'BMW 3 Series', 'D', 'PZ 00124');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (3, 'Mercedes-Benz C-Class', 'D', 'PO 55291');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (4, 'Toyota Corolla', 'C', 'PZ 992AC');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (5, 'Ford Fiesta', 'B', 'PO 3310M');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (6, 'Honda Civic', 'C', 'PY 12098');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (7, 'Tesla Model 3', 'D', 'PO 007EV');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (8, 'Porsche 911', 'S', 'PO 911S1');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (9, 'Hyundai i30', 'C', 'PZ 44122');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (10, 'Kia Sportage', 'F', 'PO 88201');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (11, 'Volvo XC60', 'F', 'PZ 771RA');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (12, 'Mazda 6', 'D', 'PO 6620L');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (13, 'Renault Clio', 'B', 'PY 55102');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (14, 'Peugeot 208', 'B', 'PO 208P1');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (15, 'Skoda Octavia', 'C', 'PZ 11922');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (16, 'Fiat 500', 'A', 'PO 500F1');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (17, 'Lexus ES', 'E', 'PO 001ES');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (18, 'Nissan Qashqai', 'F', 'PZ 3391K');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (19, 'Alfa Romeo Giulia', 'D', 'PO 000AR');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (20, 'Jaguar XF', 'E', 'PO 7711X');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (21, 'Land Rover Defender', 'F', 'PZ 90001');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (22, 'Mini Cooper', 'B', 'PO 332MC');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (23, 'Seat Leon', 'C', 'PY 99211');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (24, 'Opel Astra', 'C', 'PZ 5512H');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (25, 'Subaru Impreza', 'C', 'PO 555WR');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (26, 'Mitsubishi Outlander', 'F', 'PZ 88102');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (27, 'Suzuki Swift', 'B', 'PO 112SW');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (28, 'Chevrolet Camaro', 'S', 'PO 66V81');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (29, 'Citroen C4', 'C', 'PY 44921');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (30, 'Dacia Sandero', 'B', 'PZ 10292');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (31, 'Audi A3', 'C', 'PY 551AX');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (32, 'Volkswagen Golf', 'C', 'PO 4429Y');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (33, 'BMW 3 Series', 'D', 'PZ 8831G');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (34, 'Mercedes-Benz C-Class', 'D', 'PO 1109W');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (35, 'Toyota Corolla', 'C', 'PY 772KA');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (36, 'Ford Fiesta', 'B', 'PZ 2210S');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (37, 'Honda Civic', 'C', 'PO 3392V');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (38, 'Tesla Model 3', 'D', 'PY 005EV');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (39, 'Porsche 911', 'S', 'PO 911GT');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (40, 'Hyundai i30', 'C', 'PZ 4410X');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (41, 'Kia Sportage', 'F', 'PY 11200');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (42, 'Volvo XC60', 'F', 'PO 662RA');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (43, 'Mazda 6', 'D', 'PZ 9912L');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (44, 'Renault Clio', 'B', 'PO 1313H');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (45, 'Peugeot 208', 'B', 'PY 208X1');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (46, 'Skoda Octavia', 'C', 'PO 5592M');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (47, 'Fiat 500', 'A', 'PZ 500A1');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (48, 'Lexus ES', 'E', 'PY 8810A');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (49, 'Nissan Qashqai', 'F', 'PO 7720K');
INSERT INTO carDetails (carId, carName, carClass, licensePlate) VALUES (50, 'Alfa Romeo Giulia', 'D', 'PZ 156AR');

INSERT INTO carClasses (carClass, carIdList) VALUES ('A', [16, 47]);
INSERT INTO carClasses (carClass, carIdList) VALUES ('B', [5, 13, 14, 22, 27, 30, 36, 44, 45]);
INSERT INTO carClasses (carClass, carIdList) VALUES ('C', [0, 1, 4, 6, 9, 15, 23, 24, 25, 29, 31, 32, 35, 37, 40, 46]);
INSERT INTO carClasses (carClass, carIdList) VALUES ('D', [2, 3, 7, 12, 19, 33, 34, 38, 43, 50]);
INSERT INTO carClasses (carClass, carIdList) VALUES ('E', [17, 20, 48]);
INSERT INTO carClasses (carClass, carIdList) VALUES ('F', [10, 11, 18, 21, 26, 41, 42, 49]);
INSERT INTO carClasses (carClass, carIdList) VALUES ('S', [8, 28, 39]);
package cassdemo.backend;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class RentalService implements AutoCloseable {
    private final BackendSession session;

    public RentalService(String contactPointIP, String keyspace) throws BackendException {
        this.session = new BackendSession(contactPointIP, keyspace);
    }

    public UUID reserveRental(LocalDate dateFrom, UUID renterId, LocalDate dateTo, String carClass) throws BackendException {
        LocalDate now = LocalDate.now();
        if (!dateTo.isAfter(dateFrom)) {
            throw new BackendException("Return date must be after start date.");
        }
        if (dateFrom.isBefore(now) || dateTo.isAfter(now.plusDays(30))) {
            throw new BackendException("Reservations are only allowed within a 30-day window from today.");
        }

        long dayCount = ChronoUnit.DAYS.between(dateFrom, dateTo);
        for (int i = 0; i <= dayCount; i++) {
            LocalDate date = dateFrom.plusDays(i);
            long available = session.getAvailableCarCount(date, carClass);
            if (available <= 0) {
                throw new BackendException("Not enough cars available in class " + carClass + " for date " + date);
            }
        }

        UUID rentalId = UUID.randomUUID();
        session.insertRentalLog(dateFrom, renterId, rentalId, dateTo, carClass);

        for (int i = 0; i <= dayCount; i++) {
            session.updateAvailableCount(dateFrom.plusDays(i), carClass, -1L);
        }

        return rentalId;
    }

    public List<Car> processRentalsForClient(LocalDate date, UUID renterId) throws BackendException {
        List<RentalLog> rentals = session.selectRentals(date, renterId);
        List<Car> rentedCars = new ArrayList<>();

        for (RentalLog log : rentals) {
            Car rentedCar = findAndAssignCar(log, renterId);
            if (rentedCar == null) {
                throw new BackendException("No cars available (including upgrades) for reservation: " + log.getRentalId());
            }
            rentedCars.add(rentedCar);
        }
        return rentedCars;
    }

    private Car findAndAssignCar(RentalLog log, UUID renterId) throws BackendException {
        List<String> allClasses = Car.getCarClasses();
        int startIndex = allClasses.indexOf(log.getCarClass());
        List<String> candidateClasses = allClasses.subList(startIndex, allClasses.size());

        for (String currentClass : candidateClasses) {
            List<Integer> carIds = session.getCarIdsByClass(currentClass);
            for (Integer carId : carIds) {
                if (session.tryAssignCar(carId, renterId)) {
                    session.addRentalToHistory(carId, log.getDateFrom(), log.getDateTo(), renterId, log.getRentalId());
                    return session.getCarDetails(carId);
                }
            }
        }
        return null;
    }

    public void returnCar(int carId, LocalDate dateFrom, LocalDate dateTo, LocalDate dateReceived) throws BackendException {
        session.removeCarAssignment(carId);
        session.updateHistoryReturnDate(carId, dateFrom, dateTo, dateReceived);
    }

    public void deleteReservation(LocalDate dateFrom, UUID clientId, UUID rentalId, LocalDate dateTo, String carClass) throws BackendException {
        long dayCount = ChronoUnit.DAYS.between(dateFrom, dateTo);
        for (int i = 0; i <= dayCount; i++) {
            session.updateAvailableCount(dateFrom.plusDays(i), carClass, 1L);
        }
        session.deleteReservation(dateFrom, clientId, rentalId, dateTo, carClass);
    }

    public void initializeDatabase() throws BackendException {
        for (String carClass : Car.getCarClasses()) {
            long count = session.getCarIdsByClass(carClass).size();
            for (int i = 0; i < 30; i++) {
                session.updateAvailableCount(LocalDate.now().plusDays(i), carClass, count);
            }
        }
    }

    @Override
    public void close() throws Exception {
        session.close();
    }
}

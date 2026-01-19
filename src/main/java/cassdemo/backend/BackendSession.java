package cassdemo.backend;

import java.net.InetSocketAddress;
import java.time.LocalDate;
import java.util.*;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cassdemo.backend.BackendSession.Query.*;

/*
 * For error handling done right see:
 * https://www.datastax.com/dev/blog/cassandra-error-handling-done-right
 *
 * Performing stress tests often results in numerous WriteTimeoutExceptions,
 * ReadTimeoutExceptions (thrown by Cassandra replicas) and
 * OpetationTimedOutExceptions (thrown by the client). Remember to retry
 * failed operations until success (it can be done through the RetryPolicy mechanism:
 * https://stackoverflow.com/questions/30329956/cassandra-datastax-driver-retry-policy )
 */

public class BackendSession implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(BackendSession.class);

    private final CqlSession session;

    public enum Query {
        SELECT_AVAILABLE_CARS("SELECT count FROM availableCars WHERE date = :date AND carClass = :carClass"),
        MAKE_A_RESERVATION("INSERT INTO rentalLog (dateFrom, renterId, rentalId, dateTo, carClass) VALUES (?, ?, ?, ?, ?)"),
        DELETE_RESERVATION("DELETE FROM rentalLog WHERE dateFrom = ? AND renterId = ? AND rentalId = ?"),
        SELECT_TODAYS_CLIENTS_RENTALS("SELECT * FROM rentalLog WHERE dateFrom = ? AND renterId = ?"),
        CHECK_CARS_RENTAL_ID("SELECT rentalId FROM carRentals WHERE carId = ?"),
        TRY_RENTING_CAR("UPDATE carRentals SET rentalId = ? WHERE carId = ? IF rentalId = null"),
        ADD_RENTAL_TO_HISTORY("INSERT INTO carHistory (carId, dateFrom, dateTo, renterId, rentalId) VALUES (?,?,?,?,?)"),
        SELECT_ALL_CAR_IDS("SELECT carIdList FROM carClasses WHERE carClass = ?"),
        ADD_TO_AVAILABLE_CARS("UPDATE availableCars SET count = count + :count WHERE date = :date AND carClass = :carClass"),
        DECREASE_FROM_AVAILABLE_CARS("UPDATE availableCars SET count = count - :count WHERE date = :date AND carClass = :carClass"),
        UPDATE_DATE_RECEIVED("UPDATE carHistory SET dateReceived = ? WHERE carId = ? AND dateFrom = ? AND dateTo = ?"),
        SELECT_CAR_DETAILS("SELECT carId, carName, carClass, licensePlate FROM carDetails WHERE carId = ?"),
        DELETE_CURRENT_CAR_RENTAL("DELETE FROM carRentals WHERE carId = ?");

        public final String cql;

        Query(String cql) {
            this.cql = cql;
        }
    }

    private final EnumMap<Query, PreparedStatement> statements = new EnumMap<>(Query.class);

    public BackendSession(String contactPointIP, String keyspace) throws BackendException {

        try {
            session = CqlSession.builder()
                    .addContactPoint(new InetSocketAddress(contactPointIP, 9042))
                    .withKeyspace(keyspace)
                    .withLocalDatacenter("datacenter1")
                    .build();
        } catch (Exception e) {
            throw new BackendException("Could not connect to the cluster. " + e.getMessage() + ".", e);
        }
        for (Query q : Query.values()) {
            statements.put(q, session.prepare(q.cql));
            logger.info("Statements prepared");
        }
    }

    public long getAvailableCarCount(LocalDate date, String carClass) throws BackendException {
        BoundStatement bs = statements.get(SELECT_AVAILABLE_CARS).bind()
                .setLocalDate("date", date)
                .setString("carClass", carClass);
        try {
            Row row = session.execute(bs).one();
            if (row == null) throw new BackendException("No availability data. Run 'initialize'.");
            return row.getLong("count");
        } catch (Exception e) {
            logger.error("Failed to get available cars count", e);
            throw new BackendException("Query failed: "+e.getMessage(), e);
        }
    }

    public void updateAvailableCount(LocalDate date, String carClass, long delta) throws BackendException {
        Query query = delta > 0 ? ADD_TO_AVAILABLE_CARS : DECREASE_FROM_AVAILABLE_CARS;
        BoundStatement bs = statements.get(query).bind()
                .setLong("count", Math.abs(delta))
                .setLocalDate("date", date)
                .setString("carClass", carClass);
        try {
            session.execute(bs);
        } catch (Exception e) {
            logger.error("Failed to update available cars count", e);
            throw new BackendException("Update failed: "+e.getMessage(), e);
        }
    }

    public void insertRentalLog(LocalDate dateFrom, UUID renterId, UUID rentalId, LocalDate dateTo, String carClass) throws BackendException {
        BoundStatement bs = statements.get(MAKE_A_RESERVATION).bind(dateFrom, renterId, rentalId, dateTo, carClass);
        try {
            session.execute(bs);
        } catch (Exception e) {
            logger.error("Failed to insert rental log", e);
            throw new BackendException("Insert failed: "+e.getMessage(), e);
        }
    }

    public ArrayList<RentalLog> selectRentals(LocalDate dateFrom, UUID renterId) throws BackendException {
        BoundStatement bs = statements.get(SELECT_TODAYS_CLIENTS_RENTALS).bind(dateFrom, renterId);
        try {
            ResultSet rs = session.execute(bs);
            ArrayList<RentalLog> rentals = new ArrayList<>();
            for (Row row : rs) {
                rentals.add(new RentalLog.Builder()
                        .dateFrom(row.getLocalDate("dateFrom"))
                        .renterId(row.get("renterId", UUID.class))
                        .rentalId(row.get("rentalId", UUID.class))
                        .dateTo(row.getLocalDate("dateTo"))
                        .carClass(row.getString("carClass"))
                        .build());
            }
            return rentals;
        } catch (Exception e) {
            logger.error("Failed to select rentals", e);
            throw new BackendException("Select failed: "+e.getMessage(), e);
        }
    }

    public boolean tryAssignCar(int carId, UUID rentalId) throws BackendException {
        BoundStatement bs = statements.get(TRY_RENTING_CAR).bind(rentalId, carId);
        try {
            Row row = session.execute(bs).one();
            return row != null && row.getBoolean("[applied]");
        } catch (Exception e) {
            logger.error("Failed to assign car", e);
            throw new BackendException("Assignment failed: "+e.getMessage(), e);
        }
    }

    public void addRentalToHistory(int carId, LocalDate dateFrom, LocalDate dateTo, UUID renterId, UUID rentalId) throws BackendException {
        BoundStatement bs = statements.get(ADD_RENTAL_TO_HISTORY).bind(carId, dateFrom, dateTo, renterId, rentalId);
        try {
            session.execute(bs);
        } catch (Exception e) {
            logger.error("Failed to insert rental history", e);
            throw new BackendException("History insert failed: "+e.getMessage(), e);
        }
    }

    public void removeCarAssignment(int carId) throws BackendException {
        try {
            session.execute(statements.get(DELETE_CURRENT_CAR_RENTAL).bind(carId));
        } catch (Exception e) {
            logger.error("Failed to delete car assignment", e);
            throw new BackendException("Delete assignment failed: "+e.getMessage(), e);
        }
    }

    public void updateHistoryReturnDate(int carId, LocalDate dateFrom, LocalDate dateTo, LocalDate dateReceived) throws BackendException {
        try {
            session.execute(statements.get(UPDATE_DATE_RECEIVED).bind(dateReceived, carId, dateFrom, dateTo));
        } catch (Exception e) {
            logger.error("Failed to update rental history", e);
            throw new BackendException("History update failed: "+e.getMessage(), e);
        }
    }

    public List<Integer> getCarIdsByClass(String carClass) throws BackendException {
        try {
            Row row = session.execute(statements.get(SELECT_ALL_CAR_IDS).bind(carClass)).one();
            return row != null ? row.getList("carIdList", Integer.class) : new ArrayList<>();
        } catch (Exception e) {
            logger.error("Failed to get car ids", e);
            throw new BackendException("Car lookup failed: "+e.getMessage(), e);
        }
    }

    public Car getCarDetails(Integer carId) throws BackendException {
        try {
            Row row = session.execute(statements.get(SELECT_CAR_DETAILS).bind(carId)).one();
            if (row == null) throw new BackendException("Car not found");
            return new Car(row.getInt("carId"), row.getString("carName"), row.getString("carClass"), row.getString("licensePlate"));
        } catch (Exception e) {
            logger.error("Failed to get car details", e);
            throw new BackendException("Details lookup failed: "+e.getMessage(), e);
        }
    }

    public void deleteReservation(LocalDate dateFrom, UUID clientId, UUID rentalId, LocalDate dateTo, String carClass) throws BackendException {
        try {
            session.execute(statements.get(DELETE_RESERVATION).bind(dateFrom, clientId, rentalId, dateTo, carClass)).one();
        } catch (Exception e) {
            logger.error("Failed to delete reservation", e);
            throw new BackendException("Delete reservation failed: "+e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        if (session != null) {
            session.close();
        }
    }
}

package cassdemo.backend;

import java.net.InetSocketAddress;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.*;

import com.datastax.oss.driver.api.core.CqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

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

public class BackendSession {

	private static final Logger logger = LoggerFactory.getLogger(BackendSession.class);

	private CqlSession session;

    public enum Query {
        SELECT_AVAILABLE_CARS("SELECT count FROM availableCars WHERE date = :date AND carClass = :carClass"),
        MAKE_A_RESERVATION("INSERT INTO rentalLog (dateFrom, renterId, rentalId, dateTo, carClass) VALUES (?, ?, ?, ?, ?)"),
        DELETE_RESERVATION ("DELETE FROM rentalLog WHERE dateFrom = ? AND renterId = ? AND rentalId = ?"),
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

	public UUID reserveRental(LocalDate dateFrom, UUID renterId, LocalDate dateTo, String carClass) throws BackendException{
		LocalDate localDate = LocalDate.now();
        UUID rentalId = UUID.randomUUID();

        if (!dateTo.isAfter(dateFrom)) {
            throw new BackendException("Invalid date range. Date range must be between today and 30 days from today.");
        }
		if (dateFrom.isBefore(localDate) || dateTo.isAfter(localDate.plusDays(30))) {
            throw new BackendException("Invalid date range. Date range must be between today and 30 days from today.");
        }

		BoundStatement bs1 = statements.get(SELECT_AVAILABLE_CARS).bind().setString("carClass", carClass);
		long dayCount = dateFrom.until(dateTo, ChronoUnit.DAYS);
		for (int i = 0;i<=dayCount;i++) {
			BoundStatement bs = bs1.setLocalDate("date", dateFrom.plusDays(i));
            long count = 0;
			try{
                Row row = session.execute(bs).one();
                if (row == null) throw new BackendException("There are no cars in system. You should run \"fill\"");

                count = row.getLong("count");
			} catch (Exception e){
				throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
			}
            if(count<=0){
                throw new BackendException("Not enough cars available for this reservation");
            }
		}
		BoundStatement bs2 = statements.get(MAKE_A_RESERVATION).bind(dateFrom, renterId, rentalId, dateTo, carClass);
		try{
			session.execute(bs2);
		} catch (Exception e){
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
		BoundStatement bs3 = statements.get(DECREASE_FROM_AVAILABLE_CARS).bind().setLong("count", 1L).setString("carClass", carClass);
		for (int i = 0;i<=dayCount;i++) {
			BoundStatement bs = bs3.setLocalDate("date", dateFrom.plusDays(i));
			try {
				session.execute(bs);
			} catch (Exception e) {
				throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
			}
		}
        return rentalId;
	}

	public void deleteReservation(LocalDate dateFrom, UUID renterId, UUID rentalId, LocalDate dateTo, String carClass) throws BackendException{
		BoundStatement bs1 = statements.get(ADD_TO_AVAILABLE_CARS).bind(1L, dateFrom, carClass);
		long dayCount = dateFrom.until(dateTo, ChronoUnit.DAYS);
		for (int i = 0;i>dayCount;i++) {
			bs1 = bs1.setLocalDate("date", dateFrom.plusDays(i));
			try {
				session.execute(bs1);
			} catch (Exception e) {
				throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
			}
		}
		BoundStatement bs2 = statements.get(DELETE_RESERVATION).bind(dateFrom, renterId, rentalId, dateTo, carClass);
		try{
			session.execute(bs2);
		} catch (Exception e){
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
		logger.info("A new rental reservation has been deleted.");
	}

	public ArrayList<RentalLog> selectRentals(LocalDate dateFrom, UUID renterId) throws BackendException{
		BoundStatement bs = statements.get(SELECT_TODAYS_CLIENTS_RENTALS).bind(dateFrom, renterId);
		ResultSet rs = null;
		try{
			rs = session.execute(bs);
		} catch (Exception e){
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
		ArrayList<RentalLog> rentals = new ArrayList<>();
		for (Row row : rs){
			RentalLog rentalLog = new RentalLog.Builder()
					.dateFrom(row.getLocalDate("dateFrom"))
					.renterId(row.get("renterId", UUID.class))
					.rentalId(row.get("rentalId", UUID.class))
					.dateTo(row.getLocalDate("dateTo"))
					.carClass(row.getString("carClass"))
					.build();
			rentals.add(rentalLog);
		}
		return rentals;
	}

	public UUID getCarsRentalId(int carId) throws BackendException{
		BoundStatement bs = statements.get(CHECK_CARS_RENTAL_ID).bind(carId);
		ResultSet rs = null;
		try{
			rs = session.execute(bs);
		} catch (Exception e){
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
		return rs.one().get("rentalId", UUID.class);
	}

    public Car rentLog(RentalLog rL, UUID renterId) throws BackendException{
        int carClassIndex = Car.getCarClasses().indexOf(rL.getCarClass());
        ListIterator<String> iterator = Car.getCarClasses().listIterator(carClassIndex);
        while (iterator.hasNext()) {
            String currentOption = iterator.next();

            List<Integer> toCheck = getCarIds(currentOption);
            for (Integer carId : toCheck) {
                if (rentCar(carId, renterId)) {
                    addRentalToHistory(carId, rL.getDateFrom(), rL.getDateTo(), renterId, rL.getRentalId());
                    return getCarDetails(carId);
                }
            }
        }
        throw new BackendException("No cars available for rental.");
    }

	public boolean rentCar(int carId, UUID rentalId) throws BackendException{
		BoundStatement bs = statements.get(TRY_RENTING_CAR).bind(rentalId, carId);
		ResultSet rs = null;
		try{
			rs = session.execute(bs);
		} catch (Exception e){
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
		return rs.one().getBool("[applied]");
	}

	public void addRentalToHistory(int carId, LocalDate dateFrom, LocalDate dateTo, UUID renterId, UUID rentalId) throws BackendException{
		BoundStatement bs = statements.get(ADD_RENTAL_TO_HISTORY).bind(carId, dateFrom, dateTo, renterId, rentalId);
		ResultSet rs = null;
		try {
			rs = session.execute(bs);
		} catch (Exception e){
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
		logger.info("Added log to rental history.");
	}

	public void returnCar(int carId, LocalDate dateFrom, LocalDate dateTo, LocalDate dateReceived) throws BackendException{
		BoundStatement bs1 = statements.get(DELETE_CURRENT_CAR_RENTAL).bind(carId);
        BoundStatement bs2 = statements.get(UPDATE_DATE_RECEIVED).bind(dateReceived, carId, dateFrom, dateTo);
		try{
            session.execute(bs1);
            session.execute(bs2);
		} catch (Exception e){
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
		logger.info("Added dateReceived to log in rental history.");
	}

	public List<Integer> getCarIds(String carClass) throws BackendException{
		BoundStatement bs = statements.get(SELECT_ALL_CAR_IDS).bind(carClass);
		ResultSet rs = null;
		try {
			rs = session.execute(bs);
		}catch (Exception e){
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
		return rs.one().getList("carIdList", Integer.class);
	}

    public Car getCarDetails(Integer carId) throws BackendException{
        BoundStatement bs = statements.get(SELECT_CAR_DETAILS).bind(carId);
        ResultSet rs = null;
        try {
            rs = session.execute(bs);
        }catch (Exception e){
            throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
        }

        Row row = rs.one();

        return new Car(row.getInt("carId"), row.getString("carName"), row.getString("carClass"), row.getString("licensePlate"));
    }

    public void fillAvailableCars() throws BackendException{
        int daysInAdvance = 30;

        for (String carClass : Car.getCarClasses()){
            long count = getCarIds(carClass).size();
            for (int i = 0; i < daysInAdvance; i++){
                LocalDate date = LocalDate.now().plusDays(i);
                BoundStatement bs = statements.get(ADD_TO_AVAILABLE_CARS).bind(count, date, carClass);
                try {
                    session.execute(bs);
                }catch (Exception e){
                    throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
                }
            }
        }
    }

	protected void finalize() {
		try {
			if (session != null) {
				session.close();
			}
		} catch (Exception e) {
			logger.error("Could not close existing cluster", e);
		}
	}

}

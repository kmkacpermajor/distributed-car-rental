package cassdemo.backend;

import java.net.InetSocketAddress;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.datastax.oss.driver.api.core.CqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

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
		prepareStatements();
	}
	private static PreparedStatement SELECT_AVAILABLE_CARS;
	private static PreparedStatement MAKE_A_RESERVATION;
	private static PreparedStatement DELETE_RESERVATION;
	private static PreparedStatement SELECT_TODAYS_CLIENTS_RENTALS;
	private static PreparedStatement CHECK_CARS_RENTAL_ID;
	private static PreparedStatement TRY_RENTING_CAR;
	private static PreparedStatement ADD_RENTAL_TO_HISTORY;
	private static PreparedStatement RETURN_CAR;
	private static PreparedStatement SELECT_ALL_CAR_IDS;
	private static PreparedStatement ADD_TO_AVAILABLE_CARS;
	private static PreparedStatement DECREASE_FROM_AVAILABLE_CARS;

//	private static final String USER_FORMAT = "- %-10s  %-16s %-10s %-10s\n";
//	// private static final SimpleDateFormat df = new
//	// SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private void prepareStatements() throws BackendException {
		try {
			SELECT_AVAILABLE_CARS = session.prepare("SELECT count FROM availableCars WHERE date = ? AND carClass = ?");
			MAKE_A_RESERVATION = session.prepare("INSERT INTO rentalLog (dateFrom, renterId, rentalId, dateTo, carClass) VALUES (?, ?, ?, ?, ?)");
			DELETE_RESERVATION = session.prepare("DELETE FROM rentalLog WHERE dateFrom = ? AND renterId = ? AND rentalId = ?");
			SELECT_TODAYS_CLIENTS_RENTALS = session.prepare("SELECT * FROM rentalLog WHERE dateFrom = ? AND renterId = ?");
			CHECK_CARS_RENTAL_ID = session.prepare("SELECT rentalId FROM carRentals WHERE carId = ?");
			TRY_RENTING_CAR = session.prepare("UPDATE carRentals SET rentalId = ? WHERE carId = ? IF rentalId = null");
			ADD_RENTAL_TO_HISTORY = session.prepare("INSERT INTO carHistory (carId, dateFrom, dateTo, renterId) VALUES (?,?,?,?)");
			RETURN_CAR = session.prepare("INSERT INTO carHistory (carId, dateFrom, dateTo, dateReceived) VALUES (?,?,?,?)");
			SELECT_ALL_CAR_IDS = session.prepare("SELECT carIdList FROM carClasses WHERE carClass = ?");
			ADD_TO_AVAILABLE_CARS = session.prepare("UPDATE availableCars SET count = count + ? WHERE date = ? AND rentalId = ?");
			DECREASE_FROM_AVAILABLE_CARS = session.prepare("UPDATE availableCars SET count = count - ? WHERE date = ? AND rentalId = ?");

		} catch (Exception e) {
			throw new BackendException("Could not prepare statements. " + e.getMessage() + ".", e);
		}

		logger.info("Statements prepared");
	}

	public boolean reserveRental(LocalDate dateFrom, UUID renterId, UUID rentalId, LocalDate dateTo, String carClass) throws BackendException{
		LocalDate localDate = LocalDate.now();
		if (!dateTo.isAfter(dateFrom) || localDate.isBefore(dateFrom) || localDate.plusDays(30).isBefore(dateTo)) return false;
		BoundStatement bs1 = SELECT_AVAILABLE_CARS.bind(dateFrom, carClass);
		long dayCount = dateFrom.until(dateTo, ChronoUnit.DAYS);
		for (int i = 0;i>dayCount;i++) {
			bs1 = bs1.setLocalDate("date", dateFrom.plusDays(i));
			try{
				if(session.execute(bs1).one().getLong("count")<=0){
					return false;
				}
			} catch (Exception e){
				throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
			}
		}
		BoundStatement bs2 = MAKE_A_RESERVATION.bind(dateFrom, renterId, rentalId, dateTo, carClass);
		try{
			session.execute(bs2);
		} catch (Exception e){
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
		BoundStatement bs3 = DECREASE_FROM_AVAILABLE_CARS.bind(1, dateFrom, carClass);
		for (int i = 0;i>dayCount;i++) {
			bs3 = bs3.setLocalDate("date", dateFrom.plusDays(i));
			try {
				session.execute(bs3);
			} catch (Exception e) {
				throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
			}
		}
		return true;
	}

	public void deleteReservation(LocalDate dateFrom, UUID renterId, UUID rentalId, LocalDate dateTo, String carClass) throws BackendException{
		BoundStatement bs1 = ADD_TO_AVAILABLE_CARS.bind(1, dateFrom, carClass);
		long dayCount = dateFrom.until(dateTo, ChronoUnit.DAYS);
		for (int i = 0;i>dayCount;i++) {
			bs1 = bs1.setLocalDate("date", dateFrom.plusDays(i));
			try {
				session.execute(bs1);
			} catch (Exception e) {
				throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
			}
		}
		BoundStatement bs2 = DELETE_RESERVATION.bind(dateFrom, renterId, rentalId, dateTo, carClass);
		try{
			session.execute(bs2);
		} catch (Exception e){
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
		logger.info("A new rental reservation has been deleted.");
	}

	public ArrayList<RentalLog> selectRentals(LocalDate dateFrom, UUID renterId) throws BackendException{
		BoundStatement bs = SELECT_TODAYS_CLIENTS_RENTALS.bind(dateFrom, renterId);
		ResultSet rs = null;
		try{
			rs = session.execute(bs);
		} catch (Exception e){
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
		ArrayList<RentalLog> rentals = new ArrayList<>();
		for (Row row : rs){
			RentalLog rentalLog = new RentalLog.Builder()
					.dateFrom(LocalDate.parse(row.getString("dateFrom")))
					.renterId(row.get("renterId", UUID.class))
					.rentalId(row.get("rentalId", UUID.class))
					.dateTo(LocalDate.parse(row.getString("dateTo")))
					.carClass(row.getString("carClass"))
					.build();
			rentals.add(rentalLog);
		}
		return rentals;
	}

	public UUID getCarsRentalId(int carId) throws BackendException{
		BoundStatement bs = CHECK_CARS_RENTAL_ID.bind(carId);
		ResultSet rs = null;
		try{
			rs = session.execute(bs);
		} catch (Exception e){
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
		return rs.one().get("rentalId", UUID.class);
	}

	public boolean rentCar(int carId, UUID rentalId) throws BackendException{
		BoundStatement bs = TRY_RENTING_CAR.bind(rentalId, carId);
		ResultSet rs = null;
		try{
			rs = session.execute(bs);
		} catch (Exception e){
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
		return rs.one().getBool("[applied]");
	}

	public void addRentalToHistory(int carId, LocalDate dateFrom, LocalDate dateTo, UUID renterId) throws BackendException{
		BoundStatement bs = ADD_RENTAL_TO_HISTORY.bind(carId, dateFrom, dateTo, renterId);
		ResultSet rs = null;
		try {
			rs = session.execute(bs);
		} catch (Exception e){
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
		logger.info("Added log to rental history.");
	}

	public void returnCar(int carId, LocalDate dateFrom, LocalDate dateTo, LocalDate dateReceived) throws BackendException{
		BoundStatement bs = RETURN_CAR.bind(carId, dateFrom, dateTo, dateReceived);
		ResultSet rs = null;
		try{
			rs = session.execute(bs);
		} catch (Exception e){
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
		logger.info("Added dateReceived to log in rental history.");
	}

	public List<Integer> getCarIds(String carClass) throws BackendException{
		BoundStatement bs = SELECT_ALL_CAR_IDS.bind(carClass);
		ResultSet rs = null;
		try {
			rs = session.execute(bs);
		}catch (Exception e){
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
		return rs.one().getList("carIdList", Integer.class);
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

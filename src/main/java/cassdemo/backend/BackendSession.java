package cassdemo.backend;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

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

	public static BackendSession instance = null;

	private Session session;

	public BackendSession(String contactPoint, String keyspace) throws BackendException {

		Cluster cluster = Cluster.builder().addContactPoint(contactPoint).build();
		try {
			session = cluster.connect(keyspace);
		} catch (Exception e) {
			throw new BackendException("Could not connect to the cluster. " + e.getMessage() + ".", e);
		}
		prepareStatements();
	}
	private static PreparedStatement MAKE_A_RESERVATION;
	private static PreparedStatement DELETE_RESERVATION;
	private static PreparedStatement SELECT_TODAYS_CLIENTS_RENTALS;
	private static PreparedStatement CHECK_CARS_RENTAL_ID;
	private static PreparedStatement TRY_RENTING_CAR;
	private static PreparedStatement ADD_RENTAL_TO_HISTORY;
	private static PreparedStatement RETURN_CAR;
	private static PreparedStatement SELECT_ALL_CAR_IDS;

//	private static final String USER_FORMAT = "- %-10s  %-16s %-10s %-10s\n";
//	// private static final SimpleDateFormat df = new
//	// SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private void prepareStatements() throws BackendException {
		try {
			MAKE_A_RESERVATION = session.prepare("INSERT INTO rentalLog (dateFrom, renterId, dateTo, carClass) VALUES (?, ?, ?, ?)");
			DELETE_RESERVATION = session.prepare("DELETE FROM rentalLog WHERE dateFrom = ? AND renterId = ?");
			SELECT_TODAYS_CLIENTS_RENTALS = session.prepare("SELECT * FROM rentalLog WHERE dateFrom = ? AND renterId = ?");
			CHECK_CARS_RENTAL_ID = session.prepare("SELECT renterId FROM carRentals WHERE carId = ?");
			TRY_RENTING_CAR = session.prepare("UPDATE carRentals SET renterId = ? WHERE carId = ? IF renterId = null");
			ADD_RENTAL_TO_HISTORY = session.prepare("INSERT INTO carHistory (carId, dateFrom, dateTo, renterId) VALUES (?,?,?,?)");
			RETURN_CAR = session.prepare("INSERT INTO carHistory (carId, dateFrom, dateTo, dateReceived) VALUES (?,?,?,?)");
			SELECT_ALL_CAR_IDS = session.prepare("SELECT carIdList FROM carClasses WHERE carClass = ?");
		} catch (Exception e) {
			throw new BackendException("Could not prepare statements. " + e.getMessage() + ".", e);
		}

		logger.info("Statements prepared");
	}

	public void reserveRental(LocalDate dateFrom, UUID renterId, LocalDate dateTo, String carClass) throws BackendException{
		BoundStatement bs = new BoundStatement(MAKE_A_RESERVATION);
		bs.bind(dateFrom, renterId, dateTo, carClass);
		ResultSet rs = null;
		try{
			rs = session.execute(bs);
		} catch (Exception e){
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
		logger.info("A new rental reservation has been made.");
	}

	public void deleteReservation(LocalDate dateFrom, UUID renterId, LocalDate dateTo, String carClass) throws BackendException{
		BoundStatement bs = new BoundStatement(DELETE_RESERVATION);
		bs.bind(dateFrom, renterId, dateTo, carClass);
		ResultSet rs = null;
		try{
			rs = session.execute(bs);
		} catch (Exception e){
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
		logger.info("A new rental reservation has been deleted.");
	}

	public ArrayList<RentalLog> selectRentals(LocalDate dateFrom, UUID renterId) throws BackendException{
		BoundStatement bs = new BoundStatement(SELECT_TODAYS_CLIENTS_RENTALS);
		bs.bind(dateFrom, renterId);
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
					.renterId(row.getUUID("renterId"))
					.dateTo(LocalDate.parse(row.getString("dateTo")))
					.carClass(row.getString("carClass"))
					.build();
			rentals.add(rentalLog);
		}
		return rentals;
	}

	public UUID getCarsRenterId(int carId) throws BackendException{
		BoundStatement bs = new BoundStatement(CHECK_CARS_RENTAL_ID);
		bs.bind(carId);
		ResultSet rs = null;
		try{
			rs = session.execute(bs);
		} catch (Exception e){
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
		return rs.one().getUUID("renterId");
	}

	public boolean rentCar(int carId, UUID renterId) throws BackendException{
		BoundStatement bs = new BoundStatement(TRY_RENTING_CAR);
		bs.bind(renterId, carId);
		ResultSet rs = null;
		try{
			rs = session.execute(bs);
		} catch (Exception e){
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
		return rs.one().getBool("[applied]");
	}

	public void addRentalToHistory(int carId, LocalDate dateFrom, LocalDate dateTo, UUID renterId) throws BackendException{
		BoundStatement bs = new BoundStatement(ADD_RENTAL_TO_HISTORY);
		bs.bind(carId, dateFrom, dateTo, renterId);
		ResultSet rs = null;
		try {
			rs = session.execute(bs);
		} catch (Exception e){
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
		logger.info("Added log to rental history.");
	}

	public void returnCar(int carId, LocalDate dateFrom, LocalDate dateTo, LocalDate dateReceived) throws BackendException{
		BoundStatement bs = new BoundStatement(RETURN_CAR);
		bs.bind(carId, dateFrom, dateTo, dateReceived);
		ResultSet rs = null;
		try{
			rs = session.execute(bs);
		} catch (Exception e){
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
		logger.info("Added dateReceived to log in rental history.");
	}

	public List<Integer> getCarIds(String carClass) throws BackendException{
		BoundStatement bs = new BoundStatement(SELECT_ALL_CAR_IDS);
		bs.bind(carClass);
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
				session.getCluster().close();
			}
		} catch (Exception e) {
			logger.error("Could not close existing cluster", e);
		}
	}

}

package cassdemo.backend;

import java.net.InetSocketAddress;
import java.time.LocalDate;
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
	private static PreparedStatement MAKE_A_RESERVATION;
	private static PreparedStatement DELETE_RESERVATION;
	private static PreparedStatement SELECT_TODAYS_CLIENTS_RENTALS;
	private static PreparedStatement CHECK_CARS_RENTAL_ID;
	private static PreparedStatement TRY_RENTING_CAR;
	private static PreparedStatement ADD_RENTAL_TO_HISTORY;
	private static PreparedStatement UPDATE_DATE_RECEIVED;
    private static PreparedStatement SELECT_ALL_CAR_IDS;
    private static PreparedStatement SELECT_CAR_DETAILS;
    private static PreparedStatement UPDATE_AVAILABLE_CARS;
    private static PreparedStatement DELETE_CURRENT_CAR_RENTAL;

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
            UPDATE_DATE_RECEIVED = session.prepare("UPDATE carHistory SET dateReceived = ? WHERE carId = ? AND dateFrom = ? AND dateTo = ?");
            SELECT_ALL_CAR_IDS = session.prepare("SELECT carIdList FROM carClasses WHERE carClass = ?");
            SELECT_CAR_DETAILS = session.prepare("SELECT carId, carName, carClass, licensePlate FROM carDetails WHERE carId = ?");
            UPDATE_AVAILABLE_CARS = session.prepare("UPDATE availableCars SET count = count + ? WHERE date = ? AND carClass = ?");
            DELETE_CURRENT_CAR_RENTAL = session.prepare("DELETE FROM carRental WHERE carId = ?");
        } catch (Exception e) {
			throw new BackendException("Could not prepare statements. " + e.getMessage() + ".", e);
		}

		logger.info("Statements prepared");
	}

	public void reserveRental(LocalDate dateFrom, UUID renterId, LocalDate dateTo, String carClass) throws BackendException{
		BoundStatement bs = MAKE_A_RESERVATION.bind(dateFrom, renterId, dateTo, carClass);
		try{
            session.execute(bs);
		} catch (Exception e){
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
		logger.info("A new rental reservation has been made.");
	}

	public void deleteReservation(LocalDate dateFrom, UUID renterId, LocalDate dateTo, String carClass) throws BackendException{
		BoundStatement bs = DELETE_RESERVATION.bind(dateFrom, renterId, dateTo, carClass);
		try{
			session.execute(bs);
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
					.dateTo(LocalDate.parse(row.getString("dateTo")))
					.carClass(row.getString("carClass"))
					.build();
			rentals.add(rentalLog);
		}
		return rentals;
	}

	public UUID getCarsRenterId(int carId) throws BackendException{
		BoundStatement bs = CHECK_CARS_RENTAL_ID.bind(carId);
		ResultSet rs = null;
		try{
			rs = session.execute(bs);
		} catch (Exception e){
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
		return rs.one().get("renterId", UUID.class);
	}

	public boolean rentCar(int carId, UUID renterId) throws BackendException{
		BoundStatement bs = TRY_RENTING_CAR.bind(renterId, carId);
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
		BoundStatement bs1 = DELETE_CURRENT_CAR_RENTAL.bind(carId);
        BoundStatement bs2 = UPDATE_DATE_RECEIVED.bind(dateReceived, carId, dateFrom, dateTo);
		try{
            session.execute(bs1);
            session.execute(bs2);
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

    public Car getCarDetails(Integer carId) throws BackendException{
        BoundStatement bs = SELECT_CAR_DETAILS.bind(carId);
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
            int count = getCarIds(carClass).size();
            for (int i = 0; i < daysInAdvance; i++){
                LocalDate date = LocalDate.now().plusDays(i);
                BoundStatement bs = UPDATE_AVAILABLE_CARS.bind(count, date, carClass);
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

package cassdemo;

import java.io.IOException;
import java.time.LocalDate;
import java.util.*;

import cassdemo.backend.BackendSession;
import cassdemo.backend.Car;
import cassdemo.backend.RentalLog;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

public class Main {

	private static final String PROPERTIES_FILENAME = "config.properties";

    public static void main(String[] args) throws Exception {
        String contactPointIP = null;
        String keyspace = null;

        Properties properties = new Properties();
        try {
            properties.load(Main.class.getClassLoader().getResourceAsStream(PROPERTIES_FILENAME));

            contactPointIP = properties.getProperty("contact_point");
            keyspace = properties.getProperty("keyspace");
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        BackendSession session = new BackendSession(contactPointIP, keyspace);

        Terminal terminal = TerminalBuilder.builder().build();
        LineReader reader = LineReaderBuilder.builder()
                .history(new DefaultHistory())
                .terminal(terminal)
                .build();

        String prompt = "dist-rental> ";

        while (true) {
            String line = reader.readLine(prompt);
            if (line == null || line.equalsIgnoreCase("exit")) break;

            String[] parts = line.split(" ");
            String command = parts[0];

            switch (command) {
                case "addC":
                case "addClient":
                    UUID uuid = UUID.randomUUID();
                    System.out.println("your uuid is: "+ uuid);
                    break;
                case "res":
                case "reserve":
                    if(parts.length<5){
                        System.out.println("wrong input for reservation");
                        break;
                    }
                    session.reserveRental(LocalDate.parse(parts[1]), UUID.fromString(parts[2]), LocalDate.parse(parts[3]), parts[4]);
                    break;
                case "del":
                case "deleteReservation":
                    if(parts.length<5){
                        System.out.println("wrong input for reservation");
                        break;
                    }
                    session.deleteReservation(LocalDate.parse(parts[1]), UUID.fromString(parts[2]), LocalDate.parse(parts[3]), parts[4]);
                    break;
                case "rentAll":
                    if(parts.length<3){
                        System.out.println("wrong input for reservation");
                        break;
                    }
                    ArrayList<RentalLog> rentalLogs = session.selectRentals(LocalDate.parse(parts[1]), UUID.fromString(parts[2]));
                    for (RentalLog rL : rentalLogs){
                        int carClassIndex = Car.getCarClasses().indexOf(rL.getCarClass());
                        ListIterator<String> iterator = Car.getCarClasses().listIterator(carClassIndex);
                        while (iterator.hasNext()) {
                            String currentOption = iterator.next();

                            List<Integer> toCheck = session.getCarIds(currentOption);
                            for (Integer carId : toCheck) {
                                if (session.rentCar(carId, UUID.fromString(parts[2]))) {
                                    System.out.println("Your car is "+session.getCarDetails(carId));
                                    break;
                                }
                            }
                        }
                    }
                    break;
                case "ret":
                case "returnCar":
                    if (parts.length != 5) {
                        System.out.println("Invalid returnCar command. Usage: returnCar <carId> <dateOut> <dateReturned> <dateReturnedExpected>");
                        break;
                    }
                    session.returnCar(Integer.getInteger(parts[1]), LocalDate.parse(parts[2]), LocalDate.parse(parts[3]), LocalDate.parse(parts[4]));
                    break;
                case "fill":
                    session.fillAvailableCars();
                    System.out.println("Available cars refilled for 30 days.");
                    break;
                default:
                    System.out.println("Unknown command: " + command);
            }
        }

        System.exit(0);
    }
}

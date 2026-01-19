package cassdemo.backend;

import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

import java.io.IOException;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

@Command(name = "rental", subcommands = {
        RentalConsole.AddClient.class,
        RentalConsole.Reserve.class,
        RentalConsole.RentAll.class,
        RentalConsole.ReturnCar.class,
        RentalConsole.Initialize.class,
        RentalConsole.DeleteReservation.class,
        CommandLine.HelpCommand.class
})
public class RentalConsole {
    private final RentalService service;

    public RentalConsole(RentalService service) {
        this.service = service;
    }

    public void start() throws IOException {
        Terminal terminal = TerminalBuilder.builder().build();
        LineReader reader = LineReaderBuilder.builder()
                .history(new DefaultHistory())
                .terminal(terminal)
                .build();

        CommandLine cmd = new CommandLine(this);

        while (true) {
            String line;
            try {
                line = reader.readLine("dist-rental> ");
            } catch (Exception e) { break; }

            if (line == null || line.equalsIgnoreCase("exit")) break;
            if (line.trim().isEmpty()) continue;

            cmd.execute(line.split("\\s+"));
        }
    }

    @Command(name = "addclient", aliases = "addc", description = "Register a new client")
    static class AddClient implements Runnable {
        public void run() { System.out.println("Your RenterId is: " + UUID.randomUUID()); }
    }

    @Command(name = "reserve", aliases = "res", description = "Reserve a car")
    static class Reserve implements Runnable {
        @ParentCommand RentalConsole parent;
        @Parameters(index = "0") LocalDate from;
        @Parameters(index = "1") UUID clientId;
        @Parameters(index = "2") LocalDate to;
        @Parameters(index = "3") String carClass;

        public void run() {
            try {
                UUID id = parent.service.reserveRental(from, clientId, to, carClass);
                System.out.println("Reservation completed. RentalId: " + id);
            } catch (Exception e) { System.err.println("Error: " + e.getMessage()); }
        }
    }

    @Command(name = "rentall", aliases = "ren", description = "Process rentals for client")
    static class RentAll implements Runnable {
        @ParentCommand RentalConsole parent;
        @Parameters(index = "0") LocalDate date;
        @Parameters(index = "1") UUID clientId;

        public void run() {
            try {
                List<Car> cars = parent.service.processRentalsForClient(date, clientId);
                cars.forEach(c -> System.out.println("Rented: " + c));
            } catch (Exception e) { System.err.println("Error: " + e.getMessage()); }
        }
    }

    @Command(name = "returncar", aliases = "ret", description = "Return a car")
    static class ReturnCar implements Runnable {
        @ParentCommand RentalConsole parent;
        @Parameters(index = "0") int carId;
        @Parameters(index = "1") LocalDate dateOut;
        @Parameters(index = "2") LocalDate dateRet;
        @Parameters(index = "3") LocalDate dateExp;

        public void run() {
            try {
                parent.service.returnCar(carId, dateOut, dateRet, dateExp);
                System.out.println("Car returned successfully.");
            } catch (Exception e) { System.err.println("Error: " + e.getMessage()); }
        }
    }

    @Command(name = "deletereservation", aliases = "del", description = "Return a car")
    static class DeleteReservation implements Runnable {
        @ParentCommand RentalConsole parent;
        @Parameters(index = "0") LocalDate dateFrom;
        @Parameters(index = "1") UUID clientId;
        @Parameters(index = "2") UUID rentalId;
        @Parameters(index = "3") LocalDate dateTo;
        @Parameters(index = "4") String carClass;

        public void run() {
            try {
                parent.service.deleteReservation(dateFrom, clientId, rentalId, dateTo, carClass);
                System.out.println("Car returned successfully.");
            } catch (Exception e) { System.err.println("Error: " + e.getMessage()); }
        }
    }

    @Command(name = "initialize", aliases = "init", description = "Initialize car availability")
    static class Initialize implements Runnable {
        @ParentCommand RentalConsole parent;
        public void run() {
            try {
                parent.service.initializeDatabase();
                System.out.println("Available cars refilled.");
            } catch (Exception e) { System.err.println("Error: " + e.getMessage()); }
        }
    }
}

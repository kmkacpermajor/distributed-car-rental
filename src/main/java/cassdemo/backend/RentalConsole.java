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
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

@Command(name = "rental", subcommands = {
        RentalConsole.AddClient.class,
        RentalConsole.Reserve.class,
        RentalConsole.RentAll.class,
        RentalConsole.ReturnCar.class,
        RentalConsole.Initialize.class,
        RentalConsole.DeleteReservation.class,
        RentalConsole.Classes.class,
        CommandLine.HelpCommand.class
})
public class RentalConsole {
    private final RentalService service;

    public RentalConsole(RentalService service) {
        this.service = service;
    }

    private static class CarClassConverter implements CommandLine.ITypeConverter<String> {
        @Override
        public String convert(String value) {
            if (value == null) {
                throw new CommandLine.TypeConversionException("Car class is required. Allowed values: " + Car.getCarClasses());
            }

            String normalized = value.trim();
            if (normalized.isEmpty()) {
                throw new CommandLine.TypeConversionException("Car class is required. Allowed values: " + Car.getCarClasses());
            }

            // accept lowercase input too, but always store the canonical (uppercase) value
            normalized = normalized.toUpperCase();

            if (!Car.getCarClasses().contains(normalized)) {
                throw new CommandLine.TypeConversionException(
                        "Invalid carClass '" + value + "'. Allowed values: " + Car.getCarClasses()
                );
            }

            return normalized;
        }
    }

    private static String prettyParameterError(CommandLine.ParameterException ex) {
        Throwable cause = ex.getCause();
        if (cause instanceof CommandLine.TypeConversionException && cause.getMessage() != null && !cause.getMessage().isBlank()) {
            return cause.getMessage(); // show only the friendly converter message
        }
        return ex.getMessage();
    }

    private static List<String> getSuggestions(CommandLine root, String[] args) {
        if (root == null || args == null || args.length == 0) return Collections.emptyList();

        String token = args[0];
        if (token == null) return Collections.emptyList();
        token = token.trim();
        if (token.isEmpty()) return Collections.emptyList();

        if (root.getSubcommands().containsKey(token)) return Collections.emptyList();

        List<String> matches = new ArrayList<>();
        for (String name : root.getSubcommands().keySet()) {
            if (name != null && name.startsWith(token)) {
                matches.add(name);
            }
        }

        if (matches.size() <= 1) return Collections.emptyList();

        return matches;
    }

    public void start() throws IOException {
        Terminal terminal = TerminalBuilder.builder().build();
        LineReader reader = LineReaderBuilder.builder()
                .history(new DefaultHistory())
                .terminal(terminal)
                .build();

        CommandLine cmd = new CommandLine(this);

        cmd.setAbbreviatedSubcommandsAllowed(true);

        cmd.registerConverter(LocalDate.class, value -> {
            try {
                return LocalDate.parse(value, DateTimeFormatter.ISO_LOCAL_DATE);
            } catch (DateTimeParseException e) {
                throw new CommandLine.TypeConversionException(
                        "Invalid date '" + value + "'. Expected format: yyyy-MM-dd (example: 2026-01-27)"
                );
            }
        });

        cmd.registerConverter(UUID.class, value -> {
            try {
                return UUID.fromString(value);
            } catch (IllegalArgumentException e) {
                throw new CommandLine.TypeConversionException(
                        "Invalid UUID '" + value + "'. Expected format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
                );
            }
        });

        cmd.registerConverter(int.class, value -> {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                throw new CommandLine.TypeConversionException(
                        "Invalid number '" + value + "'. Expected an integer (example: 42)"
                );
            }
        });

        cmd.registerConverter(Integer.class, value -> {
            try {
                return Integer.valueOf(value);
            } catch (NumberFormatException e) {
                throw new CommandLine.TypeConversionException(
                        "Invalid number '" + value + "'. Expected an integer (example: 42)"
                );
            }
        });

        cmd.setParameterExceptionHandler((ex, args) -> {
            CommandLine commandLine = ex.getCommandLine();

            commandLine.getErr().println("Error: " + prettyParameterError(ex));

            List<String> suggestions = getSuggestions(cmd, args);
            if (!suggestions.isEmpty()) {
                commandLine.getErr().println("Did you mean:");
                for (String s : suggestions) {
                    commandLine.getErr().println("  " + s);
                }
            }
            return 0; // don't terminate the REPL on parse errors
        });

        cmd.setExecutionExceptionHandler((ex, commandLine, parseResult) -> {
            System.err.println("Error: " + ex.getMessage());
            return commandLine.getCommandSpec().exitCodeOnExecutionException();
        });

        while (true) {
            String line;
            try {
                line = reader.readLine("dist-rental> ");
            } catch (Exception e) {
                break;
            }

            if (line == null || line.equalsIgnoreCase("exit")) break;
            if (line.trim().isEmpty()) continue;

            cmd.execute(line.split("\\s+"));
        }
    }

    @Command(name = "addclient", description = "Register a new client")
    static class AddClient implements Callable<Integer> {
        @Override
        public Integer call() {
            System.out.println("Your RenterId is: " + UUID.randomUUID());
            return 0;
        }
    }

    @Command(name = "reserve", description = "Reserve a car")
    static class Reserve implements Callable<Integer> {
        @ParentCommand RentalConsole parent;
        @Parameters(index = "0") LocalDate from;
        @Parameters(index = "1") UUID clientId;
        @Parameters(index = "2") LocalDate to;
        @Parameters(index = "3", converter = CarClassConverter.class) String carClass;

        @Override
        public Integer call() throws Exception {
            UUID id = parent.service.reserveRental(from, clientId, to, carClass);
            System.out.println("Reservation completed. RentalId: " + id);
            return 0;
        }
    }

    @Command(name = "rentall", description = "Process rentals for client")
    static class RentAll implements Callable<Integer> {
        @ParentCommand RentalConsole parent;
        @Parameters(index = "0") LocalDate date;
        @Parameters(index = "1") UUID clientId;

        @Override
        public Integer call() throws Exception {
            List<Car> cars = parent.service.processRentalsForClient(date, clientId);
            cars.forEach(c -> System.out.println("Rented: " + c));
            return 0;
        }
    }

    @Command(name = "returncar", description = "Return a car")
    static class ReturnCar implements Callable<Integer> {
        @ParentCommand RentalConsole parent;
        @Parameters(index = "0") int carId;
        @Parameters(index = "1") LocalDate dateOut;
        @Parameters(index = "2") LocalDate dateRet;
        @Parameters(index = "3") LocalDate dateExp;

        @Override
        public Integer call() throws Exception {
            parent.service.returnCar(carId, dateOut, dateRet, dateExp);
            System.out.println("Car returned successfully.");
            return 0;
        }
    }

    @Command(name = "deletereservation", description = "Return a car")
    static class DeleteReservation implements Callable<Integer> {
        @ParentCommand RentalConsole parent;
        @Parameters(index = "0") LocalDate dateFrom;
        @Parameters(index = "1") UUID clientId;
        @Parameters(index = "2") UUID rentalId;
        @Parameters(index = "3") LocalDate dateTo;
        @Parameters(index = "4", converter = CarClassConverter.class) String carClass;

        @Override
        public Integer call() throws Exception {
            parent.service.deleteReservation(dateFrom, clientId, rentalId, dateTo, carClass);
            System.out.println("Car returned successfully.");
            return 0;
        }
    }

    @Command(name = "initialize", description = "Initialize car availability")
    static class Initialize implements Callable<Integer> {
        @ParentCommand RentalConsole parent;

        @Override
        public Integer call() throws Exception {
            parent.service.initializeDatabase();
            System.out.println("Available cars refilled.");
            return 0;
        }
    }

    @Command(name = "classes", description = "Show available car classes for date")
    static class Classes implements Callable<Integer> {
        @ParentCommand
        RentalConsole parent;
        @Parameters(index = "0")
        LocalDate date;

        @Override
        public Integer call() throws Exception {
            List<String> classes = parent.service.getAvailableCarClasses(date);
            System.out.println("Available car classes for " + date + ":");
            classes.forEach(System.out::println);
            return 0;
        }
    }
}

package cassdemo;

import java.io.IOException;
import java.time.LocalDate;
import java.util.Properties;
import java.util.UUID;

import cassdemo.backend.BackendException;
import cassdemo.backend.BackendSession;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

public class Main {

	private static final String PROPERTIES_FILENAME = "config.properties";

    public static void main(String[] args) throws Exception {
        String contactPoint = null;
        String keyspace = null;

        Properties properties = new Properties();
        try {
            properties.load(Main.class.getClassLoader().getResourceAsStream(PROPERTIES_FILENAME));

            contactPoint = properties.getProperty("contact_point");
            keyspace = properties.getProperty("keyspace");
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        BackendSession session = new BackendSession(contactPoint, keyspace);

        Terminal terminal = TerminalBuilder.builder().build();
        LineReader reader = LineReaderBuilder.builder()
                .terminal(terminal)
                .build();

        String prompt = "dist-rental> ";

        while (true) {
            String line = reader.readLine(prompt);
            if (line == null || line.equalsIgnoreCase("exit")) break;

            String[] parts = line.split(" ");
            String command = parts[0];

            switch (command) {
                case "ret":
                case "returnCar":
                    if (parts.length != 5) {
                        System.out.println("Invalid returnCar command. Usage: returnCar <carId> <dateOut> <dateReturned> <dateReturnedExpected>");
                        break;
                    }
                    session.returnCar(Integer.getInteger(parts[1]), LocalDate.parse(parts[2]), LocalDate.parse(parts[3]), LocalDate.parse(parts[4]));
                    break;
                default:
                    System.out.println("Unknown command: " + command);
            }
        }

        System.exit(0);
    }
}

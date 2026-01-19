package cassdemo;

import cassdemo.backend.RentalConsole;
import cassdemo.backend.RentalService;

import java.io.IOException;
import java.util.Properties;

public class Main {
    private static final String PROPERTIES_FILENAME = "config.properties";

    public static void main(String[] args) throws Exception {
        Properties props = loadProperties();
        String contactPoint = props.getProperty("contact_point", "127.0.0.1");
        String keyspace = props.getProperty("keyspace", "distrental");

        try (RentalService service = new RentalService(contactPoint, keyspace)) {
            new RentalConsole(service).start();
        }
        System.exit(0);
    }

    private static Properties loadProperties() {
        Properties props = new Properties();
        try (var stream = Main.class.getClassLoader().getResourceAsStream(PROPERTIES_FILENAME)) {
            if (stream != null) props.load(stream);
        } catch (IOException e) {
            System.err.println("Could not load properties: " + e.getMessage());
        }
        return props;
    }
}

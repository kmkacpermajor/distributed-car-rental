package cassdemo.api;

import cassdemo.backend.BackendException;
import cassdemo.backend.RentalService;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource(value = "classpath:config.properties", ignoreResourceNotFound = true)
public class ApiConfig {

    @Bean
    public RentalService rentalService(
            @Value("${contact_point:127.0.0.1}") String contactPointIP,
            @Value("${keyspace:distrental}") String keyspace
    ) throws BackendException {
        return new RentalService(contactPointIP, keyspace);
    }

    @Bean
    public DisposableBean rentalServiceCloser(RentalService rentalService) {
        return rentalService::close;
    }
}

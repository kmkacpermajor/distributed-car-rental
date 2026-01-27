package cassdemo.api;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;

public final class ApiMain {
    public static void main(String[] args) {
        new SpringApplicationBuilder(ApiSpringApp.class)
                .web(WebApplicationType.SERVLET)
                .run(args);
    }
}

package cassdemo.api.dto;

import java.time.LocalDate;
import java.util.UUID;

public record ProcessRentalsRequest(
        LocalDate date,
        UUID clientId
) {}

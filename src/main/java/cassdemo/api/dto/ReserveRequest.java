package cassdemo.api.dto;

import java.time.LocalDate;
import java.util.UUID;

public record ReserveRequest(
        LocalDate from,
        LocalDate to,
        UUID clientId,
        String carClass
) {}

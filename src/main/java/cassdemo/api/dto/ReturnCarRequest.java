package cassdemo.api.dto;

import java.time.LocalDate;

public record ReturnCarRequest(
        int carId,
        LocalDate dateOut,
        LocalDate dateRet,
        LocalDate dateExp
) {}

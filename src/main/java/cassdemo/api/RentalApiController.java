package cassdemo.api;

import cassdemo.backend.BackendException;
import cassdemo.backend.Car;
import cassdemo.backend.RentalService;
import cassdemo.api.dto.ProcessRentalsRequest;
import cassdemo.api.dto.ReserveRequest;
import cassdemo.api.dto.ReserveResponse;
import cassdemo.api.dto.ReturnCarRequest;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api")
public class RentalApiController {

    private final RentalService service;

    public RentalApiController(RentalService service) {
        this.service = service;
    }

    @PostMapping("/reservations")
    public ReserveResponse reserve(@RequestBody ReserveRequest req) {
        try {
            String carClass = CarClassValidation.normalizeAndValidate(req.carClass());
            UUID rentalId = service.reserveRental(req.from(), req.clientId(), req.to(), carClass);
            return new ReserveResponse(rentalId);
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage(), e);
        } catch (BackendException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage(), e);
        }
    }

    @PostMapping("/rentals/process")
    public List<Car> processRentals(@RequestBody ProcessRentalsRequest req) {
        try {
            return service.processRentalsForClient(req.date(), req.clientId());
        } catch (BackendException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage(), e);
        }
    }

    @PostMapping("/returns")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void returnCar(@RequestBody ReturnCarRequest req) {
        try {
            service.returnCar(req.carId(), req.dateOut(), req.dateRet(), req.dateExp());
        } catch (BackendException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage(), e);
        }
    }

    @GetMapping("/classes")
    public List<String> classes(@RequestParam("date") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date) {
        try {
            return service.getAvailableCarClasses(date);
        } catch (BackendException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage(), e);
        }
    }
}

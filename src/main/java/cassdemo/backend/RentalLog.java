package cassdemo.backend;

import java.time.LocalDate;
import java.util.UUID;

public class RentalLog {
    private final LocalDate dateFrom;
    private final UUID renterId;
    private final UUID rentalId;
    private final LocalDate dateTo;
    private final String carClass;

    public RentalLog(Builder builder){
        this.dateFrom = builder.dateFrom;
        this.renterId = builder.renterId;
        this.rentalId = builder.rentalId;
        this.dateTo = builder.dateTo;
        this.carClass = builder.carClass;
    }

    public LocalDate getDateFrom() {
        return dateFrom;
    }

    public UUID getRenterId() {
        return renterId;
    }

    public LocalDate getDateTo() {
        return dateTo;
    }

    public String getCarClass() {
        return carClass;
    }

    public UUID getRentalId(){
        return rentalId;
    }

    public static class Builder {
        private LocalDate dateFrom;
        private UUID renterId;
        private UUID rentalId;
        private LocalDate dateTo;
        private String carClass;

        public Builder dateFrom(LocalDate dateFrom){
            this.dateFrom = dateFrom;
            return this;
        }
        public Builder renterId(UUID renterId){
            this.renterId = renterId;
            return this;
        }
        public Builder rentalId(UUID rentalId){
            this.rentalId = rentalId;
            return this;
        }
        public Builder dateTo(LocalDate dateTo){
            this.dateTo = dateTo;
            return this;
        }
        public Builder carClass(String carClass){
            this.carClass = carClass;
            return this;
        }
        public RentalLog build() {
            return new RentalLog(this);
        }

    }

}

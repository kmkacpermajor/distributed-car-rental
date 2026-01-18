package cassdemo.backend;

import java.util.Arrays;
import java.util.LinkedList;

public class Car {
    private static final LinkedList<String> CAR_CLASSES = new LinkedList<>(Arrays.asList(
            "A", "B", "C", "D", "E", "F", "S"
    ));

    private final Integer carId;
    private final String carName;
    private final String carClass;
    private final String licensePlate;

    public Car(Integer carId, String carName, String carClass, String licensePlate) {
        this.carId = carId;
        this.carName = carName;
        this.carClass = carClass;
        this.licensePlate = licensePlate;
    }


    public Integer getCarId() {
        return carId;
    }

    public String getCarName() {
        return carName;
    }

    public String getCarClass() {
        return carClass;
    }

    public String getLicensePlate() {
        return licensePlate;
    }

    public static LinkedList<String> getCarClasses() {
        return CAR_CLASSES;
    }

    @Override
    public String toString() {
        return "Car(carId=" +
                carId +
                ", carName='" +
                carName +
                ", carClass='" +
                carClass +
                ", licensePlate='" +
                licensePlate +
                ")";
    }
}

package cassdemo.api;

import cassdemo.backend.Car;

public final class CarClassValidation {
    private CarClassValidation() {}

    public static String normalizeAndValidate(String raw) {
        if (raw == null) {
            throw new IllegalArgumentException("carClass is required. Allowed values: " + Car.getCarClasses());
        }
        String normalized = raw.trim().toUpperCase();
        if (normalized.isEmpty() || !Car.getCarClasses().contains(normalized)) {
            throw new IllegalArgumentException("Invalid carClass '" + raw + "'. Allowed values: " + Car.getCarClasses());
        }
        return normalized;
    }
}

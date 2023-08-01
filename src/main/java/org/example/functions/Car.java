package org.example.functions;


public class Car {
    Integer carId;
    String brand;
    String model;
    Long year;
    String color;
    Double mileage;
    Double price;

    public Integer getCarId() {
        return carId;
    }

    public void setCarId(final Integer carId) {
        this.carId = carId;
    }

    public String getBrand() {
        return brand;
    }

    public void setBrand(final String brand) {
        this.brand = brand;
    }

    public String getModel() {
        return model;
    }

    public void setModel(final String model) {
        this.model = model;
    }

    public Long getYear() {
        return year;
    }

    public void setYear(final Long year) {
        this.year = year;
    }

    public String getColor() {
        return color;
    }

    public void setColor(final String color) {
        this.color = color;
    }

    public Double getMileage() {
        return mileage;
    }

    public void setMileage(final Double mileage) {
        this.mileage = mileage;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(final Double price) {
        this.price = price;
    }

    public Car() {
    }

    public Car(final Integer carId, final String brand, final String model, final Long year, final String color, final Double mileage, final Double price) {
        this.carId = carId;
        this.brand = brand;
        this.model = model;
        this.year = year;
        this.color = color;
        this.mileage = mileage;
        this.price = price;
    }

    @Override
    public String toString() {
        return "CarDetails{" +
                "carId=" + carId +
                ", brand='" + brand + '\'' +
                ", model='" + model + '\'' +
                ", year=" + year +
                ", color='" + color + '\'' +
                ", mileage=" + mileage +
                ", price=" + price +
                '}';
    }
}

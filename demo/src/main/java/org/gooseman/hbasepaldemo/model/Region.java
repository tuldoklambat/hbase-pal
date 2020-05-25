package org.gooseman.hbasepaldemo.model;

import java.util.Arrays;

public enum Region {
    None ("None"),
    Asia("Asia"),
    AustraliaAndOceania("Australia and Oceania"),
    CentralAmericaAndTheCaribbean("Central America and the Caribbean"),
    Europe("Europe"),
    MiddleEastAndNorthAfrica("Middle East and North Africa"),
    NorthAmerica("North America"),
    SubSaharanAfrica("Sub-Saharan Africa");

    private String value;
    Region(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static Region convert(String value) {
        return Arrays.stream(Region.values()).filter(r -> r.value.equals(value)).findFirst().orElse(Region.None);
    }
}

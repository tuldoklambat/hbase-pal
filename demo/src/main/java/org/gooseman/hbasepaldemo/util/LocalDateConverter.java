package org.gooseman.hbasepaldemo.util;

import com.opencsv.bean.AbstractBeanField;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class LocalDateConverter extends AbstractBeanField {
    @Override
    protected Object convert(String s)  {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("M/d/yyyy");
        LocalDate parse = LocalDate.parse(s, formatter);
        return parse;
    }
}

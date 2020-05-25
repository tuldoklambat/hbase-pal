package org.gooseman.hbasepaldemo.util;

import com.opencsv.bean.AbstractBeanField;
import org.gooseman.hbasepaldemo.model.Region;

public class RegionConverter extends AbstractBeanField {
    @Override
    protected Object convert(String s) {
        return Region.convert(s);
    }
}

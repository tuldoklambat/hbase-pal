package org.gooseman.hbasepaldemo.util;

import org.apache.hadoop.hbase.util.Bytes;
import org.gooseman.hbase.HBaseColumnConverter;
import org.gooseman.hbase.HBaseUtil;
import org.gooseman.hbasepaldemo.model.Region;

public class RegionColumnConverter extends HBaseColumnConverter {
    @Override
    protected Object FromBytes(byte[] binValue, Class<?> binValueType) {
        return Region.convert(Bytes.toString(binValue));
    }

    @Override
    protected byte[] ToBytes(Object value, Class<?> valueType) {
        return HBaseUtil.ToBytes(((Region) value).getValue());
    }
}
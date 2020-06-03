package org.gooseman.hbase;

public class DefaultHBaseColumnConverter extends HBaseColumnConverter {
    @Override
    protected Object FromBytes(byte[] binValue, Class<?> binValueType) {
        return HBaseUtil.FromBytes(binValue, binValueType);
    }

    @Override
    protected byte[] ToBytes(Object value, Class<?> valueType) {
        return HBaseUtil.ToBytes(value, valueType);
    }
}

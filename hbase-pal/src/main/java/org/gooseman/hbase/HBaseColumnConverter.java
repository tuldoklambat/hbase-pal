package org.gooseman.hbase;

public abstract class HBaseColumnConverter {
    protected abstract Object FromBytes(byte[] binValue, Class<?> binValueType);
    protected abstract byte[] ToBytes(Object value, Class<?> valueType);
}

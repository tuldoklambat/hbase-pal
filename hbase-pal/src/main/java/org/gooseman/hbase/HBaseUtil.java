/*
 * Copyright (c) 2020 Gooseman Brothers (gooseman.brothers@gmail.com)
 * All rights reserved.
 *
 * THIS CODE IS PROVIDED *AS IS* BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT LIMITATION ANY IMPLIED
 * WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR A PARTICULAR PURPOSE,
 * MERCHANTABILITY OR NON-INFRINGEMENT.
 */

package org.gooseman.hbase;

import org.apache.hadoop.hbase.util.Bytes;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public final class HBaseUtil {
    private static final Map<Class<?>, Function<Object, byte[]>> ToBytes;
    static {
        ToBytes = new HashMap<>();
        ToBytes.put(String.class, o -> Bytes.toBytes((String) o));
        ToBytes.put(double.class, o -> Bytes.toBytes((double) o));
        ToBytes.put(Double.class, o -> Bytes.toBytes((Double) o));
        ToBytes.put(int.class, o -> Bytes.toBytes((int) o));
        ToBytes.put(Integer.class, o -> Bytes.toBytes((Integer) o));
        ToBytes.put(long.class, o -> Bytes.toBytes((long) o));
        ToBytes.put(Long.class, o -> Bytes.toBytes((Long) o));
        ToBytes.put(Float.class, o -> Bytes.toBytes((Float) o));
        ToBytes.put(float.class, o -> Bytes.toBytes((float) o));
        ToBytes.put(Boolean.class, o -> Bytes.toBytes((Boolean) o));
        ToBytes.put(boolean.class, o -> Bytes.toBytes((boolean) o));
        ToBytes.put(Short.class, o -> Bytes.toBytes((Short) o));
        ToBytes.put(short.class, o -> Bytes.toBytes((short) o));
        ToBytes.put(BigDecimal.class, o -> Bytes.toBytes((BigDecimal) o));
        ToBytes.put(LocalDate.class, o -> Bytes.toBytes(((LocalDate)o).toEpochDay()));
        ToBytes.put(LocalDateTime.class, o -> Bytes.toBytes(((LocalDateTime) o).toInstant(ZoneOffset.UTC).toEpochMilli()));
    }

    private static final Map<Class<?>, Function<byte[], Object>> FromBytes;
    static {
        FromBytes = new HashMap<>();
        FromBytes.put(String.class, Bytes::toString);
        FromBytes.put(double.class, Bytes::toDouble);
        FromBytes.put(Double.class, Bytes::toDouble);
        FromBytes.put(int.class, Bytes::toInt);
        FromBytes.put(Integer.class, Bytes::toInt);
        FromBytes.put(long.class, Bytes::toLong);
        FromBytes.put(Long.class, Bytes::toLong);
        FromBytes.put(Float.class, Bytes::toFloat);
        FromBytes.put(float.class, Bytes::toFloat);
        FromBytes.put(Boolean.class, Bytes::toBoolean);
        FromBytes.put(boolean.class, Bytes::toBoolean);
        FromBytes.put(Short.class, Bytes::toShort);
        FromBytes.put(short.class, Bytes::toShort);
        FromBytes.put(BigDecimal.class, Bytes::toBigDecimal);
        FromBytes.put(LocalDate.class, b -> LocalDate.ofInstant(Instant.ofEpochMilli(Bytes.toLong(b)), ZoneOffset.UTC));
        FromBytes.put(LocalDateTime.class, b -> LocalDateTime
                .ofInstant(Instant.ofEpochMilli(Bytes.toLong(b)), ZoneOffset.UTC));
    }

    private HBaseUtil() {
    }

    public static byte[] ToBytes(Object value, Class<?> valueType) {
        return value != null && ToBytes.containsKey(valueType)
                ? ToBytes.get(valueType).apply(value)
                : null;
    }

    public static byte[] ToBytes(Object value) {
        return ToBytes(value, value.getClass());
    }

    public static Object FromBytes(byte[] binValue, Class<?> valueType) {
        return FromBytes.containsKey(valueType)
                ? FromBytes.get(valueType).apply(binValue)
                : null;
    }

    public static int getSaltedHashValue(Object value, int salt) {
        return Math.abs(value.hashCode() % salt);
    }
}

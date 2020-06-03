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

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class HBaseColumnInfo {

    private final byte[] binName;
    private final byte[] binFamily;
    private final Field decoratedField;
    private final HBaseColumn hBaseColumn;

    private HBaseColumnConverter hBaseColumnConverter;

    public HBaseColumnInfo(HBaseColumn hBaseColumn, Field field) {
        this.hBaseColumn = hBaseColumn;
        binName = toBytes(hBaseColumn.name().isEmpty() ? field.getName() : hBaseColumn.name());
        binFamily = toBytes(hBaseColumn.family());
        decoratedField = field;
    }

    public byte[] getBinName() {
        return binName;
    }

    public byte[] getBinFamily() {
        return binFamily;
    }

    public Field getDecoratedField() {
        return decoratedField;
    }

    public HBaseColumnConverter getHBaseColumnConverter() throws NoSuchMethodException, IllegalAccessException,
            InvocationTargetException, InstantiationException {
        if (hBaseColumnConverter == null) {
            hBaseColumnConverter = hBaseColumn.converter().getDeclaredConstructor().newInstance();
        }
        return hBaseColumnConverter;
    }
}
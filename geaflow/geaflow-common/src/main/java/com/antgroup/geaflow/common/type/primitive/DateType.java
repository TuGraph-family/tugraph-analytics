package com.antgroup.geaflow.common.type.primitive;

import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.common.type.Types;
import com.google.common.primitives.Longs;
import java.sql.Date;

public class DateType implements IType<Date> {

    public static final DateType INSTANCE = new DateType();

    @Override
    public String getName() {
        return Types.TYPE_NAME_DATE;
    }

    @Override
    public Class<Date> getTypeClass() {
        return Date.class;
    }

    @Override
    public byte[] serialize(Date obj) {
        return Longs.toByteArray(obj.getTime());
    }

    @Override
    public Date deserialize(byte[] bytes) {
        return new Date(Longs.fromByteArray(bytes));
    }

    @Override
    public int compare(Date x, Date y) {
        return Long.compare(x.getTime(), y.getTime());
    }

    @Override
    public boolean isPrimitive() {
        return true;
    }
}

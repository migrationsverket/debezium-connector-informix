package laoflch.debezium.connector.informix;

import com.informix.jdbc.IfmxReadableType;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.util.Clock;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class InformixChangeRecordEmitter extends RelationalChangeRecordEmitter {

    public static final int OP_DELETE = 1;
    public static final int OP_INSERT = 2;
    public static final int OP_UPDATE = 3;
    // public static final int OP_UPDATE_AFTER = 4;
    public static final int OP_TRUNCATE = 5;

    private final int operation;
    private final Object[] data;
    private final Object[] dataNext;

    public InformixChangeRecordEmitter(OffsetContext offset, int operation, Object[] data, Object[] dataNext, Clock clock) {
        super(offset, clock);

        this.operation = operation;
        this.data = data;
        this.dataNext = dataNext;
    }

    @Override
    protected Operation getOperation() {
        if (operation == OP_DELETE) {
            return Operation.DELETE;
        }
        else if (operation == OP_INSERT) {
            return Operation.CREATE;
        }
        else if (operation == OP_UPDATE) {
            return Operation.UPDATE;
        }
        throw new IllegalArgumentException("Received event of unexpected command type: " + operation);
    }

    @Override
    protected Object[] getOldColumnValues() {
        switch (getOperation()) {
            case CREATE:
            case READ:
                return null;
            default:
                return data;
        }
    }

    @Override
    protected Object[] getNewColumnValues() {
        switch (getOperation()) {
            case CREATE:
            case READ:
                return data;
            case UPDATE:
                return dataNext;
            default:
                return null;
        }
    }

    /**
     * Convert columns data from Map[String,IfmxReadableType] to Object[].
     * Debezium can't convert the IfmxReadableType object to kafka direct,so use map[AnyRef](x=>x.toObject) to extract the jave type value
     * from IfmxReadableType and pass to debezium for kafka
     *
     * @param data the data from informix cdc map[String,IfmxReadableType].
     *
     * @author Laoflch Luo, Xiaolin Zhang
     */
    public static Object[] convertIfxData2Array(Map<String, IfmxReadableType> data) throws SQLException {
        if (data == null) {
            return new Object[0];
        }

        List<Object> list = new ArrayList<>();
        for (IfmxReadableType ifmxReadableType : data.values()) {
            Object toObject = ifmxReadableType.toObject();
            list.add(toObject);
        }
        return list.toArray();
    }
}
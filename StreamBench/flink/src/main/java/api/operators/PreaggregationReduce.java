package api.operators;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Created by jun on 11/01/16.
 */
public class PreaggregationReduce<IN> extends AbstractUdfStreamOperator<IN, ReduceFunction<IN>>
        implements OneInputStreamOperator<IN, IN> {

    public PreaggregationReduce(ReduceFunction<IN> userFunction) {
        super(userFunction);
    }

    private static final long serialVersionUID = 1L;

    private static final String STATE_NAME = "_op_state";

    private transient OperatorState<IN> values;

    private TypeSerializer<IN> serializer;


    @Override
    public void open() throws Exception {
        super.open();
        values = createKeyValueState(STATE_NAME, serializer, null);
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        IN value = element.getValue();
        IN currentValue = values.value();

        if (currentValue != null) {
            IN reduced = userFunction.reduce(currentValue, value);
            values.update(reduced);
            output.collect(element.replace(reduced));
        } else {
            values.update(value);
            output.collect(element.replace(value));
        }
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        output.emitWatermark(mark);
    }
}
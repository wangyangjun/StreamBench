package operators;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Created by jun on 26/02/16.
 */

public class PointAssignMap<IN, OUT>
        extends AbstractUdfStreamOperator<OUT, MapFunction<IN, OUT>>
        implements OneInputStreamOperator<IN, OUT> {

    private static final long serialVersionUID = 1L;

    public PointAssignMap(MapFunction<IN, OUT> mapper) {
        super(mapper);
        chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        OUT out = userFunction.map(element.getValue());
        if (null != out) {
            output.collect(element.replace(out));
        }
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        output.emitWatermark(mark);
    }
}
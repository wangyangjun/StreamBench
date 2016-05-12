package fi.aalto.dmg.windowing.assigners;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.AbstractTime;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.*;

/**
 * Created by jun on 23/11/15.
 */
public class JoinTimeWindows extends WindowAssigner<Object, TimeWindow> {

    private static final long serialVersionUID = 8195982129416833702L;
    private final long size;

    protected List<TimeWindow> windows;

    private JoinTimeWindows(long size) {
        this.size = size;
        windows = new ArrayList<>();
    }

    public static JoinTimeWindows of(AbstractTime size) {
        return new JoinTimeWindows(size.toMilliseconds());
    }

    @Override
    public Collection<TimeWindow> assignWindows(Object element, long timestamp) {
        windows.add(new TimeWindow(timestamp, timestamp + size));
        List<TimeWindow> windowsOfThisEle = new ArrayList<>();
        for (TimeWindow window : windows) {
            if (window.getEnd() > timestamp) {
                windowsOfThisEle.add(window);
            } else {
                windowsOfThisEle.remove(window);
            }
        }
        return windowsOfThisEle;
    }

    @Override
    public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
        if (env.getStreamTimeCharacteristic() == TimeCharacteristic.ProcessingTime) {
            return ProcessingTimeTrigger.create();
        } else {
            return EventTimeTrigger.create();
        }
    }

    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new TimeWindow.Serializer();
    }
}

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.Triggerable;
import org.apache.flink.streaming.runtime.operators.windowing.buffers.WindowBuffer;
import org.apache.flink.streaming.runtime.operators.windowing.buffers.WindowBufferFactory;
import org.apache.flink.streaming.runtime.streamrecord.MultiplexingStreamRecordSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;
import org.apache.flink.util.InstantiationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import static java.util.Objects.requireNonNull;

/**
 * CoGroup two time windowed streams
 * CoGroup function will apply on windows with the same start time
 * No trigger and evictor supports
 * Created by jun on 26/11/15.
 */
public class CoGroupOperator<K, IN1, IN2, OUT>
        extends AbstractUdfStreamOperator<OUT, CoGroupFunction<IN1, IN2, OUT>>
        implements TwoInputStreamOperator<IN1, IN2, OUT>, Triggerable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(CoGroupOperator.class);

    // ------------------------------------------------------------------------
    // Configuration values and user functions
    // ------------------------------------------------------------------------

    private final WindowAssigner<? super IN1, TimeWindow> windowAssigner1;
    private final WindowAssigner<? super IN2, TimeWindow> windowAssigner2;

    private final KeySelector<IN1, K> keySelector1;
    private final KeySelector<IN2, K> keySelector2;

    private final Trigger<? super IN1, TimeWindow> trigger1;
    private final Trigger<? super IN2, TimeWindow> trigger2;


    private final WindowBufferFactory<? super IN1, ? extends WindowBuffer<IN1>> windowBufferFactory1;
    private final WindowBufferFactory<? super IN2, ? extends WindowBuffer<IN2>> windowBufferFactory2;

    /**
     * This is used to copy the incoming element because it can be put into several window1
     * buffers.
     */
    private TypeSerializer<IN1> inputSerializer1;
    private TypeSerializer<IN2> inputSerializer2;

    /**
     * For serializing the key in checkpoints.
     */
    private final TypeSerializer<K> keySerializer;

    /**
     * For serializing the window1 in checkpoints.
     */
    private final TypeSerializer<TimeWindow> windowSerializer;

    /**
     * If this is true. The current processing time is set as the timestamp of incoming elements.
     * This for use with a {@link org.apache.flink.streaming.api.windowing.evictors.TimeEvictor}
     * if eviction should happen based on processing time.
     */
    private boolean setProcessingTime = false;

    // ------------------------------------------------------------------------
    // State that is not checkpointed
    // ------------------------------------------------------------------------

    /**
     * use these to only allow one timer in flight at a time of each type
     * Processing time timers that are currently in-flight.
     */
    // largest timestamp of window pair, Set<ContextPair>
    private transient Map<Long, Set<ContextPair>> processingTimeTimers;

    /**
     * use these to only allow one timer in flight at a time of each type
     * Current waiting watermark callbacks.
     */
    private transient Map<Long, Set<ContextPair>> watermarkTimers;

    /**
     * This is given to the {@code WindowFunction} for emitting elements with a given timestamp.
     */
    protected transient TimestampedCollector<OUT> timestampedCollector;

    /**
     * To keep track of the current watermark so that we can immediately fire if a trigger
     * registers an event time callback for a timestamp that lies in the past.
     * current watermark for stream1
     * current watermark for stream2
     */
    protected transient long currentWatermark1 = -1L;
    protected transient long currentWatermark2 = -1L;

    // ------------------------------------------------------------------------
    // State that needs to be checkpointed
    // ------------------------------------------------------------------------

    /**
     * The windows (panes) that are currently in-flight. Each pane has a {@code WindowBuffer}
     * and a {@code TriggerContext} that stores the {@code Trigger} for that pane.
     * K - key, Long - windowStartTime
     */
    protected transient Map<K, Map<Long, ContextPair>> windows;

    public CoGroupOperator(CoGroupFunction<IN1, IN2, OUT> userFunction,
                           WindowAssigner<? super IN1, TimeWindow> windowAssigner1,
                           WindowAssigner<? super IN2, TimeWindow> windowAssigner2,
                           KeySelector<IN1, K> keySelector1,
                           KeySelector<IN2, K> keySelector2,
                           WindowBufferFactory<? super IN1, ? extends WindowBuffer<IN1>> windowBufferFactory1,
                           WindowBufferFactory<? super IN2, ? extends WindowBuffer<IN2>> windowBufferFactory2,
                           TypeSerializer<IN1> inputSerializer1,
                           TypeSerializer<IN2> inputSerializer2,
                           TypeSerializer<K> keySerializer,
                           Trigger<? super IN1, TimeWindow> trigger1,
                           Trigger<? super IN2, TimeWindow> trigger2,
                           TypeSerializer<TimeWindow> windowSerializer) {
        super(userFunction);
        this.trigger1 = trigger1;
        this.trigger2 = trigger2;
        this.windowAssigner1 = requireNonNull(windowAssigner1);
        this.windowAssigner2 = requireNonNull(windowAssigner2);
        this.keySelector1 = requireNonNull(keySelector1);
        this.keySelector2 = requireNonNull(keySelector2);
        this.windowBufferFactory1 = requireNonNull(windowBufferFactory1);
        this.windowBufferFactory2 = requireNonNull(windowBufferFactory2);
        this.inputSerializer1 = requireNonNull(inputSerializer1);
        this.inputSerializer2 = requireNonNull(inputSerializer2);
        this.keySerializer = requireNonNull(keySerializer);
        this.windowSerializer = windowSerializer;
    }


    @Override
    public final void open() throws Exception {
        super.open();

        timestampedCollector = new TimestampedCollector<>(output);

        if (null == inputSerializer1 || null == inputSerializer2) {
            throw new IllegalStateException("Input serializer was not set.");
        }

        windowBufferFactory1.setRuntimeContext(getRuntimeContext());
        windowBufferFactory1.open(getUserFunctionParameters());
        windowBufferFactory2.setRuntimeContext(getRuntimeContext());
        windowBufferFactory2.open(getUserFunctionParameters());


        // these could already be initialized from restoreState()
        if (watermarkTimers == null) {
            watermarkTimers = new HashMap<>();
        }
        if (processingTimeTimers == null) {
            processingTimeTimers = new HashMap<>();
        }
        if (windows == null) {
            windows = new HashMap<>();
        }

        // re-register timers that this window1 context had set
        for (Map.Entry<K, Map<Long, ContextPair>> entry: windows.entrySet()) {
            Map<Long, ContextPair> keyWindows = entry.getValue();
            for (ContextPair context : keyWindows.values()) {
                if (context.processingTimeTimer > 0) {
                    Set<ContextPair> triggers = processingTimeTimers.get(context.processingTimeTimer);
                    if (triggers == null) {
                        getRuntimeContext().registerTimer(context.processingTimeTimer, CoGroupOperator.this);
                        triggers = new HashSet<>();
                        processingTimeTimers.put(context.processingTimeTimer, triggers);
                    }
                    triggers.add(context);
                }
                if (context.watermarkTimer > 0) {
                    Set<ContextPair> triggers = watermarkTimers.get(context.watermarkTimer);
                    if (triggers == null) {
                        triggers = new HashSet<>();
                        watermarkTimers.put(context.watermarkTimer, triggers);
                    }
                    triggers.add(context);
                }

            }
        }
    }

    @Override
    public final void close() throws Exception {
        super.close();
        // emit the elements that we still keep
        for (Map.Entry<K, Map<Long, ContextPair>> entry: windows.entrySet()) {
            Map<Long, ContextPair> keyWindows = entry.getValue();
            for (ContextPair window: keyWindows.values()) {
                emitWindow(window);
            }
        }
        windows.clear();
        windowBufferFactory1.close();
        windowBufferFactory2.close();
    }

    protected void emitWindow(ContextPair context) throws Exception {
        if(context.coGroupAble()){
            timestampedCollector.setTimestamp(context.window1.maxTimestamp());
            if (context.windowBuffer1.size() > 0 && context.windowBuffer2.size() > 0) {
                setKeyContextElement(context.windowBuffer1.getElements().iterator().next());
                userFunction.coGroup(context.windowBuffer1.getUnpackedElements(),
                        context.windowBuffer2.getUnpackedElements(), timestampedCollector);
            }
        }
    }


    private void processTriggerResult(K key, TimeWindow window) throws Exception {
        ContextPair context;
        Map<Long, ContextPair> keyWindows = windows.get(key);
        if (keyWindows == null) {
            LOG.debug("Window {} for key {} already gone.", window, key);
            return;
        }

        context = keyWindows.remove(window.getStart());
        if (keyWindows.isEmpty()) {
            windows.remove(key);
        }
        if (context == null) {
            LOG.debug("Window {} for key {} already gone.", window, key);
            return;
        }

        emitWindow(context);
    }

    @Override
    public void trigger(long time) throws Exception {
        Set<Long> toRemove = new HashSet<>();

        for (Map.Entry<Long, Set<ContextPair>> triggers: processingTimeTimers.entrySet()) {
            long actualTime = triggers.getKey();
            if (actualTime <= time) {
                for (ContextPair context: triggers.getValue()) {
                    if(null != context.window1 && null != context.window2)
                        processTriggerResult(context.key, context.window1);
                }
                toRemove.add(triggers.getKey());
            }
        }

        for (Long l: toRemove) {
            processingTimeTimers.remove(l);
        }
    }

    /**
     * Store element of stream1 in corresponding window buffers
     * @param element
     *      record of stream 1
     * @throws Exception
     */
    @Override
    public void processElement1(StreamRecord<IN1> element) throws Exception {
        if (setProcessingTime) {
            element.replace(element.getValue(), System.currentTimeMillis());
        }

        Collection<TimeWindow> elementWindows = windowAssigner1.assignWindows(element.getValue(), element.getTimestamp());

        K key = keySelector1.getKey(element.getValue());

        Map<Long, ContextPair> keyWindows = windows.get(key);
        if (keyWindows == null) {
            keyWindows = new HashMap<>();
            windows.put(key, keyWindows);
        }

        for (TimeWindow window: elementWindows) {
            long windowStartTime = window.getStart();
            ContextPair context = keyWindows.get(windowStartTime);
            if (context == null) {
                WindowBuffer<IN1> windowBuffer1 = windowBufferFactory1.create();
                WindowBuffer<IN2> windowBuffer2 = windowBufferFactory2.create();
                context = new ContextPair(key, window, null, windowBuffer1, windowBuffer2);
                keyWindows.put(windowStartTime, context);
            } else if(!context.coGroupAble()) {
                context.window1 = window;
            }
            // store element
            context.windowBuffer1.storeElement(element);
            context.onElement1(element);
        }
    }

    /**
     * Store element of stream2 in corresponding window buffers
     * @param element
     *      record of stream 2
     * @throws Exception
     */
    @Override
    public void processElement2(StreamRecord<IN2> element) throws Exception {
        if (setProcessingTime) {
            element.replace(element.getValue(), System.currentTimeMillis());
        }

        Collection<TimeWindow> elementWindows = windowAssigner2.assignWindows(element.getValue(), element.getTimestamp());

        K key = keySelector2.getKey(element.getValue());

        Map<Long, ContextPair> keyWindows = windows.get(key);
        if (keyWindows == null) {
            keyWindows = new HashMap<>();
            windows.put(key, keyWindows);
        }

        for (TimeWindow window: elementWindows) {

            long windowStartTime = window.getStart();
            ContextPair context = keyWindows.get(windowStartTime);
            if (context == null) {
                WindowBuffer<IN1> windowBuffer1 = windowBufferFactory1.create();
                WindowBuffer<IN2> windowBuffer2 = windowBufferFactory2.create();
                context = new ContextPair(key, null, window, windowBuffer1, windowBuffer2);
                keyWindows.put(windowStartTime, context);
            } else if(!context.coGroupAble()){
                context.window2 = window;
            }
            // store element
            context.windowBuffer2.storeElement(element);
            context.onElement2(element);
        }
    }

    @Override
    public void processWatermark1(Watermark mark) throws Exception {
        Set<Long> toRemove = new HashSet<>();
        Set<ContextPair> toTrigger = new HashSet<>();

        // we cannot call the Trigger in here because trigger methods might register new triggers.
        // that would lead to concurrent modification errors.
        for (Map.Entry<Long, Set<ContextPair>> triggers: watermarkTimers.entrySet()) {
            // TODO:
            if (triggers.getKey() <= mark.getTimestamp()
                    && triggers.getKey() <= this.currentWatermark2) {
                for (ContextPair context: triggers.getValue()) {
                    toTrigger.add(context);
                }
                toRemove.add(triggers.getKey());
            }
        }

        for (ContextPair context: toTrigger) {
            if (context.watermarkTimer <= mark.getTimestamp()
                    && context.watermarkTimer <= this.currentWatermark2) {
                if(null != context.window1 && null != context.window2)
                    processTriggerResult(context.key, context.window1);
            }
        }

        for (Long l: toRemove) {
            watermarkTimers.remove(l);
        }

        output.emitWatermark(mark);
        this.currentWatermark1 = mark.getTimestamp();
    }

    @Override
    public void processWatermark2(Watermark mark) throws Exception {
        Set<Long> toRemove = new HashSet<>();
        Set<ContextPair> toTrigger = new HashSet<>();

        // we cannot call the Trigger in here because trigger methods might register new triggers.
        // that would lead to concurrent modification errors.
        for (Map.Entry<Long, Set<ContextPair>> triggers: watermarkTimers.entrySet()) {
            if (triggers.getKey() <= mark.getTimestamp()
                    && triggers.getKey() <= this.currentWatermark1) {
                for (ContextPair context: triggers.getValue()) {
                    toTrigger.add(context);
                }
                toRemove.add(triggers.getKey());
            }
        }

        for (ContextPair context: toTrigger) {
            if (context.watermarkTimer <= mark.getTimestamp()
                    && context.watermarkTimer <= this.currentWatermark1) {
                if(null != context.window1 && null != context.window2)
                    processTriggerResult(context.key, context.window1);
            }
        }

        for (Long l: toRemove) {
            watermarkTimers.remove(l);
        }

        output.emitWatermark(mark);
        this.currentWatermark2 = mark.getTimestamp();
    }

    /**
     * The {@code Context} is responsible for keeping track of the state of one pane.
     *
     * <p>
     * A pane is the bucket of elements that have the same key (assigned by the
     * {@link org.apache.flink.api.java.functions.KeySelector}) and same {@link Window}. An element can
     * be in multiple panes of it was assigned to multiple windows by the
     * {@link org.apache.flink.streaming.api.windowing.assigners.WindowAssigner}. These panes all
     * have their own instance of the {@code Trigger}.
     */
    protected class ContextPair implements Trigger.TriggerContext {
        protected K key;
        protected TimeWindow window1;
        protected TimeWindow window2;


        protected WindowBuffer<IN1> windowBuffer1;
        protected WindowBuffer<IN2> windowBuffer2;

        protected HashMap<String, Serializable> state;

        // use these to only allow one timer in flight at a time of each type
        // if the trigger registers another timer this value here will be overwritten,
        // the timer is not removed from the set of in-flight timers to improve performance.
        // When a trigger fires it is just checked against the last timer that was set.
        protected long watermarkTimer; // largest timestamp of window pair
        protected long processingTimeTimer; // largest timestamp of window pair

        public ContextPair(K key,
                           TimeWindow window1,
                           TimeWindow window2,
                           WindowBuffer<IN1> windowBuffer1,
                           WindowBuffer<IN2> windowBuffer2) {
            this.key = requireNonNull(key);
            this.window1 = window1;
            this.window2 = window2;
            this.windowBuffer1 = requireNonNull(windowBuffer1);
            this.windowBuffer2 = requireNonNull(windowBuffer2);
            state = new HashMap<>();

            this.watermarkTimer = -1;
            this.processingTimeTimer = -1;
        }


        public boolean coGroupAble(){
            return this.window1!=null&&this.window2!=null;
        }
        /**
         * Constructs a new {@code Context} by reading from a {@link DataInputView} that
         * contains a serialized context that we wrote in
         * {@link #writeToState(StateBackend.CheckpointStateOutputView)}
         */
        @SuppressWarnings("unchecked")
        protected ContextPair(DataInputView in, ClassLoader userClassloader) throws Exception {
            this.key = keySerializer.deserialize(in);
            this.window1 = windowSerializer.deserialize(in);
            this.window2 = windowSerializer.deserialize(in);
            this.watermarkTimer = in.readLong();
            this.processingTimeTimer = in.readLong();

            int stateSize = in.readInt();
            byte[] stateData = new byte[stateSize];
            in.read(stateData);
            state = InstantiationUtil.deserializeObject(stateData, userClassloader);

            this.windowBuffer1 = windowBufferFactory1.create();
            this.windowBuffer2 = windowBufferFactory2.create();
            int numElements = in.readInt();

            MultiplexingStreamRecordSerializer<IN1> recordSerializer1 = new MultiplexingStreamRecordSerializer<>(inputSerializer1);
            for (int i = 0; i < numElements; i++) {
                windowBuffer1.storeElement(recordSerializer1.deserialize(in).<IN1>asRecord());
            }

            int numElements2 = in.readInt();
            MultiplexingStreamRecordSerializer<IN2> recordSerializer2 = new MultiplexingStreamRecordSerializer<>(inputSerializer2);
            for (int i = 0; i < numElements2; i++) {
                windowBuffer2.storeElement(recordSerializer2.deserialize(in).<IN2>asRecord());
            }
        }

        /**
         * Writes the {@code Context} to the given state checkpoint output.
         */
        protected void writeToState(StateBackend.CheckpointStateOutputView out) throws IOException {
            keySerializer.serialize(key, out);
            windowSerializer.serialize(window1, out);
            windowSerializer.serialize(window2, out);
            out.writeLong(watermarkTimer);
            out.writeLong(processingTimeTimer);

            byte[] serializedState = InstantiationUtil.serializeObject(state);
            out.writeInt(serializedState.length);
            out.write(serializedState, 0, serializedState.length);

            MultiplexingStreamRecordSerializer<IN1> recordSerializer1 = new MultiplexingStreamRecordSerializer<>(inputSerializer1);
            out.writeInt(windowBuffer1.size());
            for (StreamRecord<IN1> element: windowBuffer1.getElements()) {
                recordSerializer1.serialize(element, out);
            }

            MultiplexingStreamRecordSerializer<IN2> recordSerializer2 = new MultiplexingStreamRecordSerializer<>(inputSerializer2);
            out.writeInt(windowBuffer2.size());
            for (StreamRecord<IN2> element: windowBuffer2.getElements()) {
                recordSerializer2.serialize(element, out);
            }
        }

        @SuppressWarnings("unchecked")
        public <S extends Serializable> OperatorState<S> getKeyValueState(final String name, final S defaultState) {
            return new OperatorState<S>() {
                @Override
                public S value() throws IOException {
                    Serializable value = state.get(name);
                    if (value == null) {
                        state.put(name, defaultState);
                        value = defaultState;
                    }
                    return (S) value;
                }

                @Override
                public void update(S value) throws IOException {
                    state.put(name, value);
                }
            };
        }

        // time is maxTimeStamp of a window,
        // a ContextPair has two windows and should only register to the larger one
        @Override
        public void registerProcessingTimeTimer(long time) {
            if (this.processingTimeTimer == time) {
                // we already have set a trigger for that time
                return;
            }
            Set<ContextPair> triggers = processingTimeTimers.get(time);
            if (triggers == null) {
                getRuntimeContext().registerTimer(time, CoGroupOperator.this);
                triggers = new HashSet<>();
                processingTimeTimers.put(time, triggers);
            }
            this.processingTimeTimer = time;
            triggers.add(this);
        }

        @Override
        public void registerEventTimeTimer(long time) {
            if (watermarkTimer == time) {
                // we already have set a trigger for that time
                return;
            }
            Set<ContextPair> triggers = watermarkTimers.get(time);
            if (triggers == null) {
                triggers = new HashSet<>();
                watermarkTimers.put(time, triggers);
            }
            this.watermarkTimer = time;
            triggers.add(this);
        }

        // trigger1.onElement, trigger2.onElement should register context on the same timestamp
        public void onElement1(StreamRecord<IN1> element) throws Exception {
            // onElement calls registerProcessingTimeTimer or registerEventTimeTimer
            // if window1 or window2 is null, then there is no need to register this PairContext
            TimeWindow window = window1;
            if(null != window2) {
                if(window2.getEnd()>window1.getEnd()) window = window2;
                trigger1.onElement(element.getValue(), element.getTimestamp(), window, this);
            }
        }

        public void onElement2(StreamRecord<IN2> element) throws Exception {
            TimeWindow window = window2;
            if(null != window1) {
                if(window1.getEnd()>window2.getEnd()) window = window1;
                trigger2.onElement(element.getValue(), element.getTimestamp(), window, this);
            }
        }

    }

    /**
     * When this flag is enabled the current processing time is set as the timestamp of elements
     * upon arrival. This must be used, for example, when using the
     * {@link org.apache.flink.streaming.api.windowing.evictors.TimeEvictor} with processing
     * time semantics.
     */
    public CoGroupOperator<K, IN1, IN2, OUT> enableSetProcessingTime(boolean setProcessingTime) {
        this.setProcessingTime = setProcessingTime;
        return this;
    }

    // ------------------------------------------------------------------------
    //  checkpointing and recovery
    // ------------------------------------------------------------------------

    @Override
    public StreamTaskState snapshotOperatorState(long checkpointId, long timestamp) throws Exception {
        StreamTaskState taskState = super.snapshotOperatorState(checkpointId, timestamp);

        // we write the panes with the key/value maps into the stream
        StateBackend.CheckpointStateOutputView out = getStateBackend().createCheckpointStateOutputView(checkpointId, timestamp);

        int numKeys = windows.size();
        out.writeInt(numKeys);

        for (Map.Entry<K, Map<Long, ContextPair>> keyWindows: windows.entrySet()) {
            int numWindows = keyWindows.getValue().size();
            out.writeInt(numWindows);
            for (ContextPair context: keyWindows.getValue().values()) {
                context.writeToState(out);
            }
        }

        taskState.setOperatorState(out.closeAndGetHandle());
        return taskState;
    }

    @Override
    public void restoreState(StreamTaskState taskState) throws Exception {
        super.restoreState(taskState);

        final ClassLoader userClassloader = getUserCodeClassloader();

        @SuppressWarnings("unchecked")
        StateHandle<DataInputView> inputState = (StateHandle<DataInputView>) taskState.getOperatorState();
        DataInputView in = inputState.getState(userClassloader);

        int numKeys = in.readInt();
        this.windows = new HashMap<>(numKeys);
        this.processingTimeTimers = new HashMap<>();
        this.watermarkTimers = new HashMap<>();

        for (int i = 0; i < numKeys; i++) {
            int numWindows = in.readInt();
            for (int j = 0; j < numWindows; j++) {
                ContextPair context = new ContextPair(in, userClassloader);
                Map<Long, ContextPair> keyWindows = windows.get(context.key);
                if (keyWindows == null) {
                    keyWindows = new HashMap<>(numWindows);
                    windows.put(context.key, keyWindows);
                }
                keyWindows.put(context.window1.getStart(), context);
            }
        }
    }

}
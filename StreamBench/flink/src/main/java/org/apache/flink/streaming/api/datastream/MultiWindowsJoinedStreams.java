package org.apache.flink.streaming.api.datastream;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.translation.WrappingFunction;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.CoGroupOperator;
import org.apache.flink.streaming.runtime.operators.windowing.buffers.HeapWindowBuffer;
import org.apache.flink.util.Collector;

import static java.util.Objects.requireNonNull;

/**
 *{@code JoinedStreams} represents two {@link DataStream DataStreams} that have been joined.
 * A streaming join operation is evaluated over elements in separate windows.
 *
 * <p>
 * To finalize the join operation you also need to specify a {@link KeySelector}
 * and a {@link WindowAssigner} for both the first and second input and .
 *
 * <p>
 * Note: Right now, the the join is being evaluated in memory so you need to ensure that the number
 * of elements per key does not get too high. Otherwise the JVM might crash.
 *
 * <p>
 * Example:
 *
 * <pre> {@code
 * DataStream<T1> one = ...;
 * DataStream<T2> two = ...;
 *
 * MultiWindowsJoinedStreams<T1,T2> joinedStream = new MultiWindowsJoinedStreams<>(one, two);
 * DataStream<OUT> stream = joinedStream
 *      .where(new MyFirstKeySelector())
 *      .window(SlidingTimeWindows.of(Time.of(9, TimeUnit.SECONDS), Time.of(3, TimeUnit.SECONDS)))
 *      .equalTo(new MySecondKeySelector())
 *      .window(TumblingTimeWindows.of(Time.of(3, TimeUnit.SECONDS)))
 *      .apply(new JoinFunction()); or CoGroupFunction
 * } </pre>
 */
public class MultiWindowsJoinedStreams<T1, T2> {
    /** The first input stream */
    private DataStream<T1> input1;

    /** The second input stream */
    private DataStream<T2> input2;

    private int parallelism;
    /**
     * Creates new JoinedStreams data streams, which are the first step towards building a streaming co-group.
     *
     * @param input1 The first data stream.
     * @param input2 The second data stream.
     */
    public MultiWindowsJoinedStreams(DataStream<T1> input1, DataStream<T2> input2) {
        this.input1 = requireNonNull(input1);
        this.input2 = requireNonNull(input2);
        this.parallelism = Math.max(input1.getParallelism(), input2.getParallelism());
    }

    public void setParallelism(int parallelism){
        this.parallelism = parallelism;
    }
    /**
     * Specifies a {@link KeySelector} for elements from the first input.
     */
    public <KEY> Where<KEY> where(KeySelector<T1, KEY> keySelector)  {
        TypeInformation<KEY> keyType = TypeExtractor.getKeySelectorTypes(keySelector, input1.getType());
        return new Where<>(input1.clean(keySelector), keyType);
    }

    // ------------------------------------------------------------------------

    /**
     * CoGrouped streams that have the key for one side defined.
     *
     * @param <KEY> The type of the key.
     */
    public class Where<KEY> {

        private final KeySelector<T1, KEY> keySelector1;
        private final TypeInformation<KEY> keyType;

        Where(KeySelector<T1, KEY> keySelector1, TypeInformation<KEY> keyType) {
            this.keySelector1 = keySelector1;
            this.keyType = keyType;
            if(!(input1 instanceof KeyedStream)){
                input1 = input1.keyBy(keySelector1);
            }
        }

        public WithOneWindow window(WindowAssigner<? super T1, TimeWindow> windowAssigner){
            return new WithOneWindow(windowAssigner, keyType);
        }

        public class WithOneWindow {
            private final WindowAssigner<? super T1, TimeWindow> windowAssigner1;
            private final TypeInformation<KEY> keyType;

            WithOneWindow(WindowAssigner<? super T1, TimeWindow> windowAssigner, TypeInformation<KEY> keyType) {
                this.windowAssigner1 = windowAssigner;
                this.keyType = keyType;
            }

            /**
             * Specifies a {@link KeySelector} for elements from the second input.
             */
            public EqualTo equalTo(KeySelector<T2, KEY> keySelector) {
                TypeInformation<KEY> otherKey = TypeExtractor.getKeySelectorTypes(keySelector, input2.getType());
                if (!otherKey.equals(this.keyType)) {
                    throw new IllegalArgumentException("The keys for the two inputs are not equal: " +
                            "first key = " + this.keyType + " , second key = " + otherKey);
                }

                return new EqualTo(input2.clean(keySelector));
            }

            // --------------------------------------------------------------------

            /**
             * A co-group operation that has {@link KeySelector KeySelectors} defined for both inputs.
             */
            public class EqualTo {

                private final KeySelector<T2, KEY> keySelector2;

                EqualTo(KeySelector<T2, KEY> keySelector2) {
                    this.keySelector2 = requireNonNull(keySelector2);
                    if(!(input2 instanceof KeyedStream)){
                        input2 = input2.keyBy(keySelector2);
                    }
                }

                /**
                 * Specifies the window1 on which the co-group operation works.
                 */
                public WithWindow<T1, T2, KEY> window(WindowAssigner<? super T2, TimeWindow> windowAssigner2) {
                    return new WithWindow<>(input1, input2,
                            keySelector1, keySelector2, keyType,
                            windowAssigner1, windowAssigner2);
                }
            }
        }
    }

    // ------------------------------------------------------------------------

    /**
     * A join operation that has {@link KeySelector KeySelectors} defined for both inputs as
     * well as a {@link WindowAssigner}.
     * Doesn't support trigger and evictor
     *
     * @param <T1> Type of the elements from the first input
     * @param <T2> Type of the elements from the second input
     * @param <KEY> Type of the key. This must be the same for both inputs
     */
    public class WithWindow<T1, T2, KEY> {

        private final DataStream<T1> input1;
        private final DataStream<T2> input2;

        private final KeySelector<T1, KEY> keySelector1;
        private final KeySelector<T2, KEY> keySelector2;
        private final TypeInformation<KEY> keyType;

        private final WindowAssigner<? super T1, TimeWindow> windowAssigner1;
        private final WindowAssigner<? super T2, TimeWindow> windowAssigner2;

        protected WithWindow(DataStream<T1> input1,
                             DataStream<T2> input2,
                             KeySelector<T1, KEY> keySelector1,
                             KeySelector<T2, KEY> keySelector2,
                             TypeInformation<KEY> keyType,
                             WindowAssigner<? super T1, TimeWindow> windowAssigner1,
                             WindowAssigner<? super T2, TimeWindow> windowAssigner2) {

            this.input1 = requireNonNull(input1);
            this.input2 = requireNonNull(input2);

            this.keySelector1 = requireNonNull(keySelector1);
            this.keySelector2 = requireNonNull(keySelector2);
            this.keyType = requireNonNull(keyType);

            this.windowAssigner1 = requireNonNull(windowAssigner1);
            this.windowAssigner2 = requireNonNull(windowAssigner2);

        }

        public StreamExecutionEnvironment getExecutionEnvironment() {
            return input1.getExecutionEnvironment();
        }

        /**
         * Completes the join operation with the user function that is executed
         * for each combination of elements with the same key in a window1.
         */
        public <T> DataStream<T> apply(JoinFunction<T1, T2, T> function) {
            TypeInformation<T> resultType = TypeExtractor.getBinaryOperatorReturnType(
                    function,
                    JoinFunction.class,
                    true,
                    true,
                    input1.getType(),
                    input2.getType(),
                    "Join",
                    false);
            return apply(new JoinCoGroupFunction<>(function), resultType);
        }


        // TODO: inputSerializer1
        public <T> DataStream<T> apply(CoGroupFunction<T1, T2, T> function, TypeInformation<T> resultType) {
            //clean the closure
            function = input1.getExecutionEnvironment().clean(function);
            boolean setProcessingTime = input1.getExecutionEnvironment().getStreamTimeCharacteristic() == TimeCharacteristic.ProcessingTime;

            CoGroupOperator<KEY, T1, T2, T> coGroupOperator =
                    new CoGroupOperator<>(function,
                            windowAssigner1, windowAssigner2,
                            keySelector1, keySelector2,
                            new HeapWindowBuffer.Factory<T1>(),
                            new HeapWindowBuffer.Factory<T2>(),
                            input1.getType().createSerializer(getExecutionEnvironment().getConfig()),
                            input2.getType().createSerializer(getExecutionEnvironment().getConfig()),
                            TypeExtractor.getKeySelectorTypes(keySelector1, input1.getType()).createSerializer(getExecutionEnvironment().getConfig()),
                            windowAssigner1.getDefaultTrigger(getExecutionEnvironment()),
                            windowAssigner2.getDefaultTrigger(getExecutionEnvironment()),
                            windowAssigner1.getWindowSerializer(getExecutionEnvironment().getConfig())
                    ).enableSetProcessingTime(setProcessingTime);

            TwoInputTransformation<T1, T2, T>
                    twoInputTransformation = new TwoInputTransformation<>(
                    input1.getTransformation(),
                    input2.getTransformation(),
                    "Join",
                    coGroupOperator,
                    resultType,
                    parallelism
            );

            return new DataStream<>(getExecutionEnvironment(), twoInputTransformation);
        }
    }

    // ------------------------------------------------------------------------
    //  Implementation of the functions
    // ------------------------------------------------------------------------

    /**
     * CoGroup function that does a nested-loop join to get the join result.
     */
    private static class JoinCoGroupFunction<T1, T2, T>
            extends WrappingFunction<JoinFunction<T1, T2, T>>
            implements CoGroupFunction<T1, T2, T> {
        private static final long serialVersionUID = 1L;

        public JoinCoGroupFunction(JoinFunction<T1, T2, T> wrappedFunction) {
            super(wrappedFunction);
        }

        @Override
        public void coGroup(Iterable<T1> first, Iterable<T2> second, Collector<T> out) throws Exception {
            for (T1 val1: first) {
                for (T2 val2: second) {
                    out.collect(wrappedFunction.join(val1, val2));
                }
            }
        }
    }
}

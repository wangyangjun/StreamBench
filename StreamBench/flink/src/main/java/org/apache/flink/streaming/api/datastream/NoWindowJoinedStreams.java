package org.apache.flink.streaming.api.datastream;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamJoinOperator;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;

import static java.util.Objects.requireNonNull;

/**
 * Created by jun on 30/11/15.
 */
public class NoWindowJoinedStreams<IN1, IN2> {
    /** The first input stream */
    private DataStream<IN1> input1;

    /** The second input stream */
    private DataStream<IN2> input2;

    private int parallelism;

    /**
     * Creates new JoinedStreams data streams, which are the first step towards building a streaming co-group.
     *
     * @param input1 The first data stream.
     * @param input2 The second data stream.
     */
    public NoWindowJoinedStreams(DataStream<IN1> input1, DataStream<IN2> input2) {
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
    public <KEY> Where<KEY> where(KeySelector<IN1, KEY> keySelector)  {
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

        private final KeySelector<IN1, KEY> keySelector1;
        private final TypeInformation<KEY> keyType;

        Where(KeySelector<IN1, KEY> keySelector1, TypeInformation<KEY> keyType) {
            this.keySelector1 = keySelector1;
            this.keyType = keyType;
            if (!(input1 instanceof KeyedStream)) {
                input1 = input1.keyBy(keySelector1);
            }
        }

        public WithOneBuffer buffer(Time time){
            return new WithOneBuffer(time, keyType);
        }


        // --------------------------------------------------------------------

        /**
         * A co-group operation that has {@link KeySelector KeySelectors} defined for both inputs.
         */
        public class WithOneBuffer {
            private final Time time1;
            private final TypeInformation<KEY> keyType;

            WithOneBuffer(Time time, TypeInformation<KEY> keyType) {
                this.time1 = time;
                this.keyType = keyType;
            }

            /**
             * Specifies a {@link KeySelector} for elements from the second input.
             */
            public EqualTo equalTo(KeySelector<IN2, KEY> keySelector) {
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

                private final KeySelector<IN2, KEY> keySelector2;

                EqualTo(KeySelector<IN2, KEY> keySelector2) {
                    this.keySelector2 = requireNonNull(keySelector2);
                    if(!(input2 instanceof KeyedStream)){
                        input2 = input2.keyBy(keySelector2);
                    }
                }

                /**
                 * Specifies the window1 on which the co-group operation works.
                 */
                public WithTwoBuffers<IN1, IN2, KEY> buffer(Time time) {
                    return new WithTwoBuffers<>(input1, input2,
                            keySelector1, keySelector2,
                            time1, time);
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
     * @param <IN1> Type of the elements from the first input
     * @param <IN2> Type of the elements from the second input
     * @param <KEY> Type of the key. This must be the same for both inputs
     */
    public class WithTwoBuffers<IN1, IN2, KEY> {

        private final DataStream<IN1> input1;
        private final DataStream<IN2> input2;

        private final KeySelector<IN1, KEY> keySelector1;
        private final KeySelector<IN2, KEY> keySelector2;

        private final Time time1;
        private final Time time2;

        protected WithTwoBuffers(DataStream<IN1> input1,
                                 DataStream<IN2> input2,
                                 KeySelector<IN1, KEY> keySelector1,
                                 KeySelector<IN2, KEY> keySelector2,
                                 Time time1,
                                 Time time2) {

            this.input1 = requireNonNull(input1);
            this.input2 = requireNonNull(input2);

            this.keySelector1 = requireNonNull(keySelector1);
            this.keySelector2 = requireNonNull(keySelector2);

            this.time1 = requireNonNull(time1);
            this.time2 = requireNonNull(time2);

        }

        public StreamExecutionEnvironment getExecutionEnvironment() {
            return input1.getExecutionEnvironment();
        }

        /**
         * Completes the join operation with the user function that is executed
         * for each combination of elements with the same key in a window1.
         */
        public <OUT> DataStream<OUT> apply(JoinFunction<IN1, IN2, OUT> function) {
            StreamExecutionEnvironment env = getExecutionEnvironment();
            function = env.clean(function);
            boolean enableSetProcessingTime = env.getStreamTimeCharacteristic() == TimeCharacteristic.ProcessingTime;

            TypeInformation<OUT> resultType = TypeExtractor.getBinaryOperatorReturnType(
                    function,
                    JoinFunction.class,
                    true,
                    true,
                    input1.getType(),
                    input2.getType(),
                    "Join",
                    false);

            StreamJoinOperator<KEY, IN1, IN2, OUT> joinOperator
                    = new StreamJoinOperator<>(
                    function,
                    keySelector1,
                    keySelector2,
                    time1.toMilliseconds(),
                    time2.toMilliseconds(),
                    input1.getType().createSerializer(getExecutionEnvironment().getConfig()),
                    input2.getType().createSerializer(getExecutionEnvironment().getConfig())
            ).enableSetProcessingTime(enableSetProcessingTime);

            TwoInputTransformation<IN1, IN2, OUT> twoInputTransformation
                    = new TwoInputTransformation<>(
                    input1.getTransformation(),
                    input2.getTransformation(),
                    "Join",
                    joinOperator,
                    resultType,
                    parallelism
            );

            return new DataStream<>(getExecutionEnvironment(), twoInputTransformation);
        }

    }
}

package fi.aalto.dmg;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {
        String str = "Hello";
        System.out.println(str.hashCode());
        Object o = str;
        System.out.println(o.hashCode());

    }

    class MySourceFunction implements SourceFunction<Tuple2<Double, Integer>>, ResultTypeQueryable<Tuple2<Double, Integer>>{

        @Override
        public TypeInformation<Tuple2<Double, Integer>> getProducedType() {
            return null;
        }

        @Override
        public void run(SourceContext<Tuple2<Double, Integer>> ctx) throws Exception {

        }

        @Override
        public void cancel() {

        }
    }
}

package fi.aalto.dmg.datasource;

import fi.aalto.dmg.exceptions.WorkloadException;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Created by yangjun.wang on 18/10/15.
 */
public class KafkaSourceTest extends TestCase {

    public KafkaSourceTest( String testName )
    {
        super( testName );
    }

    public static Test suite()
    {
        return new TestSuite( KafkaSourceTest.class );
    }

    public void test() throws WorkloadException {
        KafkaDataSource source = new KafkaDataSource();
        System.out.println(source.getKafkaConsumerOffset());
        System.out.println(source.getKafkaProducerServers());
        System.out.println(source.getKafkaZookeeperConnect());
    }

}

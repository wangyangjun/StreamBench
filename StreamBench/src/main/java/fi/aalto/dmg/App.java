package fi.aalto.dmg;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        System.out.println( "Hello World!" );
        ClassLoader loader = App.class.getClassLoader();
        Class workerClass = loader.loadClass("fi.aalto.dmg.Workload");
        Workload workload = (Workload)workerClass.newInstance();
        workload.Strart();
    }
}

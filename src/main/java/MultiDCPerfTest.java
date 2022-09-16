import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.github.javafaker.Faker;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class MultiDCPerfTest {
    public static void main(String[] args) {
        final Faker faker = new Faker();
        File dc1Config = new File("/Users/sjacob/projects/fedex/cam_perf_multidc/src/main/resources/dc1.conf");
        CqlSession dc1Session = CqlSession.builder()
                .withConfigLoader(DriverConfigLoader.fromFile(dc1Config))
                .build();
        dc1Session.execute("TRUNCATE dcperf.users");
        dc1Session.close();
//        File dc2Config = new File("/Users/sjacob/projects/fedex/cam_perf_multidc/src/main/resources/dc2.conf");
//        CqlSession dc2Session = CqlSession.builder()
//                .withConfigLoader(DriverConfigLoader.fromFile(dc2Config))
//                .build();
        final int threadCount = 30;
        final List<Thread> threadList = new ArrayList<>();
        final CountDownLatch latch = new CountDownLatch(threadCount);  //set latch to number of threads, so that all start at same time
        for (int i = 0; i < threadCount; i++) {
            final CqlSession dc1CqlSession = CqlSession.builder()
                    .withConfigLoader(DriverConfigLoader.fromFile(dc1Config))
                    .build();
            Thread dataWriter = new Thread(new DataWriter("DataWriter", dc1CqlSession, latch));
            threadList.add(dataWriter);
        }
        //Thread dataReader = new Thread(new DataReader("DataReader", dc2Session, latch));
        for (Thread dataWriter : threadList) {
            dataWriter.start();
        }

        try {
            latch.await();  //waiting on CountDownLatch to finish
            System.out.println("All services are up, Application is starting now");
        } catch (InterruptedException ie) {
            ie.printStackTrace();
        }

    }
}

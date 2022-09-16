import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.github.javafaker.Faker;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * using ExecutorService to manage threads
 */
public class MultiDCPerfExecutorTest {
    public static void main(String[] args) {
        final Faker faker = new Faker();
        File dc1Config = new File("/Users/sjacob/projects/fedex/cam_perf_multidc/src/main/resources/dc1.conf");
        final CqlSession dc1Session = CqlSession.builder()
                .withConfigLoader(DriverConfigLoader.fromFile(dc1Config))
                .build();
        dc1Session.execute("TRUNCATE dcperf.users");


        final int threadCount = 65;
        final CountDownLatch startLatch = new CountDownLatch(threadCount);
        final CountDownLatch endLatch = new CountDownLatch(threadCount);
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            executorService.execute(new Thread(new DataWriterService("DataWriter", dc1Session, startLatch, endLatch)));
        }

        try {
            //waiting on CountDownLatch to finish
            System.out.println("All services are up, Application is starting now");
            startLatch.await();
            executorService.shutdown();
            //waiting for all tasks to finish so that we can close cqlSession
            endLatch.await();
            System.out.println("Waiting for tasks to finish..");

        } catch (Exception ie) {
            ie.printStackTrace();
        } finally {
            dc1Session.close();
        }

    }
}

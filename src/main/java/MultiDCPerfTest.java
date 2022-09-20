import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;

import java.io.File;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;

/**
 * using ExecutorService to manage threads
 */
public class MultiDCPerfTest {
    public static void main(String[] args) {

        final File dc1Config = new File("/Users/sjacob/projects/fedex/cam_perf_multidc/src/main/resources/dc1.conf");
        final CqlSession dc1CqlSession = CqlSession.builder()
                .withConfigLoader(DriverConfigLoader.fromFile(dc1Config))
                .build();
        final File dc2Config = new File("/Users/sjacob/projects/fedex/cam_perf_multidc/src/main/resources/dc2.conf");
        final CqlSession dc2CqlSession = CqlSession.builder()
                .withConfigLoader(DriverConfigLoader.fromFile(dc2Config))
                .build();
        //truncate table
        dc1CqlSession.execute("TRUNCATE dcperf.users");
        List<UUID> uuidList = new LinkedList<>();
        final int threadCount = 10;
        final LinkedBlockingDeque<Long> perfTimeQueue = new LinkedBlockingDeque<>(threadCount); //bounded Q
        final CountDownLatch startLatch = new CountDownLatch(threadCount * 2);
        final CountDownLatch endLatch = new CountDownLatch(threadCount * 2);
        final ExecutorService executorService = Executors.newFixedThreadPool(threadCount * 2);
        final String keyspace = "dcperf";
        final PreparedStatement writePreparedStatement = createWritePreparedStatement(keyspace, dc1CqlSession); //cql ps is threadsafe
        final PreparedStatement readPreparedStatement = createReadPreparedStatement(keyspace, dc2CqlSession);
        final Instant startTime = Instant.now();
        //write threads
        for (int i = 0; i < threadCount; i++) {
            final UUID uuid = Uuids.random();
            uuidList.add(uuid);
            executorService.execute(new Thread(new DataWriterService("DataWriter",
                    dc1CqlSession,
                    writePreparedStatement,
                    uuid,
                    startLatch,
                    endLatch)));
        }
        //read threads
        for (int i = 0; i < threadCount; i++) {
            executorService.execute(new Thread(new DataReaderService("DataReader",
                    dc2CqlSession,
                    readPreparedStatement,
                    uuidList.get(i),
                    startLatch,
                    endLatch, perfTimeQueue)));
        }

        try {
            //waiting on CountDownLatch to finish
            System.out.println("All services are up, Application is starting now");
            startLatch.await();
            executorService.shutdown();
            //waiting for all tasks to finish so that we can close cqlSession
            endLatch.await();
            System.out.println("Waiting for tasks to finish..");
            Long totalReadWriteTime = perfTimeQueue.stream().reduce(0L, Long::sum);
            System.out.printf("Average per thread write-read latency is %sms, for %s threads\n", (totalReadWriteTime / threadCount), threadCount);
        } catch (
                Exception ie) {
            ie.printStackTrace();
        } finally {
            if (executorService.isShutdown()) {
                dc1CqlSession.close();
                dc2CqlSession.close();
            }
        }

    }

    private static PreparedStatement createWritePreparedStatement(String keyspace, CqlSession session) {
        final Insert userInsert = insertInto(keyspace, "users")
                .value("userid", QueryBuilder.bindMarker())
                .value("firstname", QueryBuilder.bindMarker())
                .value("lastname", QueryBuilder.bindMarker())
                .value("email", QueryBuilder.bindMarker());
        SimpleStatement insertStatement = userInsert.build();
        return session.prepare(insertStatement);
    }

    private static PreparedStatement createReadPreparedStatement(String keyspace, CqlSession session) {
        SimpleStatement simpleStatement =
                SimpleStatement.builder("SELECT userid FROM users WHERE userid=?").setKeyspace(keyspace).build();
        return session.prepare(simpleStatement);
    }
}

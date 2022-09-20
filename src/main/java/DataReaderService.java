import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.github.javafaker.Faker;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;


public class DataReaderService extends Thread {
    private final CountDownLatch startLatch;
    private final CountDownLatch endLatch;
    private final String name;
    private final CqlSession cqlSession;
    private final PreparedStatement preparedStatement;
    private UUID uuid;
    private final Faker faker = new Faker();
    private Queue<Long> perfQueue;

    public DataReaderService(final String name,
                             final CqlSession cqlSession,
                             final PreparedStatement preparedStatement,
                             final UUID uuid,
                             final CountDownLatch startLatch,
                             final CountDownLatch endLatch,
                             final Queue<Long> perfQueue) {
        this.name = name;
        this.cqlSession = cqlSession;
        this.preparedStatement = preparedStatement;
        this.uuid = uuid;
        this.startLatch = startLatch;
        this.endLatch = endLatch;
        this.perfQueue = perfQueue;

    }

    @Override
    public void run() {
//TODO check if data is read by checking if execute returns a result set
        try {
            System.out.printf("[ %s ] created reader, blocked by the latch...\n", getName());
            startLatch.countDown();
            startLatch.await();
            final Instant threadStartTime = Instant.now();
            // System.out.printf("[ %s ] starts at: %s\n", getName(), threadStartTime);

            final BoundStatement statement = preparedStatement.bind()
                    .setUuid(0, uuid);
            cqlSession.execute(statement);
            final Instant threadEndTime = Instant.now();
            //ReadWritePerf readWritePerf = new ReadWritePerf();
            // readWritePerf.setInstanceName(getName());
            // readWritePerf.setRunTime(ChronoUnit.MILLIS.between(threadStartTime, threadEndTime));
            perfQueue.add(ChronoUnit.MILLIS.between(threadStartTime, threadEndTime));
            System.out.printf("[ %s ] writetime: %s, readtime:  %s, diff:%sms\n", getName(), threadStartTime, Instant.now(), ChronoUnit.MILLIS.between(threadStartTime, threadEndTime));
            endLatch.countDown();

        } catch (InterruptedException e) {
            // handle exception
        }
    }


}

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
    private final UUID uuid;
    private final Faker faker = new Faker();
    final private Queue<Long> perfQueue;

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
        try {
            startLatch.countDown();
            final BoundStatement statement = preparedStatement.bind().setUuid(0, uuid);
            startLatch.await();
            final Instant threadStartTime = Instant.now();
            while (cqlSession.execute(statement).one() == null) {
                //Thread.sleep(25);
                //System.out.println(uuid);
            }
            ;//loop until a result is found
            final Instant threadEndTime = Instant.now();
            final Long diffTime = ChronoUnit.MILLIS.between(threadStartTime, threadEndTime);
            perfQueue.add(diffTime);
            //System.out.printf("[ %s ] writetime: %s, readtime:  %s, diff:%sms\n", getName(), threadStartTime, threadEndTime, diffTime);
            endLatch.countDown();


        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}

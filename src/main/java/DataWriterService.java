import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.github.javafaker.Faker;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;


public class DataWriterService extends Thread {
    private final CountDownLatch startLatch;
    private final CountDownLatch endLatch;
    private final String name;
    private final CqlSession cqlSession;
    private final PreparedStatement preparedStatement;
    private UUID uuid;
    private final Faker faker = new Faker();

    public DataWriterService(final String name,
                             final CqlSession cqlSession,
                             final PreparedStatement preparedStatement,
                             final UUID uuid,
                             final CountDownLatch startLatch,
                             final CountDownLatch endLatch) {
        this.name = name;
        this.cqlSession = cqlSession;
        this.preparedStatement = preparedStatement;
        this.uuid = uuid;
        this.startLatch = startLatch;
        this.endLatch = endLatch;

    }

    @Override
    public void run() {

        try {
            startLatch.countDown();
            final BoundStatement statement = bindStatement(preparedStatement);
            startLatch.await();
            final Instant threadStartTime = Instant.now();
            cqlSession.execute(statement);
            endLatch.countDown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private BoundStatement bindStatement(final PreparedStatement ps) {
        return preparedStatement.bind()
                .setUuid(0, uuid)
                .setString(1, faker.name().firstName())
                .setString(2, faker.name().lastName())
                .setString(3, faker.internet().emailAddress());

    }
}

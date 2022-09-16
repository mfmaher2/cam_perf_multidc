import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.github.javafaker.Faker;

import java.time.Instant;
import java.util.concurrent.CountDownLatch;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;

public class DataWriterService extends Thread {
    private final CountDownLatch startLatch;
    private final CountDownLatch endLatch;
    private final String name;
    private CqlSession cqlSession;
    final Faker faker = new Faker();

    public DataWriterService(String name, CqlSession cqlSession, CountDownLatch startLatch, CountDownLatch endLatch) {
        this.name = name;
        this.cqlSession = cqlSession;
        this.startLatch = startLatch;
        this.endLatch = endLatch;

    }

    @Override
    public void run() {

        try {
            System.out.printf("[ %s ] created, blocked by the latch...\n", getName());
            startLatch.countDown();
            startLatch.await();
            System.out.printf("[ %s ] starts at: %s\n", getName(), Instant.now());
            Insert userInsert = insertInto("dcperf", "users")
                    .value("userid", literal(Uuids.random()))
                    .value("firstname", literal(faker.name().firstName()))
                    .value("lastname", literal(faker.name().lastName()))
                    .value("email", literal(faker.internet().emailAddress()));
            cqlSession.execute(userInsert.toString());
            endLatch.countDown();

        } catch (InterruptedException e) {
            // handle exception
        }
    }


}

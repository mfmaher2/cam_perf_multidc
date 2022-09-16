import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.github.javafaker.Faker;

import java.time.Instant;
import java.util.concurrent.CountDownLatch;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;

public class DataWriter extends Thread {
    private final CountDownLatch latch;
    private final String name;
    private CqlSession cqlSession;
    final Faker faker = new Faker();

    public DataWriter(String name, CqlSession cqlSession, CountDownLatch latch) {
        this.name = name;
        this.cqlSession = cqlSession;
        this.latch = latch;

    }

    @Override
    public void run() {

        try {
            System.out.printf("[ %s ] created, blocked by the latch...\n", getName());
            latch.countDown();
            latch.await();
            System.out.printf("[ %s ] starts at: %s\n", getName(), Instant.now());
            Insert userInsert = insertInto("dcperf", "users")
                    .value("userid", literal(Uuids.random()))
                    .value("firstname", literal(faker.name().firstName()))
                    .value("lastname", literal(faker.name().lastName()))
                    .value("email", literal(faker.internet().emailAddress()));
            cqlSession.execute(userInsert.toString());
            // do actual work here...
        } catch (InterruptedException e) {
            // handle exception
        } finally {
            cqlSession.close();
        }
    }


}

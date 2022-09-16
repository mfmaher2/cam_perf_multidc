import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import java.time.Instant;
import java.util.concurrent.CountDownLatch;

public class DataReader extends Thread {
    private final CountDownLatch latch;
    private final String name;
    private CqlSession cqlSession;

    public DataReader(String name, CqlSession cqlSession, CountDownLatch latch) {
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

            //wait till data is available
            //and count should match total inserts
            Long recCount = 0L;
            do {
                ResultSet rs = cqlSession.execute("select count(*) from dcperf.users");

                Row row = rs.one();
                recCount = row.getLong(0);

            } while (recCount <= 0);
            System.out.println("reccount: " + recCount);
        } catch (InterruptedException e) {
            // handle exception
        }
    }


}

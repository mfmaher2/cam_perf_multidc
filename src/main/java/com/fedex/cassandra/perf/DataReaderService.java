package com.fedex.cassandra.perf;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;


public class DataReaderService extends Thread {
    private final CountDownLatch startLatch;
    private final CountDownLatch endLatch;
    private final CqlSession cqlSession;
    private final PreparedStatement preparedStatement;
    private final UUID uuid;
    private final Queue<Long> perfTimeQueue;

    public DataReaderService(final CqlSession cqlSession,
                             final PreparedStatement preparedStatement,
                             final UUID uuid,
                             final CountDownLatch startLatch,
                             final CountDownLatch endLatch,
                             final Queue<Long> perfTimeQueue) {

        this.cqlSession = cqlSession;
        this.preparedStatement = preparedStatement;
        this.uuid = uuid;
        this.startLatch = startLatch;
        this.endLatch = endLatch;
        this.perfTimeQueue = perfTimeQueue;

    }

    @Override
    public void run() {
        try {
            startLatch.countDown();
            final BoundStatement statement = preparedStatement.bind().setUuid(0, uuid);
            startLatch.await();
            final Instant threadStartTime = Instant.now();
            //loop until a result is found
            while (cqlSession.execute(statement).one() == null) {
                Thread.sleep(1);
                //System.out.println(uuid);
            }
            final Instant threadEndTime = Instant.now();
            final Long diffTime = ChronoUnit.MILLIS.between(threadStartTime, threadEndTime);
            perfTimeQueue.add(diffTime);
            //System.out.printf("[ %s ] writetime: %s, readtime:  %s, diff:%sms\n", getName(), threadStartTime, threadEndTime, diffTime);
            endLatch.countDown();


        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}

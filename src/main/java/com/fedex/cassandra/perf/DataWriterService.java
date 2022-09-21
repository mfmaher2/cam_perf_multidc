package com.fedex.cassandra.perf;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.github.javafaker.Faker;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;


public class DataWriterService extends Thread {
    private final CountDownLatch startLatch;
    private final CountDownLatch endLatch;
    private final CqlSession cqlSession;
    private final PreparedStatement preparedStatement;
    private final UUID uuid;
    private static final Faker faker = new Faker();

    public DataWriterService(final CqlSession cqlSession,
                             final PreparedStatement preparedStatement,
                             final UUID uuid,
                             final CountDownLatch startLatch,
                             final CountDownLatch endLatch) {
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
            cqlSession.execute(statement); //write
            endLatch.countDown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    final BoundStatement bindStatement(final PreparedStatement ps) {
        return ps.bind()
                .setUuid(0, uuid)
                .setString(1, faker.name().firstName())
                .setString(2, faker.name().lastName())
                .setString(3, faker.internet().emailAddress());

    }
}

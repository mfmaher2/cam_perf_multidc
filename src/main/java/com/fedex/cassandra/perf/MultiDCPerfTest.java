package com.fedex.cassandra.perf;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import org.apache.commons.cli.*;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;

/**
 * Multi data center performance tester
 * Inserts data into DC1 and reads the same records from DC2 to test the latency of replication.
 * <p>
 * using ExecutorService to manage threads
 */
public class MultiDCPerfTest {
    public static void main(String[] args) {

        File dc1Config = null;
        File dc2Config = null;
        int threadCount = 30;

        //parse CLI arguments
        try {
            if (args.length > 0) {
                CommandLine cmd;
                final Options options = setCliArguments();
                CommandLineParser parser = new DefaultParser();
                cmd = parser.parse(options, args);
                dc1Config = new File(cmd.getOptionValue("dc1"));
                dc2Config = new File(cmd.getOptionValue("dc2"));
                threadCount = Integer.parseInt(cmd.getOptionValue("i"));
            } else { //default
                dc1Config = getFileFromResource("dc1.conf");
                dc2Config = getFileFromResource("dc2.conf");
            }

        } catch (ParseException | URISyntaxException e) {
            e.printStackTrace();
        }


        //dc1 session
        final CqlSession dc1CqlSession = CqlSession.builder()
                .withConfigLoader(DriverConfigLoader.fromFile(dc1Config))
                .build();
        //dc2 session
        final CqlSession dc2CqlSession = CqlSession.builder()
                .withConfigLoader(DriverConfigLoader.fromFile(dc2Config))
                .build();

        //truncate table
        final String keyspace = "dcperf";
        final String table = "user";
        final String truncateCql = "TRUNCATE " + keyspace + "." + table;
        dc1CqlSession.execute(truncateCql);

        //start write, read threads
        final List<UUID> uuidList = new LinkedList<>(); //uuid list for read query
        final LinkedBlockingQueue<Long> perfTimeQueue = new LinkedBlockingQueue<>(threadCount); //bounded Q
        final CountDownLatch startLatch = new CountDownLatch(threadCount * 2); //latch for thread start
        final CountDownLatch endLatch = new CountDownLatch(threadCount * 2);   //thread end latch to wait for results and closing cql sessions
        final ExecutorService executorService = Executors.newFixedThreadPool(threadCount * 2);
        final PreparedStatement writePs = createWritePreparedStatement(keyspace, dc1CqlSession); //cql ps is threadsafe
        final PreparedStatement readPs = createReadPreparedStatement(keyspace, dc2CqlSession);
        System.out.println("Init write threads");
        //write threads
        for (int i = 0; i < threadCount; i++) {
            final UUID uuid = Uuids.random();
            uuidList.add(uuid);
            executorService.execute(new Thread(new DataWriterService(dc1CqlSession,
                    writePs,
                    uuid,
                    startLatch,
                    endLatch)));
        }
        System.out.println("Init read threads");
        //read threads
        for (int i = 0; i < threadCount; i++) {
            executorService.execute(new Thread(new DataReaderService(dc2CqlSession,
                    readPs,
                    uuidList.get(i),
                    startLatch,
                    endLatch, perfTimeQueue)));
        }

        try {
            //waiting on startLatch to finish countdown
            System.out.println("All service threads are up and running ..");
            startLatch.await();
            executorService.shutdown();
            //waiting for endLatch to finish countdown, so that we can close cqlSessions
            System.out.println("Waiting for tasks to finish..");
            endLatch.await();
            System.out.println("perfTimes in ms:" + perfTimeQueue);
            Long totalReadWriteTime = perfTimeQueue.stream().reduce(0L, Long::sum);
            System.out.printf("Average write-read latency is %sms, for %s threads\n", (totalReadWriteTime / threadCount), threadCount);
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

    private static PreparedStatement createWritePreparedStatement(final String keyspace, final CqlSession session) {
        final Insert userInsert = insertInto(keyspace, "user")
                .value("userid", QueryBuilder.bindMarker())
                .value("firstname", QueryBuilder.bindMarker())
                .value("lastname", QueryBuilder.bindMarker())
                .value("email", QueryBuilder.bindMarker());
        final PreparedStatement prepare = session.prepare(userInsert.build());
        return prepare;
    }

    private static PreparedStatement createReadPreparedStatement(final String keyspace, final CqlSession session) {
        final SimpleStatement simpleStatement =
                SimpleStatement.builder("SELECT userid FROM user WHERE userid=?").setKeyspace(keyspace).build();
        return session.prepare(simpleStatement);
    }


    private static File getFileFromResource(String fileName) throws URISyntaxException {

        ClassLoader classLoader = MultiDCPerfTest.class.getClassLoader();
        URL resource = classLoader.getResource(fileName);
        if (resource == null) {
            throw new IllegalArgumentException("file not found! " + fileName);
        } else {
            return new File(resource.toURI());
        }

    }

    private static Options setCliArguments() {
        Options options = new Options();
        Option dc1 = new Option("dc1", "dc1config", true, "dc1 config file path");
        dc1.setRequired(true);
        options.addOption(dc1);

        Option dc2 = new Option("dc2", "dc2config", true, "dc2 config file path");
        dc2.setRequired(true);
        options.addOption(dc2);

        Option instances = new Option("i", "instances", true, "read write thread count");
        instances.setRequired(true);
        options.addOption(instances);

        return options;
    }


}

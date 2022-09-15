import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.github.javafaker.Faker;

import java.io.File;
import java.net.InetSocketAddress;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;

import com.datastax.oss.driver.api.core.uuid.Uuids;

public class CassandraConfigFileTest {
    public static void main(String[] args) {
        final Faker faker = new Faker();
        File file = new File("/Users/sjacob/projects/fedex/ds_dc_perf_analysis/src/main/resources/dc1.conf");
        CqlSession session = CqlSession.builder()
                .withConfigLoader(DriverConfigLoader.fromFile(file))
                .build();
      //insert
       Insert userInsert =  insertInto("dcperf","users")
                .value("userid", literal(Uuids.random()))
                .value("firstname", literal(faker.name().firstName()))
                .value("lastname", literal(faker.name().lastName()))
                .value("email", literal(faker.internet().emailAddress()));
        session.execute(userInsert.toString());

        //read
        ResultSet rs = session.execute("select *  from dcperf.users");
        Row row = rs.one();
        System.out.println(row.getString("firstname"));
        session.close();

    }
}

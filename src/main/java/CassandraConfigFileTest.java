import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.github.javafaker.Faker;

import java.io.File;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;

public class CassandraConfigFileTest {
    public static void main(String[] args) {
        final Faker faker = new Faker();
        File dc1Config = new File("/Users/sjacob/projects/fedex/cam_perf_multidc/src/main/resources/dc1.conf");
        CqlSession dc1Session = CqlSession.builder()
                .withConfigLoader(DriverConfigLoader.fromFile(dc1Config))
                .build();

        File dc2Config = new File("/Users/sjacob/projects/fedex/cam_perf_multidc/src/main/resources/dc2.conf");
        CqlSession dc2Session = CqlSession.builder()
                .withConfigLoader(DriverConfigLoader.fromFile(dc2Config))
                .build();

        //insert
        Insert userInsert = insertInto("dcperf", "users")
                .value("userid", literal(Uuids.random()))
                .value("firstname", literal(faker.name().firstName()))
                .value("lastname", literal(faker.name().lastName()))
                .value("email", literal(faker.internet().emailAddress()));
        dc1Session.execute(userInsert.toString());

        //read
        ResultSet rs = dc2Session.execute("select *  from dcperf.users");
        Row row = rs.one();
        System.out.println(row.getString("firstname"));
        dc1Session.close();
        dc2Session.close();

    }
}

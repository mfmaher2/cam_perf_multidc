import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

public class CassandraConfigTest {
    public static void main(String[] args) {
        try (CqlSession session = CqlSession.builder().build()) {

            ResultSet rs = session.execute("select release_version from system.local");
            Row row = rs.one();
            assert row != null;
            String releaseVersion = row.getString("release_version");
            System.out.printf("Cassandra version is: %s%n", releaseVersion);
        }
    }
}

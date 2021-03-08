package io.sean.test.kafka;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringRunner;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;

@RunWith(SpringRunner.class)
public class TestJdbc {
    @Before
    public void setUp() throws Exception {
       Class.forName("com.mysql.cj.jdbc.Driver");
    }
    @Test
    public void batch() throws SQLException {
        try(Connection connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:13306/demo","root","123456");) {
            connection.setAutoCommit(false);
            PreparedStatement stat = connection.prepareStatement("insert into demo(`id`,`name`,`areacode`) values (?,?,?)");
            for (int i = 2; i < 10; i++) {
                stat.setInt(1, i);
                stat.setString(2, "zxy");
                stat.setString(3, "aaa");
                stat.addBatch();
            }
            stat.setInt(1, 13);
            stat.setString(2, "zxy");
            stat.setString(3, "aaa");
            stat.addBatch();
            int[] arr = stat.executeBatch();
            connection.commit();

            Arrays.stream(arr).forEach((r) -> {
                System.out.println("rrr---" + r);
            });

        }catch(SQLException e){
            e.printStackTrace();
        }
    }
}

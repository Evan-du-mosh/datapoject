import java.io.FileReader;
import java.sql.*;
import com.opencsv.CSVReader;

public class DatabaseManipulation implements DataManipulation {
    private Connection con = null;
    private ResultSet resultSet;

    private String host = "localhost";
    private String dbname = "postgres";
    private String user = "postgres";
    private String pwd = "060921";
    private String port = "5432";

    private void getConnection() {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (Exception e) {
            System.err.println("Cannot find the PostgreSQL driver. Check CLASSPATH.");
            System.exit(1);
        }

        try {
            String url = "jdbc:postgresql://" + host + ":" + port + "/" + dbname;
            con = DriverManager.getConnection(url, user, pwd);
            con.setAutoCommit(false); // ✅ 批量导入时手动提交
        } catch (SQLException e) {
            System.err.println("Database connection failed");
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    private void closeConnection() {
        if (con != null) {
            try {
                con.close();
                con = null;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void createUserTable() {
        String createTableSQL = """
            CREATE TABLE IF NOT EXISTS users (
                AuthorId INT PRIMARY KEY,
                AuthorName TEXT,
                Gender VARCHAR(10),
                Age INT,
                Followers INT,
                Following INT,
                FollowerUsers TEXT,
                FollowingUsers TEXT
            );
            """;
        try (Statement stmt = con.createStatement()) {
            stmt.execute(createTableSQL);
            System.out.println("✅ user 表已创建或已存在。");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // ✅ 使用 OpenCSV 安全解析包含逗号的字段
    public void insertUserFromCSV(String csvPath) {
        getConnection();
        createUserTable();

        String insertSQL = """
            INSERT INTO users (AuthorId, AuthorName, Gender, Age, Followers, Following, FollowerUsers, FollowingUsers)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (AuthorId) DO UPDATE SET
                AuthorName = EXCLUDED.AuthorName,
                Gender = EXCLUDED.Gender,
                Age = EXCLUDED.Age,
                Followers = EXCLUDED.Followers,
                Following = EXCLUDED.Following,
                FollowerUsers = EXCLUDED.FollowerUsers,
                FollowingUsers = EXCLUDED.FollowingUsers;
            """;

        try (CSVReader reader = new CSVReader(new FileReader(csvPath));
             PreparedStatement ps = con.prepareStatement(insertSQL)) {

            String[] data;
            reader.readNext(); // 跳过表头
            int count = 0;

            while ((data = reader.readNext()) != null) {
                if (data.length < 8) continue; // 跳过不完整行

                ps.setInt(1, Integer.parseInt(data[0].trim()));
                ps.setString(2, data[1].trim());
                ps.setString(3, data[2].trim());
                ps.setInt(4, Integer.parseInt(data[3].trim()));
                ps.setInt(5, Integer.parseInt(data[4].trim()));
                ps.setInt(6, Integer.parseInt(data[5].trim()));
                ps.setString(7, data[6]);
                ps.setString(8, data[7]);
                ps.addBatch();

                count++;
                if (count % 1000 == 0) { // ✅ 每1000条执行一次批量提交，防止内存溢出
                    ps.executeBatch();
                    con.commit();
                }
            }

            ps.executeBatch();
            con.commit();
            System.out.println("✅ CSV 用户数据导入完成，总行数：" + count);

        } catch (Exception e) {
            e.printStackTrace();
            try {
                con.rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        } finally {
            closeConnection();
        }
    }

    // main 测试
    public static void main(String[] args) {
        DatabaseManipulation db = new DatabaseManipulation();
        db.insertUserFromCSV("E:/学习资料/数据库原理/data/final_data/user.csv");
    }

    @Override
    public int addOneMovie(String str) {
        return 0;
    }

    @Override
    public String allContinentNames() {
        return "";
    }

    @Override
    public String continentsWithCountryCount() {
        return "";
    }

    @Override
    public String FullInformationOfMoviesRuntime(int min, int max) {
        return "";
    }

    @Override
    public String findMovieById(int id) {
        return "";
    }
}

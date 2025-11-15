import java.sql.*;
import java.time.Duration;
import java.time.LocalDateTime;
import java.nio.file.Files;
import java.nio.file.Path;

public class JdbcCsvImport {
    public static void main(String[] args) {
        String usersCsv = "E:/ASHSTUDY/data/user.csv";
        String reviewsCsv = "E:/ASHSTUDY/data/reviews.csv";
        String recipesCsv = "E:/ASHSTUDY/data/recipes.csv";

        String url = "jdbc:postgresql://localhost:5432/postgres";
        String user = "postgres";
        String pwd = "060921";

        ImportResult result = new ImportResult();
        result.startTime = LocalDateTime.now();

        try {
            try {
                Class.forName("org.postgresql.Driver");
            } catch (ClassNotFoundException e) {
                result.errorMessage = "PostgreSQL JDBC driver not found: " + e.getMessage();
                System.err.println(result.errorMessage);
                System.out.println(result);
                return;
            }

            if (!Files.exists(Path.of(usersCsv))) {
                System.err.println("用户 CSV 不存在: " + usersCsv);
            }
            if (!Files.exists(Path.of(reviewsCsv))) {
                System.err.println("评论 CSV 不存在: " + reviewsCsv);
            }
            if (!Files.exists(Path.of(recipesCsv))) {
                System.err.println("菜谱 CSV 不存在: " + recipesCsv);
            }

            try (Connection con = DriverManager.getConnection(url, user, pwd);
                 Statement st = con.createStatement()) {

                con.setAutoCommit(false);

                st.execute("CREATE TEMP TABLE IF NOT EXISTS import_timing AS SELECT clock_timestamp() AS start_time");
                st.execute("TRUNCATE import_timing");
                st.execute("INSERT INTO import_timing SELECT clock_timestamp()");

                safeExecute(st, "DROP TABLE IF EXISTS staging_users");
                safeExecute(st, "CREATE TABLE staging_users (AuthorId INT, AuthorName VARCHAR(255), Gender VARCHAR(10), Age INT, Followers INT, Following INT, FollowerUsers TEXT, FollowingUsers TEXT)");
                copyFromCsv(st, "staging_users", usersCsv);

                safeExecute(st, "DROP TABLE IF EXISTS staging_reviews");
                safeExecute(st, "CREATE TABLE staging_reviews (ReviewId INT, RecipeId DECIMAL(10,1), AuthorId INT, AuthorName TEXT, Rating INT, Review TEXT, DateSubmitted TIMESTAMPTZ, DateModified TIMESTAMPTZ, Likes TEXT)");
                copyFromCsv(st, "staging_reviews", reviewsCsv);

                safeExecute(st, "DROP TABLE IF EXISTS staging_recipes");
                safeExecute(st, "CREATE TABLE staging_recipes (RecipeId INT, Name TEXT, AuthorId INT, AuthorName TEXT, CookTime INTERVAL, PrepTime INTERVAL, TotalTime INTERVAL, DatePublished TIMESTAMPTZ, Description TEXT, RecipeCategory TEXT, Keywords TEXT, RecipeIngredientParts TEXT, AggregatedRating DECIMAL(3,1), ReviewCount DECIMAL(5,1), Calories DECIMAL(8,1), FatContent DECIMAL(8,1), SaturatedFatContent DECIMAL(8,1), CholesterolContent DECIMAL(8,1), SodiumContent DECIMAL(8,1), CarbohydrateContent DECIMAL(8,1), FiberContent DECIMAL(8,1), SugarContent DECIMAL(8,1), ProteinContent DECIMAL(8,1), RecipeServings DECIMAL(10,1), RecipeYield TEXT, RecipeInstructions TEXT, FavoriteUsers TEXT)");
                copyFromCsv(st, "staging_recipes", recipesCsv);

                // 创建最终表
                safeExecute(st, "DROP TABLE IF EXISTS Users_rf");
                safeExecute(st, "CREATE TABLE Users_rf (\"AuthorID\" INT PRIMARY KEY, \"Name\" VARCHAR(255), Age INT, Gender VARCHAR(10), \"Following Count\" INT, \"Follower Count\" INT)");
                safeExecute(st, "INSERT INTO Users_rf SELECT AuthorId, AuthorName, Age, Gender, Following, Followers FROM staging_users");

                safeExecute(st, "DROP TABLE IF EXISTS Reviews_rf");
                safeExecute(st, "CREATE TABLE Reviews_rf (\"ReviewID\" INT PRIMARY KEY, Rating INT, \"Review Content\" TEXT, \"Date Submitted\" TIMESTAMPTZ, \"Date Modified\" TIMESTAMPTZ)");
                safeExecute(st, "INSERT INTO Reviews_rf SELECT ReviewId, Rating, Review, DateSubmitted, DateModified FROM staging_reviews");

                safeExecute(st, "DROP TABLE IF EXISTS Recipes_rf");
                safeExecute(st, "CREATE TABLE Recipes_rf (\"RecipeID\" INT PRIMARY KEY, RecipeName TEXT, \"Date Published\" TIMESTAMPTZ, Category TEXT, Description TEXT, \"Prep Time\" INTERVAL, \"Cook Time\" INTERVAL, \"Total Time\" INTERVAL, \"Recipe Yield\" TEXT, \"Recipe Ingredient Parts\" TEXT, \"Recipe Servings\" DECIMAL(10,1), \"Recipe Instructions\" TEXT)");
                safeExecute(st, "INSERT INTO Recipes_rf SELECT RecipeId, Name, DatePublished, RecipeCategory, Description, PrepTime, CookTime, TotalTime, RecipeYield, RecipeIngredientParts, RecipeServings, RecipeInstructions FROM staging_recipes");

                // Likes
                safeExecute(st, "DROP TABLE IF EXISTS Likes");
                safeExecute(st, "CREATE TABLE Likes (ReviewId INT, AuthorId INT, PRIMARY KEY (ReviewId, AuthorId), FOREIGN KEY (ReviewId) REFERENCES Reviews_rf(\"ReviewID\") ON DELETE CASCADE, FOREIGN KEY (AuthorId) REFERENCES Users_rf(\"AuthorID\") ON DELETE CASCADE)");
                safeExecute(st, "INSERT INTO Likes (ReviewId, AuthorId) SELECT r.ReviewId, CAST(trim(both '\"' FROM v) AS INT) FROM staging_reviews r, unnest(string_to_array(r.Likes, ',')) AS v WHERE trim(both '\"' FROM v) ~ '^[0-9]+$'");

                // Favorites
                safeExecute(st, "DROP TABLE IF EXISTS Favorites");
                safeExecute(st, "CREATE TABLE Favorites (RecipeId INT, AuthorId INT, PRIMARY KEY (RecipeId, AuthorId), FOREIGN KEY (RecipeId) REFERENCES Recipes_rf(\"RecipeID\") ON DELETE CASCADE, FOREIGN KEY (AuthorId) REFERENCES Users_rf(\"AuthorID\") ON DELETE CASCADE)");
                safeExecute(st, "INSERT INTO Favorites (RecipeId, AuthorId) SELECT r.RecipeId, CAST(trim(both '\"' FROM v) AS INT) FROM staging_recipes r, unnest(string_to_array(r.FavoriteUsers, ',')) AS v WHERE trim(both '\"' FROM v) ~ '^[0-9]+$'");

                // Follow
                safeExecute(st, "DROP TABLE IF EXISTS Follow");
                safeExecute(st, "CREATE TABLE Follow (FollowerID INT, FollowingID INT, PRIMARY KEY (FollowerID, FollowingID), FOREIGN KEY (FollowerID) REFERENCES Users_rf(\"AuthorID\") ON DELETE CASCADE, FOREIGN KEY (FollowingID) REFERENCES Users_rf(\"AuthorID\") ON DELETE CASCADE)");
                safeExecute(st, "INSERT INTO Follow (FollowerID, FollowingID) SELECT u.AuthorId, CAST(trim(both '\"' FROM v) AS INT) FROM staging_users u, unnest(string_to_array(u.FollowingUsers, ',')) AS v WHERE trim(both '\"' FROM v) ~ '^[0-9]+$'");
                safeExecute(st, "INSERT INTO Follow (FollowerID, FollowingID) SELECT CAST(trim(both '\"' FROM v) AS INT), u.AuthorId FROM staging_users u, unnest(string_to_array(u.FollowerUsers, ',')) AS v WHERE trim(both '\"' FROM v) ~ '^[0-9]+$' ON CONFLICT DO NOTHING");

                // Nutrient
                safeExecute(st, "DROP TABLE IF EXISTS Nutrient");
                safeExecute(st, "CREATE TABLE Nutrient (RecipeID INT PRIMARY KEY, Calories DECIMAL(8,1), FatContent DECIMAL(8,1), SaturatedFatContent DECIMAL(8,1), CholesterolContent DECIMAL(8,1), SodiumContent DECIMAL(8,1), CarbohydrateContent DECIMAL(8,1), FiberContent DECIMAL(8,1), SugarContent DECIMAL(8,1), ProteinContent DECIMAL(8,1), FOREIGN KEY (RecipeID) REFERENCES Recipes_rf(\"RecipeID\") ON DELETE CASCADE)");
                safeExecute(st, "INSERT INTO Nutrient SELECT RecipeId, Calories, FatContent, SaturatedFatContent, CholesterolContent, SodiumContent, CarbohydrateContent, FiberContent, SugarContent, ProteinContent FROM staging_recipes");

                // Ingredient
                safeExecute(st, "DROP TABLE IF EXISTS Ingredient");
                safeExecute(st, "CREATE TABLE Ingredient (RecipeID INT, Ingredient TEXT, PRIMARY KEY (RecipeID, Ingredient), FOREIGN KEY (RecipeID) REFERENCES Recipes_rf(\"RecipeID\") ON DELETE CASCADE)");
                safeExecute(st, "INSERT INTO Ingredient SELECT RecipeId, RecipeIngredientParts FROM staging_recipes");

                // KeyWord
                safeExecute(st, "DROP TABLE IF EXISTS KeyWord");
                safeExecute(st, "CREATE TABLE KeyWord (RecipeID INT PRIMARY KEY, KeyWord TEXT, FOREIGN KEY (RecipeID) REFERENCES Recipes_rf(\"RecipeID\") ON DELETE CASCADE)");
                safeExecute(st, "INSERT INTO KeyWord SELECT RecipeId, Keywords FROM staging_recipes");

                // Instruction
                safeExecute(st, "DROP TABLE IF EXISTS Instruction");
                safeExecute(st, "CREATE TABLE Instruction (RecipeID INT, Step_Number INT, Instruction TEXT, PRIMARY KEY (RecipeID, Step_Number), FOREIGN KEY (RecipeID) REFERENCES Recipes_rf(\"RecipeID\") ON DELETE CASCADE)");
                safeExecute(st, "INSERT INTO Instruction SELECT r.RecipeId, row_number() OVER (PARTITION BY r.RecipeId ORDER BY v), trim(v) FROM staging_recipes r, unnest(string_to_array(r.RecipeInstructions, ',')) AS v WHERE r.RecipeInstructions IS NOT NULL AND r.RecipeInstructions <> ''");

                // 获取导入耗时
                try (ResultSet rs = st.executeQuery("SELECT start_time, clock_timestamp() as end_time, EXTRACT(EPOCH FROM (clock_timestamp() - start_time)) * 1000 as duration_ms FROM import_timing")) {
                    if (rs.next()) {
                        Timestamp tsStart = rs.getTimestamp("start_time");
                        Timestamp tsEnd = rs.getTimestamp("end_time");
                        long durationMs = rs.getLong("duration_ms");
                        if (tsStart != null) result.startTime = tsStart.toLocalDateTime();
                        if (tsEnd != null) result.endTime = tsEnd.toLocalDateTime();
                        result.durationMs = durationMs;
                    }
                }

                // 每个表格计数
                result.importedRows = countRows(st, "Users_rf") + countRows(st, "Reviews_rf") + countRows(st, "Recipes_rf");
                result.usersRows = countRows(st, "Users_rf");
                result.reviewsRows = countRows(st, "Reviews_rf");
                result.recipesRows = countRows(st, "Recipes_rf");
                result.likesRows = countRows(st, "Likes");
                result.favoritesRows = countRows(st, "Favorites");
                result.followRows = countRows(st, "Follow");
                result.nutrientRows = countRows(st, "Nutrient");
                result.ingredientRows = countRows(st, "Ingredient");
                result.keywordRows = countRows(st, "KeyWord");
                result.instructionRows = countRows(st, "Instruction");

                con.commit();
                result.success = true;

                safeExecute(st, "DROP TABLE IF EXISTS import_timing");
            }

        } catch (Exception e) {
            result.success = false;
            result.errorMessage = e.getMessage();
            System.err.println("导入过程中发生异常: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (result.endTime == null) result.endTime = LocalDateTime.now();
            if (result.durationMs == 0 && result.startTime != null && result.endTime != null) {
                result.duration = Duration.between(result.startTime, result.endTime);
            }
            System.out.println(result);
        }
    }

    private static void safeExecute(Statement st, String sql) {
        try {
            st.execute(sql);
        } catch (SQLException e) {
            System.err.println("执行 SQL 失败: " + sql);
            System.err.println("错误: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private static void copyFromCsv(Statement st, String tableName, String filePath) {
        if (filePath == null) {
            throw new IllegalArgumentException("filePath is null for COPY " + tableName);
        }
        String safePath = filePath.replace("'", "''");
        String copySql = "COPY " + tableName + " FROM '" + safePath + "' CSV HEADER";
        safeExecute(st, copySql);
    }

    private static int countRows(Statement st, String tableName) {
        String sql = "SELECT COUNT(*) FROM " + tableName;
        try (ResultSet rs = st.executeQuery(sql)) {
            if (rs.next()) return rs.getInt(1);
        } catch (SQLException e) {
            return 0;
        }
        return 0;
    }

    static class ImportResult {
        public boolean success = false;
        public LocalDateTime startTime;
        public LocalDateTime endTime;
        public Duration duration;
        public long durationMs = 0;
        public int importedRows = 0;

        // 每个表格行数
        public int usersRows = 0;
        public int reviewsRows = 0;
        public int recipesRows = 0;
        public int likesRows = 0;
        public int favoritesRows = 0;
        public int followRows = 0;
        public int nutrientRows = 0;
        public int ingredientRows = 0;
        public int keywordRows = 0;
        public int instructionRows = 0;

        public String errorMessage;

        @Override
        public String toString() {
            if (success) {
                long ms = durationMs > 0 ? durationMs : (duration != null ? duration.toMillis() : 0);
                return String.format("导入成功! 耗时: %d 毫秒\n" +
                                "Users_rf: %d, Reviews_rf: %d, Recipes_rf: %d\n" +
                                "Likes: %d, Favorites: %d, Follow: %d\n" +
                                "Nutrient: %d, Ingredient: %d, KeyWord: %d, Instruction: %d\n" +
                                "总导入记录（Users+Reviews+Recipes 合计）: %d",
                        ms, usersRows, reviewsRows, recipesRows, likesRows, favoritesRows, followRows,
                        nutrientRows, ingredientRows, keywordRows, instructionRows, importedRows);
            } else {
                return "导入失败: " + (errorMessage != null ? errorMessage : "未知错误");
            }
        }
    }
}

package io.sustc.service.impl;

import io.sustc.dto.ReviewRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.RecipeRecord;
import io.sustc.service.DatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * It's important to mark your implementation class with {@link Service} annotation.
 * As long as the class is annotated and implements the corresponding interface, you can place it under any package.
 */
@Service
@Slf4j
public class DatabaseServiceImpl implements DatabaseService {

    /**
     * Getting a {@link DataSource} instance from the framework, whose connections are managed by HikariCP.
     * <p>
     * Marking a field with {@link Autowired} annotation enables our framework to automatically
     * provide you a well-configured instance of {@link DataSource}.
     * Learn more: <a href="https://www.baeldung.com/spring-dependency-injection">Dependency Injection</a>
     */
    @Autowired
    private DataSource dataSource;

    @Autowired
    private JdbcTemplate jdbcTemplate;


    @Override
    public List<Integer> getGroupMembers() {
        //TODO: replace this with your own student IDs in your group
        return Arrays.asList(12410631, 12210909);
    }

    @Override
    @Transactional
    public void importData(
            List<ReviewRecord> reviewRecords,
            List<UserRecord> userRecords,
            List<RecipeRecord> recipeRecords) {
        try {
            // ddl to create tables.
            createTables();
            System.out.println("表创建逻辑执行完成"); // 打印日志

            // TODO: implement your import logic
            importUsers(userRecords);
            System.out.println("用户数据导入完成，数量：" + (userRecords == null ? 0 : userRecords.size()));
            importUserFollows(userRecords);

            importRecipes(recipeRecords);
            System.out.println("食谱数据导入完成，数量：" + (recipeRecords == null ? 0 : recipeRecords.size()));
            importRecipeIngredients(recipeRecords);

            importReviews(reviewRecords);
            System.out.println("评论数据导入完成，数量：" + (reviewRecords == null ? 0 : reviewRecords.size()));
            importReviewLikes(reviewRecords);


            // 手动提交事务（若用JdbcTemplate/原生JDBC）
            // jdbcTemplate.execute("COMMIT"); // 可选，Spring 事务默认自动提交
        } catch (Exception e) {
            // 打印所有异常（包括RuntimeException）
            System.err.println("导入数据时发生异常：" + e.getMessage());
            e.printStackTrace();
            throw e; // 抛出异常，让外层感知
        }
    }


    private void createTables() {
        String[] createTableSQLs = {
                // 创建users表
                "CREATE TABLE IF NOT EXISTS users (" +
                        "    AuthorId BIGINT PRIMARY KEY, " +
                        "    AuthorName VARCHAR(255) NOT NULL, " +
                        "    Gender VARCHAR(10) CHECK (Gender IN ('Male', 'Female')), " +
                        "    Age INTEGER CHECK (Age > 0), " +
                        "    Followers INTEGER DEFAULT 0 CHECK (Followers >= 0), " +
                        "    Following INTEGER DEFAULT 0 CHECK (Following >= 0), " +
                        "    Password VARCHAR(255), " +
                        "    IsDeleted BOOLEAN DEFAULT FALSE" +
                        ")",

                // 创建recipes表
                "CREATE TABLE IF NOT EXISTS recipes (" +
                        "    RecipeId BIGINT PRIMARY KEY, " +
                        "    Name VARCHAR(500) NOT NULL, " +
                        "    AuthorId BIGINT NOT NULL, " +
                        "    CookTime VARCHAR(50), " +
                        "    PrepTime VARCHAR(50), " +
                        "    TotalTime VARCHAR(50), " +
                        "    DatePublished TIMESTAMP, " +
                        "    Description TEXT, " +
                        "    RecipeCategory VARCHAR(255), " +
                        "    AggregatedRating DECIMAL(3,2) CHECK (AggregatedRating >= 0 AND AggregatedRating <= 5), " +
                        "    ReviewCount INTEGER DEFAULT 0 CHECK (ReviewCount >= 0), " +
                        "    Calories DECIMAL(10,2), " +
                        "    FatContent DECIMAL(10,2), " +
                        "    SaturatedFatContent DECIMAL(10,2), " +
                        "    CholesterolContent DECIMAL(10,2), " +
                        "    SodiumContent DECIMAL(10,2), " +
                        "    CarbohydrateContent DECIMAL(10,2), " +
                        "    FiberContent DECIMAL(10,2), " +
                        "    SugarContent DECIMAL(10,2), " +
                        "    ProteinContent DECIMAL(10,2), " +
                        "    RecipeServings VARCHAR(100), " +
                        "    RecipeYield VARCHAR(100), " +
                        "    FOREIGN KEY (AuthorId) REFERENCES users(AuthorId)" +
                        ")",

                // 创建reviews表
                "CREATE TABLE IF NOT EXISTS reviews (" +
                        "    ReviewId BIGINT PRIMARY KEY, " +
                        "    RecipeId BIGINT NOT NULL, " +
                        "    AuthorId BIGINT NOT NULL, " +
                        "    Rating INTEGER, " +
                        "    Review TEXT, " +
                        "    DateSubmitted TIMESTAMP, " +
                        "    DateModified TIMESTAMP, " +
                        "    FOREIGN KEY (RecipeId) REFERENCES recipes(RecipeId), " +
                        "    FOREIGN KEY (AuthorId) REFERENCES users(AuthorId)" +
                        ")",

                // 创建recipe_ingredients表
                "CREATE TABLE IF NOT EXISTS recipe_ingredients (" +
                        "    RecipeId BIGINT, " +
                        "    IngredientPart VARCHAR(500), " +
                        "    PRIMARY KEY (RecipeId, IngredientPart), " +
                        "    FOREIGN KEY (RecipeId) REFERENCES recipes(RecipeId)" +
                        ")",

                // 创建review_likes表
                "CREATE TABLE IF NOT EXISTS review_likes (" +
                        "    ReviewId BIGINT, " +
                        "    AuthorId BIGINT, " +
                        "    PRIMARY KEY (ReviewId, AuthorId), " +
                        "    FOREIGN KEY (ReviewId) REFERENCES reviews(ReviewId), " +
                        "    FOREIGN KEY (AuthorId) REFERENCES users(AuthorId)" +
                        ")",

                // 创建user_follows表
                "CREATE TABLE IF NOT EXISTS user_follows (" +
                        "    FollowerId BIGINT, " +
                        "    FollowingId BIGINT, " +
                        "    PRIMARY KEY (FollowerId, FollowingId), " +
                        "    FOREIGN KEY (FollowerId) REFERENCES users(AuthorId), " +
                        "    FOREIGN KEY (FollowingId) REFERENCES users(AuthorId), " +
                        "    CHECK (FollowerId != FollowingId)" +
                        ")"
        };

        for (String sql : createTableSQLs) {
            jdbcTemplate.execute(sql);
        }
    }

    private void importUsers(List<UserRecord> userRecords) {
        String sql = "INSERT INTO users (AuthorId, AuthorName, Gender, Age, Followers, Following, Password) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (AuthorId) DO UPDATE " +
                "SET AuthorName=EXCLUDED.AuthorName, Gender=EXCLUDED.Gender, Age=EXCLUDED.Age, " +
                "Followers=EXCLUDED.Followers, Following=EXCLUDED.Following, Password=EXCLUDED.Password";

        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                UserRecord user = userRecords.get(i);
                ps.setLong(1, user.getAuthorId());
                ps.setString(2, user.getAuthorName());
                ps.setString(3, user.getGender());
                ps.setInt(4, user.getAge());
                ps.setInt(5, user.getFollowers());
                ps.setInt(6, user.getFollowing());
                ps.setString(7, user.getPassword());
            }

            @Override
            public int getBatchSize() {
                return userRecords.size();
            }
        });
    }

    private void importRecipes(List<RecipeRecord> recipeRecords) {
        String sql = "INSERT INTO recipes (RecipeId, Name, AuthorId, CookTime, PrepTime, TotalTime, " +
                "DatePublished, Description, RecipeCategory, AggregatedRating, ReviewCount, " +
                "Calories, FatContent, SaturatedFatContent, CholesterolContent, SodiumContent, " +
                "CarbohydrateContent, FiberContent, SugarContent, ProteinContent, RecipeServings, RecipeYield) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (RecipeId) DO UPDATE SET " +
                "Name=EXCLUDED.Name, AuthorId=EXCLUDED.AuthorId, CookTime=EXCLUDED.CookTime, PrepTime=EXCLUDED.PrepTime, " +
                "TotalTime=EXCLUDED.TotalTime, DatePublished=EXCLUDED.DatePublished, Description=EXCLUDED.Description, " +
                "RecipeCategory=EXCLUDED.RecipeCategory, AggregatedRating=EXCLUDED.AggregatedRating, " +
                "ReviewCount=EXCLUDED.ReviewCount, Calories=EXCLUDED.Calories, FatContent=EXCLUDED.FatContent, " +
                "SaturatedFatContent=EXCLUDED.SaturatedFatContent, CholesterolContent=EXCLUDED.CholesterolContent, " +
                "SodiumContent=EXCLUDED.SodiumContent, CarbohydrateContent=EXCLUDED.CarbohydrateContent, " +
                "FiberContent=EXCLUDED.FiberContent, SugarContent=EXCLUDED.SugarContent, " +
                "ProteinContent=EXCLUDED.ProteinContent, RecipeServings=EXCLUDED.RecipeServings, " +
                "RecipeYield=EXCLUDED.RecipeYield";

        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                RecipeRecord recipe = recipeRecords.get(i);
                ps.setLong(1, recipe.getRecipeId());
                ps.setString(2, recipe.getName());
                ps.setLong(3, recipe.getAuthorId());
                ps.setString(4, recipe.getCookTime());
                ps.setString(5, recipe.getPrepTime());
                ps.setString(6, recipe.getTotalTime());
                ps.setTimestamp(7, recipe.getDatePublished() != null ? new java.sql.Timestamp(recipe.getDatePublished().getTime()) : null);
                ps.setString(8, recipe.getDescription());
                ps.setString(9, recipe.getRecipeCategory());
                ps.setFloat(10, recipe.getAggregatedRating());
                ps.setInt(11, recipe.getReviewCount());
                ps.setFloat(12, recipe.getCalories());
                ps.setFloat(13, recipe.getFatContent());
                ps.setFloat(14, recipe.getSaturatedFatContent());
                ps.setFloat(15, recipe.getCholesterolContent());
                ps.setFloat(16, recipe.getSodiumContent());
                ps.setFloat(17, recipe.getCarbohydrateContent());
                ps.setFloat(18, recipe.getFiberContent());
                ps.setFloat(19, recipe.getSugarContent());
                ps.setFloat(20, recipe.getProteinContent());
                ps.setInt(21, recipe.getRecipeServings());
                ps.setString(22, recipe.getRecipeYield());
            }

            @Override
            public int getBatchSize() {
                return recipeRecords.size();
            }
        });
    }

    private void importRecipeIngredients(List<RecipeRecord> recipeRecords) {
        String sql = "INSERT INTO recipe_ingredients (RecipeId, IngredientPart) " +
                "VALUES (?, ?) ON CONFLICT (RecipeId, IngredientPart) DO NOTHING";

        for (RecipeRecord recipe : recipeRecords) {
            if (recipe.getIngredients() == null || recipe.getIngredients().isEmpty()) {
                continue;
            }
            jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    ps.setLong(1, recipe.getRecipeId());
                    ps.setString(2, recipe.getIngredients().get(i));
                }

                @Override
                public int getBatchSize() {
                    return recipe.getIngredients().size();
                }
            });
        }
    }

    private void importReviews(List<ReviewRecord> reviewRecords) {
        String sql = "INSERT INTO reviews (ReviewId, RecipeId, AuthorId, Rating, Review, DateSubmitted, DateModified) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (ReviewId) DO UPDATE SET " +
                "RecipeId=EXCLUDED.RecipeId, AuthorId=EXCLUDED.AuthorId, Rating=EXCLUDED.Rating, " +
                "Review=EXCLUDED.Review, DateSubmitted=EXCLUDED.DateSubmitted, DateModified=EXCLUDED.DateModified";

        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                ReviewRecord review = reviewRecords.get(i);
                ps.setLong(1, review.getReviewId());
                ps.setLong(2, review.getRecipeId());
                ps.setLong(3, review.getAuthorId());
                ps.setFloat(4, review.getRating());
                ps.setString(5, review.getReview());
                ps.setTimestamp(6, review.getDateSubmitted() != null ? new java.sql.Timestamp(review.getDateSubmitted().getTime()) : null);
                ps.setTimestamp(7, review.getDateModified() != null ? new java.sql.Timestamp(review.getDateModified().getTime()) : null);
            }

            @Override
            public int getBatchSize() {
                return reviewRecords.size();
            }
        });
    }

    private void importReviewLikes(List<ReviewRecord> reviewRecords) {
        String sql = "INSERT INTO review_likes (ReviewId, AuthorId) " +
                "VALUES (?, ?) ON CONFLICT (ReviewId, AuthorId) DO NOTHING";

        for (ReviewRecord review : reviewRecords) {
            long reviewId = review.getReviewId();
            List<Long> likeAuthorIds = review.getLikeUsers(); // 从评论记录中获取点赞用户ID列表

            if (likeAuthorIds == null || likeAuthorIds.size() == 0) {
                continue;
            }

            jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    ps.setLong(1, reviewId); // 评论ID
                    ps.setLong(2, likeAuthorIds.get(i)); // 点赞用户的ID
                }

                @Override
                public int getBatchSize() {
                    return likeAuthorIds.size();
                }
            });
        }
    }
    private void importUserFollows(List<UserRecord> userRecords) {
        String sql = "INSERT INTO user_follows (FollowerId, FollowingId) " +
                "VALUES (?, ?) ON CONFLICT (FollowerId, FollowingId) DO NOTHING";

        for (UserRecord user : userRecords) {
            long currentUserId = user.getAuthorId();
            long[] followerUsers = user.getFollowerUsers(); // 关注当前用户的人
            long[] followingUsers = user.getFollowingUsers(); // 当前用户关注的人

            // 存储该用户所有有效关注关系（FollowerId, FollowingId）
            List<long[]> allRelations = new ArrayList<>();

            // 1. 处理当前用户关注的人 → (currentUserId, 被关注人ID)
            if (followingUsers != null && followingUsers.length > 0) {
                for (long followedId : followingUsers) {
                    if (currentUserId != followedId) { // 排除自关注
                        allRelations.add(new long[]{currentUserId, followedId});
                    }
                }
            }

            // 2. 处理关注当前用户的人 → (关注人ID, currentUserId)
            if (followerUsers != null && followerUsers.length > 0) {
                for (long followerId : followerUsers) {
                    if (followerId != currentUserId) { // 排除自关注
                        allRelations.add(new long[]{followerId, currentUserId});
                    }
                }
            }

            // 无有效关系则跳过
            if (allRelations.isEmpty()) {
                continue;
            }

            // 批量插入该用户的所有关注关系
            jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    long[] relation = allRelations.get(i);
                    ps.setLong(1, relation[0]); // FollowerId
                    ps.setLong(2, relation[1]); // FollowingId
                }

                @Override
                public int getBatchSize() {
                    return allRelations.size();
                }
            });
        }
    }
    /*
     * The following code is just a quick example of using jdbc datasource.
     * Practically, the code interacts with database is usually written in a DAO layer.
     *
     * Reference: [Data Access Object pattern](https://www.baeldung.com/java-dao-pattern)
     */

    @Override
    public void drop() {
        // You can use the default drop script provided by us in most cases,
        // but if it doesn't work properly, you may need to modify it.
        // This method will delete all the tables in the public schema.

        String sql = "DO $$\n" +
                "DECLARE\n" +
                "    tables CURSOR FOR\n" +
                "        SELECT tablename\n" +
                "        FROM pg_tables\n" +
                "        WHERE schemaname = 'public';\n" +
                "BEGIN\n" +
                "    FOR t IN tables\n" +
                "    LOOP\n" +
                "        EXECUTE 'DROP TABLE IF EXISTS ' || QUOTE_IDENT(t.tablename) || ' CASCADE;';\n" +
                "    END LOOP;\n" +
                "END $$;\n";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Integer sum(int a, int b) {
        String sql = "SELECT ?+?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, a);
            stmt.setInt(2, b);
            log.info("SQL: {}", stmt);

            ResultSet rs = stmt.executeQuery();
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}

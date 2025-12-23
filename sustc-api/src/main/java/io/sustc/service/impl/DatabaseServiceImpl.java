package io.sustc.service.impl;

import io.sustc.dto.ReviewRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.RecipeRecord;
import io.sustc.service.DatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.transaction.annotation.Transactional;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
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
            createTables();
            importUsers(userRecords);
            importUserFollows(userRecords);
            importRecipes(recipeRecords);
            importRecipeIngredients(recipeRecords);
            importReviews(reviewRecords);
            importReviewLikes(reviewRecords);
        } catch (Exception e) {
            System.err.println("导入数据时发生异常：" + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }

    private void createTables() {
        String[] createTableSQLs = {
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
                        "    AggregatedRating FLOAT CHECK (AggregatedRating >= 0 AND AggregatedRating <= 5), " +
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
                        "    RecipeServings INTEGER, " +
                        "    RecipeYield VARCHAR(100), " +
                        "    FOREIGN KEY (AuthorId) REFERENCES users(AuthorId)" +
                        ")",

                "CREATE TABLE IF NOT EXISTS reviews (" +
                        "    ReviewId BIGINT PRIMARY KEY , " +
                        "    RecipeId BIGINT NOT NULL, " +
                        "    AuthorId BIGINT NOT NULL, " +
                        "    Rating INTEGER, " +
                        "    Review TEXT, " +
                        "    DateSubmitted TIMESTAMP, " +
                        "    DateModified TIMESTAMP, " +
                        "    FOREIGN KEY (RecipeId) REFERENCES recipes(RecipeId), " +
                        "    FOREIGN KEY (AuthorId) REFERENCES users(AuthorId)" +
                        ")",

                "CREATE TABLE IF NOT EXISTS recipe_ingredients (" +
                        "    RecipeId BIGINT, " +
                        "    IngredientPart VARCHAR(500), " +
                        "    PRIMARY KEY (RecipeId, IngredientPart), " +
                        "    FOREIGN KEY (RecipeId) REFERENCES recipes(RecipeId)" +
                        ")",

                "CREATE TABLE IF NOT EXISTS review_likes (" +
                        "    ReviewId BIGINT, " +
                        "    AuthorId BIGINT, " +
                        "    PRIMARY KEY (ReviewId, AuthorId), " +
                        "    FOREIGN KEY (ReviewId) REFERENCES reviews(ReviewId), " +
                        "    FOREIGN KEY (AuthorId) REFERENCES users(AuthorId)" +
                        ")",

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
//        createIndexAndView();
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
            // 使用 getRecipeIngredientParts() 而不是 getIngredients()
            String[] ingredientParts = recipe.getRecipeIngredientParts();
            if (ingredientParts == null || ingredientParts.length == 0) {
                continue;
            }

            jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    ps.setLong(1, recipe.getRecipeId());
                    ps.setString(2, ingredientParts[i]);  // 正确的方法名
                }

                @Override
                public int getBatchSize() {
                    return ingredientParts.length;  // 正确的方法名
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
            List<Long> likeAuthorIds = review.getLikeUsers();
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
            long[] followerUsers = user.getFollowerUsers();
            long[] followingUsers = user.getFollowingUsers();

            List<long[]> allRelations = new ArrayList<>();

            if (followingUsers != null && followingUsers.length > 0) {
                for (long followedId : followingUsers) {
                    if (currentUserId != followedId) {
                        allRelations.add(new long[]{currentUserId, followedId});
                    }
                }
            }
            if (followerUsers != null && followerUsers.length > 0) {
                for (long followerId : followerUsers) {
                    if (followerId != currentUserId) {
                        allRelations.add(new long[]{followerId, currentUserId});
                    }
                }
            }

            if (allRelations.isEmpty()) {
                continue;
            }

            jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    long[] relation = allRelations.get(i);
                    ps.setLong(1, relation[0]);
                    ps.setLong(2, relation[1]);
                }

                @Override
                public int getBatchSize() {
                    return allRelations.size();
                }
            });
        }
    }

    @Override
    public void drop() {
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

    public void createIndexAndView() {
        // 创建索引
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_users_followers ON users(Followers DESC)");
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_users_age ON users(Age DESC)");
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_recipes_category ON recipes(RecipeCategory)");
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_recipes_rating ON recipes(AggregatedRating DESC)");
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_recipes_author_date ON recipes(AuthorId, DatePublished DESC)");
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_recipes_calories ON recipes(Calories)");
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_recipes_category_rating_date ON recipes(RecipeCategory, AggregatedRating DESC, DatePublished DESC)");
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_reviews_recipe_rating ON reviews(RecipeId, Rating DESC)");
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_reviews_author_date ON reviews(AuthorId, DateSubmitted DESC)");
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_reviews_recipe_date ON reviews(RecipeId, DateSubmitted DESC)");
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_recipe_ingredients_part ON recipe_ingredients(IngredientPart)");
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_review_likes_author ON review_likes(AuthorId)");
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_user_follows_follower ON user_follows(FollowerId)");
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_user_follows_following ON user_follows(FollowingId)");

        // 创建或替换视图
        jdbcTemplate.execute("CREATE OR REPLACE VIEW v_user_statistics AS " +
                "SELECT u.AuthorId, u.AuthorName, u.Gender, u.Age, u.Followers, u.Following, " +
                "COUNT(DISTINCT r.RecipeId) AS recipe_count, COUNT(DISTINCT rv.ReviewId) AS review_count, " +
                "COUNT(DISTINCT rl.ReviewId) AS like_count, MAX(r.DatePublished) AS last_recipe_date, " +
                "MAX(rv.DateSubmitted) AS last_review_date " +
                "FROM users u LEFT JOIN recipes r ON u.AuthorId = r.AuthorId " +
                "LEFT JOIN reviews rv ON u.AuthorId = rv.AuthorId " +
                "LEFT JOIN review_likes rl ON rv.ReviewId = rl.ReviewId AND rl.AuthorId = u.AuthorId " +
                "WHERE u.IsDeleted = FALSE GROUP BY u.AuthorId, u.AuthorName, u.Gender, u.Age, u.Followers, u.Following");

        jdbcTemplate.execute("CREATE OR REPLACE VIEW v_recipe_details AS " +
                "SELECT r.RecipeId, r.Name AS recipe_name, r.RecipeCategory, r.AggregatedRating, " +
                "r.ReviewCount, r.Calories, r.FatContent, r.ProteinContent, r.DatePublished, " +
                "u.AuthorId, u.AuthorName, u.Followers AS author_followers, " +
                "COUNT(DISTINCT rv.ReviewId) AS total_reviews, COUNT(DISTINCT rl.AuthorId) AS total_likes, " +
                "ARRAY_AGG(DISTINCT ri.IngredientPart) AS ingredients, " +
                "STRING_AGG(DISTINCT ri.IngredientPart, ', ') AS ingredients_text " +
                "FROM recipes r JOIN users u ON r.AuthorId = u.AuthorId " +
                "LEFT JOIN recipe_ingredients ri ON r.RecipeId = ri.RecipeId " +
                "LEFT JOIN reviews rv ON r.RecipeId = rv.RecipeId " +
                "LEFT JOIN review_likes rl ON rv.ReviewId = rl.ReviewId " +
                "WHERE u.IsDeleted = FALSE GROUP BY r.RecipeId, r.Name, r.RecipeCategory, " +
                "r.AggregatedRating, r.ReviewCount, r.Calories, r.FatContent, r.ProteinContent, " +
                "r.DatePublished, u.AuthorId, u.AuthorName, u.Followers");

        jdbcTemplate.execute("CREATE MATERIALIZED VIEW IF NOT EXISTS mv_hot_recipes AS " +
                "SELECT r.RecipeId, r.Name, u.AuthorName, r.RecipeCategory, r.AggregatedRating, " +
                "COUNT(DISTINCT rv.ReviewId) AS total_reviews, " +
                "r.AggregatedRating * 10 + COUNT(DISTINCT rv.ReviewId) AS hot_level " +
                "FROM recipes r JOIN users u ON r.AuthorId = u.AuthorId " +
                "LEFT JOIN reviews rv ON r.RecipeId = rv.RecipeId " +
                "LEFT JOIN review_likes rl ON rv.ReviewId = rl.ReviewId " +
                "WHERE u.IsDeleted = FALSE GROUP BY r.RecipeId, r.Name, r.RecipeCategory, " +
                "r.AggregatedRating, r.ReviewCount, r.DatePublished, u.AuthorName " +
                "HAVING COUNT(DISTINCT rv.ReviewId) >= 5 ORDER BY hot_level DESC LIMIT 100");

        jdbcTemplate.execute("CREATE OR REPLACE VIEW v_healthy_recipes AS " +
                "SELECT r.RecipeId, r.Name, r.RecipeCategory, r.Calories, r.ProteinContent, " +
                "r.FatContent, r.CarbohydrateContent, r.AggregatedRating, " +
                "ROUND(r.ProteinContent * 4.0 / NULLIF(r.Calories, 0) * 100, 2) AS protein_ratio " +
                "FROM recipes r WHERE r.Calories <= 500 AND r.ProteinContent >= 20 AND r.FatContent <= 30 " +
                "ORDER BY protein_ratio DESC, r.AggregatedRating DESC");

        jdbcTemplate.execute("CREATE OR REPLACE VIEW v_review_analysis AS " +
                "SELECT rv.ReviewId, rv.Rating, rv.DateSubmitted, r.Name AS recipe_name, " +
                "r.RecipeCategory, u.AuthorName AS reviewer_name, u.Followers AS reviewer_followers, " +
                "COUNT(DISTINCT rl.AuthorId) AS like_count, LENGTH(rv.Review) AS review_length, " +
                "CASE WHEN rv.Rating >= 4 THEN 'Positive' WHEN rv.Rating >= 3 THEN 'Neutral' ELSE 'Negative' END AS sentiment " +
                "FROM reviews rv JOIN recipes r ON rv.RecipeId = r.RecipeId " +
                "JOIN users u ON rv.AuthorId = u.AuthorId " +
                "LEFT JOIN review_likes rl ON rv.ReviewId = rl.ReviewId " +
                "WHERE u.IsDeleted = FALSE ORDER BY rv.DateSubmitted DESC");
    }

}

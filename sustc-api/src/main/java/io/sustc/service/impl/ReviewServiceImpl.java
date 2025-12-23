package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.PageResult;
import io.sustc.dto.RecipeRecord;
import io.sustc.dto.ReviewRecord;
import io.sustc.service.RecipeService;
import io.sustc.service.ReviewService;
import io.sustc.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class ReviewServiceImpl implements ReviewService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private UserService userService;

    @Autowired
    private RecipeService recipeService;

    @Override
    @Transactional
    public long addReview(AuthInfo auth, long recipeId, int rating, String review) {
        long userId = userService.login(auth);
        if (userId == -1) {
            throw new SecurityException("Invalid or inactive user");
        }
        RecipeRecord recipe = recipeService.getRecipeById(recipeId);
        if (recipe == null) {
            throw new IllegalArgumentException("Recipe does not exist");
        }
        if (rating < 1 || rating > 5) {
            throw new IllegalArgumentException("Rating must be between 1 and 5");
        }
        String maxIdSql = "SELECT COALESCE(MAX(ReviewId), 0) FROM reviews";
        Long maxId = jdbcTemplate.queryForObject(maxIdSql, Long.class);
        long reviewId = maxId + 1;

        String sql = "INSERT INTO reviews (ReviewId, RecipeId, AuthorId, Rating, Review, DateSubmitted, DateModified) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";
        Timestamp now = new Timestamp(System.currentTimeMillis());
        jdbcTemplate.update(sql, reviewId, recipeId, userId, rating, review, now, now);

        refreshRecipeAggregatedRating(recipeId);

        return reviewId;
    }

    @Override
    @Transactional
    public void editReview(AuthInfo auth, long recipeId, long reviewId, int rating, String review) {
        if (rating < 1 || rating > 5) {
            throw new IllegalArgumentException("Rating must be between 1 and 5");
        }
        if (review == null || review.trim().isEmpty()) {
            throw new IllegalArgumentException("Review text cannot be empty");
        }
        String recipeExistsSql = "SELECT EXISTS (SELECT 1 FROM recipes WHERE RecipeId = ?)";
        Boolean recipeExists = jdbcTemplate.queryForObject(recipeExistsSql, Boolean.class, recipeId);
        if (recipeExists == null || !recipeExists) {
            throw new IllegalArgumentException("Recipe does not exist");
        }
        long userId = userService.login(auth);
        if (userId == -1) {
            throw new SecurityException("Invalid or inactive user");
        }
        String reviewCheckSql =
                "SELECT AuthorId FROM reviews WHERE ReviewId = ? AND RecipeId = ?";

        Long authorId;
        try {
            authorId = jdbcTemplate.queryForObject(reviewCheckSql, Long.class, reviewId, recipeId);
        } catch (EmptyResultDataAccessException e) {
            throw new IllegalArgumentException("Review does not exist for this recipe");
        }

        if (authorId == null) {
            throw new IllegalArgumentException("Review does not exist");
        }

        if (authorId != userId) {
            throw new SecurityException("Only the author can edit the review");
        }
        String updateSql = "UPDATE reviews SET Rating = ?, Review = ?, DateModified = ? " +
                "WHERE ReviewId = ? AND RecipeId = ? AND AuthorId = ?";

        Timestamp now = new Timestamp(System.currentTimeMillis());
        int updated = jdbcTemplate.update(updateSql, rating, review, now, reviewId, recipeId, userId);

        if (updated == 0) {
            throw new SecurityException("Failed to update review");
        }
        refreshRecipeAggregatedRating(recipeId);
    }

    @Override
    @Transactional
    public void deleteReview(AuthInfo auth, long recipeId, long reviewId) {
        if (!recipeExists(recipeId)) {
            throw new IllegalArgumentException("Recipe does not exist");
        }
        long userId = userService.login(auth);
        if (userId == -1) {
            throw new SecurityException("Invalid or inactive user");
        }

        String sql = "SELECT AuthorId FROM reviews WHERE ReviewId = ? AND RecipeId = ?";
        Long authorId;
        try {
            authorId = jdbcTemplate.queryForObject(sql, Long.class, reviewId, recipeId);
        } catch (EmptyResultDataAccessException e) {
            throw new IllegalArgumentException("Review does not exist for this recipe");
        }

        if (authorId == null || authorId != userId) {
            String message = authorId == null ?
                    "Review does not exist for this recipe" :
                    "Only the author can delete the review";
            throw authorId == null ?
                    new IllegalArgumentException(message) :
                    new SecurityException(message);
        }

        jdbcTemplate.update("DELETE FROM review_likes WHERE ReviewId = ?", reviewId);
        jdbcTemplate.update("DELETE FROM reviews WHERE ReviewId = ? AND RecipeId = ?", reviewId, recipeId);
        refreshRecipeAggregatedRating(recipeId);
    }

    private boolean recipeExists(long recipeId) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM recipes WHERE RecipeId = ?",
                Integer.class, recipeId
        );
        return count != null && count > 0;
    }

    @Override
    @Transactional
    public long likeReview(AuthInfo auth, long reviewId) {
        long userId = userService.login(auth);
        if (userId == -1) {
            throw new SecurityException("Invalid or inactive user");
        }

        Boolean reviewExists = jdbcTemplate.queryForObject(
                "SELECT EXISTS (SELECT 1 FROM reviews WHERE ReviewId = ?)",
                Boolean.class,
                reviewId
        );
        if (reviewExists == null || !reviewExists) {
            throw new IllegalArgumentException("Review does not exist");
        }

        Long reviewAuthor = jdbcTemplate.queryForObject(
                "SELECT AuthorId FROM reviews WHERE ReviewId = ?",
                Long.class,
                reviewId
        );
        if (reviewAuthor != null && reviewAuthor.equals(userId)) {
            throw new SecurityException("Cannot like your own review");
        }

        String sql = "INSERT INTO review_likes (ReviewId, AuthorId) VALUES (?, ?) ON CONFLICT DO NOTHING";
        jdbcTemplate.update(sql, reviewId, userId);

        // 查询总点赞数
        return jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM review_likes WHERE ReviewId = ?",
                Long.class,
                reviewId
        );
    }

    @Override
    @Transactional
    public long unlikeReview(AuthInfo auth, long reviewId) {
        long userId = userService.login(auth);
        if (userId == -1) {
            throw new SecurityException("Invalid or inactive user");
        }

        Boolean reviewExists = jdbcTemplate.queryForObject(
                "SELECT EXISTS (SELECT 1 FROM reviews WHERE ReviewId = ?)",
                Boolean.class,
                reviewId
        );
        if (reviewExists == null || !reviewExists) {
            throw new IllegalArgumentException("Review does not exist");
        }

        String sql = "DELETE FROM review_likes WHERE ReviewId = ? AND AuthorId = ?";
        jdbcTemplate.update(sql, reviewId, userId);

        return jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM review_likes WHERE ReviewId = ?",
                Long.class,
                reviewId
        );
    }

    @Override
    public PageResult<ReviewRecord> listByRecipe(long recipeId, int page, int size, String sort) {
        if (page < 1 || size <= 0) {
            throw new IllegalArgumentException("Invalid page or size");
        }

        String sortClause = "r.DateModified DESC";
        if ("likes_desc".equals(sort)) {
            sortClause = "LikeCount DESC";
        } else if ("date_desc".equals(sort)) {
            sortClause = "r.DateSubmitted DESC";
        } else if ("date_asc".equals(sort)) {
            sortClause = "r.DateSubmitted ASC";
        } else if ("rating_desc".equals(sort)) {
            sortClause = "r.Rating DESC";
        } else if ("rating_asc".equals(sort)) {
            sortClause = "r.Rating ASC";
        }

        int offset = (page - 1) * size;
        String sql = "SELECT r.*, u.AuthorName, COUNT(rl.AuthorId) AS LikeCount " +
                "FROM reviews r " +
                "LEFT JOIN review_likes rl ON r.ReviewId = rl.ReviewId " +
                "JOIN users u ON r.AuthorId = u.AuthorId " +
                "WHERE r.RecipeId = ? " +
                "GROUP BY r.ReviewId, u.AuthorName " +
                "ORDER BY " + sortClause + " " +
                "LIMIT ? OFFSET ?";

        List<ReviewRecord> records = jdbcTemplate.query(sql, new ReviewRowMapper(jdbcTemplate), recipeId, size, offset);

        long total = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM reviews WHERE RecipeId = ?",
                Long.class,
                recipeId
        );

        return new PageResult<>(records, page, size, total);
    }

    @Override
    @Transactional
    public RecipeRecord refreshRecipeAggregatedRating(long recipeId) {
        RecipeRecord recipe = recipeService.getRecipeById(recipeId);
        if (recipe == null) {
            throw new IllegalArgumentException("Recipe does not exist or is deleted");
        }

        String sql = "SELECT " +
                "    AVG(Rating) AS avg_rating, " +
                "    COUNT(*) AS review_count " +
                "FROM reviews " +
                "WHERE RecipeId = ?";

        Map<String, Object> stats = jdbcTemplate.queryForMap(sql, recipeId);
        BigDecimal avgRatingBD = (BigDecimal) stats.get("avg_rating");
        Double avgRating = avgRatingBD != null ? avgRatingBD.doubleValue() : 0.0;
        Long reviewCount = (Long) stats.get("review_count");

        // 更新食谱评分
        String updateSql = "UPDATE recipes SET " +
                "AggregatedRating = ?, " +
                "ReviewCount = ? " +
                "WHERE RecipeId = ?";

        jdbcTemplate.update(updateSql,
                avgRating != null ? roundToTwoDecimals(avgRating) : null,
                reviewCount != null ? reviewCount.intValue() : 0,
                recipeId
        );

        return recipeService.getRecipeById(recipeId);
    }

    // 工具方法：保留两位小数
    private Double roundToTwoDecimals(Double value) {
        return Math.round(value * 100.0) / 100.0;
    }

    private static class ReviewRowMapper implements RowMapper<ReviewRecord> {
        private JdbcTemplate jdbcTemplate;
        public ReviewRowMapper(JdbcTemplate jdbcTemplate) {
            this.jdbcTemplate = jdbcTemplate;
        }
        @Override
        public ReviewRecord mapRow(ResultSet rs, int rowNum) throws SQLException {
            ReviewRecord record = new ReviewRecord();
            record.setReviewId(rs.getLong("ReviewId"));
            record.setRecipeId(rs.getLong("RecipeId"));
            record.setAuthorId(rs.getLong("AuthorId"));
            record.setAuthorName(rs.getString("AuthorName"));
            record.setRating(rs.getInt("Rating"));
            record.setReview(rs.getString("Review"));
            record.setDateSubmitted(rs.getTimestamp("DateSubmitted"));
            record.setDateModified(rs.getTimestamp("DateModified"));

            List<Long> likeUserIds = jdbcTemplate.query(
                    "SELECT AuthorId FROM review_likes WHERE ReviewId = ?",
                    (rs1, rowNum1) -> rs1.getLong("AuthorId"),
                    record.getReviewId()
            );
            record.setLikeUsers(likeUserIds);
            return record;
        }
    }

}
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
import org.springframework.jdbc.core.BeanPropertyRowMapper;
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
import java.util.Random;

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
        // 验证用户
        long userId = userService.login(auth);
        if (userId == -1) {
            throw new SecurityException("Invalid or inactive user");
        }

        // 验证食谱存在
        RecipeRecord recipe = recipeService.getRecipeById(recipeId);
        if (recipe == null) {
            throw new IllegalArgumentException("Recipe does not exist");
        }

        // 验证评分
        if (rating < 1 || rating > 5) {
            throw new IllegalArgumentException("Rating must be between 1 and 5");
        }

        // 生成ReviewId
        long reviewId = new java.util.Random().nextLong(1000000);

        // 插入评论
        String sql = "INSERT INTO reviews (ReviewId, RecipeId, AuthorId, Rating, Review, DateSubmitted, DateModified) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";
        Timestamp now = new Timestamp(System.currentTimeMillis());
        jdbcTemplate.update(sql, reviewId, recipeId, userId, rating, review, now, now);

        // 刷新食谱评分
        refreshRecipeAggregatedRating(recipeId);

        return reviewId;
    }

    @Override
    @Transactional
    public void editReview(AuthInfo auth, long recipeId, long reviewId, int rating, String review) {
        // 验证用户
        long userId = userService.login(auth);
        if (userId == -1) {
            throw new SecurityException("Invalid or inactive user");
        }

        // 验证评论归属
        Boolean isAuthor = jdbcTemplate.queryForObject(
                "SELECT EXISTS (SELECT 1 FROM reviews WHERE ReviewId = ? AND RecipeId = ? AND AuthorId = ?)",
                Boolean.class,
                reviewId,
                recipeId,
                userId
        );
        if (isAuthor == null || !isAuthor) {
            throw new SecurityException("Only the author can edit the review");
        }

        // 验证评分
        if (rating < 1 || rating > 5) {
            throw new IllegalArgumentException("Rating must be between 1 and 5");
        }

        // 更新评论
        String sql = "UPDATE reviews SET Rating = ?, Review = ?, DateModified = ? WHERE ReviewId = ? AND RecipeId = ?";
        jdbcTemplate.update(sql, rating, review, new Timestamp(System.currentTimeMillis()), reviewId, recipeId);

        // 刷新食谱评分
        refreshRecipeAggregatedRating(recipeId);
    }

    @Override
    @Transactional
    public void deleteReview(AuthInfo auth, long recipeId, long reviewId) {
        // 验证用户
        long userId = userService.login(auth);
        if (userId == -1) {
            throw new SecurityException("Invalid or inactive user");
        }

        // 验证评论归属
        Boolean isAuthor = jdbcTemplate.queryForObject(
                "SELECT EXISTS (SELECT 1 FROM reviews WHERE ReviewId = ? AND RecipeId = ? AND AuthorId = ?)",
                Boolean.class,
                reviewId,
                recipeId,
                userId
        );
        if (isAuthor == null || !isAuthor) {
            throw new SecurityException("Only the author can delete the review");
        }

        // 级联删除点赞
        jdbcTemplate.update("DELETE FROM review_likes WHERE ReviewId = ?", reviewId);

        // 删除评论
        jdbcTemplate.update("DELETE FROM reviews WHERE ReviewId = ? AND RecipeId = ?", reviewId, recipeId);

        // 刷新食谱评分
        refreshRecipeAggregatedRating(recipeId);
    }

    @Override
    @Transactional
    public long likeReview(AuthInfo auth, long reviewId) {
        // 验证用户
        long userId = userService.login(auth);
        if (userId == -1) {
            throw new SecurityException("Invalid or inactive user");
        }

        // 验证评论存在
        Boolean reviewExists = jdbcTemplate.queryForObject(
                "SELECT EXISTS (SELECT 1 FROM reviews WHERE ReviewId = ?)",
                Boolean.class,
                reviewId
        );
        if (reviewExists == null || !reviewExists) {
            throw new IllegalArgumentException("Review does not exist");
        }

        // 验证是否为自己的评论
        Long reviewAuthor = jdbcTemplate.queryForObject(
                "SELECT AuthorId FROM reviews WHERE ReviewId = ?",
                Long.class,
                reviewId
        );
        if (reviewAuthor != null && reviewAuthor.equals(userId)) {
            throw new SecurityException("Cannot like your own review");
        }

        // 点赞（幂等操作）
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
        // 验证用户
        long userId = userService.login(auth);
        if (userId == -1) {
            throw new SecurityException("Invalid or inactive user");
        }

        // 验证评论存在
        Boolean reviewExists = jdbcTemplate.queryForObject(
                "SELECT EXISTS (SELECT 1 FROM reviews WHERE ReviewId = ?)",
                Boolean.class,
                reviewId
        );
        if (reviewExists == null || !reviewExists) {
            throw new IllegalArgumentException("Review does not exist");
        }

        // 取消点赞（幂等操作）
        String sql = "DELETE FROM review_likes WHERE ReviewId = ? AND AuthorId = ?";
        jdbcTemplate.update(sql, reviewId, userId);

        // 查询总点赞数
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

        // 排序处理
        String sortClause = "DateModified DESC";
        if ("likes_desc".equals(sort)) {
            sortClause = "(SELECT COUNT(*) FROM review_likes WHERE review_likes.ReviewId = reviews.ReviewId) DESC";
        }

        // 分页查询
        int offset = (page - 1) * size;
        String sql = "SELECT r.*, COUNT(rl.AuthorId) AS LikeCount " +
                "FROM reviews r " +
                "LEFT JOIN review_likes rl ON r.ReviewId = rl.ReviewId " +
                "WHERE r.RecipeId = ? " +
                "GROUP BY r.ReviewId " +
                "ORDER BY " + sortClause + " " +
                "LIMIT ? OFFSET ?";

        List<ReviewRecord> records = jdbcTemplate.query(sql, new ReviewRowMapper(jdbcTemplate), recipeId, size, offset);

        // 总条数
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
        // 验证食谱存在且未删除
        RecipeRecord recipe = recipeService.getRecipeById(recipeId);
        if (recipe == null) {
            throw new IllegalArgumentException("Recipe does not exist or is deleted");
        }

        // 计算平均评分和评论数
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

        // 返回更新后的食谱
        return recipeService.getRecipeById(recipeId);
    }

    // 工具方法：保留两位小数
    private Double roundToTwoDecimals(Double value) {
        return Math.round(value * 100.0) / 100.0;
    }

    // 自定义RowMapper：将ResultSet映射为ReviewRecord
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
            record.setRating(rs.getFloat("Rating"));
            record.setReview(rs.getString("Review"));
            record.setDateSubmitted(rs.getTimestamp("DateSubmitted"));
            record.setDateModified(rs.getTimestamp("DateModified"));
            // 查询评论的点赞用户列表
            List<Long> likeUserIds = jdbcTemplate.query(
                    "SELECT AuthorId FROM review_likes WHERE ReviewId = ?",
                    (rs1, rowNum1) -> rs1.getLong("AuthorId"),
                    record.getReviewId()
            );
            // 设置点赞用户列表（使用之前实现的setLikeUsers方法）
            record.setLikeUsers(likeUserIds);
            return record;
        }
    }

}
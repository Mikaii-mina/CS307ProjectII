package io.sustc.service.impl;

import io.sustc.dto.*;
import io.sustc.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.*;
import java.util.*;

@Service
@Slf4j
public class UserServiceImpl implements UserService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    public long register(RegisterUserReq req) {
        if (req == null) {
            throw new IllegalArgumentException("Request cannot be null");
        }

        if (!StringUtils.hasText(req.getName())) {
            throw new IllegalArgumentException("Username cannot be empty");
        }

        String genderStr = extractGenderString(req);
        if (!StringUtils.hasText(genderStr)) {
            throw new IllegalArgumentException("Gender cannot be empty");
        }

        if (!StringUtils.hasText(req.getPassword())) {
            throw new IllegalArgumentException("Password cannot be empty");
        }

        LocalDate birthday = extractBirthday(req);
        if (birthday == null) {
            throw new IllegalArgumentException("Birthday cannot be empty");
        }
        if (usernameExists(req.getName())) {
            throw new IllegalArgumentException("Username already exists");
        }

        String dbGender;
        String inputGender = genderStr.toUpperCase();
        if ("MALE".equals(inputGender)) {
            dbGender = "Male";
        } else if ("FEMALE".equals(inputGender)) {
            dbGender = "Female";
        } else {
            throw new IllegalArgumentException("Invalid gender. Must be MALE or FEMALE");
        }
        int age = calculateAge(birthday);
        if (age <= 0) {
            throw new IllegalArgumentException("Invalid birthday");
        }
        Long maxId = jdbcTemplate.queryForObject(
                "SELECT COALESCE(MAX(AuthorId), 0) FROM users",
                Long.class
        );
        if (maxId == null) {
            maxId = 0L;
        }
        long authorId = maxId + 1;
        String sql = "INSERT INTO users (AuthorId, AuthorName, Gender, Age, Followers, Following, Password, IsDeleted) " +
                "VALUES (?, ?, ?, ?, 0, 0, ?, FALSE)";

        try {
            jdbcTemplate.update(sql,
                    authorId,
                    req.getName().trim(),
                    dbGender,
                    age,
                    req.getPassword()
            );

            return authorId;
        } catch (Exception e) {
            throw new IllegalArgumentException("Registration failed: " + e.getMessage());
        }
    }

    private String extractGenderString(RegisterUserReq req) {
        Object gender = req.getGender();
        if (gender == null) {
            return null;
        }

        if (gender instanceof String) {
            return (String) gender;
        } else if (gender instanceof Enum) {
            return ((Enum<?>) gender).name();
        } else {
            return gender.toString();
        }
    }

    private LocalDate extractBirthday(RegisterUserReq req) {
        Object birthday = req.getBirthday();
        if (birthday == null) {
            return null;
        }

        if (birthday instanceof LocalDate) {
            return (LocalDate) birthday;
        } else if (birthday instanceof String) {
            try {
                return LocalDate.parse((String) birthday);
            } catch (Exception e) {
                throw new IllegalArgumentException("Invalid birthday format");
            }
        } else if (birthday instanceof java.sql.Date) {
            return ((java.sql.Date) birthday).toLocalDate();
        } else if (birthday instanceof java.util.Date) {
            return ((java.util.Date) birthday).toInstant()
                    .atZone(ZoneId.systemDefault())
                    .toLocalDate();
        } else {
            throw new IllegalArgumentException("Unsupported birthday type: " + birthday.getClass());
        }
    }

    private boolean isValidPassword(String password) {
        if (password == null || password.isEmpty()) {
            return false;
        }
        return !password.trim().isEmpty();
    }

    private boolean usernameExists(String username) {
        String sql = "SELECT COUNT(*) FROM users WHERE LOWER(AuthorName) = LOWER(?)";
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, username.trim());
        return count != null && count > 0;
    }

    private int calculateAge(LocalDate birthday) {
        if (birthday == null) {
            return -1;
        }

        LocalDate now = LocalDate.now();
        if (birthday.isAfter(now)) {
            throw new IllegalArgumentException("Birthday cannot be in the future");
        }

        int age = Period.between(birthday, now).getYears();

        return Math.max(age, 0);
    }

    @Override
    public long login(AuthInfo auth) {
        if (auth == null || auth.getAuthorId() <= 0 || !StringUtils.hasText(auth.getPassword())) {
            return -1;
        }

        try {
            return jdbcTemplate.queryForObject(
                    "SELECT AuthorId FROM users WHERE AuthorId = ? AND IsDeleted = FALSE AND Password = ?",
                    Long.class,
                    auth.getAuthorId(),
                    auth.getPassword()
            );
        } catch (EmptyResultDataAccessException e) {
            return -1;
        }
    }

    @Override
    public boolean deleteAccount(AuthInfo auth, long userId) {
        long loginUserId = login(auth);
        if (loginUserId == -1) {
            throw new SecurityException("Invalid or inactive user");
        }
        if (loginUserId != userId) {
            throw new SecurityException("Only can delete your own account");
        }
        Boolean isActive = jdbcTemplate.queryForObject(
                "SELECT IsDeleted FROM users WHERE AuthorId = ?",
                Boolean.class,
                userId
        );
        if (isActive == null || isActive) {
            return false;
        }
        jdbcTemplate.update("UPDATE users SET IsDeleted = TRUE WHERE AuthorId = ?", userId);
        jdbcTemplate.update("DELETE FROM user_follows WHERE FollowerId = ? OR FollowingId = ?", userId, userId);

        return true;
    }

    @Override
    public boolean follow(AuthInfo auth, long followeeId) {
        long followerId = login(auth);
        if (followerId == -1) {
            throw new SecurityException("Invalid or inactive user");
        }
        if (followerId == followeeId) {
            throw new SecurityException("Cannot follow yourself");
        }

        String checkUsersSql =
                "SELECT " +
                        "(SELECT COUNT(*) FROM users WHERE AuthorId = ? AND (IsDeleted IS NULL OR IsDeleted = FALSE)) as followerExists, " +
                        "(SELECT COUNT(*) FROM users WHERE AuthorId = ? AND (IsDeleted IS NULL OR IsDeleted = FALSE)) as followeeExists";

        Map<String, Object> result = jdbcTemplate.queryForMap(checkUsersSql, followerId, followeeId);

        Long followerExists = (Long) result.get("followerexists");
        Long followeeExists = (Long) result.get("followeeexists");

        if (followerExists == null || followerExists == 0) {
            throw new SecurityException("Current user does not exist or is deleted");
        }

        if (followeeExists == null || followeeExists == 0) {
            throw new SecurityException("User to follow does not exist or is deleted");
        }

        Boolean isFollowing = jdbcTemplate.queryForObject(
                "SELECT EXISTS (SELECT 1 FROM user_follows WHERE FollowerId = ? AND FollowingId = ?)",
                Boolean.class,
                followerId,
                followeeId
        );

        if (isFollowing != null && isFollowing) {
            int deleted = jdbcTemplate.update(
                    "DELETE FROM user_follows WHERE FollowerId = ? AND FollowingId = ?",
                    followerId, followeeId
            );

            if (deleted > 0) {
                jdbcTemplate.update("UPDATE users SET Following = GREATEST(Following - 1, 0) WHERE AuthorId = ?", followerId);
                jdbcTemplate.update("UPDATE users SET Followers = GREATEST(Followers - 1, 0) WHERE AuthorId = ?", followeeId);
            }

            return false;
        } else {
            try {
                jdbcTemplate.update(
                        "INSERT INTO user_follows (FollowerId, FollowingId) VALUES (?, ?)",
                        followerId, followeeId
                );

                jdbcTemplate.update("UPDATE users SET Following = Following + 1 WHERE AuthorId = ?", followerId);
                jdbcTemplate.update("UPDATE users SET Followers = Followers + 1 WHERE AuthorId = ?", followeeId);

                return true;
            } catch (Exception e) {
                return false;
            }
        }
    }
    @Override
    public UserRecord getById(long userId) {
        if (userId <= 0) {
            return null;
        }
        try {
            String userSql = "SELECT AuthorId, AuthorName, Gender, Age, Password, IsDeleted " +
                    "FROM users WHERE AuthorId = ? AND (IsDeleted IS NULL OR IsDeleted = FALSE)";

            UserRecord user = jdbcTemplate.queryForObject(userSql, (rs, rowNum) -> {
                UserRecord record = new UserRecord();
                record.setAuthorId(rs.getLong("AuthorId"));
                record.setAuthorName(rs.getString("AuthorName"));
                record.setGender(rs.getString("Gender"));
                record.setAge(rs.getInt("Age"));
                record.setPassword(rs.getString("Password"));
                record.setDeleted(rs.getBoolean("IsDeleted"));
                return record;
            }, userId);

            if (user != null) {
                user.setFollowers(getFollowersCount(userId));
                user.setFollowing(getFollowingCount(userId));
                user.setFollowerUsers(getFollowerIds(userId));
                user.setFollowingUsers(getFollowingIds(userId));
            }

            return user;
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }
    private int getFollowersCount(long userId) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM user_follows WHERE FollowingId = ?",
                Integer.class, userId
        );
        return count != null ? count : 0;
    }

    private int getFollowingCount(long userId) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM user_follows WHERE FollowerId = ?",
                Integer.class, userId
        );
        return count != null ? count : 0;
    }

    private long[] getFollowerIds(long userId) {
        List<Long> ids = jdbcTemplate.queryForList(
                "SELECT FollowerId FROM user_follows WHERE FollowingId = ?",
                Long.class, userId
        );
        return ids.stream().mapToLong(Long::longValue).toArray();
    }

    private long[] getFollowingIds(long userId) {
        List<Long> ids = jdbcTemplate.queryForList(
                "SELECT FollowingId FROM user_follows WHERE FollowerId = ?",
                Long.class, userId
        );
        return ids.stream().mapToLong(Long::longValue).toArray();
    }

    @Override
    public void updateProfile(AuthInfo auth, String gender, Integer age) {
        long userId = login(auth);
        if (userId == -1) {
            throw new SecurityException("Invalid or inactive user");
        }

        StringBuilder sql = new StringBuilder("UPDATE users SET ");
        List<Object> params = new ArrayList<>();
        boolean hasUpdate = false;

        if (gender != null) {
            if (!Arrays.asList("Male", "Female").contains(gender)) {
                throw new IllegalArgumentException("Invalid gender");
            }
            sql.append("Gender = ?, ");
            params.add(gender);
            hasUpdate = true;
        }

        if (age != null) {
            if (age <= 0) {
                throw new IllegalArgumentException("Age must be positive");
            }
            sql.append("Age = ?, ");
            params.add(age);
            hasUpdate = true;
        }

        if (!hasUpdate) {
            return;
        }

        sql.setLength(sql.length() - 2);
        sql.append(" WHERE AuthorId = ?");
        params.add(userId);

        jdbcTemplate.update(sql.toString(), params.toArray());
    }

    @Override
    public PageResult<FeedItem> feed(AuthInfo auth, int page, int size, String category) {
        // 验证用户
        long userId = login(auth);
        if (userId == -1) {
            throw new SecurityException("Invalid or inactive user");
        }

        // 调整分页参数
        page = Math.max(page, 1);
        size = Math.max(1, Math.min(size, 200));
        int offset = (page - 1) * size;

        StringBuilder sql = new StringBuilder(
                "SELECT r.RecipeId, r.Name, r.AuthorId, u.AuthorName, " +
                        "r.AggregatedRating, r.ReviewCount " +
                        "FROM recipes r " +
                        "JOIN users u ON r.AuthorId = u.AuthorId " +
                        "JOIN user_follows uf ON r.AuthorId = uf.FollowingId " +
                        "WHERE uf.FollowerId = ? AND u.IsDeleted = FALSE "
        );
        List<Object> params = new ArrayList<>();
        params.add(userId);

        // 分类过滤
        if (StringUtils.hasText(category)) {
            sql.append("AND r.RecipeCategory = ? ");
            params.add(category);
        }

        // 排序和分页
        sql.append("ORDER BY r.DatePublished DESC, r.RecipeId DESC LIMIT ? OFFSET ?");
        params.add(size);
        params.add(offset);

        List<FeedItem> items = jdbcTemplate.query(sql.toString(), new FeedItemRowMapper(), params.toArray());

        StringBuilder countSql = new StringBuilder(
                "SELECT COUNT(*) " +
                        "FROM recipes r " +
                        "JOIN users u ON r.AuthorId = u.AuthorId " +
                        "JOIN user_follows uf ON r.AuthorId = uf.FollowingId " +
                        "WHERE uf.FollowerId = ? AND u.IsDeleted = FALSE "
        );
        List<Object> countParams = new ArrayList<>();
        countParams.add(userId);
        if (StringUtils.hasText(category)) {
            countSql.append("AND r.RecipeCategory = ? ");
            countParams.add(category);
        }
        long total = jdbcTemplate.queryForObject(countSql.toString(), Long.class, countParams.toArray());

        return new PageResult<>(items, page, size, total);
    }

    @Override
    public Map<String, Object> getUserWithHighestFollowRatio() {
        String sql = "SELECT " +
                "    u.AuthorId, " +
                "    u.AuthorName, " +
                "    (COUNT(DISTINCT uf1.FollowerId) * 1.0) / COUNT(DISTINCT uf2.FollowingId) AS Ratio " +
                "FROM users u " +
                "LEFT JOIN user_follows uf1 ON u.AuthorId = uf1.FollowingId " +
                "LEFT JOIN user_follows uf2 ON u.AuthorId = uf2.FollowerId " +
                "WHERE u.IsDeleted = FALSE " +
                "GROUP BY u.AuthorId, u.AuthorName " +
                "HAVING COUNT(DISTINCT uf2.FollowingId) > 0 " +
                "ORDER BY Ratio DESC, u.AuthorId ASC " +
                "LIMIT 1";

        try {
            return jdbcTemplate.queryForObject(sql, new RowMapper<Map<String, Object>>() {
                @Override
                public Map<String, Object> mapRow(ResultSet rs, int rowNum) throws SQLException {
                    Map<String, Object> result = new HashMap<>();
                    result.put("AuthorId", rs.getLong("AuthorId"));
                    result.put("AuthorName", rs.getString("AuthorName"));
                    result.put("Ratio", rs.getDouble("Ratio"));
                    return result;
                }
            });
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    private static class FeedItemRowMapper implements RowMapper<FeedItem> {
        @Override
        public FeedItem mapRow(ResultSet rs, int rowNum) throws SQLException {
            FeedItem item = new FeedItem();
            item.setRecipeId(rs.getLong("RecipeId"));
            item.setName(rs.getString("Name"));
            item.setAuthorId(rs.getLong("AuthorId"));
            item.setAuthorName(rs.getString("AuthorName"));

            // 处理可能为null的评分
            double rating = rs.getDouble("AggregatedRating");
            item.setAggregatedRating(rs.wasNull() ? 0.0 : rating);

            // 处理可能为null的评论数
            int reviewCount = rs.getInt("ReviewCount");
            item.setReviewCount(rs.wasNull() ? 0 : reviewCount);

            // 这些字段设为null
            item.setDatePublished(null);
            item.setRecipeCategory(null);

            return item;
        }
    }

}
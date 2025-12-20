package io.sustc.service.impl;

import io.sustc.dto.*;
import io.sustc.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class UserServiceImpl implements UserService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    public long register(RegisterUserReq req) {
        // 1. 修正：必填字段校验（修复性别匹配+空值校验）
        if (!StringUtils.hasText(req.getName()) || // 用户名非空
                req.getGender() == null || // 性别枚举非空
                !Arrays.asList(RegisterUserReq.Gender.MALE, RegisterUserReq.Gender.FEMALE).contains(req.getGender()) || // 枚举值匹配
                !StringUtils.hasText(req.getBirthday()) || // 生日非空
                !StringUtils.hasText(req.getPassword())) { // 密码非空
            log.error("注册失败：有必填字段未填（用户名/性别/生日/密码不能为空）");
            return -1;
        }

        // 2. 修正：用户名唯一性校验（字段名统一为AuthorName）
        Boolean nameExists = jdbcTemplate.queryForObject(
                "SELECT EXISTS (SELECT 1 FROM users WHERE AuthorName = ?)",
                Boolean.class,
                req.getName()
        );
        if (Boolean.TRUE.equals(nameExists)) { // 更安全的空值判断
            log.error("注册失败：用户名{}已存在", req.getName());
            return -1;
        }

        // 3. 生日转年龄（关键：补充Age字段）
        int age = 0;
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            LocalDate birthDate = LocalDate.parse(req.getBirthday(), formatter);
            age = Period.between(birthDate, LocalDate.now()).getYears();
        } catch (Exception e) {
            log.error("生日格式错误，无法转换为年龄：{}", req.getBirthday());
            return -1;
        }

        // 4. 生成唯一AuthorId（避免重复，优化随机数逻辑）
        long authorId;
        Boolean idExists;
        do {
            authorId = new Random().nextLong(1000000);
            // 检查AuthorId是否已存在
            idExists = jdbcTemplate.queryForObject(
                    "SELECT EXISTS (SELECT 1 FROM users WHERE AuthorId = ?)",
                    Boolean.class,
                    authorId
            );
        } while (Boolean.TRUE.equals(idExists)); // 确保ID唯一

        // 5. 修正SQL：字段匹配（去掉多余的Age？或用计算后的age）
        // 注意：原SQL中的Age字段来自生日转换，Gender用枚举的字符串（统一大小写）
        String sql = "INSERT INTO users (AuthorId, AuthorName, Gender, Age, Followers, Following, Password) " +
                "VALUES (?, ?, ?, ?, 0, 0, ?)";
        jdbcTemplate.update(sql,
                authorId,
                req.getName(),
                req.getGender().toString(),
                age, // 生日计算的年龄
                req.getPassword() // 实际项目需用BCrypt加密：BCrypt.hashpw(req.getPassword(), BCrypt.gensalt())
        );

        return authorId;
    }

    @Override
    public long login(AuthInfo auth) {
        if (auth == null || auth.getAuthorId() <= 0 || !StringUtils.hasText(auth.getPassword())) {
            return -1;
        }

        try {
            // 验证用户存在、未删除且密码匹配（实际项目需密码加密校验）
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
        // 验证用户
        long loginUserId = login(auth);
        if (loginUserId == -1) {
            throw new SecurityException("Invalid or inactive user");
        }

        // 验证操作目标是自己
        if (loginUserId != userId) {
            throw new SecurityException("Only can delete your own account");
        }

        // 验证用户存在且活跃
        Boolean isActive = jdbcTemplate.queryForObject(
                "SELECT IsDeleted FROM users WHERE AuthorId = ?",
                Boolean.class,
                userId
        );
        if (isActive == null || isActive) {
            return false; // 已删除
        }

        // 软删除用户
        jdbcTemplate.update("UPDATE users SET IsDeleted = TRUE WHERE AuthorId = ?", userId);

        // 移除所有关注关系
        jdbcTemplate.update("DELETE FROM user_follows WHERE FollowerId = ? OR FollowingId = ?", userId, userId);

        return true;
    }

    @Override
    public boolean follow(AuthInfo auth, long followeeId) {
        // 验证用户
        long followerId = login(auth);
        if (followerId == -1) {
            throw new SecurityException("Invalid or inactive user");
        }

        // 验证不能关注自己
        if (followerId == followeeId) {
            throw new SecurityException("Cannot follow yourself");
        }

        // 验证被关注者存在
        Boolean followeeExists = jdbcTemplate.queryForObject(
                "SELECT EXISTS (SELECT 1 FROM users WHERE AuthorId = ? AND IsDeleted = FALSE)",
                Boolean.class,
                followeeId
        );
        if (followeeExists == null || !followeeExists) {
            return false;
        }

        // 切换关注状态
        Boolean isFollowing = jdbcTemplate.queryForObject(
                "SELECT EXISTS (SELECT 1 FROM user_follows WHERE FollowerId = ? AND FollowingId = ?)",
                Boolean.class,
                followerId,
                followeeId
        );

        if (isFollowing != null && isFollowing) {
            // 取消关注
            jdbcTemplate.update("DELETE FROM user_follows WHERE FollowerId = ? AND FollowingId = ?", followerId, followeeId);
            // 更新关注数
            jdbcTemplate.update("UPDATE users SET Following = Following - 1 WHERE AuthorId = ?", followerId);
            jdbcTemplate.update("UPDATE users SET Followers = Followers - 1 WHERE AuthorId = ?", followeeId);
            return false;
        } else {
            // 关注
            jdbcTemplate.update("INSERT INTO user_follows (FollowerId, FollowingId) VALUES (?, ?)", followerId, followeeId);
            // 更新关注数
            jdbcTemplate.update("UPDATE users SET Following = Following + 1 WHERE AuthorId = ?", followerId);
            jdbcTemplate.update("UPDATE users SET Followers = Followers + 1 WHERE AuthorId = ?", followeeId);
            return true;
        }
    }


    @Override
    public UserRecord getById(long userId) {
        try {
            return jdbcTemplate.queryForObject(
                    "SELECT AuthorId, AuthorName, Gender, Age, Followers, Following FROM users WHERE AuthorId = ?",
                    new UserRowMapper(),
                    userId
            );
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @Override
    public void updateProfile(AuthInfo auth, String gender, Integer age) {
        // 验证用户
        long userId = login(auth);
        if (userId == -1) {
            throw new SecurityException("Invalid or inactive user");
        }

        // 构建更新SQL
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
            return; // 无更新内容
        }

        // 移除末尾逗号并添加WHERE条件
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

        // 构建查询SQL
        StringBuilder sql = new StringBuilder(
                "SELECT r.RecipeId, r.Name, r.AuthorId, u.AuthorName, r.DatePublished, r.RecipeCategory " +
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

        // 查询结果
        List<FeedItem> items = jdbcTemplate.query(sql.toString(), new FeedItemRowMapper(), params.toArray());

        // 总条数
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

    private static class UserRowMapper implements RowMapper<UserRecord> {
        @Override
        public UserRecord mapRow(ResultSet rs, int rowNum) throws SQLException {
            UserRecord record = new UserRecord();
            record.setAuthorId(rs.getLong("AuthorId"));
            record.setAuthorName(rs.getString("AuthorName"));
            record.setGender(rs.getString("Gender"));
            record.setAge(rs.getInt("Age"));
            record.setFollowers(rs.getInt("Followers"));
            record.setFollowing(rs.getInt("Following"));
            return record;
        }
    }

    // 自定义RowMapper：将ResultSet映射为FeedItem
    private static class FeedItemRowMapper implements RowMapper<FeedItem> {
        @Override
        public FeedItem mapRow(ResultSet rs, int rowNum) throws SQLException {
            FeedItem item = new FeedItem();
            item.setRecipeId(rs.getLong("RecipeId"));
            item.setName(rs.getString("Name"));
            item.setAuthorId(rs.getLong("AuthorId"));
            item.setAuthorName(rs.getString("AuthorName"));
            item.setDatePublished(rs.getTimestamp("DatePublished"));
            item.setCategory(rs.getString("RecipeCategory"));
            return item;
        }
    }

}
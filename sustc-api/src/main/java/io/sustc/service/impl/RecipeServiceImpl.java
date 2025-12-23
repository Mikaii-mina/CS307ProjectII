package io.sustc.service.impl;

import io.sustc.dto.*;
import io.sustc.service.RecipeService;
import io.sustc.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import java.sql.*;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.*;

@Service
@Slf4j
public class RecipeServiceImpl implements RecipeService {
    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private UserService userService;

    @Override
    public String getNameFromID(long id) {
        try {
            return jdbcTemplate.queryForObject(
                    "SELECT Name FROM recipes WHERE RecipeId = ? AND EXISTS (SELECT 1 FROM users WHERE AuthorId = recipes.AuthorId AND IsDeleted = FALSE)",
                    String.class,
                    id
            );
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @Override
    public RecipeRecord getRecipeById(long recipeId) {
        if (recipeId <= 0) {
            throw new IllegalArgumentException("Recipe ID must be positive");
        }
        String sql = "SELECT " +
                "r.RecipeId, " +
                "r.Name, " +
                "r.AuthorId, " +
                "u.AuthorName, " +
                "r.CookTime, " +
                "r.PrepTime, " +
                "r.TotalTime, " +
                "r.DatePublished, " +
                "r.Description, " +
                "r.RecipeCategory, " +
                "r.AggregatedRating, " +
                "r.ReviewCount, " +
                "r.Calories, " +
                "r.FatContent, " +
                "r.SaturatedFatContent, " +
                "r.CholesterolContent, " +
                "r.SodiumContent, " +
                "r.CarbohydrateContent, " +
                "r.FiberContent, " +
                "r.SugarContent, " +
                "r.ProteinContent, " +
                "r.RecipeServings, " +
                "r.RecipeYield " +
                "FROM recipes r " +
                "JOIN users u ON r.AuthorId = u.AuthorId " +
                "WHERE r.RecipeId = ? AND u.IsDeleted = FALSE";
        try {
            return jdbcTemplate.queryForObject(sql, new RecipeRowMapper(jdbcTemplate), recipeId);
        } catch (EmptyResultDataAccessException e) {
//            log.warn("Recipe not found with id: {}", recipeId);
            return null;
        }
    }

    @Override
    public PageResult<RecipeRecord> searchRecipes(String keyword, String category, Double minRating,
                                                  Integer page, Integer size, String sort) {
        if (page < 1 || size <= 0) {
            throw new IllegalArgumentException("Invalid page or size");
        }

        StringBuilder sql = new StringBuilder(
                "SELECT r.*, u.AuthorName FROM recipes r " +
                        "JOIN users u ON r.AuthorId = u.AuthorId " +
                        "WHERE (u.IsDeleted IS NULL OR u.IsDeleted = FALSE) AND 1=1 "
        );

        List<Object> params = new ArrayList<>();

        if (StringUtils.hasText(keyword)) {
            sql.append("AND (r.Name ILIKE ? OR r.Description ILIKE ?) ");
            String likeParam = "%" + keyword + "%";
            params.add(likeParam);
            params.add(likeParam);
        }

        if (StringUtils.hasText(category)) {
            sql.append("AND r.RecipeCategory = ? ");
            params.add(category);
        }

        if (minRating != null) {
            sql.append("AND r.AggregatedRating >= ? ");
            params.add(minRating);
        }

        sql.append("ORDER BY ");
        if ("rating_desc".equals(sort)) {
            sql.append("r.AggregatedRating DESC NULLS LAST, r.RecipeId DESC ");
        } else if ("rating_asc".equals(sort)) {
            sql.append("r.AggregatedRating ASC NULLS LAST, r.RecipeId ASC ");
        } else if ("date_desc".equals(sort)) {
            sql.append("r.DatePublished DESC, r.RecipeId DESC ");
        } else if ("date_asc".equals(sort)) {
            sql.append("r.DatePublished ASC, r.RecipeId ASC ");
        } else if ("calories_asc".equals(sort)) {
            sql.append("r.Calories ASC NULLS LAST, r.RecipeId ASC ");
        } else if ("calories_desc".equals(sort)) {
            sql.append("r.Calories DESC NULLS LAST, r.RecipeId DESC ");
        } else {
            sql.append("r.RecipeId DESC ");
        }

        // 分页
        int offset = (page - 1) * size;
        sql.append("LIMIT ? OFFSET ?");
        params.add(size);
        params.add(offset);

        List<RecipeRecord> records = jdbcTemplate.query(sql.toString(), new RecipeRowMapper(jdbcTemplate), params.toArray());

        for (RecipeRecord record : records) {
            String ingredientSql = "SELECT IngredientPart FROM recipe_ingredients WHERE RecipeId = ? ";
            List<String> ingredients = jdbcTemplate.query(
                    ingredientSql,
                    (rs, rowNum) -> rs.getString("IngredientPart"),
                    record.getRecipeId()
            );
            record.setRecipeIngredientParts(ingredients.toArray(new String[0]));
        }

        StringBuilder countSql = new StringBuilder(
                "SELECT COUNT(*) FROM recipes r " +
                        "JOIN users u ON r.AuthorId = u.AuthorId " +
                        "WHERE (u.IsDeleted IS NULL OR u.IsDeleted = FALSE) AND 1=1 "
        );

        List<Object> countParams = new ArrayList<>();

        if (StringUtils.hasText(keyword)) {
            countSql.append("AND (r.Name ILIKE ? OR r.Description ILIKE ?) ");
            countParams.add("%" + keyword + "%");
            countParams.add("%" + keyword + "%");
        }

        if (StringUtils.hasText(category)) {
            countSql.append("AND r.RecipeCategory = ? ");
            countParams.add(category);
        }

        if (minRating != null) {
            countSql.append("AND r.AggregatedRating >= ? ");
            countParams.add(minRating);
        }

        long total = jdbcTemplate.queryForObject(countSql.toString(), Long.class, countParams.toArray());

        return new PageResult<>(records, page, size, total);
    }

    @Override
    public long createRecipe(RecipeRecord dto, AuthInfo auth) {
        long userId = userService.login(auth);
        if (userId == -1) {
            throw new SecurityException("Invalid or inactive user");
        }

        if (!StringUtils.hasText(dto.getName())) {
            throw new IllegalArgumentException("Recipe name cannot be empty");
        }

        String maxIdSql = "SELECT COALESCE(MAX(RecipeId), 0) FROM recipes";
        Long maxId = jdbcTemplate.queryForObject(maxIdSql, Long.class);
        long recipeId = maxId + 1;

        String sql = "INSERT INTO recipes (RecipeId, Name, AuthorId, CookTime, PrepTime, TotalTime, " +
                "DatePublished, Description, RecipeCategory, AggregatedRating, ReviewCount, " +
                "Calories, FatContent, SaturatedFatContent, CholesterolContent, SodiumContent, " +
                "CarbohydrateContent, FiberContent, SugarContent, ProteinContent, " +
                "RecipeServings, RecipeYield) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        jdbcTemplate.update(sql,
                recipeId,
                dto.getName(),
                userId,
                dto.getCookTime(),
                dto.getPrepTime(),
                dto.getTotalTime(),
                dto.getDatePublished() != null ?
                        new java.sql.Timestamp(dto.getDatePublished().getTime()) :
                        null,
                dto.getDescription(),
                dto.getRecipeCategory(),
                dto.getAggregatedRating(),
                0,
                dto.getCalories(),
                dto.getFatContent(),
                dto.getSaturatedFatContent(),
                dto.getCholesterolContent(),
                dto.getSodiumContent(),
                dto.getCarbohydrateContent(),
                dto.getFiberContent(),
                dto.getSugarContent(),
                dto.getProteinContent(),
                dto.getRecipeServings(),
                dto.getRecipeYield()
        );

        String[] ingredientParts = dto.getRecipeIngredientParts();
        if (ingredientParts != null && ingredientParts.length > 0) {
            String ingredientSql = "INSERT INTO recipe_ingredients (RecipeId, IngredientPart) VALUES (?, ?)";
            for (String ingredient : ingredientParts) {
                jdbcTemplate.update(ingredientSql, recipeId, ingredient);
            }
        }

        return recipeId;
    }

    @Override
    public void deleteRecipe(long recipeId, AuthInfo auth) {
        long userId = userService.login(auth);
        if (userId == -1) {
            throw new SecurityException("Invalid or inactive user");
        }

        Boolean isAuthor = jdbcTemplate.queryForObject(
                "SELECT EXISTS (SELECT 1 FROM recipes WHERE RecipeId = ? AND AuthorId = ?)",
                Boolean.class,
                recipeId,
                userId
        );
        if (isAuthor == null || !isAuthor) {
            throw new SecurityException("Only the author can delete the recipe");
        }
        jdbcTemplate.update("DELETE FROM review_likes WHERE ReviewId IN (SELECT ReviewId FROM reviews WHERE RecipeId = ?)", recipeId);
        jdbcTemplate.update("DELETE FROM reviews WHERE RecipeId = ?", recipeId);
        jdbcTemplate.update("DELETE FROM recipe_ingredients WHERE RecipeId = ?", recipeId);
        jdbcTemplate.update("DELETE FROM recipes WHERE RecipeId = ?", recipeId);
    }

    @Override
    public void updateTimes(AuthInfo auth, long recipeId, String cookTimeIso, String prepTimeIso) {
        long userId = userService.login(auth);
        if (userId == -1) {
            throw new SecurityException("Invalid or inactive user");
        }
        Boolean isAuthor = jdbcTemplate.queryForObject(
                "SELECT EXISTS (SELECT 1 FROM recipes WHERE RecipeId = ? AND AuthorId = ?)",
                Boolean.class,
                recipeId,
                userId
        );
        if (isAuthor == null || !isAuthor) {
            throw new SecurityException("Only the author can update the recipe");
        }
        Duration cookDuration = null;
        if (cookTimeIso != null) {
            try {
                cookDuration = Duration.parse(cookTimeIso);
                if (cookDuration.isNegative()) {
                    throw new IllegalArgumentException("Invalid cook time: negative duration");
                }
            } catch (DateTimeParseException e) {
                throw new IllegalArgumentException("Invalid cook time ISO format");
            }
        }

        Duration prepDuration = null;
        if (prepTimeIso != null) {
            try {
                prepDuration = Duration.parse(prepTimeIso);
                if (prepDuration.isNegative()) {
                    throw new IllegalArgumentException("Invalid prep time: negative duration");
                }
            } catch (DateTimeParseException e) {
                throw new IllegalArgumentException("Invalid prep time ISO format");
            }
        }
        Duration totalDuration = Duration.ZERO;
        if (cookDuration != null) totalDuration = totalDuration.plus(cookDuration);
        if (prepDuration != null) totalDuration = totalDuration.plus(prepDuration);
        String totalTimeIso = totalDuration.toString();

        StringBuilder sql = new StringBuilder("UPDATE recipes SET ");
        List<Object> params = new ArrayList<>();
        if (cookTimeIso != null) {
            sql.append("CookTime = ?, ");
            params.add(cookTimeIso);
        }
        if (prepTimeIso != null) {
            sql.append("PrepTime = ?, ");
            params.add(prepTimeIso);
        }
        sql.append("TotalTime = ? WHERE RecipeId = ?");
        params.add(totalTimeIso);
        params.add(recipeId);

        jdbcTemplate.update(sql.toString(), params.toArray());
    }

    @Override
    public Map<String, Object> getClosestCaloriePair() {
        String sql = "WITH ranked_recipes AS (" +
                "    SELECT RecipeId, Calories, " +
                "           LAG(RecipeId) OVER (ORDER BY Calories, RecipeId) AS prev_id, " +
                "           LAG(Calories) OVER (ORDER BY Calories, RecipeId) AS prev_cal " +
                "    FROM recipes " +
                "    WHERE Calories IS NOT NULL" +
                ") " +
                "SELECT " +
                "    LEAST(RecipeId, prev_id) AS recipe_a, " +
                "    GREATEST(RecipeId, prev_id) AS recipe_b, " +
                "    LEAST(Calories, prev_cal) AS cal_a, " +
                "    GREATEST(Calories, prev_cal) AS cal_b, " +
                "    ABS(Calories - prev_cal) AS diff " +
                "FROM ranked_recipes " +
                "WHERE prev_id IS NOT NULL " +
                "ORDER BY diff ASC, recipe_a ASC, recipe_b ASC " +
                "LIMIT 1";

        try {
            return jdbcTemplate.queryForObject(sql, new RowMapper<Map<String, Object>>() {
                @Override
                public Map<String, Object> mapRow(ResultSet rs, int rowNum) throws SQLException {
                    Map<String, Object> result = new HashMap<>();
                    result.put("RecipeA", rs.getLong("recipe_a"));
                    result.put("RecipeB", rs.getLong("recipe_b"));
                    result.put("CaloriesA", rs.getDouble("cal_a"));
                    result.put("CaloriesB", rs.getDouble("cal_b"));
                    result.put("Difference", rs.getDouble("diff"));
                    return result;
                }
            });
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @Override
    public List<Map<String, Object>> getTop3MostComplexRecipesByIngredients() {
        String sql = "SELECT " +
                "    r.RecipeId, " +
                "    r.Name, " +
                "    COUNT(ri.IngredientPart) AS IngredientCount " +
                "FROM recipes r " +
                "JOIN recipe_ingredients ri ON r.RecipeId = ri.RecipeId " +
                "GROUP BY r.RecipeId, r.Name " +
                "ORDER BY IngredientCount DESC, r.RecipeId ASC " +
                "LIMIT 3";

        return jdbcTemplate.query(sql, new RowMapper<Map<String, Object>>() {
            @Override
            public Map<String, Object> mapRow(ResultSet rs, int rowNum) throws SQLException {
                Map<String, Object> result = new HashMap<>();
                result.put("RecipeId", rs.getLong("RecipeId"));
                result.put("Name", rs.getString("Name"));
                result.put("IngredientCount", rs.getInt("IngredientCount"));
                return result;
            }
        });
    }

    private static class RecipeRowMapper implements RowMapper<RecipeRecord> {
        private JdbcTemplate jdbcTemplate;

        public RecipeRowMapper(JdbcTemplate jdbcTemplate) {
            this.jdbcTemplate = jdbcTemplate;
        }

        @Override
        public RecipeRecord mapRow(ResultSet rs, int rowNum) throws SQLException {
            RecipeRecord record = new RecipeRecord();
            record.setRecipeId(rs.getLong("RecipeId"));
            record.setName(rs.getString("Name"));
            record.setAuthorId(rs.getLong("AuthorId"));
            record.setAuthorName(rs.getString("AuthorName"));
            record.setCookTime(rs.getString("CookTime"));
            record.setPrepTime(rs.getString("PrepTime"));
            record.setTotalTime(rs.getString("TotalTime"));
            record.setDatePublished(rs.getTimestamp("DatePublished"));
            record.setDescription(rs.getString("Description"));
            record.setRecipeCategory(rs.getString("RecipeCategory"));
            record.setAggregatedRating(rs.getFloat("AggregatedRating"));
            record.setReviewCount(rs.getInt("ReviewCount"));
            record.setCalories(rs.getFloat("Calories"));
            record.setFatContent(rs.getFloat("FatContent"));
            record.setSaturatedFatContent(rs.getFloat("SaturatedFatContent"));
            record.setCholesterolContent(rs.getFloat("CholesterolContent"));
            record.setSodiumContent(rs.getFloat("SodiumContent"));
            record.setCarbohydrateContent(rs.getFloat("CarbohydrateContent"));
            record.setFiberContent(rs.getFloat("FiberContent"));
            record.setSugarContent(rs.getFloat("SugarContent"));
            record.setProteinContent(rs.getFloat("ProteinContent"));
            record.setRecipeServings(rs.getInt("RecipeServings"));
            record.setRecipeYield(rs.getString("RecipeYield"));
            List<String> ingredients = this.jdbcTemplate.query(
                    "SELECT IngredientPart FROM recipe_ingredients WHERE RecipeId = ?",
                    (rs1, rowNum1) -> rs1.getString("IngredientPart"),
                    record.getRecipeId()
            );
            if (ingredients != null && !ingredients.isEmpty()) {
                record.setRecipeIngredientParts(ingredients.toArray(new String[0]));
            } else {
                record.setRecipeIngredientParts(new String[0]);
            }

            return record;
        }
    }
}
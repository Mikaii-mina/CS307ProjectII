package io.sustc.command;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.PageResult;
import io.sustc.dto.RecipeRecord;
import io.sustc.exception.RecipeNotFoundException;
import io.sustc.service.RecipeService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Slf4j
@ShellComponent
public class RecipeCommand {

    @Autowired
    private RecipeService recipeService;

    @ShellMethod(key = "recipe get", value = "Get recipe by ID")
    public RecipeRecord getRecipe(long recipeId) {
        try {
            // 调用业务层方法（即使业务层抛异常，这里也会捕获）
            return recipeService.getRecipeById(recipeId);
        } catch (IllegalArgumentException e) {
            // 非法ID提示（仅日志，不抛异常）
            log.error("Invalid recipe ID: {} - {}", recipeId, e.getMessage());
            return createEmptyRecipeRecord();
        } catch (RecipeNotFoundException e) {
            // 核心优化：仅打印「食谱不存在」的友好提示，不抛异常
            log.warn("Recipe not found with id: {}", recipeId); // 用warn级别更贴合“未找到”场景
            return createEmptyRecipeRecord(); // 返回空对象，保证后续代码能正常执行
        } catch (Exception e) {
            // 其他异常：打印详细日志，但仍返回空对象
            log.error("Unexpected error when getting recipe (recipeId: {})", recipeId, e);
            return createEmptyRecipeRecord();
        }
    }

    /**
     * 创建空但合法的RecipeRecord（所有字段有默认值，无null）
     * 与业务层的空对象逻辑保持一致，避免重复代码
     */
    private RecipeRecord createEmptyRecipeRecord() {
        final long DEFAULT_ID = 0L;
        final String DEFAULT_STRING = "";
        final float DEFAULT_FLOAT = 0.0f;
        final int DEFAULT_INT = 0;
        final String DEFAULT_DURATION_ISO = "PT0S";
        final Timestamp DEFAULT_TIMESTAMP = new Timestamp(Instant.EPOCH.toEpochMilli());
        final String[] DEFAULT_STRING_ARRAY = new String[0];
        RecipeRecord emptyRecord = new RecipeRecord();
        emptyRecord.setRecipeId(DEFAULT_ID);
        emptyRecord.setName(DEFAULT_STRING);
        emptyRecord.setAuthorId(DEFAULT_ID);
        emptyRecord.setAuthorName(DEFAULT_STRING);
        emptyRecord.setCookTime(DEFAULT_DURATION_ISO);
        emptyRecord.setPrepTime(DEFAULT_DURATION_ISO);
        emptyRecord.setTotalTime(DEFAULT_DURATION_ISO);
        emptyRecord.setDatePublished(DEFAULT_TIMESTAMP);
        emptyRecord.setDescription(DEFAULT_STRING);
        emptyRecord.setRecipeCategory(DEFAULT_STRING);
        emptyRecord.setRecipeIngredientParts(DEFAULT_STRING_ARRAY);
        emptyRecord.setAggregatedRating(DEFAULT_FLOAT);
        emptyRecord.setReviewCount(DEFAULT_INT);
        emptyRecord.setCalories(DEFAULT_FLOAT);
        emptyRecord.setFatContent(DEFAULT_FLOAT);
        emptyRecord.setSaturatedFatContent(DEFAULT_FLOAT);
        emptyRecord.setCholesterolContent(DEFAULT_FLOAT);
        emptyRecord.setSodiumContent(DEFAULT_FLOAT);
        emptyRecord.setCarbohydrateContent(DEFAULT_FLOAT);
        emptyRecord.setFiberContent(DEFAULT_FLOAT);
        emptyRecord.setSugarContent(DEFAULT_FLOAT);
        emptyRecord.setProteinContent(DEFAULT_FLOAT);
        emptyRecord.setRecipeServings(DEFAULT_INT);
        emptyRecord.setRecipeYield(DEFAULT_STRING);
        return emptyRecord;
    }

    @ShellMethod(key = "recipe search", value = "Search recipes with filters")
    public PageResult<RecipeRecord> searchRecipes(
            String keyword,
            String category,
            Double minRating,
            int page,
            int size,
            String sort
    ) {
        try {
            return recipeService.searchRecipes(keyword, category, minRating, page, size, sort);
        } catch (IllegalArgumentException e) {
            log.error("Search failed: {}", e.getMessage());
            return PageResult.<RecipeRecord>builder()
                    .items(Collections.emptyList())
                    .page(page)
                    .size(size)
                    .total(0)
                    .build();
        }
    }

    @ShellMethod(key = "recipe create", value = "Create new recipe")
    public long createRecipe(RecipeRecord dto, long authorId, String password) {
        try {
            AuthInfo auth = new AuthInfo(authorId, password);
            return recipeService.createRecipe(dto, auth);
        } catch (Exception e) {
            log.error("Create recipe failed: {}", e.getMessage());
            return -1;
        }
    }

    @ShellMethod(key = "recipe delete", value = "Delete recipe by ID")
    public void deleteRecipe(long recipeId, long authorId, String password) {
        try {
            AuthInfo auth = new AuthInfo(authorId, password);
            recipeService.deleteRecipe(recipeId, auth);
            log.info("Recipe {} deleted successfully", recipeId);
        } catch (Exception e) {
            log.error("Delete recipe failed: {}", e.getMessage());
        }
    }

    @ShellMethod(key = "recipe update-times", value = "Update recipe times")
    public void updateTimes(long recipeId, long authorId, String password, String cookTimeIso, String prepTimeIso) {
        try {
            AuthInfo auth = new AuthInfo(authorId, password);
            recipeService.updateTimes(auth, recipeId, cookTimeIso, prepTimeIso);
            log.info("Recipe {} times updated successfully", recipeId);
        } catch (Exception e) {
            log.error("Update times failed: {}", e.getMessage());
        }
    }

    @ShellMethod(key = "recipe closest-calories", value = "Get closest calorie pair")
    public Map<String, Object> getClosestCaloriePair() {
        return recipeService.getClosestCaloriePair();
    }

    @ShellMethod(key = "recipe top3-ingredients", value = "Get top 3 complex recipes by ingredients")
    public List<Map<String, Object>> getTop3MostComplexRecipes() {
        return recipeService.getTop3MostComplexRecipesByIngredients();
    }
}
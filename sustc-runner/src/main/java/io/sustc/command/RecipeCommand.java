package io.sustc.command;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.PageResult;
import io.sustc.dto.RecipeRecord;
import io.sustc.service.RecipeService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
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
        return recipeService.getRecipeById(recipeId);
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
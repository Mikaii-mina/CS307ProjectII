package io.sustc.exception;

public class RecipeNotFoundException extends RuntimeException {
    public RecipeNotFoundException(long recipeId) {
        super("Recipe not found with id: " + recipeId); // 明确错误信息
    }
}

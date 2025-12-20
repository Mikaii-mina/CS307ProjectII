package io.sustc.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * The recipe record used for data import
 *
 * @implNote You may implement your own {@link java.lang.Object#toString()} since the default one in {@link lombok.Data} prints all array values.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RecipeRecord implements Serializable {

    /**
     * The id of recipe, unique
     */
    private long RecipeId;

    /**
     * The name of recipe
     */
    private String name;

    /**
     * The id of this recipe's author
     */
    private long authorId;

    /**
     * The name of this recipe's author
     */
    private String authorName;

    /**
     * Cooking operation time in ISO 8601 duration format (e.g., PT1H30M for one hour thirty minutes)
     */
    private String cookTime;

    /**
     * Preparation time in ISO 8601 duration format
     */
    private String prepTime;

    /**
     * Total time (CookTime + PrepTime) in ISO 8601 duration format
     */
    private String totalTime;

    /**
     * The release time of this recipe
     */
    private Timestamp datePublished;

    /**
     * Description created by author
     */
    private String description;

    /**
     * The category of this recipe belong to
     */
    private String recipeCategory;

    /**
     * Ingredients composition of this recipe
     *
     * <p>The ingredient parts <b>must be sorted</b> in
     * <b>case-insensitive lexicographical order</b>
     * (i.e., ordering is determined by {@code String::compareToIgnoreCase}).</p>
     */
    private String[] recipeIngredientParts;

    /**
     * The score obtained of this recipe
     */
    private float aggregatedRating;

    /**
     * The reviewer's number of this recipe
     */
    private int reviewCount;

    /**
     * Calories of this recipe
     */
    private float calories;

    /**
     * Fat content
     */
    private float fatContent;

    /**
     * Saturated fat content
     */
    private float saturatedFatContent;

    /**
     * Cholesterol content
     */
    private float cholesterolContent;

    /**
     * Sodium content
     */
    private float sodiumContent;

    /**
     * Carbohydrate content
     */
    private float carbohydrateContent;

    /**
     * Fiber content
     */
    private float fiberContent;

    /**
     * Sugar content
     */
    private float sugarContent;

    /**
     * Protein content
     */
    private float proteinContent;

    /**
     * The number of people that the recipe can serve
     */
    private int recipeServings;

    /**
     * The output of the recipe (the quantity, weight or volume of the food)
     */
    private String recipeYield;

    /**
     * 获取食材列表
     * @return 不可修改的食材列表（避免外部修改影响内部数组），如果数组为null则返回空列表
     */
    public List<String> getIngredients() {
        if (recipeIngredientParts == null) {
            // 返回空列表而非null，避免空指针异常
            return Collections.emptyList();
        }
        // 将数组转换为不可修改的列表，防止外部修改内部数据
        return Collections.unmodifiableList(Arrays.asList(recipeIngredientParts));
    }

    /**
     * 设置食材列表
     * @param ingredients 食材列表（null则清空内部数组）
     */
    public void setIngredients(List<String> ingredients) {
        if (ingredients == null || ingredients.isEmpty()) {
            this.recipeIngredientParts = null;
        } else {
            // 将列表转换为数组存储
            this.recipeIngredientParts = ingredients.toArray(new String[0]);
        }
    }

}
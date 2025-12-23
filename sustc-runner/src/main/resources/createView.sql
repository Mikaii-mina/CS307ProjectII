--用户详细视图
CREATE OR REPLACE VIEW v_user_statistics AS
SELECT
    u.AuthorId,
    u.AuthorName,
    u.Gender,
    u.Age,
    u.Followers,
    u.Following,
    COUNT(DISTINCT r.RecipeId) AS recipe_count,
    COUNT(DISTINCT rv.ReviewId) AS review_count,
    COUNT(DISTINCT rl.ReviewId) AS like_count,
    MAX(r.DatePublished) AS last_recipe_date,
    MAX(rv.DateSubmitted) AS last_review_date
FROM users u
         LEFT JOIN recipes r ON u.AuthorId = r.AuthorId
         LEFT JOIN reviews rv ON u.AuthorId = rv.AuthorId
         LEFT JOIN review_likes rl ON rv.ReviewId = rl.ReviewId AND rl.AuthorId = u.AuthorId
WHERE u.IsDeleted = FALSE
GROUP BY u.AuthorId, u.AuthorName, u.Gender, u.Age, u.Followers, u.Following;

-- 食谱详细视图
CREATE OR REPLACE VIEW v_recipe_details AS
SELECT
    r.RecipeId,
    r.Name AS recipe_name,
    r.RecipeCategory,
    r.AggregatedRating,
    r.ReviewCount,
    r.Calories,
    r.FatContent,
    r.ProteinContent,
    r.DatePublished,
    u.AuthorId,
    u.AuthorName,
    u.Followers AS author_followers,
    COUNT(DISTINCT rv.ReviewId) AS total_reviews,
    COUNT(DISTINCT rl.AuthorId) AS total_likes,
    ARRAY_AGG(DISTINCT ri.IngredientPart) AS ingredients,
    STRING_AGG(DISTINCT ri.IngredientPart, ', ') AS ingredients_text
FROM recipes r
         JOIN users u ON r.AuthorId = u.AuthorId
         LEFT JOIN recipe_ingredients ri ON r.RecipeId = ri.RecipeId
         LEFT JOIN reviews rv ON r.RecipeId = rv.RecipeId
         LEFT JOIN review_likes rl ON rv.ReviewId = rl.ReviewId
WHERE u.IsDeleted = FALSE
GROUP BY r.RecipeId, r.Name, r.RecipeCategory, r.AggregatedRating,
         r.ReviewCount, r.Calories, r.FatContent, r.ProteinContent,
         r.DatePublished, u.AuthorId, u.AuthorName, u.Followers;

-- 热门食谱视图
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_hot_recipes AS
SELECT
    r.RecipeId,
    r.Name,
    u.AuthorName,
    r.RecipeCategory,
    r.AggregatedRating,
    COUNT(DISTINCT rv.ReviewId) AS total_reviews,
    r.AggregatedRating * 10 + COUNT(DISTINCT rv.ReviewId) AS hot_level
FROM recipes r
         JOIN users u ON r.AuthorId = u.AuthorId
         LEFT JOIN reviews rv ON r.RecipeId = rv.RecipeId
         LEFT JOIN review_likes rl ON rv.ReviewId = rl.ReviewId
WHERE u.IsDeleted = FALSE
GROUP BY r.RecipeId, r.Name, r.RecipeCategory, r.AggregatedRating,
    r.ReviewCount, r.DatePublished, u.AuthorName
HAVING COUNT(DISTINCT rv.ReviewId) >= 5
ORDER BY hot_level DESC
    LIMIT 100;

-- 健康食谱视图（低卡路里，高蛋白, 低脂肪）
CREATE OR REPLACE VIEW v_healthy_recipes AS
SELECT
    r.RecipeId,
    r.Name,
    r.RecipeCategory,
    r.Calories,
    r.ProteinContent,
    r.FatContent,
    r.CarbohydrateContent,
    r.AggregatedRating,
    ROUND(r.ProteinContent * 4.0 / NULLIF(r.Calories, 0) * 100, 2) AS protein_ratio
FROM recipes r
WHERE r.Calories <= 500
  AND r.ProteinContent >= 20
  AND r.FatContent <= 30
ORDER BY protein_ratio DESC, r.AggregatedRating DESC;

-- 评论分析视图
CREATE OR REPLACE VIEW v_review_analysis AS
SELECT
    rv.ReviewId,
    rv.Rating,
    rv.DateSubmitted,
    r.Name AS recipe_name,
    r.RecipeCategory,
    u.AuthorName AS reviewer_name,
    u.Followers AS reviewer_followers,
    COUNT(DISTINCT rl.AuthorId) AS like_count,
    LENGTH(rv.Review) AS review_length,
    CASE
        WHEN rv.Rating >= 4 THEN 'Positive'
        WHEN rv.Rating >= 3 THEN 'Neutral'
        ELSE 'Negative'
        END AS sentiment
FROM reviews rv
         JOIN recipes r ON rv.RecipeId = r.RecipeId
         JOIN users u ON rv.AuthorId = u.AuthorId
         LEFT JOIN review_likes rl ON rv.ReviewId = rl.ReviewId
WHERE u.IsDeleted = FALSE
ORDER BY rv.DateSubmitted DESC;
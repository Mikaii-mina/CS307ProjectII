-- 粉丝最多用户
CREATE INDEX IF NOT EXISTS idx_users_followers
    ON users(Followers DESC);
-- 年龄最大用户
CREATE INDEX IF NOT EXISTS idx_users_age
    ON users(Age DESC);

-- 特定分类的食谱
CREATE INDEX IF NOT EXISTS idx_recipes_category
    ON recipes(RecipeCategory);
-- 高分食谱
CREATE INDEX IF NOT EXISTS idx_recipes_rating
    ON recipes(AggregatedRating DESC);
-- 作者最新发布的食谱
CREATE INDEX IF NOT EXISTS idx_recipes_author_date
    ON recipes(AuthorId, DatePublished DESC);
-- 卡路里最低食谱
CREATE INDEX IF NOT EXISTS idx_recipes_calories
    ON recipes(Calories);
-- 高分最新食谱
CREATE INDEX IF NOT EXISTS idx_recipes_category_rating_date
    ON recipes(RecipeCategory, AggregatedRating DESC, DatePublished DESC);

-- 食谱最新评分
CREATE INDEX IF NOT EXISTS idx_reviews_recipe_rating
    ON reviews(RecipeId, Rating DESC);

-- 用户最新评论
CREATE INDEX IF NOT EXISTS idx_reviews_author_date
    ON reviews(AuthorId, DateSubmitted DESC);

-- 食谱最新评论
CREATE INDEX IF NOT EXISTS idx_reviews_recipe_date
    ON reviews(RecipeId, DateSubmitted DESC);

-- 按成分搜索食谱
CREATE INDEX IF NOT EXISTS idx_recipe_ingredients_part
    ON recipe_ingredients(IngredientPart);

-- 按用户查询（用户点赞历史）
CREATE INDEX IF NOT EXISTS idx_review_likes_author
    ON review_likes(AuthorId);

-- 查询用户的关注/粉丝
CREATE INDEX IF NOT EXISTS idx_user_follows_follower
    ON user_follows(FollowerId);

CREATE INDEX IF NOT EXISTS idx_user_follows_following
    ON user_follows(FollowingId);

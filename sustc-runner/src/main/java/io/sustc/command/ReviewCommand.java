package io.sustc.command;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.PageResult;
import io.sustc.dto.RecipeRecord;
import io.sustc.dto.ReviewRecord;
import io.sustc.service.ReviewService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;

import java.util.Collections;

@Slf4j
@ShellComponent
public class ReviewCommand {

    @Autowired
    private ReviewService reviewService;

    @ShellMethod(key = "review add", value = "Add new review")
    public long addReview(long authorId, String password, long recipeId, int rating, String review) {
        try {
            AuthInfo auth = new AuthInfo(authorId, password);
            return reviewService.addReview(auth, recipeId, rating, review);
        } catch (Exception e) {
            log.error("Add review failed: {}", e.getMessage());
            return -1;
        }
    }

    @ShellMethod(key = "review edit", value = "Edit existing review")
    public void editReview(long authorId, String password, long recipeId, long reviewId, int rating, String review) {
        try {
            AuthInfo auth = new AuthInfo(authorId, password);
            reviewService.editReview(auth, recipeId, reviewId, rating, review);
            log.info("Review {} edited successfully", reviewId);
        } catch (Exception e) {
            log.error("Edit review failed: {}", e.getMessage());
        }
    }

    @ShellMethod(key = "review delete", value = "Delete review")
    public void deleteReview(long authorId, String password, long recipeId, long reviewId) {
        try {
            AuthInfo auth = new AuthInfo(authorId, password);
            reviewService.deleteReview(auth, recipeId, reviewId);
            log.info("Review {} deleted successfully", reviewId);
        } catch (Exception e) {
            log.error("Delete review failed: {}", e.getMessage());
        }
    }

    @ShellMethod(key = "review like", value = "Like a review")
    public long likeReview(long userId, String password, long reviewId) {
        try {
            AuthInfo auth = new AuthInfo(userId, password);
            return reviewService.likeReview(auth, reviewId);
        } catch (Exception e) {
            log.error("Like review failed: {}", e.getMessage());
            return -1;
        }
    }

    @ShellMethod(key = "review unlike", value = "Unlike a review")
    public long unlikeReview(long userId, String password, long reviewId) {
        try {
            AuthInfo auth = new AuthInfo(userId, password);
            return reviewService.unlikeReview(auth, reviewId);
        } catch (Exception e) {
            log.error("Unlike review failed: {}", e.getMessage());
            return -1;
        }
    }

    @ShellMethod(key = "review list", value = "List reviews by recipe")
    public PageResult<ReviewRecord> listReviews(long recipeId, int page, int size, String sort) {
        try {
            return reviewService.listByRecipe(recipeId, page, size, sort);
        } catch (IllegalArgumentException e) {
            log.error("List reviews failed: {}", e.getMessage());
            return PageResult.<ReviewRecord>builder()
                    .items(Collections.emptyList())
                    .page(page)
                    .size(size)
                    .total(0)
                    .build();
        }
    }

    @ShellMethod(key = "review refresh-rating", value = "Refresh recipe aggregated rating")
    public RecipeRecord refreshRating(long recipeId) {
        try {
            return reviewService.refreshRecipeAggregatedRating(recipeId);
        } catch (IllegalArgumentException e) {
            log.error("Refresh rating failed: {}", e.getMessage());
            return null;
        }
    }
}
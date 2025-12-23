package io.sustc.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The review record used for data import
 * @implNote You may implement your own {@link java.lang.Object#toString()} since the default one in {@link lombok.Data} prints all array values.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ReviewRecord implements Serializable {

    /**
     * The id of this review, unique
     */
    private long reviewId;

    /**
     * The id of the reviewed recipe
     */
    private long recipeId;

    /**
     * The id of this review's author
     */
    private long authorId;

    /**
     * The name of this review's author
     */
    private String authorName;

    /**
     * The score given to this recipe
     */
    private float rating;

    /**
     * Review content
     */
    private String review;

    /**
     * The date of review submitted
     */
    private Timestamp dateSubmitted;

    /**
     * The date of review modified
     */
    private Timestamp dateModified;

    /**
     * List of users who have given this review a like
     */
    private long[] likes;

    /**
     * 设置点赞用户列表（将List<Long>转换为long[]存储）
     * @param likeUsers 点赞用户ID的列表
     */
    public void setLikeUsers(List<Long> likeUsers) {
        if (likeUsers == null) {
            this.likes = new long[0]; // 空列表时设为空数组
            return;
        }
        this.likes = likeUsers.stream()
                .mapToLong(Long::longValue)
                .toArray();
    }

    /**
     * 获取点赞用户列表（将long[]转换为List<Long>返回）
     * @return 点赞用户ID的列表
     */
    public List<Long> getLikeUsers() {
        if (likes == null) {
            return new ArrayList<>(); // 空数组时返回空列表
        }
        // 将long[]转换为List<Long>
        return Arrays.stream(likes)
                .boxed()
                .collect(Collectors.toList());
    }
}

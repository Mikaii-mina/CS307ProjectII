package io.sustc.command;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.FeedItem;
import io.sustc.dto.PageResult;
import io.sustc.dto.RegisterUserReq;
import io.sustc.dto.UserRecord;
import io.sustc.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;

import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Map;

@Slf4j
@ShellComponent
public class UserCommand {

    @Autowired
    private UserService userService;

    @ShellMethod(key = "user register", value = "Register new user (gender: Male/Female/Unknown, birthday: yyyy-MM-dd)")
    public long register(String name, String gender, String birthday, String password) {
        try {
            // 1. 转换性别为枚举类型（容错处理）
            RegisterUserReq.Gender genderEnum;
            try {
                genderEnum = RegisterUserReq.Gender.valueOf(gender.toUpperCase());
            } catch (IllegalArgumentException e) {
                log.warn("Invalid gender '{}', use UNKNOWN instead", gender);
                genderEnum = RegisterUserReq.Gender.UNKNOWN;
            }

            // 2. 构建RegisterUserReq（匹配真实字段）
            RegisterUserReq req = RegisterUserReq.builder()
                    .name(name)          // 修正：原authorName改为name
                    .gender(genderEnum)  // 修正：字符串转枚举
                    .birthday(birthday)  // 修正：age改为birthday字符串
                    .password(password)
                    .build();

            // 调用服务层（重点：打印req详情+异常栈）
            log.info("Start register user, req: {}", req); // 打印入参，确认参数正确
            long userId = userService.register(req);
            log.info("User register success, userId: {}", userId);
            return userId;
        } catch (Exception e) {
            log.error("User registration failed: {}", e.getMessage());
            return -1; // 注册失败返回-1
        }
    }

    // 其他方法（login/deleteAccount等）保持不变，以下仅保留核心修正后的register方法，其余省略
    @ShellMethod(key = "user login", value = "User login")
    public long login(long userId, String password) {
        AuthInfo auth = new AuthInfo(userId, password);
        return userService.login(auth);
    }

    @ShellMethod(key = "user delete", value = "Delete user account")
    public boolean deleteAccount(long operatorId, String password, long userId) {
        try {
            AuthInfo auth = new AuthInfo(operatorId, password);
            return userService.deleteAccount(auth, userId);
        } catch (Exception e) {
            log.error("Delete account failed: {}", e.getMessage());
            return false;
        }
    }

    @ShellMethod(key = "user follow", value = "Follow/unfollow user")
    public boolean follow(long userId, String password, long followeeId) {
        try {
            AuthInfo auth = new AuthInfo(userId, password);
            return userService.follow(auth, followeeId);
        } catch (Exception e) {
            log.error("Follow operation failed: {}", e.getMessage());
            return false;
        }
    }

    @ShellMethod(key = "user get", value = "Get user by ID")
    public UserRecord getUser(long userId) {
        return userService.getById(userId);
    }

    @ShellMethod(key = "user update", value = "Update user profile (gender: Male/Female/Unknown, birthday: yyyy-MM-dd)")
    public void updateProfile(long userId, String password, String gender, String birthday) {
        try {
            AuthInfo auth = new AuthInfo(userId, password);

            // 2. 生日转年龄（Integer）
            Integer age = null;
            if (birthday != null && !birthday.isBlank()) {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                LocalDate birthDate = LocalDate.parse(birthday, formatter);
                LocalDate now = LocalDate.now();
                age = Period.between(birthDate, now).getYears();
            }

            // 3. 调用updateProfile（传入Integer类型的age）
            // 注意：若updateProfile第二个参数是String类型gender，替换为gender；若是枚举则传genderEnum
            userService.updateProfile(auth, gender, age);
            log.info("Profile updated successfully");
        } catch (Exception e) {
            log.error("Update profile failed: {}", e.getMessage());
        }
    }

    @ShellMethod(key = "user feed", value = "Get user recipe feed")
    public PageResult<FeedItem> getFeed(long userId, String password, int page, int size, String category) {
        try {
            AuthInfo auth = new AuthInfo(userId, password);
            return userService.feed(auth, page, size, category);
        } catch (Exception e) {
            log.error("Get feed failed: {}", e.getMessage());
            return PageResult.<FeedItem>builder()
                    .items(Collections.emptyList())
                    .page(page)
                    .size(size)
                    .total(0)
                    .build();
        }
    }

    @ShellMethod(key = "user top-follow-ratio", value = "Get user with highest follow ratio")
    public Map<String, Object> getTopFollowRatioUser() {
        return userService.getUserWithHighestFollowRatio();
    }
}
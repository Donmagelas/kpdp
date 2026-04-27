package com.kpdp.utils;

import com.kpdp.dto.UserDTO;

/**
 * 当前线程用户容器。
 */
public final class UserHolder {

    private static final ThreadLocal<UserDTO> THREAD_LOCAL = new ThreadLocal<>();

    private UserHolder() {
    }

    /**
     * 保存用户到当前线程。
     *
     * @param user 用户信息
     */
    public static void saveUser(UserDTO user) {
        THREAD_LOCAL.set(user);
    }

    /**
     * 获取当前线程用户。
     *
     * @return 用户信息
     */
    public static UserDTO getUser() {
        return THREAD_LOCAL.get();
    }

    /**
     * 清理当前线程用户，防止线程复用污染。
     */
    public static void removeUser() {
        THREAD_LOCAL.remove();
    }
}

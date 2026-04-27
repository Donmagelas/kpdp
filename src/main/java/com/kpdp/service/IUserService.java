package com.kpdp.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.kpdp.dto.LoginFormDTO;
import com.kpdp.dto.Result;
import com.kpdp.entity.User;

/**
 * 用户服务，仅保留秒杀链路需要的登录能力。
 */
public interface IUserService extends IService<User> {

    /**
     * 通过手机号直接登录，返回 token。
     *
     * @param loginForm 登录参数
     * @return 登录结果
     */
    Result login(LoginFormDTO loginForm);

}

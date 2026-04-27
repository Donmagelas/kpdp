package com.kpdp.controller;

import com.kpdp.dto.LoginFormDTO;
import com.kpdp.dto.Result;
import com.kpdp.service.IUserService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import jakarta.annotation.Resource;

/**
 * 用户接口，只保留秒杀所需的登录入口。
 */
@RestController
@RequestMapping("/user")
public class UserController {

    @Resource
    private IUserService userService;

    /**
     * 仅通过手机号创建或复用用户，并下发登录 token。
     *
     * @param loginForm 登录参数
     * @return token
     */
    @PostMapping("/login")
    public Result login(@RequestBody LoginFormDTO loginForm) {
        return userService.login(loginForm);
    }

}

package com.kpdp.dto;

import lombok.Data;

/**
 * 简化后的登录参数。
 */
@Data
public class LoginFormDTO {

    /**
     * 手机号，用于识别用户身份。
     */
    private String phone;

}

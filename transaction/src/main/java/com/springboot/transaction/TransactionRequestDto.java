package com.springboot.transaction;

import lombok.Data;

@Data
public class TransactionRequestDto {

    private String fromUser;

    private String toUser;

    private int amount;

    private String purpose;

}

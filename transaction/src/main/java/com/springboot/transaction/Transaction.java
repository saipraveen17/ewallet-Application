package com.springboot.transaction;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;
import java.util.UUID;

@Entity
@Table(name="transactions")
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Transaction {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int id;

    private String fromUser;

    private String toUser;

    private int amount;

    @Column(unique = true)
    private String transactionId;

    @Enumerated(value = EnumType.STRING)
    private TransactionStatus  transactionStatus;

    private Date transactionDate;

    private String purpose;

}
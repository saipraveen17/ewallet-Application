package com.springboot.wallet;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class WalletService {

    @Autowired
    WalletRepository walletRepository;

    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;

    @Autowired
    ObjectMapper objectMapper;

    @KafkaListener(topics = "create_wallet", groupId = "group123")
    public void createWallet(String message) {

        //Create wallet using the username got from user service and set initial balance
        Wallet wallet = Wallet.builder().userName(message)
                                            .balance(100).build();
        walletRepository.save(wallet);

        System.out.println("Wallet created for username: "+message);
    }

    @KafkaListener(topics = "update_wallet", groupId = "group123")
    public void updateWallet(String message) throws JsonProcessingException {

        //Convert the given string into JsonObject.
        JSONObject jsonObject = objectMapper.readValue(message, JSONObject.class);

        //Extract info from JsonObject
        String sender = (String) jsonObject.get("fromUser");
        String receiver = (String) jsonObject.get("toUser");
        String transactionId = (String) jsonObject.get("transactionId");
        int transactionAmount = (Integer) jsonObject.get("amount");

        System.out.println(sender+" -- "+receiver+"-- "+transactionAmount+" -- "+transactionId);

        //Create return object and set the values
        JSONObject returnObject = new JSONObject();
        returnObject.put("transactionId", transactionId);

        Wallet senderWallet = walletRepository.findWalletByUserName(sender);
        Wallet receiverWallet = walletRepository.findWalletByUserName(receiver);

        //If required amount is not available set it as failed else success
        if(senderWallet.getBalance() >= transactionAmount) {

            //Sufficient balance
            returnObject.put("status", "SUCCESS");

            kafkaTemplate.send("update_transaction",objectMapper.writeValueAsString(returnObject));

            //Update the wallets

            senderWallet.setBalance(senderWallet.getBalance() - transactionAmount);
            walletRepository.save(senderWallet);

            receiverWallet.setBalance(receiverWallet.getBalance() + transactionAmount);
            walletRepository.save(receiverWallet);

        }
        else {

            //Insufficient balance
            returnObject.put("status", "FAILED");
            kafkaTemplate.send("update_transaction", objectMapper.writeValueAsString(returnObject));

            //No update of wallets
        }

    }
}

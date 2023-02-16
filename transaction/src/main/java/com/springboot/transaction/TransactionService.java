package com.springboot.transaction;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.util.Date;
import java.util.UUID;

@Service
public class TransactionService {

    @Autowired
    TransactionRepository transactionRepository;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;

    @Autowired
    RestTemplate restTemplate;

    public void createTransaction(TransactionRequestDto transactionRequest) throws JsonProcessingException {

        //Create transaction using Dto and set status as pending.
        Transaction transaction = Transaction.builder().fromUser(transactionRequest.getFromUser())
                                                            .toUser(transactionRequest.getToUser())
                                                                .amount(transactionRequest.getAmount())
                                                                    .purpose(transactionRequest.getPurpose())
                                                                        .transactionDate(new Date())
                                                                            .transactionId(UUID.randomUUID().toString())
                                                                                .transactionStatus(TransactionStatus.PENDING).build();
        //Save it to Db
        transactionRepository.save(transaction);

        //Create JSON object
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("fromUser",transaction.getFromUser());
        jsonObject.put("toUser", transaction.getToUser());
        jsonObject.put("amount",transaction.getAmount());
        jsonObject.put("transactionId", transaction.getTransactionId());

        //Convert JSON object to string and send to wallet service to carry the transaction further.
        String message = objectMapper.writeValueAsString(jsonObject);
        kafkaTemplate.send("update_wallet",message);

    }

    @KafkaListener(topics = "update_transaction",groupId = "group123")
    public void updateTransaction(String message) throws JsonProcessingException {

        //Convert the string to JSON object
        JSONObject transactionRequest = objectMapper.readValue(message, JSONObject.class);

        //Extract data from the JSON object
        String transactionStatus = (String) transactionRequest.get("status");
        String transactionId = (String) transactionRequest.get("transactionId");

        System.out.println("Reading the transaction Table Entries "+transactionStatus+"---"+transactionId);

        //Get the transaction and update the status and save the transaction
        Transaction transaction = transactionRepository.findByTransactionId(transactionId);
        transaction.setTransactionStatus(TransactionStatus.valueOf(transactionStatus));

        transactionRepository.save(transaction);

        //Send email notification after a transaction
        callNotificationService(transaction);

    }

    public void callNotificationService(Transaction transaction) {

        //Extract required data from transaction
        String fromUserName  = transaction.getFromUser();
        String toUserName = transaction.getToUser();
        String transactionId = transaction.getTransactionId();

        //Create HTTP request for sender
        URI uri = URI.create("http://localhost:9991/user/findEmailDto/"+fromUserName);
        HttpEntity httpEntity = new HttpEntity(new HttpHeaders());

        //Send the request and get the email Dto response in JSON object
        JSONObject fromUserObject = restTemplate.exchange(uri, HttpMethod.GET, httpEntity, JSONObject.class).getBody();

        String senderName = (String) fromUserObject.get("name");
        String senderEmail = (String) fromUserObject.get("email");

        //Similarly to receiver
        uri = URI.create("http://localhost:9991/user/findEmailDto/"+toUserName);

        JSONObject toUserObject = restTemplate.exchange(uri, HttpMethod.GET, httpEntity, JSONObject.class).getBody();

        String receiverName = (String) toUserObject.get("name");
        String receiverEmail = (String) toUserObject.get("email");

        //Create the JSON object with email address and email body to send it to email service
        JSONObject emailRequest = new JSONObject();

        emailRequest.put("email", senderEmail);

        String senderMessage = String.format("Hi %s \n"+"The transaction with transactionId %s has been %s, Rs. %d debited from your wallet.",
                senderName, transactionId, transaction.getTransactionStatus(), transaction.getAmount());

        emailRequest.put("message", senderMessage);

        //Convert the JSON object to string and send it through kafka
        String message = emailRequest.toString();

        kafkaTemplate.send("send_email",message);

        //If transaction is failed, we don't send the mail to receiver
        if(transaction.getTransactionStatus().equals("FAILED")) {
            return;
        }

        emailRequest.put("email", receiverEmail);

        String receiverMessage = String.format("Hi %s \n"+"You have received money of Rs. %d from %s, credited in your wallet",
                receiverName, transaction.getAmount(), senderName);

        emailRequest.put("message",receiverMessage);

        message = emailRequest.toString();

        kafkaTemplate.send("send_email",message);

    }
}

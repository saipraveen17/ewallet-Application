package com.springboot.notification;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.mail.MessagingException;
import jakarta.mail.internet.MimeMessage;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Service;

@Service
public class EmailService {

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    private JavaMailSender javaMailSender;

    @KafkaListener(topics = {"send_email"}, groupId = "group123")
    public void sendEmailMessage(String message) throws JsonProcessingException, MessagingException {

        //Decoding the message to JsonObject
        JSONObject emailRequest = objectMapper.readValue(message, JSONObject.class);

        //Get the mail and message from JsonObject
        String email = (String) emailRequest.get("email");
        String emailMessageBody = (String) emailRequest.get("message");

        //Create a mimeMessage and set the variables and send it.
        MimeMessage mimeMessage = javaMailSender.createMimeMessage();
        MimeMessageHelper mimeMessageHelper = new MimeMessageHelper(mimeMessage,true);

        mimeMessageHelper.setFrom("etransaction.notifications@gmail.com");
        mimeMessageHelper.setTo(email);
        mimeMessageHelper.setText(emailMessageBody);
        mimeMessageHelper.setSubject("Transaction Notification");

        javaMailSender.send(mimeMessage);
        System.out.println("Mail with attachment sent successfully...");

    }
}

package com.springboot.user;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Map;

@Service
public class UserService {

    @Autowired
    UserRepository userRepository;

    @Autowired
    RedisTemplate redisTemplate;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;

    public String addUser(UserRequestDto userRequest) {

        //Converting Dto to User Entity
        User user = User.builder().userName(userRequest.getUserName())
                                    .name(userRequest.getName())
                                        .age(userRequest.getAge())
                                            .email(userRequest.getEmail())
                                                .mobileNo(userRequest.getMobileNo()).build();
        //Save to DB
        userRepository.save(user);

        //Save to cache
        saveToCache(user);

        //Send create wallet message to wallet module for creation of wallet for this user
        kafkaTemplate.send("create_wallet", user.getUserName());

        return "User added Successfully";
    }

    private void saveToCache(User user) {

        //Convert the user then store it in redis
        Map map = objectMapper.convertValue(user, Map.class);

        String key = "USER_KEY"+user.getUserName();
        System.out.println("The user key is "+key);
        redisTemplate.opsForHash().putAll(key, map);

        //Set the expiry
        redisTemplate.expire(key, Duration.ofHours(12));

    }


    public User findUserByUserName(String userName) {

        //Get the data from redis
        Map map = redisTemplate.opsForHash().entries("USER_KEY"+userName);

        User user = null;

        //If not present retrieve from DB
        if(map==null||map.size()==0) {

            user = userRepository.findByUserName(userName);
            saveToCache(user);
            return user;
        }
        else {  //else convert the retrieved data

            user = objectMapper.convertValue(map, User.class);
            return user;
        }
    }

    public UserResponseDto findEmailAndNameDto(String userName){


        Map map = redisTemplate.opsForHash().entries("USER_KEY"+userName);

        User user = null;

        if(map==null||map.size()==0) {

            user = userRepository.findByUserName(userName);
            saveToCache(user);
        }
        else {

            user = objectMapper.convertValue(map, User.class);
        }

        //Create the response Dto from the user object and return it
        UserResponseDto userResponseDto = UserResponseDto.builder().email(user.getEmail()).name(user.getName()).build();

        return userResponseDto;
    }

}

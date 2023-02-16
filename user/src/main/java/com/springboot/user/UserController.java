package com.springboot.user;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/user")
public class UserController
{

    @Autowired
    UserService userService;

    @PostMapping("/add")
    public String createUser(@RequestBody()UserRequestDto userRequest){
        return userService.addUser(userRequest);
    }

    @GetMapping("/findByUser/{userName}")
    public User getUserByUserName(@PathVariable("userName")String userName){

        return userService.findUserByUserName(userName);
    }

    @GetMapping("/findEmailDto/{userName}")
    public UserResponseDto getEmailNameDto(@PathVariable("userName")String userName){

        return userService.findEmailAndNameDto(userName);

    }

}
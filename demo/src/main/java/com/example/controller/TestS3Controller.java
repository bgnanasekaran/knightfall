package com.example.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

@RestController
public class TestS3Controller {

    private Logger LOGGER = LoggerFactory.getLogger(TestS3Controller.class);

    @RequestMapping("/test")
    public void test(){
        LOGGER.info("this is a test app ");
    }

}

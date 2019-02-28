package com.example.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TestS3Controller {

    private Logger LOGGER = LoggerFactory.getLogger(TestS3Controller.class);

    @RequestMapping("/test")
    public void test() throws JsonProcessingException {
        LOGGER.info("this is a test app ");
    }
}

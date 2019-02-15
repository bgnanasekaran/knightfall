package com.example.controller;

import com.amazonaws.services.s3.AmazonS3;
import com.example.manager.S3CredentialsManager;
import com.example.manager.S3ServiceManager;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

@RestController
public class TransactionS3Controller {


    private S3CredentialsManager credentialsManager;

    private S3ServiceManager s3ServiceManager;

    private Logger LOGGER = LoggerFactory.getLogger(TransactionS3Controller.class);

    @RequestMapping(value = "/customer/transactions/{id}/{uuid}", method= RequestMethod.GET, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public String test2(@PathVariable String id, @PathVariable String uuid, @RequestHeader(value="file_format", required = false) String fileFormat, @RequestHeader(value="access_key", required = false) String accessKey, @RequestHeader(value="secret_key", required = false) String secretKey, @RequestHeader(value="bucket_name", required = false) String bucketName){
        LOGGER.debug("Fetching Transxns of loyalty id " + id + " with uuid " + uuid );
        final AmazonS3 s3Client = credentialsManager.getS3Client(accessKey, secretKey);
        return s3ServiceManager.get(s3Client, id, uuid, fileFormat, bucketName);
    }

    @RequestMapping(value = "/customer/transactions/{id}", method= RequestMethod.PUT, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public void test3(@PathVariable String id, @RequestBody JsonNode node, @RequestHeader(value="file_format", required = false) String fileFormat, @RequestHeader(value="access_key", required = false) String accessKey, @RequestHeader(value="secret_key", required = false) String secretKey, @RequestHeader(value="bucket_name", required = false) String bucketName){
        LOGGER.debug("adding Transxns of loyalty id " + id );
        final AmazonS3 s3 = credentialsManager.getS3Client(accessKey, secretKey);
        s3ServiceManager.create(s3, id, node, fileFormat, bucketName );
    }

    @Autowired
    public void setCredentialsManager(S3CredentialsManager credentialsManager) {
        this.credentialsManager = credentialsManager;
    }

    @Autowired
    public void setS3ServiceManager(S3ServiceManager s3ServiceManager) {
        this.s3ServiceManager = s3ServiceManager;
    }
}

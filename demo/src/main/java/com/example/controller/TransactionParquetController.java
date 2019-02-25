package com.example.controller;

import com.example.manager.S3ServiceManager;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

@RestController
public class TransactionParquetController {

    private Logger LOGGER = LoggerFactory.getLogger(TransactionParquetController.class);

    private S3ServiceManager serviceManager;

    @RequestMapping(value = "/online/transactions/{id}", method= RequestMethod.PUT, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public void test(@PathVariable String id, @RequestBody JsonNode node, @RequestHeader(value="access_key", required = false) String accessKey, @RequestHeader(value="secret_key", required = false) String secretKey, @RequestHeader(value="bucket_name", required = false) String bucketName) {
        LOGGER.info("Creating online transactions ");
        serviceManager.parquetCreate(id, node, accessKey, secretKey, bucketName);
    }

    @RequestMapping(value = "/online/transactions/{id}/{uuid}", method= RequestMethod.GET, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public String test2(@PathVariable String id, @PathVariable String uuid,  @RequestHeader(value="access_key", required = false) String accessKey, @RequestHeader(value="secret_key", required = false) String secretKey, @RequestHeader(value="bucket_name", required = false) String bucketName){
        LOGGER.debug("Fetching Transxns of loyalty id " + id + " with uuid " + uuid );
        JsonNode node =  serviceManager.parquetGet(id, uuid, accessKey, secretKey, bucketName);
        if(node != null){
            LOGGER.info(node.toString());
            return node.toString();
        }
        return null;
    }

    @Autowired
    public void setServiceManager(S3ServiceManager serviceManager) {
        this.serviceManager = serviceManager;
    }
}

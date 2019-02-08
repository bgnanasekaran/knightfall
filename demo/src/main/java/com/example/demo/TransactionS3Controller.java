package com.example.demo;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.io.*;

@RestController
public class TransactionS3Controller {

    Logger LOGGER = LoggerFactory.getLogger(TransactionS3Controller.class);

    @RequestMapping("/test")
    public void test(){
        LOGGER.info("this is a test app ");
        System.out.println("this is a test app ");
    }

    @RequestMapping(value = "/customer/transactions/{id}/{uuid}", method= RequestMethod.GET, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public String test2(@PathVariable String id, @PathVariable String uuid){
        LOGGER.info("Fetching Transxns of loyalty id " + id + " with uuid " + uuid );
        final AmazonS3 s3 = AmazonS3Client.builder().withCredentials(new DefaultAWSCredentialsProviderChain())
                .withRegion("us-east-1").build();

        StringBuilder stringBuilder = new StringBuilder();
        try {
            String fileName = id + "/"+ uuid + ".json";
            S3Object o = s3.getObject("*****", fileName);
            S3ObjectInputStream s3is = o.getObjectContent();
            BufferedReader reader = new BufferedReader(new InputStreamReader(s3is));
            String line = null;
            LOGGER.info("fetching Transxns in progress");
            while ((line = reader.readLine()) != null) {
                stringBuilder.append(line);
            }
            LOGGER.info("fetched Transxns successfully");
        } catch (AmazonServiceException e) {
            stringBuilder.append("{\n");
            stringBuilder.append('"'+ "error" + '"' );
            stringBuilder.append(":" );
            stringBuilder.append('"'+ e.getErrorMessage() + '"' + "\n" );
            stringBuilder.append("}");
        } catch (FileNotFoundException  e) {
            stringBuilder.append("{\n");
            stringBuilder.append('"'+ "error" + '"' );
            stringBuilder.append(":" );
            stringBuilder.append('"'+ e.getMessage() + '"' + "\n" );
            stringBuilder.append("}");
        } catch (IOException e) {
            stringBuilder.append("{\n");
            stringBuilder.append('"'+ "error" + '"' );
            stringBuilder.append(":" );
            stringBuilder.append('"'+ e.getMessage() + '"' + "\n" );
            stringBuilder.append("}");
        }
        return stringBuilder.toString();

    }



    @RequestMapping(value = "/customer/transactions/{id}", method= RequestMethod.PUT, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public void test3(@PathVariable String id, @RequestBody JsonNode node){
        LOGGER.info("adding Transxns of loyalty id " + id );
        if (node != null){
            final AmazonS3 s3 = AmazonS3Client.builder().withCredentials(new DefaultAWSCredentialsProviderChain())
                    .withRegion("us-east-1").build();
            S3Object s3Object = new S3Object();
            ObjectMetadata objectMetadata = new ObjectMetadata();
            JsonNode txnNode = node.get("Transaction");
            if(txnNode != null)
            {
                LOGGER.info("adding Transxns of loyalty id " + id + " in progress" );
                JsonNode headerNode = txnNode.get("header");
                if(headerNode != null) {
                    String uuid = headerNode.get("uuid").asText();
                    String fileName = id + "/" + uuid + ".json";
                    s3Object.setObjectMetadata(objectMetadata);
                    s3.putObject("******", fileName, node.asText());
                    LOGGER.info("added Transxns of id " + uuid + " for loyalty id " + id + " successfully" );
                }
            }
        }
    }

}

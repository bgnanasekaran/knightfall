package com.example.demo;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.io.*;

@RestController
public class TransactionS3Controller {

    @RequestMapping("/test")
    public void test(){
        System.out.println("this is a test app ");
    }

    @RequestMapping(value = "/customer/transactions/{id}", method= RequestMethod.GET, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public String test2(@PathVariable String id){
        BasicAWSCredentials awsCreds = new BasicAWSCredentials("*******", "*******");
        final AmazonS3 s3 = AmazonS3Client.builder().withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                .withRegion("us-east-1").build();
        StringBuilder stringBuilder = new StringBuilder();
        try {
            String fileName = id + ".json";
            S3Object o = s3.getObject("bharanibucket", fileName);
            S3ObjectInputStream s3is = o.getObjectContent();
            BufferedReader reader = new BufferedReader(new InputStreamReader(s3is));
            String line = null;
            while ((line = reader.readLine()) != null) {
                stringBuilder.append(line);
            }
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

        if (node != null){
            BasicAWSCredentials awsCreds = new BasicAWSCredentials("AKIAJNW2OMFZN2K63JOA", "9T8jcqg9wL+XftqJXZ6Z3Uh/OwBjbLPNAYqfK4+e");
            final AmazonS3 s3 = AmazonS3Client.builder().withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                    .withRegion("us-east-1").build();
            S3Object s3Object = new S3Object();
            ObjectMetadata objectMetadata = new ObjectMetadata();
            JsonNode txnNode = node.get("Transaction");
            if(txnNode != null)
            {
                JsonNode headerNode = txnNode.get("header");
                if(headerNode != null) {
                    String uuid = headerNode.get("uuid").asText();
                    String fileName = id + "/" + uuid + ".json";
                    s3Object.setObjectMetadata(objectMetadata);
                    s3.putObject("bharanibucket", fileName, node.asText());
                }
            }
        }
    }

}

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
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.io.*;

@RestController
public class TestController {

    @RequestMapping("/test")
    public void test(){
        System.out.println("this is a test app ");
    }

    @RequestMapping(value = "/test/get/{id}", method= RequestMethod.GET, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public String test2(@PathVariable String id){
        BasicAWSCredentials awsCreds = new BasicAWSCredentials("AKIAJNW2OMFZN2K63JOA", "9T8jcqg9wL+XftqJXZ6Z3Uh/OwBjbLPNAYqfK4+e");
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



    @RequestMapping(value = "/test/create", method= RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public void test3(@RequestBody JsonNode node){

        if (node != null){
            System.out.println(node.get("id").asText());
        }
    }

}

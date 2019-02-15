package com.example.manager;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

@Component
public class S3CredentialsManager {

    private AmazonS3 s3Client;

    public AmazonS3 getS3Client(String accessKey, String secretKey){
        if(StringUtils.isNotEmpty(accessKey) && StringUtils.isNotEmpty(secretKey)){
            BasicAWSCredentials awsCreds = new BasicAWSCredentials(accessKey, secretKey);
            s3Client = AmazonS3Client.builder().withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                    .withRegion("us-east-1").build();
        }else{
            s3Client = AmazonS3Client.builder().withCredentials(new DefaultAWSCredentialsProviderChain())
                    .withRegion("us-east-1").build();
        }
        return s3Client;
    }
}

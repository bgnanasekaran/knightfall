package com.example.manager;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.atomic.AtomicReference;

@Service
public class S3ServiceManager {

    private Logger LOGGER = LoggerFactory.getLogger(S3ServiceManager.class);

    public void create(AmazonS3 s3, String id, JsonNode jsonNode, String fileFormat, String bucketName){
        if (jsonNode != null){
            JsonNode txnNode = jsonNode.get("Transaction");
            if(txnNode != null)
            {
                LOGGER.debug("adding Transxns of loyalty id " + id + " in progress" );
                JsonNode headerNode = txnNode.get("header");
                if(headerNode != null) {
                    String uuid = headerNode.get("uuid").asText();
                    AtomicReference<String> fileName = new AtomicReference<>(id + "/" + uuid + ".parque");
                    if(StringUtils.equals(fileFormat, "json")){
                        fileName.set(id + "/" + uuid + ".json");
                    }
                    S3Object s3Object = new S3Object();
                    ObjectMetadata objectMetadata = new ObjectMetadata();
                    s3Object.setObjectMetadata(objectMetadata);
                    s3.putObject(bucketName, fileName.get(), jsonNode.toString());
                    LOGGER.debug("added Transxns of id " + uuid + " for loyalty id " + id + " successfully" );
                }
            }
        }
    }

    public String get(AmazonS3 s3Client, String id, String uuid, String fileFormat, String bucketName){
        StringBuilder stringBuilder = new StringBuilder();
        try {
            AtomicReference<String> fileName = new AtomicReference<>(id + "/" + uuid + ".parque");
            if(StringUtils.equals(fileFormat, "json")){
                fileName.set(id + "/" + uuid + ".json");
            }
            S3Object o = s3Client.getObject(bucketName, fileName.get());
            S3ObjectInputStream s3is = o.getObjectContent();
            BufferedReader reader = new BufferedReader(new InputStreamReader(s3is));
            String line;
            LOGGER.info("fetching Transxns in progress");
            while ((line = reader.readLine()) != null) {
                stringBuilder.append(line);
            }
            LOGGER.info("fetched Transxns successfully");
        } catch (AmazonServiceException | IOException e) {
            stringBuilder.append("{\n");
            stringBuilder.append('"'+ "error" + '"' );
            stringBuilder.append(":" );
            stringBuilder.append('"');
            stringBuilder.append(e.getMessage());
            stringBuilder.append('"');
            stringBuilder.append("\n");
            stringBuilder.append("}");
        }
        LOGGER.info(stringBuilder.toString());
        return stringBuilder.toString();
    }
}

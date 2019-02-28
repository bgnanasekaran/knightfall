package com.example.controller;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

@RestController
public class SparkParquetS3Controller {

    private Logger LOGGER = LoggerFactory.getLogger(SparkParquetS3Controller.class);

    @RequestMapping(value = "/spark/transactions/{id}", method= RequestMethod.PUT, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public void test(@PathVariable String id, @RequestBody JsonNode node, @RequestHeader(value="access_key", required = false) String accessKey, @RequestHeader(value="secret_key", required = false) String secretKey, @RequestHeader(value="bucket_name", required = false) String bucketName) {
        LOGGER.info("Creating online transactions ");
        sparkCreate();
    }

    public void sparkCreate(){
        SparkSession sparkSession = SparkSession.builder().master("local[*]").
                config("spark.executor.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true").getOrCreate();
        List<String> stringList = new ArrayList<>();
        stringList.add("test2");
        sparkSession.sparkContext().hadoopConfiguration().set("fs.s3a.access.key", "*********");
        sparkSession.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key", "*********");
        try {
            Dataset dataSet = sparkSession.createDataset(stringList, Encoders.STRING());
            dataSet.write().mode("append").parquet("s3a://bharanibucket/test123");
        } catch (Exception e){
            LOGGER.error("Error Msg", e);
        }
    }

}

package com.example.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

@RestController
public class TransactionParquetController {

    private Logger LOGGER = LoggerFactory.getLogger(TransactionParquetController.class);

    @RequestMapping(value = "/online/transactions/{id}", method= RequestMethod.PUT, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public void test(@PathVariable String id, @RequestBody JsonNode node, @RequestHeader(value="access_key", required = false) String accessKey, @RequestHeader(value="secret_key", required = false) String secretKey) throws JsonProcessingException {
        LOGGER.info("Creating online transactions ");
        parquetWrite(id, node, accessKey, secretKey);
    }

    public void parquetWrite(String id, JsonNode node, String accessKey, String secretKey) throws JsonProcessingException {

        if (node != null){
            JsonNode txnNode = node.get("Transaction");
            if(txnNode != null)
            {
                LOGGER.debug("adding Transxns of loyalty id " + id + " in progress" );
                JsonNode headerNode = txnNode.get("header");
                if(headerNode != null) {

                    String uuid = headerNode.get("uuid").asText();
                    StringBuilder pathBuilder = new StringBuilder();
                    pathBuilder.append("s3a://bharanibucket/");
                    pathBuilder.append(id);
                    pathBuilder.append("/");
                    pathBuilder.append(uuid);
                    pathBuilder.append(".");
                    pathBuilder.append("parquet");

                    String schema = "{\"namespace\": \"org.bharani.parquet\"," //Not used in Parquet, can put anything
                            + "\"type\": \"record\"," //Must be set as record
                            + "\"name\": \"testParquet\"," //Not used in Parquet, can put anything
                            + "\"fields\": ["
                            + " {\"name\": \"Transaction\",  \"type\": [\"string\", \"null\"]}"
                            + " ]}";

                    Schema.Parser parser = new Schema.Parser().setValidate(true);
                    Schema avroSchema = parser.parse(schema);

                    GenericData.Record record = new GenericData.Record(avroSchema);
                    record.put("Transaction", txnNode.toString());

                    try {
                        Configuration conf = new Configuration();
                        if(StringUtils.isNotEmpty(accessKey) && StringUtils.isNotEmpty(secretKey)){
                            conf.set("fs.s3a.access.key", accessKey);
                            conf.set("fs.s3a.secret.key", secretKey);
                        }else{
                            conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
                        }
                        //Below are some other helpful settings
                        //conf.set("fs.s3a.endpoint", "s3.amazonaws.com");
                        //conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
                        //conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()); // Not needed unless you reference the hadoop-hdfs library.
                        //conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName()); // Uncomment if you get "No FileSystem for scheme: file" errors.

                        Path path = new Path(pathBuilder.toString());

                        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(path)
                                .withSchema(avroSchema)
                                .withCompressionCodec(CompressionCodecName.SNAPPY)
                                .withConf(conf)
                                .build()) {
                            writer.write(record);
                        }
                    } catch (Exception ex) {
                        LOGGER.error("Exception occured :: ", ex);
                    }
                }
            }
        }

    }

}

package com.example.manager;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
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

    private  String PARQUET_SCHEMA_TEMPLATE = "{\"namespace\": \"org.bharani.parquet\"," //Not used in Parquet, can put anything
            + "\"type\": \"record\"," //Must be set as record
            + "\"name\": \"testParquet\"," //Not used in Parquet, can put anything
            + "\"fields\": ["
            + " {\"name\": \"Transaction\",  \"type\": [\"string\", \"null\"]}"
            + " ]}";

    public void create(AmazonS3 s3, String id, JsonNode jsonNode, String fileFormat, String bucketName){
        if (jsonNode != null){
            JsonNode txnNode = jsonNode.get("Transaction");
            if(txnNode != null)
            {
                LOGGER.debug("adding Transxns of loyalty id " + id + " in progress" );
                JsonNode headerNode = txnNode.get("header");
                if(headerNode != null) {
                    String uuid = headerNode.get("uuid").asText();
                    AtomicReference<String> fileName = new AtomicReference<>(id + "/" + uuid + ".parquet");
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
            AtomicReference<String> fileName = new AtomicReference<>(id + "/" + uuid + ".parquet");
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


    public JsonNode parquetGet(String id, String uuid, String accessKey, String secretKey, String bucketName){

        StringBuilder pathBuilder = getPath(bucketName, id, uuid);

        Path path = new Path(pathBuilder.toString());

        Schema schema = new Schema.Parser().parse(PARQUET_SCHEMA_TEMPLATE);
        Configuration conf = generateConfiguration(accessKey, secretKey);
        AvroReadSupport.setRequestedProjection(conf, schema);
        JsonNode node = null;
        try {
            ParquetReader<GenericRecord> parquetReader = AvroParquetReader.<GenericRecord>builder(path).withConf(conf).build();
            GenericRecord genericRecord = parquetReader.read();
            ObjectMapper objectMapper = new ObjectMapper();
            node = objectMapper.readTree(genericRecord.toString());
        }
        catch (Exception e){
            LOGGER.error("Exception occured during read :: ", e);
        }
        return node;
    }

    public void parquetCreate(String id, JsonNode node, String accessKey, String secretKey, String bucketName) {

        if (node != null){
            JsonNode txnNode = node.get("Transaction");
            if(txnNode != null)
            {
                LOGGER.debug("adding Transxns of loyalty id " + id + " in progress" );
                JsonNode headerNode = txnNode.get("header");
                if(headerNode != null) {

                    String uuid = headerNode.get("uuid").asText();
                    StringBuilder pathBuilder = getPath(bucketName, id, uuid);

                    Schema.Parser parser = new Schema.Parser().setValidate(true);
                    Schema avroSchema = parser.parse(PARQUET_SCHEMA_TEMPLATE);

                    GenericData.Record record = new GenericData.Record(avroSchema);
                    record.put("Transaction", txnNode.toString());

                    try {
                        Configuration conf = generateConfiguration(accessKey, secretKey);
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

    private StringBuilder getPath(String bucketName, String id, String uuid) {
        StringBuilder pathBuilder = new StringBuilder();
        pathBuilder.append("s3a://");
        pathBuilder.append(bucketName);
        pathBuilder.append("/");
        pathBuilder.append(id);
        pathBuilder.append("/");
        pathBuilder.append(uuid);
        pathBuilder.append(".");
        pathBuilder.append("parquet");
        return pathBuilder;
    }

    private Configuration generateConfiguration(String accessKey, String secretKey) {
        Configuration conf = new Configuration();
        if (StringUtils.isNotEmpty(accessKey) && StringUtils.isNotEmpty(secretKey)) {
            conf.set("fs.s3a.access.key", accessKey);
            conf.set("fs.s3a.secret.key", secretKey);
        } else {
            conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");
        }
        return conf;
    }

    public void sparkWrite(){

        SparkSession sparkSession = SparkSession.builder().getOrCreate();
        Dataset<Row> dataSet = sparkSession.emptyDataFrame();
        dataSet.write().parquet("s3a://bharanibucket/test.parquet");


    }
}

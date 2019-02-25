package com.example.controller;

import com.amazonaws.services.s3.model.ParquetInput;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.MutableDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class TestS3Controller {

    private Logger LOGGER = LoggerFactory.getLogger(TestS3Controller.class);

    @RequestMapping("/test")
    public void test() throws JsonProcessingException {
        LOGGER.info("this is a test app ");
       // sparkWrite();
        parquetWrite();
    }

    public void sparkWrite(){
        SparkSession sparkSession = SparkSession.builder().master("local[*]").
                config("spark.executor.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true").getOrCreate();
/*        SparkConf sparkConf = new SparkConf().setAppName("testApp");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);*/
        List<String> stringList = new ArrayList<>();
        stringList.add("test1");
/*        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
        JavaRDD javaRDD = javaSparkContext.parallelize(stringList);
        Dataset<Row> dataSet  = sparkSession.createDataFrame(javaRDD, (StructType) Encoders.STRING());
        dataSet.write().parquet("s3a://bharanibucket/test.parquet");
        sparkSession.cr*/
        /*Dataset<Row> dataSet = sparkSession.createDataset("test data", Encoders.STRING());
        JavaSparkContext
        dataSet.write().parquet("s3a://bharanibucket/test.parquet");
        sparkSession.createDataFrame*/
        sparkSession.sparkContext().setLocalProperty("com.amazonaws.services.s3.enableV4", "true");
        //arn:aws:s3:::bharanibucket
        //sparkSession.sparkContext().hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com");
        sparkSession.sparkContext().hadoopConfiguration().set("fs.s3a.access.key", "***********");
        sparkSession.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key", "***********");
        /*sparkSession.sparkContext().hadoopConfiguration().set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem");
        sparkSession.sparkContext().hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "***********");
        sparkSession.sparkContext().hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "***********");*/
        try {
            Dataset dataSet = sparkSession.createDataset(stringList, Encoders.STRING());
            dataSet.write().parquet("s3a://***********/test123");
        } catch (Exception e){
            LOGGER.error("Error Msg", e);
        }
       // dataSet.write().parquet("/temp/test");

        ParquetInput parquetInput = new ParquetInput();
        //ParquetReader parquetReader = ParquetReader.builder()
       // ParquetWriter.Builder

    }

    public void parquetRead() throws IOException {

        String SCHEMA_TEMPLATE = "{" +
                "\"type\": \"record\",\n" +
                "    \"name\": \"schema\",\n" +
                "    \"fields\": [\n" +
                "        {\"name\": \"timeStamp\", \"type\": \"string\"},\n" +
                "        {\"name\": \"temperature\", \"type\": \"double\"},\n" +
                "        {\"name\": \"pressure\", \"type\": \"double\"}\n" +
                "    ]" +
                "}";
        String PATH_SCHEMA = "s3a";
        Path internalPath = new Path(PATH_SCHEMA, "bharanibucket", "test");
        Schema schema = new Schema.Parser().parse(SCHEMA_TEMPLATE);
        Configuration configuration = new Configuration();
        AvroReadSupport.setRequestedProjection(configuration, schema);
        ParquetReader<GenericRecord> parquetReader = AvroParquetReader.<GenericRecord>builder(internalPath).withConf(configuration).build();
        GenericRecord genericRecord = parquetReader.read();

        while(genericRecord != null) {
            Map<String, String> valuesMap = new HashMap<>();
            GenericRecord finalGenericRecord = genericRecord;
            genericRecord.getSchema().getFields().forEach(field -> valuesMap.put(field.name(), finalGenericRecord.get(field.name()).toString()));

            genericRecord = parquetReader.read();
        }
    }

    public void parquetWrite() throws JsonProcessingException {
        String schema = "{\"namespace\": \"org.bharani.parquet\"," //Not used in Parquet, can put anything
                + "\"type\": \"record\"," //Must be set as record
                + "\"name\": \"testParquet\"," //Not used in Parquet, can put anything
                + "\"fields\": ["
                + " {\"name\": \"myInteger\", \"type\": \"int\"}," //Required field
                + " {\"name\": \"myString\",  \"type\": [\"string\", \"null\"]},"
                + " {\"name\": \"myDecimal\", \"type\": [{\"type\": \"fixed\", \"size\":16, \"logicalType\": \"decimal\", \"name\": \"mydecimaltype1\", \"precision\": 32, \"scale\": 4}, \"null\"]},"
                + " {\"name\": \"myDate\", \"type\": [{\"type\": \"int\", \"logicalType\" : \"date\"}, \"null\"]}"
                + " ]}";


        Schema.Parser parser = new Schema.Parser().setValidate(true);
        Schema avroSchema = parser.parse(schema);

        GenericData.Record record = new GenericData.Record(avroSchema);
       // record.put("myInteger", 1);
        record.put("myString", "string value 1");



        BigDecimal myDecimalValue = new BigDecimal("99.9999");

        //First we need to make sure the huge decimal matches our schema scale:
        myDecimalValue = myDecimalValue.setScale(4, RoundingMode.HALF_UP);

        //Next we get the decimal value as one BigInteger (like there was no decimal point)
        BigInteger myUnscaledDecimalValue = myDecimalValue.unscaledValue();

        //Finally we serialize the integer
        byte[] decimalBytes = myUnscaledDecimalValue.toByteArray();

        //We need to create an Avro 'Fixed' type and pass the decimal schema once more here:
        GenericData.Fixed fixed = new GenericData.Fixed(new Schema.Parser().parse("{\"type\": \"fixed\", \"size\":16, \"precision\": 32, \"scale\": 4, \"name\":\"mydecimaltype1\"}"));

        byte[] myDecimalBuffer = new byte[16];
        if (myDecimalBuffer.length >= decimalBytes.length) {
            //Because we set our fixed byte array size as 16 bytes, we need to
            //pad-left our original value's bytes with zeros
            int myDecimalBufferIndex = myDecimalBuffer.length - 1;
            for(int i = decimalBytes.length - 1; i >= 0; i--){
                myDecimalBuffer[myDecimalBufferIndex] = decimalBytes[i];
                myDecimalBufferIndex--;
            }

            //Save result
            fixed.bytes(myDecimalBuffer);
        } else {
            throw new IllegalArgumentException(String.format("Decimal size: %d was greater than the allowed max: %d", decimalBytes.length, myDecimalBuffer.length));
        }

        //We can finally write our decimal to our record
        record.put("myDecimal", fixed);

        //Get epoch value
        MutableDateTime epoch = new MutableDateTime(0l, DateTimeZone.UTC);

        DateTime currentDate = new DateTime(); //Can take Java Date in constructor
        Days days = Days.daysBetween(epoch, currentDate);

        //We can write number of days since epoch into the record
        record.put("myDate", days.getDays());

        try {
            Configuration conf = new Configuration();
            conf.set("fs.s3a.access.key", "***********");
            conf.set("fs.s3a.secret.key", "***********");
            //Below are some other helpful settings
            //conf.set("fs.s3a.endpoint", "s3.amazonaws.com");
            //conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
            //conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()); // Not needed unless you reference the hadoop-hdfs library.
            //conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName()); // Uncomment if you get "No FileSystem for scheme: file" errors.

            Path path = new Path("s3a://***********/test/data3.parquet");

            //Use path below to save to local file system instead
            //Path path = new Path("data.parquet");

            /*            .withCompressionCodec(CompressionCodecName.SNAPPY)

                    .withPageSize(4 * 1024 * 1024) //For compression
                    .withRowGroupSize(16 * 1024 * 1024) //For write buffering (Page size)
                    */
            try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(path)
                    .withSchema(avroSchema)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withConf(conf)
                    .build()) {

                //We only have one record to write in our example
                writer.write(record);
            }
        } catch (Exception ex) {
            ex.printStackTrace(System.out);
        }
    }

}

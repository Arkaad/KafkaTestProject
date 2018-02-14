package com.testcase.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

/**
 * Created by Arka Dutta on 14-Feb-18.
 */
public class AvroParser {
    public static void main(String[] args) {
        Schema.Parser parser = new Schema.Parser();
        try {
            Schema schema = parser.parse(new File("D:\\TestProjects\\KafkaTestProject\\src\\main\\resources\\user.avsc"));

            GenericRecord record = new GenericData.Record(schema);
            record.put("colId", "789456123741852963325698741256325897");
            record.put("startTime", System.currentTimeMillis());
            record.put("endTime", System.currentTimeMillis());

            DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<>(schema));
            dataFileWriter.setCodec(CodecFactory.nullCodec());
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            dataFileWriter.create(schema, outputStream);
            dataFileWriter.append(record);

            dataFileWriter.flush();
            byte[] byteArray = outputStream.toByteArray();
            for (byte b : byteArray) {
                System.out.println(b);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static byte[] getByteArray(String colId) throws IOException {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(new File("D:\\TestProjects\\KafkaTestProject\\src\\main\\resources\\user.avsc"));

        GenericRecord record = new GenericData.Record(schema);
        record.put("colId", colId);
        record.put("startTime", System.currentTimeMillis());
        record.put("endTime", System.currentTimeMillis());

        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<>(schema));
        dataFileWriter.setCodec(CodecFactory.nullCodec());
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        dataFileWriter.create(schema, outputStream);
        dataFileWriter.append(record);

        dataFileWriter.flush();
        return outputStream.toByteArray();
    }
}

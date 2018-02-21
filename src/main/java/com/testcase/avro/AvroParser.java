package com.testcase.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.LinkedHashMap;

/**
 * Created by Arka Dutta on 14-Feb-18.
 */
public class AvroParser {
    private static Schema schema;

    public static void createAvroSchema() throws JSONException {
        JSONObject jsonObject = new JSONObject(new LinkedHashMap());
        jsonObject.put("type", "record");
        jsonObject.put("name", "TABLE_ARRAY");
        JSONArray fieldsArray = new JSONArray();
        jsonObject.put("fields", fieldsArray);
        fieldsArray.put(getFieldObject("columnId", "string"));
        fieldsArray.put(getFieldObject("creationTime", "long"));
        fieldsArray.put(getFieldObject("jobId", "string"));
        fieldsArray.put(getFieldObject("interval", "int"));
        fieldsArray.put(getFieldObject("lngPrjId", "string"));
        fieldsArray.put(getFieldObject("sample", "string"));
        schema = new Schema.Parser().parse(jsonObject.toString());
    }

    private static JSONObject getFieldObject(String colName, String colType) throws JSONException {
        JSONObject fieldObject = new JSONObject(new LinkedHashMap());
        fieldObject.put("name", colName);
        fieldObject.put("type", colType);
        return fieldObject;
    }

    public static void main(String[] args) {
        try {
//            GenericRecord record = new GenericData.Record(schema);
//            record.put("colId", "789456123741852963325698741256325897");
//            record.put("startTime", System.currentTimeMillis());
//            record.put("endTime", System.currentTimeMillis());
//
//            DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<>(schema));
//            dataFileWriter.setCodec(CodecFactory.nullCodec());
//            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
//            dataFileWriter.create(schema, outputStream);
//            dataFileWriter.append(record);
//
//            dataFileWriter.flush();
//            System.out.println(new String(outputStream.toByteArray()));
            byte[] arr = getBinaryAvroData(System.currentTimeMillis(), 6656321656864654554L, 545656546513546686L, 1);
            System.out.println(new String(arr));
            deserialize(arr);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Deprecated
    public static byte[] getByteArray(int interval) throws IOException {
        ByteArrayOutputStream stream = null;
        try {
            long prjId = -454541316L;
            long jobId = 1245454L;
            GenericRecord record = new GenericData.Record(schema);
            record.put("creationTime", System.currentTimeMillis());
            record.put("jobId", jobId);
            record.put("interval", interval);
            record.put("lngPrjId", prjId);

            stream = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(stream, null);

            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
            datumWriter.write(record, encoder);
            encoder.flush();
            return stream.toByteArray();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            if (stream != null) {
                stream.close();
            }
        }
    }


    public static byte[] getBinaryAvroData(long creationTime, long prjId, long jobId, int interval) throws Exception {
        ByteArrayOutputStream stream = null;
        try {
            GenericRecord genericRecord = new GenericData.Record(schema);
            genericRecord.put("creationTime", creationTime);
            genericRecord.put("jobId", jobId);
            genericRecord.put("interval", interval);
            genericRecord.put("lngPrjId", prjId);

            stream = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(stream, null);

            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
            datumWriter.write(genericRecord, encoder);
            encoder.flush();
            return stream.toByteArray();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            if (stream != null) {
                stream.close();
            }
        }
    }

    public static void deserialize(byte[] avroFileContentWithoutSchema) throws IOException {
        // create a record using schema
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(avroFileContentWithoutSchema, null);
        if (schema == null) {
            createAvroSchema();
        }
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        while (true) {
            try {
                GenericRecord genericRecord = datumReader.read(null, decoder);
                System.out.println(genericRecord.get("columnId") + "\t"
                        + genericRecord.get("creationTime") + "\t"
                        + genericRecord.get("jobId") + "\t"
                        + genericRecord.get("interval") + "\t"
                        + genericRecord.get("lngPrjId") + "\t"
                        + genericRecord.get("sample"));
            } catch (EOFException e) {
                break;
            }
        }
    }
}

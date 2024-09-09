package com.vgarg.javaexamples.kafka.avroexamples;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class AvroSimpleExample {

    public static void main(String[] args) {
        // Create a SimpleRecord object with a String value
        SimpleRecord recordWithString = new SimpleRecord();
        recordWithString.setData("This is a string");

        // Debugging: Check if the schema is properly initialized
        if (recordWithString.getSchema() == null) {
            System.err.println("Schema is null!");
            return;
        } else {
            System.out.println("Schema: " + recordWithString.getSchema().toString(true));
        }

        // Specify a writable file system path
        File outputFile = new File("output/simple_output.avro");
        outputFile.getParentFile().mkdirs();  // Create directory if it doesn't exist

        // Serialize the records
        try {
            DatumWriter<SimpleRecord> datumWriter = new SpecificDatumWriter<>(SimpleRecord.class);
            DataFileWriter<SimpleRecord> dataFileWriter = new DataFileWriter<>(datumWriter);

            dataFileWriter.create(recordWithString.getSchema(), outputFile);
            dataFileWriter.append(recordWithString);

            dataFileWriter.close();
            System.out.println("Serialization successful.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}


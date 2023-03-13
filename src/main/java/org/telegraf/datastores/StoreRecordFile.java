package org.telegraf.datastores;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SequenceWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

public class StoreRecordFile implements Storable {
    private static final Logger logger = LogManager.getLogger(StoreRecordPrometheus.class);
    private File file;
    private FileWriter file_writer = null;
    private SequenceWriter sequence_writer = null;
    private final ObjectMapper mapper = new ObjectMapper();

    public StoreRecordFile(String filename) {
        try {
            file = new File("/metrics/" + filename + ".json");
            Files.deleteIfExists(file.toPath());
            file_writer = new FileWriter(file, true);
            sequence_writer = mapper.writer().writeValuesAsArray(file_writer);
        } catch (
                IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void store_record(String file, String metric, Map<String, String> record, List<String> labelKeys, List<String> labelValues, String measurement) {
        try {
            sequence_writer.write(record);
        } catch (IOException ex) {
            logger.error("File storage failed.", ex);
        }
    }

    @Override
    protected void finalize() {
        try {
            file_writer.close();
            sequence_writer.close();
        } catch (IOException e) {
            logger.error("Error in closing file writer.", e);
        }
    }
}

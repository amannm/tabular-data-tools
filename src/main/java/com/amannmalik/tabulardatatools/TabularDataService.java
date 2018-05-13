package com.amannmalik.tabulardatatools;

import com.amannmalik.tabulardatatools.config.TabularFileReaderConfig;
import com.amannmalik.tabulardatatools.config.TabularFileWriterConfig;
import com.amannmalik.tabulardatatools.gateway.AwsStorageGateway;
import com.amannmalik.tabulardatatools.gateway.DatabaseGateway;
import com.amannmalik.tabulardatatools.gateway.HiveDatabaseGateway;
import com.amannmalik.tabulardatatools.gateway.StorageGateway;
import com.amannmalik.tabulardatatools.processor.TabularFileProcessor;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class TabularDataService {

    private final StorageGateway storage;
    private final DatabaseGateway database;

    public TabularDataService() {
        this.storage = new AwsStorageGateway();
        this.database = new HiveDatabaseGateway();
    }

    public void createFileFromUpload(URI newFileUri, Path sourceFilePath) {
        storage.put(newFileUri, sourceFilePath);
    }

    public void createTableFromFile(URI newTableUri, URI sourceFileUri, TabularFileReaderConfig config) throws IOException {
        Path inputFile = createLocalFile();
        storage.get(sourceFileUri, inputFile);
        Path outputFile = createLocalFile();
        List<String> columnNames = TabularFileProcessor.convertCsvToOrc(inputFile, outputFile, config);
        deleteLocalFile(inputFile);
        storage.put(newTableUri, outputFile);
        deleteLocalFile(outputFile);
        database.register(newTableUri, columnNames);
    }

    public void deleteTable(URI tableUri) {
        database.unregister(tableUri);
        storage.delete(tableUri);
    }

    public void createFileFromTable(URI newFileUri, URI sourceTableUri, TabularFileWriterConfig config) throws IOException {
        Path inputFile = createLocalFile();
        storage.get(sourceTableUri, inputFile);
        Path outputFile = createLocalFile();
        TabularFileProcessor.convertOrcToCsv(inputFile, outputFile, config);
        deleteLocalFile(inputFile);
        storage.put(newFileUri, outputFile);
        deleteLocalFile(outputFile);
    }

    public void deleteFile(URI fileUri) {
        storage.delete(fileUri);
    }

    private Path createLocalFile() throws IOException {
        return Files.createTempFile("", "");
    }

    private void deleteLocalFile(Path path) throws IOException {
        Files.delete(path);
    }
}

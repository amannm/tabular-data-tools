package com.amannmalik.tabulardatatools.calcite;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonString;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class S3Schema extends AbstractSchema {

    private Path metadataPath;
    private Map<String, Table> tableMap;

    public S3Schema(Path metadataPath) {
        super();
        this.metadataPath = metadataPath;
    }

    @Override
    protected Map<String, Table> getTableMap() {
        if (tableMap == null) {
            try {
                tableMap = createTableMap();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return tableMap;
    }


    private Map<String, Table> createTableMap() throws IOException {
        JsonObject metadataObject;
        try(JsonReader reader = Json.createReader(Files.newInputStream(metadataPath))){
            metadataObject = reader.readObject();
        }
        return metadataObject.getJsonArray("tables").stream()
                .map(v->(JsonObject)v)
                .collect(Collectors.toMap(
                        o-> o.getString("name"),
                        this::createTable));
    }

    private S3Table createTable(JsonObject metadata) {
        URI uri = URI.create(metadata.getString("location"));
        List<String> fields = metadata.getJsonArray("fields").stream()
                .map(v -> ((JsonString) v).getString())
                .collect(Collectors.toList());
        return new S3Table(uri, fields);
    }
}

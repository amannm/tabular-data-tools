package com.amannmalik.tabulardatatools.calcite;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public class S3SchemaFactory implements SchemaFactory {

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        String metadataPathString = (String) operand.get("metadataPath");
        Path path = Paths.get(metadataPathString);
        return new S3Schema(path);
    }
}

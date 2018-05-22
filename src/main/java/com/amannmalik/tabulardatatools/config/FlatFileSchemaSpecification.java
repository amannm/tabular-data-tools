package com.amannmalik.tabulardatatools.config;

import java.util.Map;

public class FlatFileSchemaSpecification {

    public final Map<String, FlatFileTableSpecification> tables;

    public FlatFileSchemaSpecification(Map<String, FlatFileTableSpecification> tables) {
        this.tables = tables;
    }
}

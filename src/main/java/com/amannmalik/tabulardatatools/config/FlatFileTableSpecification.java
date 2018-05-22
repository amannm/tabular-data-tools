package com.amannmalik.tabulardatatools.config;

import java.net.URI;

public class FlatFileTableSpecification {

    public final String name;
    public final URI location;
    public final FlatFileInputSpecification format;
    public final TableStructureSpecification structure;

    public FlatFileTableSpecification(String name, URI location, FlatFileInputSpecification format, TableStructureSpecification structure) {
        this.name = name;
        this.location = location;
        this.format = format;
        this.structure = structure;
    }
}

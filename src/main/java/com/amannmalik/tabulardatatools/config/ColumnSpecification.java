package com.amannmalik.tabulardatatools.config;

public class ColumnSpecification {

    public final String label;
    public final String datatype;
    public final Boolean isNullable;
    public final String description;

    public ColumnSpecification(String label, String datatype) {
        this.label = label;
        this.datatype = datatype;
        this.isNullable = null;
        this.description = null;
    }

    public ColumnSpecification(String label, String datatype, Boolean isNullable, String description) {
        this.label = label;
        this.datatype = datatype;
        this.isNullable = isNullable;
        this.description = description;
    }

}

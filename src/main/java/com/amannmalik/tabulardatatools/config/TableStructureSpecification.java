package com.amannmalik.tabulardatatools.config;

import java.util.List;

public class TableStructureSpecification {

    public final List<ColumnSpecification> columns;

    public TableStructureSpecification(List<ColumnSpecification> columns) {
        this.columns = columns;
    }
}

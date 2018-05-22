package com.amannmalik.tabulardatatools.calcite;

import com.amannmalik.tabulardatatools.config.FlatFileSchemaSpecification;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Map;
import java.util.stream.Collectors;

public class S3Schema extends AbstractSchema {

    private FlatFileSchemaSpecification specification;

    private Map<String, Table> tableMap;

    public S3Schema(FlatFileSchemaSpecification specification) {
        super();
        this.specification = specification;
    }

    @Override
    protected Map<String, Table> getTableMap() {
        if (tableMap == null) {
            tableMap = createTableMap();
        }
        return tableMap;
    }


    private Map<String, Table> createTableMap() {
        return specification.tables.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> new S3Table(e.getValue())));

    }

}

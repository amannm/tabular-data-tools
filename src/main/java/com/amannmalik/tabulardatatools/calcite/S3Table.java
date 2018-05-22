package com.amannmalik.tabulardatatools.calcite;

import com.amannmalik.tabulardatatools.config.FlatFileTableSpecification;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class S3Table extends AbstractTable implements ProjectableFilterableTable {

    private final FlatFileTableSpecification specification;

    public S3Table(FlatFileTableSpecification specification) {
        this.specification = specification;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataType sqlType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        List<String> fieldNames = specification.structure.columns.stream()
                .map(c -> c.label)
                .collect(Collectors.toList());
        List<RelDataType> types = specification.structure.columns.stream()
                .map(f -> sqlType)
                .collect(Collectors.toList());
        return typeFactory.createStructType(types, fieldNames);
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, int[] projects) {
        String selectList = IntStream.of(projects)
                .boxed()
                .map(i -> i + 1)
                .map(i -> "_" + i)
                .collect(Collectors.joining(", "));
        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                return new S3SelectEnumerator(specification.location, specification.format, String.format("SELECT %s FROM S3Object", selectList));
            }
        };
    }
}

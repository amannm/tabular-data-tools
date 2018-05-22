package com.amannmalik.tabulardatatools.gateway;

import com.amannmalik.tabulardatatools.config.ColumnSpecification;

import java.net.URI;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class HiveDatabaseGateway implements DatabaseGateway {

    private final String connectionString;

    private static final Pattern SELECT_PATTERN = Pattern.compile("(^|.*(?<=\\)))\\s*(SELECT .*$)", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL);

    public HiveDatabaseGateway() {

        String hostname = System.getenv("HIVE_METASTORE_HOSTNAME");
        if (hostname == null) {
            hostname = "localhost";
        }

        String portString = System.getenv("HIVE_METASTORE_PORT");
        int port;
        if (portString == null) {
            port = 10000;
        } else {
            try {
                port = Integer.parseInt(portString);
            } catch (NumberFormatException e) {
                throw new RuntimeException(e);
            }
        }

        connectionString = String.format("jdbc:hive2://%s:%s/default", hostname, port);

        try {
            Class.forName("com.amazon.hive.jdbc41.HS2Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public void register(URI uri, List<ColumnSpecification> columnSpecifications) {

        String dropStatement = generateDropStatement(uri.toString());
        String createStatement = String.format("CREATE EXTERNAL TABLE `%s` (%s) STORED AS ORC LOCATION '%s'",
                uri,
                columnSpecifications.stream().map(cd -> String.format("`%s` %s", cd.label, cd.datatype)).collect(Collectors.joining(",")),
                uri);

        try (Connection conn = openConnection()) {
            conn.setAutoCommit(false);
            executeUpdateStatement(conn, dropStatement);
            executeUpdateStatement(conn, createStatement);
            conn.commit();
            conn.setAutoCommit(true);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void generate(URI uri, Map<String, URI> inputReferenceMap, String script) {

        String selectStatement = injectScriptReferences(inputReferenceMap, script);

        List<ColumnSpecification> columnSpecifications = determineOutputSchema(selectStatement);

        register(uri, columnSpecifications);

        String insertStatement = reprocessScript(uri, selectStatement);

        try (Connection conn = openConnection()) {
            executeUpdateStatement(conn, insertStatement);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    public void unregister(URI uri) {

        String dropStatement = generateDropStatement(uri.toString());

        try (Connection conn = openConnection()) {
            executeUpdateStatement(conn, dropStatement);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private List<ColumnSpecification> determineOutputSchema(String script) {
        List<ColumnSpecification> columnSpecifications;
        try (Connection connection = openConnection()) {
            ResultSetMetaData metaData;
            try (PreparedStatement stmt = connection.prepareStatement(script)) {
                metaData = stmt.getMetaData();
            }
            int columnCount = metaData.getColumnCount();
            columnSpecifications = new ArrayList<>(columnCount);
            for (int i = 1; i <= columnCount; i++) {
                String columnLabel = metaData.getColumnLabel(i);
                String columnTypeName = metaData.getColumnTypeName(i);
                switch (metaData.isNullable(i)) {
                    case 0:
                        columnTypeName += " NOT NULL";
                        break;
                    case 1:
                        columnTypeName += " NULL";
                        break;
                    default:
                        break;
                }
                //TODO: assess necessity
                //metaData.getPrecision(i);
                //metaData.getScale(i);
                columnSpecifications.add(new ColumnSpecification(columnLabel, columnTypeName));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return columnSpecifications;
    }

    private String injectScriptReferences(Map<String, URI> inputReferenceMap, String script) {
        for (Map.Entry<String, URI> entry : inputReferenceMap.entrySet()) {
            //TODO: check if the inputURIs are all already registered
            script = script.replace(entry.getKey(), entry.getValue().toString());
        }
        return script;
    }

    private String reprocessScript(URI uri, String script) {

        //TODO: strip out stuff like comments before getting here
        String modifiedScript;
        Matcher matcher = SELECT_PATTERN.matcher(script);
        if(matcher.find()) {
            String preSelect = matcher.group(1).trim();
            String select = matcher.group(2).trim();
            String cteLabel = UUID.randomUUID().toString();
            modifiedScript = String.format("`%s` AS (\n%s\n)\nINSERT OVERWRITE TABLE `%s` SELECT * FROM `%s`",
                    cteLabel,
                    select,
                    uri,
                    cteLabel);
            if(preSelect.isEmpty()) {
                modifiedScript = "WITH " + modifiedScript;
            } else {
                modifiedScript = preSelect + ",\n" + modifiedScript;
            }
        } else {
            throw new RuntimeException("malformed or non-conformant SQL select statement provided");
        }
        return modifiedScript;
    }

    private Connection openConnection() throws SQLException {
        return DriverManager.getConnection(connectionString);
    }

    private static int executeUpdateStatement(Connection connection, String statement) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement(statement)) {
            return stmt.executeUpdate();
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        }
    }

    private static String generateDropStatement(String tableName) {
        return String.format("DROP TABLE IF EXISTS `%s`", tableName);
    }

}

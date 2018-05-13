package com.amannmalik.tabulardatatools.gateway;

import java.net.URI;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

public class HiveMetastoreGateway implements MetastoreGateway {

    private final String connectionString;

    public HiveMetastoreGateway() {

        String hostname = System.getenv("HIVE_METASTORE_HOSTNAME");
        if(hostname == null) {
            hostname = "localhost";
        }

        String portString = System.getenv("HIVE_METASTORE_PORT");
        int port;
        if(portString == null) {
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

    public void register(URI uri, List<String> columnLabels) {

        String tableName = generateTableName(uri);
        String dropStatement = generateDropStatement(tableName);
        String createStatement = String.format("CREATE EXTERNAL TABLE `%s` (%s) STORED AS ORC LOCATION '%s'",
                tableName,
                columnLabels.stream().map(label -> String.format("`%s` STRING", label)).collect(Collectors.joining(",")),
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

    public void unregister(URI uri) {

        String tableName = generateTableName(uri);
        String dropStatement = generateDropStatement(tableName);

        try (Connection conn = openConnection()) {
            conn.setAutoCommit(false);
            executeUpdateStatement(conn, dropStatement);
            conn.commit();
            conn.setAutoCommit(true);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
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

    private static String generateTableName(URI tableUri) {
        return Paths.get(tableUri.getPath()).getFileName().toString();
    }

    private static String generateDropStatement(String tableName) {
        return String.format("DROP TABLE IF EXISTS `%s`", tableName);
    }

}

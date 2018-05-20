package com.amannmalik.tabulardatatools.calcite;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.apache.calcite.linq4j.Enumerator;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;

public class S3SelectResultSetEnumerator implements Enumerator<Object[]> {

    private final CsvParser csvParser;
    private String[] currentLine;

    public S3SelectResultSetEnumerator(InputStream inputStream, CsvParserSettings csvParserSettings) {
        CsvParser csvParser = new CsvParser(csvParserSettings);
        csvParser.beginParsing(inputStream, StandardCharsets.UTF_8);
        this.csvParser = csvParser;
    }

    @Override
    public Object[] current() {
        if (currentLine == null) {
            throw new NoSuchElementException();
        }
        return currentLine;
    }

    @Override
    public boolean moveNext() {
        String[] nextLine = csvParser.parseNext();
        if (nextLine == null) {
            return false;
        } else {
            currentLine = nextLine;
            return true;
        }
    }

    @Override
    public void reset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        csvParser.stopParsing();
    }


}

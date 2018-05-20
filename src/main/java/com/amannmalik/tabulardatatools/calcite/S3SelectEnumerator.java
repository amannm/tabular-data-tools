package com.amannmalik.tabulardatatools.calcite;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.*;
import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.apache.calcite.linq4j.Enumerator;

import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;

public class S3SelectEnumerator implements Enumerator<Object[]> {


    private String[] currentLine;

    private final CsvParser csvParser;

    public S3SelectEnumerator(URI location, String query) {
        CsvFormat csvFormat = new CsvFormat();
        csvFormat.setDelimiter(',');
        csvFormat.setLineSeparator("\n");
        csvFormat.setQuote('\"');
        csvFormat.setQuoteEscape('\\');
        CsvParserSettings parserSettings = new CsvParserSettings();
        parserSettings.setHeaderExtractionEnabled(false);
        parserSettings.setFormat(csvFormat);
        csvParser = new CsvParser(parserSettings);
        InputStream inputStream = executeQuery(location, query);
        csvParser.beginParsing(inputStream, StandardCharsets.UTF_8);

    }

    private InputStream executeQuery(URI location, String query) {

        AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();

        AmazonS3URI amazonS3URI = new AmazonS3URI(location);

        SelectObjectContentRequest request = new SelectObjectContentRequest();
        request.setBucketName(amazonS3URI.getBucket());
        request.setKey(amazonS3URI.getKey());
        request.setExpression(query);
        request.setExpressionType(ExpressionType.SQL);
        CSVInput csvInput = new CSVInput();
        InputSerialization inputSerialization = new InputSerialization();
        inputSerialization.setCsv(csvInput);
        inputSerialization.setCompressionType(CompressionType.NONE);
        request.setInputSerialization(inputSerialization);

        CSVOutput csvOutput = new CSVOutput();
        csvOutput.setFieldDelimiter(',');
        csvOutput.setRecordDelimiter('\n');
        csvOutput.setQuoteCharacter('\"');
        csvOutput.setQuoteEscapeCharacter('\\');
        OutputSerialization outputSerialization = new OutputSerialization();
        outputSerialization.setCsv(csvOutput);
        request.setOutputSerialization(outputSerialization);

        SelectObjectContentResult result = s3.selectObjectContent(request);
        return result.getPayload().getRecordsInputStream();
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

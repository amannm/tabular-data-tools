package com.amannmalik.tabulardatatools.calcite;

import com.amannmalik.tabulardatatools.config.FlatFileInputSpecification;
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

    private static final CsvFormat INTERNAL_SERIALIZATION_FORMAT;
    static {
        INTERNAL_SERIALIZATION_FORMAT = new CsvFormat();
        INTERNAL_SERIALIZATION_FORMAT.setDelimiter(',');
        INTERNAL_SERIALIZATION_FORMAT.setLineSeparator("\n");
        INTERNAL_SERIALIZATION_FORMAT.setQuote('\"');
        INTERNAL_SERIALIZATION_FORMAT.setQuoteEscape('\\');
    }

    public S3SelectEnumerator(URI location, FlatFileInputSpecification inputSpecification, String query) {
        CsvParserSettings parserSettings = new CsvParserSettings();
        parserSettings.setFormat(INTERNAL_SERIALIZATION_FORMAT);
        parserSettings.setHeaderExtractionEnabled(false);
        csvParser = new CsvParser(parserSettings);
        InputStream inputStream = executeQuery(location, inputSpecification, query);
        csvParser.beginParsing(inputStream, StandardCharsets.UTF_8);
    }

    private InputStream executeQuery(URI location, FlatFileInputSpecification inputSpecification, String query) {

        AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();

        AmazonS3URI amazonS3URI = new AmazonS3URI(location);

        SelectObjectContentRequest request = new SelectObjectContentRequest();
        request.setBucketName(amazonS3URI.getBucket());
        request.setKey(amazonS3URI.getKey());
        request.setExpression(query);
        request.setExpressionType(ExpressionType.SQL);

        CSVInput csvInput = new CSVInput();
        csvInput.setFileHeaderInfo(inputSpecification.header ? FileHeaderInfo.USE : FileHeaderInfo.NONE);
        csvInput.setFieldDelimiter(inputSpecification.delimiter);
        csvInput.setQuoteCharacter(inputSpecification.quoteChar);
        csvInput.setQuoteEscapeCharacter(inputSpecification.escape);
        csvInput.setRecordDelimiter(inputSpecification.lineSeparator);
        csvInput.setComments(inputSpecification.commentChar);

        InputSerialization inputSerialization = new InputSerialization();
        inputSerialization.setCsv(csvInput);
        switch(inputSpecification.compression) {
            case GZIP:
                inputSerialization.setCompressionType(CompressionType.GZIP);
                break;
            case NONE:
            default:
                inputSerialization.setCompressionType(CompressionType.NONE);
        }

        request.setInputSerialization(inputSerialization);

        CSVOutput csvOutput = new CSVOutput();
        csvOutput.setFieldDelimiter(INTERNAL_SERIALIZATION_FORMAT.getDelimiter());
        csvOutput.setRecordDelimiter(INTERNAL_SERIALIZATION_FORMAT.getLineSeparatorString());
        csvOutput.setQuoteCharacter(INTERNAL_SERIALIZATION_FORMAT.getQuote());
        csvOutput.setQuoteEscapeCharacter(INTERNAL_SERIALIZATION_FORMAT.getQuoteEscape());

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

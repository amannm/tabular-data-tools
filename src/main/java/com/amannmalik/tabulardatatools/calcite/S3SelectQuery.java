package com.amannmalik.tabulardatatools.calcite;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.*;
import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParserSettings;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;

import java.net.URI;

public class S3SelectQuery extends AbstractEnumerable<Object[]> {

    private final URI location;
    private final String query;

    public S3SelectQuery(URI location, String query) {
        this.location = location;
        this.query = query;
    }

    @Override
    public Enumerator<Object[]> enumerator() {
        SelectObjectContentResult result = executeQuery();
        SelectRecordsInputStream recordsInputStream = result.getPayload().getRecordsInputStream();
        CsvFormat csvFormat = new CsvFormat();
        csvFormat.setDelimiter(',');
        csvFormat.setLineSeparator("\n");
        csvFormat.setQuote('\"');
        csvFormat.setQuoteEscape('\\');
        CsvParserSettings parserSettings = new CsvParserSettings();
        parserSettings.setHeaderExtractionEnabled(false);
        parserSettings.setFormat(csvFormat);
        return new S3SelectResultSetEnumerator(recordsInputStream, parserSettings);
    }


    private SelectObjectContentResult executeQuery() {

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

        return s3.selectObjectContent(request);
    }
}

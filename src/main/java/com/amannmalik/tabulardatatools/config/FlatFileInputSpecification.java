package com.amannmalik.tabulardatatools.config;

public class FlatFileInputSpecification {

    public final char delimiter;
    public final String lineSeparator;
    public final char quoteChar;
    public final char escape;
    public final char commentChar;
    public final boolean header;
    public final boolean strictQuotes;
    public final boolean ignoreLeadingWhiteSpace;
    public final boolean ignoreQuotations;
    public final NullFieldIndicator nullFieldIndicator;
    public final CompressionType compression;

    public FlatFileInputSpecification(char delimiter, String lineSeparator, char quoteChar, char escape, char commentChar, boolean header, boolean strictQuotes, boolean ignoreLeadingWhiteSpace, boolean ignoreQuotations, NullFieldIndicator nullFieldIndicator, CompressionType compression) {
        this.delimiter = delimiter;
        this.lineSeparator = lineSeparator;
        this.quoteChar = quoteChar;
        this.escape = escape;
        this.commentChar = commentChar;
        this.header = header;
        this.strictQuotes = strictQuotes;
        this.ignoreLeadingWhiteSpace = ignoreLeadingWhiteSpace;
        this.ignoreQuotations = ignoreQuotations;
        this.nullFieldIndicator = nullFieldIndicator;
        this.compression = compression;
    }

    public enum CompressionType {
        NONE,
        GZIP
    }

    public enum NullFieldIndicator {
        EMPTY_SEPARATORS,
        EMPTY_QUOTES,
        BOTH,
        NEITHER;
    }

}

package com.amannmalik.tabulardatatools.config;

public class TabularFileReaderConfig {

    public final char separator;
    public final char quoteChar;
    public final char escape;
    public final boolean strictQuotes;
    public final boolean ignoreLeadingWhiteSpace;
    public final boolean ignoreQuotations;
    public final NullFieldIndicator nullFieldIndicator;

    public enum NullFieldIndicator {
        EMPTY_SEPARATORS,
        EMPTY_QUOTES,
        BOTH,
        NEITHER;
    }

    TabularFileReaderConfig(char separator, char quoteChar, char escape, boolean strictQuotes, boolean ignoreLeadingWhiteSpace, boolean ignoreQuotations, NullFieldIndicator nullFieldIndicator) {
        this.separator = separator;
        this.quoteChar = quoteChar;
        this.escape = escape;
        this.strictQuotes = strictQuotes;
        this.ignoreLeadingWhiteSpace = ignoreLeadingWhiteSpace;
        this.ignoreQuotations = ignoreQuotations;
        this.nullFieldIndicator = nullFieldIndicator;
    }


}

package com.amannmalik.tabulardatatools.config;

public class FlatFileOutputSpecification {
    public final char separator;
    public final char quoteChar;
    public final char escapeChar;
    public final String lineEnd;

    public FlatFileOutputSpecification(char separator, char quoteChar, char escapeChar, String lineEnd) {
        this.separator = separator;
        this.quoteChar = quoteChar;
        this.escapeChar = escapeChar;
        this.lineEnd = lineEnd;
    }
}

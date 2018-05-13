package com.amannmalik.tabulardatatools.config;

public class TabularFileWriterConfig {
    public final char separator;
    public final char quoteChar;
    public final char escapeChar;
    public final String lineEnd;

    public TabularFileWriterConfig(char separator, char quoteChar, char escapeChar, String lineEnd) {
        this.separator = separator;
        this.quoteChar = quoteChar;
        this.escapeChar = escapeChar;
        this.lineEnd = lineEnd;
    }
}

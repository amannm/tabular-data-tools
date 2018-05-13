package com.amannmalik.tabulardatatools.processor;

import com.amannmalik.tabulardatatools.config.TabularFileReaderConfig;
import com.amannmalik.tabulardatatools.config.TabularFileWriterConfig;
import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionCodec;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.OrcUtils;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.impl.BufferChunk;
import org.apache.orc.impl.InStream;
import org.apache.orc.impl.OrcCodecPool;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;


public class TabularFileProcessor {

    public static void extractOrcMetadata(java.nio.file.Path path) throws IOException {
        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
        int readSize = 16384;
        long fileSize = channel.size();
        ByteBuffer buffer = ByteBuffer.allocate(readSize);
        channel.read(buffer, fileSize - readSize);
        buffer.flip();

        int psLen = buffer.get(readSize - 1) & 0xff;
        int psOffset = readSize - 1 - psLen;

        byte[] psBuffer = new byte[psLen];
        System.arraycopy(buffer.array(), psOffset, psBuffer, 0, psLen);
        OrcProto.PostScript ps = OrcProto.PostScript.parseFrom(psBuffer);

        int footerSize = (int) ps.getFooterLength();
        CompressionKind kind = CompressionKind.valueOf(ps.getCompression().name());
        CompressionCodec codec = OrcCodecPool.getCodec(kind);

        int footerOffset = psOffset - footerSize;

        buffer.position(footerOffset);
        buffer.limit(footerOffset + footerSize);
        int blockSize = (int) ps.getCompressionBlockSize();
        OrcProto.Footer footer = OrcProto.Footer.parseFrom(InStream.createCodedInputStream("footer", Collections.singletonList(new BufferChunk(buffer, 0)), footerSize, codec, blockSize));

        TypeDescription typeDescription = OrcUtils.convertTypeFromProtobuf(footer.getTypesList(), 0);


    }

    @Deprecated
    private static String generateHiveS3Ddl(TypeDescription schema, URI tableS3Uri) {
        String tableName = Paths.get(tableS3Uri.getPath()).getFileName().toString();
        List<TypeDescription> children = schema.getChildren();
        List<String> fieldNames = schema.getFieldNames();
        int numCols = children.size();
        List<String> ddlLines = new ArrayList<>(numCols);
        for(int i = 0; i < numCols; i++) {
            String name = fieldNames.get(i);
            TypeDescription datatype = children.get(i);
            ddlLines.add(String.format("%s %s", name, datatype));
        }
        return String.format("CREATE EXTERNAL TABLE \"%s\" (%s) STORED AS ORC LOCATION '%s'",
                tableName,
                ddlLines.stream().collect(Collectors.joining(",")),
                tableS3Uri);
    }


    public static List<String> convertCsvToOrc(Path csvInputPath, Path orcOutputPath, TabularFileReaderConfig csvReaderConfig) throws IOException {

        CsvFormat csvFormat = new CsvFormat();
        csvFormat.setDelimiter(csvReaderConfig.separator);
        csvFormat.setQuote(csvReaderConfig.quoteChar);
        csvFormat.setQuoteEscape(csvReaderConfig.escape);
        CsvParserSettings parserSettings = new CsvParserSettings();
        parserSettings.setHeaderExtractionEnabled(true);
        parserSettings.setFormat(csvFormat);
        CsvParser csvParser = new CsvParser(parserSettings);
        BufferedReader reader = Files.newBufferedReader(csvInputPath, StandardCharsets.UTF_8);
        csvParser.beginParsing(reader);
        String[] headers = csvParser.getContext().headers();

        TypeDescription orcSchema = TypeDescription.createStruct();
        for (String s : headers) {
            orcSchema = orcSchema.addField(s, TypeDescription.createString());
        }
        Configuration conf = new Configuration();
        OrcFile.WriterOptions options = OrcFile.writerOptions(conf)
                .setSchema(orcSchema)
                .compress(CompressionKind.SNAPPY);
        Writer orcWriter = OrcFile.createWriter(new org.apache.hadoop.fs.Path(orcOutputPath.toString()), options);


        VectorizedRowBatch orcBatch = orcSchema.createRowBatch();

        String[] csvRecord;
        while ((csvRecord = csvParser.parseNext()) != null) {
            int rowNum = orcBatch.size++;
            for (int i = 0; i < orcBatch.numCols; i++) {
                BytesColumnVector orcColumn = (BytesColumnVector) orcBatch.cols[i];
                byte[] orcColumnValue = csvRecord[i].getBytes(StandardCharsets.UTF_8);
                orcColumn.setVal(rowNum, orcColumnValue);
            }
            if (orcBatch.size == orcBatch.getMaxSize()) {
                orcWriter.addRowBatch(orcBatch);
                orcBatch.reset();
            }
        }
        if (orcBatch.size != 0) {
            orcWriter.addRowBatch(orcBatch);
            orcBatch.reset();
        }
        orcWriter.close();
        csvParser.stopParsing();

        return Arrays.asList(headers);
    }

    public static void convertOrcToCsv(Path orcInputPath, Path csvOutputPath, TabularFileWriterConfig tabularFileWriterConfig) throws IOException {

        BufferedWriter outputWriter = Files.newBufferedWriter(csvOutputPath, StandardCharsets.UTF_8);
        CsvWriterSettings csvWriterSettings = new CsvWriterSettings();
        CsvFormat csvFormat = new CsvFormat();
        csvFormat.setQuote(tabularFileWriterConfig.quoteChar);
        csvFormat.setDelimiter(tabularFileWriterConfig.separator);
        csvFormat.setLineSeparator(tabularFileWriterConfig.lineEnd);
        csvFormat.setQuoteEscape(tabularFileWriterConfig.escapeChar);
        csvWriterSettings.setFormat(csvFormat);
        CsvWriter csvWriter = new CsvWriter(outputWriter,csvWriterSettings);

        Configuration conf = new Configuration();
        Reader orcReader = OrcFile.createReader(new org.apache.hadoop.fs.Path(orcInputPath.toString()), OrcFile.readerOptions(conf));
        TypeDescription orcSchema = orcReader.getSchema();
        RecordReader orcRecords = orcReader.rows();
        VectorizedRowBatch orcBatch = orcSchema.createRowBatch();
        List<String> fieldNames = orcSchema.getFieldNames();

        csvWriter.writeHeaders(fieldNames);

        while (orcRecords.nextBatch(orcBatch)) {
            for (int rowIndex = 0; rowIndex < orcBatch.size; rowIndex++) {
                for(int i = 0; i < orcBatch.cols.length; i++) {
                    BytesColumnVector orcColumn = (BytesColumnVector) orcBatch.cols[i];
                    String csvColumnValue = new String(orcColumn.vector[rowIndex], orcColumn.start[rowIndex], orcColumn.length[rowIndex], StandardCharsets.UTF_8);
                    csvWriter.addValue(csvColumnValue);
                }
                csvWriter.writeValuesToRow();
            }
            float progress = orcRecords.getProgress();
        }
        orcRecords.close();
        csvWriter.close();
    }
}

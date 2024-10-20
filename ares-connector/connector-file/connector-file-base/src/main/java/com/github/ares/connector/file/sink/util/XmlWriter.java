package com.github.ares.connector.file.sink.util;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.common.exceptions.CommonErrorCode;
import com.github.ares.common.utils.EncodingUtils;
import com.github.ares.common.utils.JsonUtils;
import com.github.ares.connector.file.exception.FileConnectorException;
import com.github.ares.connector.file.sink.config.FileSinkConfig;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.List;

/** The XmlWriter class provides functionality to write data in XML format. */
public class XmlWriter {

    private final FileSinkConfig fileSinkConfig;
    private final List<Integer> sinkColumnsIndexInRow;
    private final AresRowType aresRowType;
    private final Document document;
    private final Element rootElement;
    private final String fieldDelimiter;
    private OutputFormat format;

    public XmlWriter(
            FileSinkConfig fileSinkConfig,
            List<Integer> sinkColumnsIndexInRow,
            AresRowType aresRowType) {
        this.fileSinkConfig = fileSinkConfig;
        this.sinkColumnsIndexInRow = sinkColumnsIndexInRow;
        this.aresRowType = aresRowType;

        this.fieldDelimiter = fileSinkConfig.getFieldDelimiter();

        setXmlOutputFormat();
        document = DocumentHelper.createDocument();
        rootElement = document.addElement(fileSinkConfig.getXmlRootTag());
    }

    public void writeData(AresRow aresRow) {
        Element rowElement = rootElement.addElement(fileSinkConfig.getXmlRowTag());
        boolean useAttributeFormat = fileSinkConfig.getXmlUseAttrFormat();

        sinkColumnsIndexInRow.stream()
                .map(
                        index ->
                                new AbstractMap.SimpleEntry<>(
                                        aresRowType.getFieldName(index),
                                        convertToXmlString(
                                                aresRow.getField(index),
                                                aresRowType.getFieldType(index))))
                .forEach(
                        entry -> {
                            if (useAttributeFormat) {
                                rowElement.addAttribute(entry.getKey(), entry.getValue());
                            } else {
                                rowElement.addElement(entry.getKey()).addText(entry.getValue());
                            }
                        });
    }

    private String convertToXmlString(Object fieldValue, AresDataType<?> fieldType) {
        if (fieldValue == null) {
            return "";
        }

        switch (fieldType.getSqlType()) {
            case STRING:
            case DATE:
            case TIME:
            case TIMESTAMP:
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case DOUBLE:
            case FLOAT:
            case DECIMAL:
            case BOOLEAN:
                return fieldValue.toString();
            case NULL:
                return "";
            case ROW:
                Object[] fields = ((AresRow) fieldValue).getFields();
                String[] strings = new String[fields.length];
                for (int i = 0; i < fields.length; i++) {
                    strings[i] =
                            convertToXmlString(
                                    fields[i], ((AresRowType) fieldType).getFieldType(i));
                }
                return String.join(fieldDelimiter, strings);
            case MAP:
            case ARRAY:
                return JsonUtils.toJsonString(fieldValue);
            case BYTES:
                return new String((byte[]) fieldValue, StandardCharsets.UTF_8);
            default:
                throw new FileConnectorException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        "Ares format not support this data type " + fieldType.getSqlType());
        }
    }

    public void flushAndCloseXmlWriter(OutputStream output) throws IOException {
        XMLWriter xmlWriter = new XMLWriter(output, format);
        xmlWriter.write(document);
        xmlWriter.close();
    }

    private void setXmlOutputFormat() {
        this.format = OutputFormat.createPrettyPrint();
        this.format.setNewlines(true);
        this.format.setNewLineAfterDeclaration(true);
        this.format.setSuppressDeclaration(false);
        this.format.setExpandEmptyElements(false);
        this.format.setIndent("\t");
        Charset charset = EncodingUtils.tryParseCharset(fileSinkConfig.getEncoding());
        this.format.setEncoding(charset.name());
    }
}

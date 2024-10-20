package com.github.ares.format.text.splitor;


import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Slf4j
public class CsvLineSplitor implements TextLineSplitor, Serializable {
    private Map<Character, CSVFormat> splitorFormatMap = new HashMap<>();

    @Override
    public String[] spliteLine(String line, String splitor) {
        Character splitChar = splitor.charAt(0);
        if (Objects.isNull(splitorFormatMap.get(splitChar))) {
            splitorFormatMap.put(splitChar, CSVFormat.DEFAULT.withDelimiter(splitChar));
        }
        CSVFormat format = splitorFormatMap.get(splitChar);
        CSVParser parser = null;
        // Method to parse the line into CSV with the given separator
        try {
            // Create CSV parser
            parser = CSVParser.parse(line, format);
            // Parse the CSV records
            List<String> res = new ArrayList<>();
            for (CSVRecord record : parser.getRecords()) {
                for (String value : record) {
                    res.add(value);
                }
            }
            return res.toArray(new String[0]);
        } catch (Exception e) {
            log.error(ExceptionUtils.getMessage(e));
            return new String[0];
        } finally {
            if (Objects.nonNull(parser)) {
                try {
                    parser.close();
                } catch (IOException e) {
                    log.error(ExceptionUtils.getMessage(e));
                }
            }
        }
    }
}

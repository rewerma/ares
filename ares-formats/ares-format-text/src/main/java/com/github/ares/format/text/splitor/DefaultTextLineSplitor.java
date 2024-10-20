package com.github.ares.format.text.splitor;

import java.io.Serializable;

public class DefaultTextLineSplitor implements TextLineSplitor, Serializable {

    @Override
    public String[] spliteLine(String line, String seperator) {
        return line.split(seperator, -1);
    }
}

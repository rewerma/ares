package com.github.ares.parser.antlr4.plsql;

import com.github.ares.org.antlr.v4.runtime.BaseErrorListener;
import com.github.ares.org.antlr.v4.runtime.RecognitionException;
import com.github.ares.org.antlr.v4.runtime.Recognizer;

import java.util.ArrayList;
import java.util.List;

public class CustomErrorListener extends BaseErrorListener {
    private final List<String> errors = new ArrayList<>();

    public List<String> getErrors() {
        return errors;
    }

    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine,
                            String msg, RecognitionException e) {
        errors.add("Parse error, line: " + line + ":" + charPositionInLine + " " + msg);
    }
}

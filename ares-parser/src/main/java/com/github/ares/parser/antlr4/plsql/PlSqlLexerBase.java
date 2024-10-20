package com.github.ares.parser.antlr4.plsql;

import com.github.ares.org.antlr.v4.runtime.CharStream;
import com.github.ares.org.antlr.v4.runtime.Lexer;

public abstract class PlSqlLexerBase extends Lexer
{
    public PlSqlLexerBase self;
    
    public PlSqlLexerBase(CharStream input)
    {
        super(input);
        self = this;
    }

    protected boolean IsNewlineAtPos(int pos)
    {
        int la = _input.LA(pos);
        return la == -1 || la == '\n';
    }
}

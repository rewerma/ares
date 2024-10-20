package com.github.ares.spark.function.utils;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext;
import org.apache.spark.sql.catalyst.expressions.codegen.ExprCode;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.IndexedSeq;
import scala.collection.Seq;

public class DoubleTypeExpression extends Expression {

    public boolean nullable() {
        return false;
    }


    public Object eval(InternalRow input) {
        return null;
    }


    public ExprCode doGenCode(CodegenContext ctx, ExprCode ev) {
        return null;
    }


    public DataType dataType() {
        return DataTypes.DoubleType;
    }


    public Seq<Expression> children() {
        return null;
    }

    public Expression withNewChildrenInternal(IndexedSeq<Expression> newChildren) {
        return null;
    }


    public boolean canEqual(Object that) {
        return false;
    }


    public Object productElement(int n) {
        return null;
    }


    public int productArity() {
        return 0;
    }
}

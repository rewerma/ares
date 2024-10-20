package com.github.ares.spark.function.utils;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext;
import org.apache.spark.sql.catalyst.expressions.codegen.ExprCode;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.IndexedSeq;
import scala.collection.Seq;

public class ArrayTypeExpression extends Expression {
    @Override
    public boolean nullable() {
        return false;
    }

    @Override
    public Object eval(InternalRow input) {
        return null;
    }

    @Override
    public ExprCode doGenCode(CodegenContext ctx, ExprCode ev) {
        return null;
    }

    @Override
    public DataType dataType() {
        return DataTypes.createArrayType(DataTypes.ByteType);
    }

    @Override
    public Seq<Expression> children() {
        return null;
    }

    @Override
    public Expression withNewChildrenInternal(IndexedSeq<Expression> newChildren) {
        return null;
    }

    @Override
    public boolean canEqual(Object that) {
        return false;
    }

    @Override
    public Object productElement(int n) {
        return null;
    }

    @Override
    public int productArity() {
        return 0;
    }
}

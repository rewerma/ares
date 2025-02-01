
package com.github.ares.connctor.jdbc.internal.split;

import com.github.ares.common.exceptions.AresException;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * This query parameters generator is an helper class to parameterize from/to queries on a numeric
 * column. The generated array of from/to values will be equally sized to fetchSize (apart from the
 * last one), ranging from minVal up to maxVal.
 *
 * <p>For example, if there's a table <CODE>BOOKS</CODE> with a numeric PK <CODE>id</CODE>, using a
 * query like:
 *
 * <PRE>
 * SELECT * FROM BOOKS WHERE id BETWEEN ? AND ?
 * </PRE>
 *
 * <p>You can take advantage of this class to automatically generate the parameters of the BETWEEN
 * clause, based on the passed constructor parameters.
 */
public class JdbcNumericBetweenParametersProvider implements JdbcParameterValuesProvider {

    private final BigDecimal minVal;
    private final BigDecimal maxVal;

    private long batchSize;
    private int batchNum;

    /**
     * NumericBetweenParametersProviderJdbc constructor.
     *
     * @param minVal the lower bound of the produced "from" values
     * @param maxVal the upper bound of the produced "to" values
     */
    public JdbcNumericBetweenParametersProvider(BigDecimal minVal, BigDecimal maxVal) {
        if (minVal.compareTo(maxVal) > 0) {
            throw new AresException("minVal must not be larger than maxVal");
        }
        this.minVal = minVal;
        this.maxVal = maxVal;
    }

    /**
     * NumericBetweenParametersProviderJdbc constructor.
     *
     * @param fetchSize the max distance between the produced from/to pairs
     * @param minVal    the lower bound of the produced "from" values
     * @param maxVal    the upper bound of the produced "to" values
     */
    public JdbcNumericBetweenParametersProvider(
            long fetchSize, BigDecimal minVal, BigDecimal maxVal) {
        if (minVal.compareTo(maxVal) > 0) {
            throw new AresException("minVal must not be larger than maxVal");
        }
        this.minVal = minVal;
        this.maxVal = maxVal;
        ofBatchSize(fetchSize);
    }

    public JdbcNumericBetweenParametersProvider ofBatchSize(long batchSize) {
        if (batchSize <= 0) {
            throw new AresException("Batch size must be positive");
        }

        BigDecimal maxElemCount = (maxVal.subtract(minVal)).add(BigDecimal.valueOf(1));
        if (BigDecimal.valueOf(batchSize).compareTo(maxElemCount) > 0) {
            batchSize = maxElemCount.longValue();
        }
        this.batchSize = batchSize;
        this.batchNum =
                new Double(
                        Math.ceil(
                                (maxElemCount.divide(BigDecimal.valueOf(batchSize)))
                                        .doubleValue()))
                        .intValue();
        return this;
    }

    public JdbcNumericBetweenParametersProvider ofBatchNum(int batchNum) {
        if (batchSize <= 0) {
            throw new AresException("Batch size must be positive");
        }

        BigDecimal maxElemCount = (maxVal.subtract(minVal)).add(BigDecimal.valueOf(1));
        if (BigDecimal.valueOf(batchNum).compareTo(maxElemCount) > 0) {
            batchNum = maxElemCount.intValue();
        }
        this.batchNum = batchNum;
        // For the presence of a decimal we take the integer up
        this.batchSize =
                (maxElemCount.divide(BigDecimal.valueOf(batchNum), 2, RoundingMode.HALF_UP))
                        .setScale(0, RoundingMode.CEILING)
                        .longValue();
        return this;
    }

    @Override
    public Serializable[][] getParameterValues() {
        if (batchSize <= 0) {
            throw new AresException("Batch size and batch number must be positive. Have you called `ofBatchSize` or `ofBatchNum`?");
        }

        BigDecimal maxElemCount = (maxVal.subtract(minVal)).add(BigDecimal.valueOf(1));
        BigDecimal bigBatchNum =
                maxElemCount
                        .subtract(BigDecimal.valueOf(batchSize - 1))
                        .multiply(BigDecimal.valueOf(batchNum));

        Serializable[][] parameters = new Serializable[batchNum][2];
        BigDecimal start = minVal;
        for (int i = 0; i < batchNum; i++) {
            BigDecimal end =
                    start.add(BigDecimal.valueOf(batchSize))
                            .subtract(BigDecimal.valueOf(1))
                            .subtract(
                                    BigDecimal.valueOf(i).compareTo(bigBatchNum) >= 0
                                            ? BigDecimal.ONE
                                            : BigDecimal.ZERO);
            parameters[i] = new BigDecimal[]{start, end};
            start = end.add(BigDecimal.valueOf(1));
        }
        return parameters;
    }
}

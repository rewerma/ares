package com.github.ares.connctor.jdbc.internal;

import com.github.ares.api.table.catalog.TablePath;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.api.table.type.RowKind;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.connctor.jdbc.config.JdbcSourceConfig;
import com.github.ares.connctor.jdbc.internal.converter.JdbcRowConverter;
import com.github.ares.connctor.jdbc.internal.dialect.JdbcDialect;
import com.github.ares.connctor.jdbc.internal.dialect.JdbcDialectLoader;
import com.github.ares.connctor.jdbc.source.ChunkSplitter;
import com.github.ares.connctor.jdbc.source.JdbcSourceSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * InputFormat to read data from a database and generate Rows. The InputFormat has to be configured
 * using the supplied InputFormatBuilder. A valid RowTypeInfo must be properly configured in the
 * builder
 */
public class JdbcInputFormat implements Serializable {

    private static final long serialVersionUID = 2L;
    private static final Logger LOG = LoggerFactory.getLogger(JdbcInputFormat.class);

    private final JdbcDialect jdbcDialect;
    private final JdbcRowConverter jdbcRowConverter;
    private final Map<TablePath, AresRowType> tables;
    private final ChunkSplitter chunkSplitter;

    private transient String splitTableId;
    private transient AresRowType splitRowType;
    private transient PreparedStatement statement;
    private transient ResultSet resultSet;
    private volatile boolean hasNext;

    public JdbcInputFormat(JdbcSourceConfig config, Map<TablePath, AresRowType> tables) {
        this.jdbcDialect =
                JdbcDialectLoader.load(
                        config.getJdbcConnectionConfig().getUrl(), config.getCompatibleMode());
        this.chunkSplitter = ChunkSplitter.create(config);
        this.jdbcRowConverter = jdbcDialect.getRowConverter();
        this.tables = tables;
    }

    public void openInputFormat() {
    }

    public void closeInputFormat() throws IOException {
        close();

        if (chunkSplitter != null) {
            chunkSplitter.close();
        }
    }

    /**
     * Connects to the source database and executes the query
     *
     * @param inputSplit which is ignored if this InputFormat is executed as a non-parallel source,
     *                   a "hook" to the query parameters otherwise (using its <i>parameterId</i>)
     * @throws IOException if there's an error during the execution of the query
     */
    public void open(JdbcSourceSplit inputSplit) throws IOException {
        try {
            splitRowType = tables.get(inputSplit.getTablePath());
            splitTableId = inputSplit.getTablePath().toString();

            statement = chunkSplitter.generateSplitStatement(inputSplit);
            resultSet = statement.executeQuery();
            hasNext = resultSet.next();
        } catch (SQLException se) {
            throw new AresException(
                    "open() failed." + se.getMessage(),
                    se);
        }
    }

    /**
     * Closes all resources used.
     *
     * @throws IOException Indicates that a resource could not be closed.
     */
    public void close() throws IOException {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                LOG.info("ResultSet couldn't be closed - " + e.getMessage());
            }
        }
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                LOG.info("Statement couldn't be closed - " + e.getMessage());
            }
        }
    }

    /**
     * Checks whether all data has been read.
     *
     * @return boolean value indication whether all data has been read.
     */
    public boolean reachedEnd() {
        return !hasNext;
    }

    /**
     * Convert a row of data to AresRow
     */
    public AresRow nextRecord() {
        try {
            if (!hasNext) {
                return null;
            }
            AresRow AresRow = jdbcRowConverter.toInternal(resultSet, splitRowType);
            AresRow.setTableId(splitTableId);
            AresRow.setRowKind(RowKind.INSERT);

            // update hasNext after we've read the record
            hasNext = resultSet.next();
            return AresRow;
        } catch (SQLException se) {
            throw new AresException(
                    "Couldn't read data - " + se.getMessage(),
                    se);
        } catch (NullPointerException npe) {
            throw new AresException(
                    "Couldn't access resultSet",
                    npe);
        }
    }
}

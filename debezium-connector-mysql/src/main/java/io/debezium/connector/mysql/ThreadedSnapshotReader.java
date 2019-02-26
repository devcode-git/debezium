/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.mysql.RecordMakers.RecordsForTable;
import io.debezium.connector.mysql.SnapshotReader.RecordRecorder;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.JdbcConnection.StatementFactory;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

public class ThreadedSnapshotReader {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final ExecutorService queryExecutor;
    private MySqlTaskContext context;
    private MySqlJdbcContext connectionContext;
    private StatementFactory statementFactory;
    private ReadFieldFactory readFieldFactory;

    public ThreadedSnapshotReader(MySqlTaskContext context, MySqlJdbcContext connectionContext, StatementFactory statementFactory, ReadFieldFactory readFieldFactory) {
        this.context = context;
        this.connectionContext = connectionContext;
        this.statementFactory = statementFactory;
        this.readFieldFactory = readFieldFactory;
        this.queryExecutor = Executors.newFixedThreadPool(context.snapShotBatchThreads());
    }

    public void shutdown() {
        this.queryExecutor.shutdownNow();
        this.connectionContext.closePool();
    }

    public long executeQuery(MySqlSchema schema, TableId tableId, RecordRecorder recorder, 
        RecordsForTable recordMaker, long ts, String sqlTemplate, Long requestedStart, Long requestedEnd) {
        
        final String sqlUse = "use " + quote(tableId.catalog()) + ";";
        final Map<TableId, String> selectCountOverrides = getSnapshotSelectCountOverridesByTable();
        final String sqlCount = selectCountOverrides.getOrDefault(tableId, "SELECT COUNT(*) FROM "  + tableId.table() + ";");

        logger.info("Counting rows using query: {}", sqlCount);

        // Count the rows, we will get some extra rows as we do the count after the binlog position was stored 
        // Also we're not using any db locks for the threads
        final AtomicLong totalTableRows = new AtomicLong();
        try {
            try(final JdbcConnection conn = connectionContext.pooledJdbc()) {
                conn.executeWithoutCommitting(sqlUse);
                conn.query(sqlCount, rs -> {
                    if (rs.next()) {
                        totalTableRows.set(rs.getLong(1));
                    }
                });
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        logger.info("Total rows: {}", totalTableRows.get());

        final long start = requestedStart != null ? requestedStart : 0;
        final long end = requestedEnd != null ? Math.min(totalTableRows.get(), requestedEnd) : totalTableRows.get();

        final List<Future<Long>> queryTaskResults = new ArrayList<>();

        final int batchSize = context.snapShotBatchSize();

        for(long i = start; i <= end; i += batchSize) {
            final long batchStart = i;
            final long batchEnd = Math.min(i + batchSize, totalTableRows.get());
            final String sql = String.format(sqlTemplate, batchStart, batchEnd);
            logger.trace("Query: {}", sql);
            final Table table = schema.tableFor(tableId);
            final int numColumns = table.columns().size();

            final Callable<Long> queryTask = () -> {
                final AtomicLong result = new AtomicLong();
                try (JdbcConnection conn = connectionContext.pooledJdbc()) {
                    conn.executeWithoutCommitting(sqlUse);  
                    
                    conn.query(sql, statementFactory, rs -> {
                        long rowNum = handleResult(table, rs, numColumns, recorder, recordMaker, ts);
                        result.set(rowNum);
                    });
                }
                return result.get();
            };

            final Future<Long> queryTaskResult = queryExecutor.submit(queryTask);
            queryTaskResults.add(queryTaskResult);
            
        }

        queryExecutor.shutdown();
        try {
            queryExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        long totalRows = queryTaskResults.stream().mapToLong( r -> {
            try {
                return r.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).sum();

        return totalRows;

    }

    protected String quote(TableId id) {
        return quote(id.catalog()) + "." + quote(id.table());
    }

    protected String quote(String dbOrTableName) {
        return "`" + dbOrTableName + "`";
    }
    

    protected long handleResult(Table table, ResultSet rs, int numColumns, RecordRecorder recorder, RecordsForTable recordMaker, long ts) {
        try {
            final Object[] row = new Object[numColumns];
            long rowNum = 0;
            while (rs.next()) {
                for (int col = 0; col != numColumns; ++col) {
                    Column actualColumn = table.columns().get(col);
                    row[col] = readFieldFactory.readField(rs, col + 1, actualColumn);
                }
                recorder.recordRow(recordMaker, row, ts);
                rowNum++;
            }
            return rowNum;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @FunctionalInterface
    public interface ReadFieldFactory {
        Object readField(ResultSet rs, int fieldNo, Column actualColumn) throws SQLException;
    }

    /**
     * Returns any SELECT COUNT overrides, if present.
     */
    private Map<TableId, String> getSnapshotSelectCountOverridesByTable() {
        final String tableList = context.getSnapshotSelectCountOverrides();

        if (tableList == null) {
            return Collections.emptyMap();
        }

        final Map<TableId, String> snapshotSelectCountOverridesByTable = new HashMap<>();

        for (String table : tableList.split(",")) {
            snapshotSelectCountOverridesByTable.put(
                TableId.parse(table),
                context.config().getString(MySqlConnectorConfig.SNAPSHOT_SELECT_COUNT_STATEMENT_OVERRIDES_BY_TABLE + "." + table)
            );
        }

        return snapshotSelectCountOverridesByTable;
    }

}
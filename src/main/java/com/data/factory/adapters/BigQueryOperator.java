package com.data.factory.adapters;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Locale;


import com.data.factory.exceptions.DataWareHouseException;
import com.data.factory.models.Contract;
import com.data.factory.ports.DataWareHouse;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.JobInfo.SchemaUpdateOption;
import com.google.cloud.bigquery.LoadJobConfiguration.Builder;
import com.google.common.collect.ImmutableList;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BigQueryOperator implements DataWareHouse {

    private String projectId;

    public BigQueryOperator(String projectId) throws DataWareHouseException {
        this.projectId = validateProjectID(projectId);
    }

    private String validateProjectID(String tmp) throws DataWareHouseException {
        if (tmp == null || tmp.isEmpty()) {
            throw new DataWareHouseException("projectId cannot be null or empty!");
        }
        return tmp;
    }

    private GoogleCredentials getCredentials() throws DataWareHouseException {
        try {
            return GoogleCredentials.getApplicationDefault();
        } catch (IOException e) {
            throw new DataWareHouseException(e.getMessage());
        }
    }

    private BigQuery getBq() throws DataWareHouseException {
        return BigQueryOptions
                .newBuilder()
                .setCredentials(getCredentials())
                .setProjectId(projectId).build()
                .getService();
    }

    @Override
    public Boolean datasetExists(String dataset) throws DataWareHouseException {
        BigQuery bq = getBq();
        return bq.getDataset(DatasetId.of(dataset)) != null;
    }

    @Override
    public void createDataset(String dataset) throws DataWareHouseException {
        log.info("Creating dataset {} in BigQuery", dataset);
        DatasetInfo datasetInfo = DatasetInfo.newBuilder(dataset).build();
        BigQuery bq = getBq();
        try {
            bq.create(datasetInfo);
        } catch (com.google.cloud.bigquery.BigQueryException e) {
            throw new DataWareHouseException(
                    String.format("Unable to create dataset %s - {%s} {%s}", dataset, e.getClass(), e.getMessage()));
        }
    }

    @Override
    public BigInteger loadTable(Contract request) throws DataWareHouseException {
        if (request == null) throw new DataWareHouseException("request cannot be null.");
        return runJob(request);
    }

    private BigInteger runJob(Contract request) throws DataWareHouseException {
        LoadJobConfiguration loadJobCnf = getDefaultLoadJobCnf(request);
        BigQuery bq = getBq();
        Job job;
        try {
            job = bq.create(JobInfo.of(JobId.of(java.util.UUID.randomUUID().toString()), loadJobCnf));
        } catch (com.google.cloud.bigquery.BigQueryException e) {
            throw new DataWareHouseException(String.format("Unable to create load job into table %s.%s - {%s} : {%s}",
                    request.getDataset(), this.getTableName(request), e.getClass(), e.getMessage()));
        }
        try {
            Job completedJob = job.waitFor();
            if (completedJob == null) {
                throw new DataWareHouseException("Job not executed since it no longer exists.");
            } else if (completedJob.getStatus().getError() != null) {
                throw new DataWareHouseException(String.format("Error loading table to BigQuery: %s",
                        completedJob.getStatus().getError().toString()));
            } else {
                TableId tableId = TableId.of(request.getDataset(), this.getTableName(request));
                BigInteger numRows = bq.getTable(tableId).getNumRows();
                log.info("Successfully loaded {} rows into {}.{}.", numRows, request.getDataset(), this.getTableName(request));
                return numRows;
            }
        } catch (Exception e) {
            Thread.currentThread().interrupt();
            throw new DataWareHouseException(e.getMessage());
        }
    }


    private String getProcessDateFormat(Contract request) throws DataWareHouseException {
        String processDateNoDash = request.getProcessDate().replace("-", "");
        if (request.getPartitionTable().getPartitionType().equals("YEAR"))
            return processDateNoDash.substring(0, 4);
        else if (request.getPartitionTable().getPartitionType().equals("MONTH"))
            return processDateNoDash.substring(0, 6);
        else if (request.getPartitionTable().getPartitionType().equals("DAY"))
            return processDateNoDash.substring(0, processDateNoDash.length());
        else
            throw new DataWareHouseException(String.format("Unimplemented partitionType [%s].", request.getPartitionTable().getPartitionType()));
    }

    private String getTableName(Contract request) throws DataWareHouseException {
        if (request.isPartitionedTable()) {
            if (request.getPartitionTable().containsPartitionField()) {
                return request.getTable();
            } else return request.getTable() + "$" + this.getProcessDateFormat(request);
        } else return request.getTable();
    }


    private TimePartitioning getPartitioningType(Contract request) {
        TimePartitioning.Type timePartitionType = TimePartitioning.Type.DAY;

        if (request.getPartitionTable().getPartitionType().equals("YEAR"))
            timePartitionType = TimePartitioning.Type.YEAR;
        if (request.getPartitionTable().getPartitionType().equals("MONTH"))
            timePartitionType = TimePartitioning.Type.MONTH;
        if (request.getPartitionTable().getPartitionType().equals("DAY"))
            timePartitionType = TimePartitioning.Type.DAY;

        if (request.getPartitionTable().containsPartitionField())
            return TimePartitioning
                    .newBuilder(timePartitionType)
                    .setField(request.getPartitionTable().getPartitionField())
                    .setRequirePartitionFilter(true)
                    .build();
        else
            return TimePartitioning
                    .newBuilder(timePartitionType)
                    .setRequirePartitionFilter(true)
                    .build();
    }

    private LoadJobConfiguration getDefaultLoadJobCnf(Contract request) throws DataWareHouseException {
        String fileFormat = request.getFormat().toLowerCase(Locale.ROOT);
        TableId tableId = TableId.of(request.getDataset(), this.getTableName(request));
        Builder loadJobConf;

        if (fileFormat.equals("avro")) {
            loadJobConf = LoadJobConfiguration
                    .newBuilder(tableId, request.getLandingPath() + "*.avro")
                    .setFormatOptions(FormatOptions.avro())
                    .setUseAvroLogicalTypes(true);
        } else if (fileFormat.equals("parquet")) {
            loadJobConf = LoadJobConfiguration
                    .newBuilder(tableId, request.getLandingPath() + "*.parquet")
                    .setFormatOptions(FormatOptions.parquet());
        }
        else {
            throw new DataWareHouseException(String.format("unimplemented file format [%s]", fileFormat));
        }

        loadJobConf.setCreateDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED);
        loadJobConf.setWriteDisposition(JobInfo.WriteDisposition.WRITE_TRUNCATE);

        // If table is partitioned
        if (request.isPartitionedTable()) {
            TimePartitioning partitioning = getPartitioningType(request);
            loadJobConf = loadJobConf
                    .setTimePartitioning(partitioning);
            if (!request.getPartitionTable().containsPartitionField())
                loadJobConf = loadJobConf
                        .setSchemaUpdateOptions(ImmutableList.of(SchemaUpdateOption.ALLOW_FIELD_ADDITION));
        }
        // If table contains bigQueryOptions.
        if (request.containsBqOptions()) {
            loadJobConf = loadJobConf.setClustering(
                    Clustering
                            .newBuilder()
                            .setFields(Arrays.asList(request.getBqOption().getClusterBy().split(","))).build());
        }
        return loadJobConf.build();
    }
}
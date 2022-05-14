package com.data.factory.models;

import com.data.factory.exceptions.RequestException;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@AllArgsConstructor
@NoArgsConstructor
@Getter
public class Contract {

    @JsonProperty
    private String landingPath;

    @JsonProperty
    private String projectId;

    @JsonProperty
    private String dataset;

    @JsonProperty
    private String table;

    @JsonProperty
    private String processDate;

    @JsonProperty
    private String format;

    // Optional fields
    @JsonProperty
    private BqOption bqOption;

    @JsonProperty
    private PartitionTable partitionTable;

    public Boolean isValid() throws RequestException{
        validateVariable(landingPath,"landingPath");
        validateVariable(projectId,"projectId");
        validateVariable(dataset,"dataset");
        validateVariable(table,"table");
        validateVariable(processDate,"processDate");
        isValidDate(processDate);

        if (this.isPartitionedTable()) this.partitionTable.isValid();

        if(this.containsBqOptions()) this.bqOption.isValid();

        return Boolean.TRUE;
    }

    public Boolean containsBqOptions(){
        return !(bqOption == null);
    }

    public Boolean isPartitionedTable(){
        return !(partitionTable == null);
    }

    private Boolean validateVariable(String value,String variable) throws RequestException{
        if(value==null || value.isEmpty()){
            throw new RequestException(String.format("%s cannot be null or empty!",variable));
        }
        return Boolean.TRUE;
    }


    public Boolean isValidDate(String datePartition) throws RequestException{
        Pattern pattern = Pattern.compile("(\\d{4}\\-\\d{2}\\-\\d{2})");
        Matcher matcher = pattern.matcher(datePartition);
        if(!matcher.find()){
            throw new RequestException("datePartition doesn't date with pattern yyyy-mm-dd");
        }
        return Boolean.TRUE;
    }
}

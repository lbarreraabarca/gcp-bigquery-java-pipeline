package com.data.factory.models;

import com.data.factory.exceptions.RequestException;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.ArrayList;
import java.util.Locale;

@ToString
@NoArgsConstructor
@AllArgsConstructor
public class PartitionTable {

    @JsonProperty
    private String partitionType;
    @JsonProperty
    private String partitionField;

    public Boolean isValid() throws RequestException {
        if(partitionType == null || partitionType.isEmpty()) throw new RequestException("partitionType cannot be null or empty.");
        else if (!this.isValidPartitionType(partitionType)) throw new RequestException("partitionType must be the following values DAY, HOUR, MONTH, or YEAR.");
        else this.partitionType = partitionType.toUpperCase(Locale.ROOT);

        if(partitionField == null) partitionField = "";

        return Boolean.TRUE;
    }

    public PartitionTable(String partitionType) {
        this.partitionType = partitionType;
    }
    public Boolean isValidPartitionType(String partitionType){
        ArrayList<String> validPartitionTypeValues = new ArrayList<>();
        validPartitionTypeValues.add("HOUR");
        validPartitionTypeValues.add("DAY");
        validPartitionTypeValues.add("MONTH");
        validPartitionTypeValues.add("YEAR");
        return validPartitionTypeValues.contains(partitionType);
    }

    public Boolean containsPartitionField(){
        if (!(partitionField == null))
            return ! partitionField.isEmpty();
        else return !(partitionField == null);
    }

    public String getPartitionField() {
        return partitionField;
    }

    public String getPartitionType() {
        return partitionType;
    }
}

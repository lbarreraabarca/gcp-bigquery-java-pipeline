package com.data.factory.models;

import com.data.factory.exceptions.RequestException;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class BqOption {

    private String clusterBy;

    public Boolean isValid() throws RequestException{
        if(this.clusterBy == null || this.clusterBy.isEmpty()) throw new RequestException("clusterBy cannot be null or empty!");
        return Boolean.TRUE;
    }

}


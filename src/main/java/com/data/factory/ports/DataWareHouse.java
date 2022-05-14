package com.data.factory.ports;

import com.data.factory.exceptions.DataWareHouseException;
import com.data.factory.models.Contract;

import java.math.BigInteger;

public interface DataWareHouse {
    Boolean datasetExists(String dataset) throws DataWareHouseException;
    void createDataset(String dataset) throws DataWareHouseException;
    BigInteger loadTable(Contract request) throws DataWareHouseException;
}

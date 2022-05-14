package com.data.factory.services;


import com.data.factory.exceptions.ServiceException;
import com.data.factory.models.Contract;
import com.data.factory.ports.DataWareHouse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

public class Service {
    private static final Logger log = LoggerFactory.getLogger(Service.class);

    private DataWareHouse dwh;
    private static String INVALID_ATTRIBUTE = "%s cannot be null or empty.";

    public Service(DataWareHouse dwh) throws ServiceException {
        if (dwh == null) throw new ServiceException(String.format(INVALID_ATTRIBUTE, "dwh"));
        else this.dwh = dwh;
    }

    public ResponseEntity<String> invoke(Contract request) throws ServiceException {
        try {
            if (!dwh.datasetExists(request.getDataset())) dwh.createDataset(request.getDataset());
            dwh.loadTable(request);
            return new ResponseEntity<>("OK", HttpStatus.OK);
        } catch (Exception e){
            throw new ServiceException(String.format("%s %s", e.getClass(), e.getMessage()));
        }
    }
}

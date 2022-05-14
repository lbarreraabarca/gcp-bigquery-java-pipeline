package com.data.factory;

import com.data.factory.adapters.Base64Encoder;
import com.data.factory.adapters.BigQueryOperator;
import com.data.factory.exceptions.ControllerException;
import com.data.factory.models.Contract;
import com.data.factory.models.EnvironmentVariables;
import com.data.factory.ports.DataWareHouse;
import com.data.factory.ports.Encoder;
import com.data.factory.services.Service;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class App {
    private static final Logger LOG = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws ControllerException {
        try {
            LOG.info("Starting pipeline.");
            EnvironmentVariables env = new EnvironmentVariables();
            String encodedInput = env.getEncodedInput();
            Encoder encoder = new Base64Encoder();
            String decodedInput = encoder.decode(encodedInput);
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);
            String jsonNode = objectMapper.readTree(decodedInput).at("/gcpCloudStorage").toString();
            Contract request = objectMapper.readValue(jsonNode, Contract.class);

            DataWareHouse dwh = new BigQueryOperator(env.getProjectId());
            Service service = new Service(dwh);
            service.invoke(request);
            LOG.info("Pipeline finished successfully.");
        } catch (Exception e){
            throw new ControllerException(String.format("%s %s", e.getClass(), e.getMessage()));
        }
    }
}


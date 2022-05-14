package com.data.factory.models;

import com.data.factory.exceptions.EnvironmentException;
import lombok.Getter;

@Getter
public class EnvironmentVariables {
    private String projectId;
    private String encodedInput;

    public EnvironmentVariables() throws EnvironmentException {
        this.projectId = this.validateProjectId();
        this.encodedInput = this.validateEncodedInput();
    }

    public String validateProjectId() throws EnvironmentException {
        String temp = System.getenv("ENV_GCP_PROJECT_ID");
        if (temp == null || temp.isEmpty()) throw new EnvironmentException("c cannot be null or empty.");
        return temp;
    }

    public String validateEncodedInput() throws EnvironmentException {
        String temp = System.getenv("ENV_INPUT_CONTRACT");
        if (temp == null || temp.isEmpty()) throw new EnvironmentException("ENV_INPUT_CONTRACT cannot be null or empty.");
        return temp;
    }
}

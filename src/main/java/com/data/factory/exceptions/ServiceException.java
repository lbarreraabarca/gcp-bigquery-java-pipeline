package com.data.factory.exceptions;

public class ServiceException extends ControllerException{
    public ServiceException(String errorMessage) {
        super(errorMessage);
    }
}
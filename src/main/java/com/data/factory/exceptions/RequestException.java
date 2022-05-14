package com.data.factory.exceptions;

public class RequestException extends ControllerException{
    public RequestException(String errorMessage) {
        super(errorMessage);
    }
}
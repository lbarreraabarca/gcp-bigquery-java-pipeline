package com.data.factory.adapters;

import com.data.factory.exceptions.EncoderException;
import com.data.factory.ports.Encoder;

import java.util.Base64;

public class Base64Encoder implements Encoder {

    public String encode(String input) throws EncoderException {
        if (input == null || input.isEmpty()) throw new EncoderException("input cannot be null or empty.");
        else return Base64.getEncoder().encodeToString(input.getBytes());
    }

    public String decode(String input) throws EncoderException {
        if (input == null || input.isEmpty()) throw new EncoderException("input cannot be null or empty.");
        else {
            byte[] bytes = Base64.getDecoder().decode(input);
            return new String(bytes);
        }
    }
}

package com.data.factory.ports;

import com.data.factory.exceptions.EncoderException;

public interface Encoder {
    String encode(String input) throws EncoderException;
    String decode(String input) throws EncoderException;
}

package com.data.factory.adapters;

import com.data.factory.exceptions.EncoderException;
import com.data.factory.ports.Encoder;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class Base64EncoderTest {

    @Test
    void encoderTest() throws EncoderException {
        String decodeMessage = "hello_world";
        Encoder encoder = new Base64Encoder();
        String expected = "aGVsbG9fd29ybGQ=";
        Assert.assertEquals(expected, encoder.encode(decodeMessage));
    }

    @Test
    void encoderWhenInputIsNullTest() {
        Encoder encoder = new Base64Encoder();
        Assertions.assertThrows(EncoderException.class, () ->  encoder.encode(null));
    }

    @Test
    void encoderWhenInputIsEmptyTest() {
        Encoder encoder = new Base64Encoder();
        Assertions.assertThrows(EncoderException.class, () ->  encoder.encode(""));
    }

    @Test
    void decoderTest() throws EncoderException {
        String encodeMessage = "aGVsbG9fd29ybGQ=";
        Encoder encoder = new Base64Encoder();
        String expected = "hello_world";

        Assert.assertEquals(expected, encoder.decode(encodeMessage));
    }

    @Test
    void decoderWhenInputIsNullTest() {
        Encoder encoder = new Base64Encoder();
        Assertions.assertThrows(EncoderException.class, () ->  encoder.decode(null));
    }

    @Test
    void decoderWhenInputIsEmptyTest() {
        Encoder encoder = new Base64Encoder();
        Assertions.assertThrows(EncoderException.class, () ->  encoder.decode(""));
    }
}

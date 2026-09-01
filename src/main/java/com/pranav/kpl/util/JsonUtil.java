package com.pranav.kpl.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;

@Component
public class JsonUtil {
    private static final Logger logger = LoggerFactory.getLogger(JsonUtil.class);
    private final ObjectMapper objectMapper;

    public JsonUtil(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public byte[] toJsonBytes(Object object) {
        try {
            return objectMapper.writeValueAsBytes(object);
        } catch (Exception e) {
            logger.error("Error converting object to JSON bytes", e);
            throw new RuntimeException("Failed to serialize object to JSON", e);
        }
    }

    public String toJsonString(Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (Exception e) {
            logger.error("Error converting object to JSON string", e);
            throw new RuntimeException("Failed to serialize object to JSON", e);
        }
    }

    public ByteBuffer toJsonByteBuffer(Object object) {
        return ByteBuffer.wrap(toJsonBytes(object));
    }

    public <T> T fromJson(String json, Class<T> valueType) {
        try {
            return objectMapper.readValue(json, valueType);
        } catch (Exception e) {
            logger.error("Error parsing JSON string", e);
            throw new RuntimeException("Failed to deserialize JSON", e);
        }
    }

    public <T> T fromJsonBytes(byte[] jsonBytes, Class<T> valueType) {
        try {
            return objectMapper.readValue(jsonBytes, valueType);
        } catch (Exception e) {
            logger.error("Error parsing JSON bytes", e);
            throw new RuntimeException("Failed to deserialize JSON", e);
        }
    }
}

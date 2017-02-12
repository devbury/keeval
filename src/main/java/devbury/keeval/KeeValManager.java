/*
 * Copyright 2017 David Noel
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package devbury.keeval;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.util.FileCopyUtils;

import java.io.IOException;
import java.io.StringWriter;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class KeeValManager {

    private static final Logger logger = LoggerFactory.getLogger(KeeValManager.class);

    private final ObjectMapper objectMapper;
    private final JdbcTemplate jdbcTemplate;

    public KeeValManager(DataSourceProvider dataSourceProvider, ObjectMapperProvider objectMapperProvider) {
        this.objectMapper = objectMapperProvider.objectMapper();
        this.jdbcTemplate = new JdbcTemplate(dataSourceProvider.dataSource());
    }

    public <T> KeeValRepository<T> repository(Class<T> type) {
        return new KeeValRepository<>(type, this);
    }

    <T> Map<String, String> findAllRawAsMap(Class<T> type) {
        return jdbcTemplate.query("SELECT KEY, VALUE FROM OBJECTS WHERE TYPE = ?", new Object[]{type.getName()},
                rawKeyValueRowMapper())
                .stream().collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue));
    }

    <T> Optional<String> findRawByKey(Object key, Class<T> type) {
        try {
            return Optional.of(jdbcTemplate.queryForObject("SELECT VALUE FROM OBJECTS WHERE KEY = ? AND TYPE = ?",
                    new Object[]{key, type.getName()}, rawValueRowMapper()));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    <T> Optional<T> findByKey(Object key, Class<T> type) {
        try {
            return Optional.of(jdbcTemplate.queryForObject("SELECT VALUE FROM OBJECTS WHERE KEY = ? AND TYPE = ?",
                    new Object[]{key, type.getName()}, valueRowMapper(type)));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    void update(Object key, Object value) {
        try {
            String stringVal = objectMapper.writeValueAsString(value);
            jdbcTemplate.update("UPDATE OBJECTS SET VALUE = ? WHERE KEY = ? AND TYPE = ?", stringVal, key, value
                    .getClass()
                    .getCanonicalName());
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    void createOrUpdate(Object key, Object value) {
        try {
            create(key, value);
        } catch (DuplicateKeyException e) {
            update(key, value);
        }
    }

    <T> Map<String, T> findAllAsMap(Class<T> type) {
        return jdbcTemplate.query("SELECT KEY, VALUE FROM OBJECTS WHERE TYPE = ?",
                new Object[]{type.getName()}, keyValueRowMapper(type))
                .stream().collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue));
    }

    <T> List<T> findAll(Class<T> type) {
        return jdbcTemplate.query("SELECT VALUE FROM OBJECTS WHERE TYPE = ?",
                new Object[]{type.getName()}, valueRowMapper(type));
    }

    <T> void delete(Object key, Class<T> type) {
        jdbcTemplate.update("DELETE FROM OBJECTS WHERE KEY = ? AND TYPE = ?", key, type.getName());
    }

    void create(Object key, Object value) {
        try {
            String stringVal = objectMapper.writeValueAsString(value);
            jdbcTemplate.update("INSERT INTO OBJECTS(KEY, TYPE, VALUE) VALUES (?,?,?)", key, value.getClass()
                    .getCanonicalName(), stringVal);
            logger.debug("saved type : {}, key : {}, value : {}", value.getClass().getCanonicalName(), key, stringVal);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private RowMapper<String> rawValueRowMapper() {
        return (rs, i) -> rawValue(rs);
    }

    private <T> RowMapper<T> valueRowMapper(Class<T> type) {
        return (rs, i) -> inflate(rs, type);
    }

    private RowMapper<KeyValue<String>> rawKeyValueRowMapper() {
        return (rs, i) -> new KeyValue<>(rs.getString("KEY"), rawValue(rs));
    }

    private <T> RowMapper<KeyValue<T>> keyValueRowMapper(Class<T> type) {
        return (rs, i) -> new KeyValue<>(rs.getString("KEY"), inflate(rs, type));
    }

    private String rawValue(ResultSet rs) throws SQLException {
        Clob clob = rs.getClob("VALUE");
        StringWriter stringValue = new StringWriter((int) clob.length());
        try {
            FileCopyUtils.copy(clob.getCharacterStream(), stringValue);
            return stringValue.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private <T> T inflate(ResultSet rs, Class<T> type) throws SQLException {
        String value = rawValue(rs);
        try {
            logger.debug("Inflating to type {}, {}", type, value);
            return objectMapper.readValue(value, type);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static class KeyValue<T> {
        final String key;
        final T value;

        KeyValue(String key, T value) {
            this.key = key;
            this.value = value;
        }

        String getKey() {
            return key;
        }

        T getValue() {
            return value;
        }
    }
}
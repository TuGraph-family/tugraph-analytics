/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.geaflow.ai.common.id;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Stable keys for extracted facts and their source records. Fact keys are semantic:
 * the document and source span belong to evidence keys, not to fact keys.
 *
 * <p>The binary format uses length-prefixed UTF-8 strings and explicit value types.
 * Property maps are sorted by key, while lists retain their order. Changing this
 * encoding requires a new key format version.</p>
 */
public final class IdempotencyKeys {

    private static final String FORMAT_VERSION = "v1";
    private static final char[] HEX = "0123456789abcdef".toCharArray();

    private IdempotencyKeys() {
    }

    public static String vertexFact(String schemaVersion, String label, String entityId,
                                    Map<String, ?> properties) {
        return key("vertex", out -> {
            writeString(out, required(schemaVersion, "schemaVersion"));
            writeString(out, required(label, "label"));
            writeString(out, required(entityId, "entityId"));
            writeValue(out, Objects.requireNonNull(properties, "properties"));
        });
    }

    public static String edgeFact(String schemaVersion, String label, String sourceId,
                                  String destinationId, Map<String, ?> properties) {
        return key("edge", out -> {
            writeString(out, required(schemaVersion, "schemaVersion"));
            writeString(out, required(label, "label"));
            writeString(out, required(sourceId, "sourceId"));
            writeString(out, required(destinationId, "destinationId"));
            writeValue(out, Objects.requireNonNull(properties, "properties"));
        });
    }

    public static String tripleFact(String schemaVersion, Term subject, String predicate,
                                    Term object) {
        return key("triple", out -> {
            writeString(out, required(schemaVersion, "schemaVersion"));
            Objects.requireNonNull(subject, "subject").writeTo(out);
            writeString(out, required(predicate, "predicate"));
            Objects.requireNonNull(object, "object").writeTo(out);
        });
    }

    public static String chunk(String schemaVersion, String documentId, long start,
                               long end, String content) {
        return key("chunk", out -> {
            writeString(out, required(schemaVersion, "schemaVersion"));
            writeString(out, required(documentId, "documentId"));
            writeSpan(out, start, end);
            writeString(out, Objects.requireNonNull(content, "content"));
        });
    }

    public static String evidence(String schemaVersion, String factKey, String documentId,
                                  long start, long end) {
        return key("evidence", out -> {
            writeString(out, required(schemaVersion, "schemaVersion"));
            writeString(out, required(factKey, "factKey"));
            writeString(out, required(documentId, "documentId"));
            writeSpan(out, start, end);
        });
    }

    private static String key(String kind, Encoder encoder) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            writeString(out, FORMAT_VERSION);
            writeString(out, kind);
            encoder.write(out);
            byte[] hash = MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray());
            char[] hex = new char[hash.length * 2];
            for (int i = 0; i < hash.length; i++) {
                hex[2 * i] = HEX[(hash[i] & 0xff) >>> 4];
                hex[2 * i + 1] = HEX[hash[i] & 0x0f];
            }
            return "gf-id:" + FORMAT_VERSION + ":" + kind + ":" + new String(hex);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot encode idempotency key", e);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private static void writeSpan(DataOutputStream out, long start, long end) throws IOException {
        if (start < 0 || end <= start) {
            throw new IllegalArgumentException("source span must satisfy 0 <= start < end");
        }
        out.writeLong(start);
        out.writeLong(end);
    }

    private static String required(String value, String name) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return value;
    }

    private static void writeString(DataOutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static void writeValue(DataOutputStream out, Object value) throws IOException {
        if (value == null) {
            out.writeByte(0);
        } else if (value instanceof String) {
            out.writeByte(1);
            writeString(out, (String) value);
        } else if (value instanceof Boolean) {
            out.writeByte(2);
            out.writeBoolean((Boolean) value);
        } else if (isSupportedNumber(value)) {
            out.writeByte(3);
            writeString(out, value.getClass().getName());
            writeString(out, value instanceof BigDecimal
                ? ((BigDecimal) value).toPlainString() : value.toString());
        } else if (value instanceof Map) {
            out.writeByte(4);
            Map<String, Object> sorted = new TreeMap<>();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                if (!(entry.getKey() instanceof String)) {
                    throw new IllegalArgumentException("property map keys must be strings");
                }
                sorted.put((String) entry.getKey(), entry.getValue());
            }
            out.writeInt(sorted.size());
            for (Map.Entry<String, Object> entry : sorted.entrySet()) {
                writeString(out, entry.getKey());
                writeValue(out, entry.getValue());
            }
        } else if (value instanceof List) {
            out.writeByte(5);
            List<?> list = (List<?>) value;
            out.writeInt(list.size());
            for (Object item : list) {
                writeValue(out, item);
            }
        } else {
            throw new IllegalArgumentException("unsupported property value type: "
                + value.getClass().getName());
        }
    }

    private static boolean isSupportedNumber(Object value) {
        return value instanceof Byte || value instanceof Short || value instanceof Integer
            || value instanceof Long || value instanceof Float || value instanceof Double
            || value instanceof BigInteger || value instanceof BigDecimal;
    }

    private interface Encoder {
        void write(DataOutputStream out) throws IOException;
    }

    /** A typed subject or object in an extracted triple. */
    public static final class Term {

        private final String kind;
        private final String value;
        private final String datatype;
        private final String language;

        private Term(String kind, String value, String datatype, String language) {
            this.kind = kind;
            this.value = value;
            this.datatype = datatype;
            this.language = language;
        }

        public static Term entity(String entityId) {
            return new Term("entity", required(entityId, "entityId"), "", "");
        }

        public static Term literal(String value, String datatype, String language) {
            return new Term("literal", Objects.requireNonNull(value, "value"),
                datatype == null ? "" : datatype, language == null ? "" : language);
        }

        private void writeTo(DataOutputStream out) throws IOException {
            writeString(out, kind);
            writeString(out, value);
            writeString(out, datatype);
            writeString(out, language);
        }
    }
}

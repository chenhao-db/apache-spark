/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.variant;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonToken;

import static org.apache.spark.variant.VariantUtil.*;

public class VariantBuilder {
  /**
   * Parse a JSON string as a Variant value.
   * @throws VariantSizeLimitException if attempts to build a variant with value or metadata bigger
   * than 16MiB.
   * @throws IOException if any JSON parsing error happens.
   */
  public static Variant parseJson(String json) throws IOException {
    try (JsonParser parser = new JsonFactory().createParser(json)) {
      parser.nextToken();
      VariantBuilder builder = new VariantBuilder();
      builder.buildJson(parser);
      return builder.result();
    }
  }

  // Build the variant metadata from `dictionaryKeys` and return the variant result.
  private Variant result() {
    int numKeys = dictionaryKeys.size();
    // Use long to avoid overflow in accumulating lengths.
    long metadataSize = 1 + U24_SIZE + (numKeys + 1L) * U24_SIZE;
    for (byte[] key : dictionaryKeys) {
      metadataSize += key.length;
    }
    if (metadataSize > SIZE_LIMIT) {
      throw new VariantSizeLimitException();
    }
    byte[] metadata = new byte[(int) metadataSize];
    writeLong(metadata, 0, VERSION, 1);
    writeLong(metadata, 1, numKeys, U24_SIZE);
    int currentOffset = (numKeys + 1) * U24_SIZE;
    for (int i = 0; i < numKeys; ++i) {
      writeLong(metadata, (1 + U24_SIZE) + i * U24_SIZE, currentOffset, U24_SIZE);
      byte[] key = dictionaryKeys.get(i);
      System.arraycopy(key, 0, metadata, (1 + U24_SIZE) + currentOffset, key.length);
      currentOffset += key.length;
    }
    writeLong(metadata, (1 + U24_SIZE) + numKeys * U24_SIZE, currentOffset, U24_SIZE);
    return new Variant(Arrays.copyOfRange(value, 0, writePos), metadata);
  }

  private void checkCapacity(int additional) {
    int required = writePos + additional;
    if (required > value.length) {
      // Allocate a new buffer with a capacity of the next power of 2 of `required`.
      int newCapacity = Integer.highestOneBit(required);
      newCapacity = newCapacity < required ? newCapacity * 2 : newCapacity;
      if (newCapacity > SIZE_LIMIT) {
        throw new VariantSizeLimitException();
      }
      byte[] newValue = new byte[newCapacity];
      System.arraycopy(value, 0, newValue, 0, writePos);
      value = newValue;
    }
  }

  // Temporarily store the information of a field. We need to collect all fields in an JSON objects,
  // sort them by their keys, and build the variant object in sorted order.
  private static final class FieldEntry implements Comparable<FieldEntry> {
    final String key;
    final int id;
    final int offset;

    FieldEntry(String key, int id, int offset) {
      this.key = key;
      this.id = id;
      this.offset = offset;
    }

    @Override
    public int compareTo(FieldEntry other) {
      return key.compareTo(other.key);
    }
  }

  private void buildJson(JsonParser parser) throws IOException {
    JsonToken token = parser.currentToken();
    if (token == null) {
      throw new JsonParseException(parser, "Unexpected null token");
    }
    switch (token) {
      case START_OBJECT: {
        ArrayList<FieldEntry> fields = new ArrayList<>();
        int start = writePos;
        int maxId = 0;
        while (parser.nextToken() != JsonToken.END_OBJECT) {
          String key = parser.currentName();
          parser.nextToken();
          int id;
          if (dictionary.containsKey(key)) {
            id = dictionary.get(key);
          } else {
            id = dictionaryKeys.size();
            dictionary.put(key, id);
            dictionaryKeys.add(key.getBytes(StandardCharsets.UTF_8));
          }
          maxId = Math.max(maxId, id);
          int offset = writePos - start;
          fields.add(new FieldEntry(key, id, offset));
          buildJson(parser);
        }
        int dataSize = writePos - start;
        int size = fields.size();
        Collections.sort(fields);
        // Check for duplicate field keys. Only need to check adjacent key because they are sorted.
        for (int i = 1; i < size; ++i) {
          String key = fields.get(i - 1).key;
          if (key.equals(fields.get(i).key)) {
            throw new JsonParseException(parser, "Duplicate key: " + key);
          }
        }
        boolean largeSize = size > U8_MAX;
        int sizeBytes = largeSize ? U24_SIZE : 1;
        int idSize = getIntegerSize(maxId);
        int offsetSize = getIntegerSize(dataSize);
        // The space for header byte, object size, id list, and offset list.
        int headerSize = 1 + sizeBytes + size * idSize + (size + 1) * offsetSize;
        checkCapacity(headerSize);
        // Shift the just-written field data to make room for the object header section.
        System.arraycopy(value, start, value, start + headerSize, dataSize);
        writePos += headerSize;
        value[start] = objectHeader(largeSize, idSize, offsetSize);
        writeLong(value, start + 1, size, sizeBytes);
        int idStart = start + 1 + sizeBytes;
        int offsetStart = idStart + size * idSize;
        for (int i = 0; i < size; ++i) {
          writeLong(value, idStart + i * idSize, fields.get(i).id, idSize);
          writeLong(value, offsetStart + i * offsetSize, fields.get(i).offset, offsetSize);
        }
        writeLong(value, offsetStart + size * offsetSize, dataSize, offsetSize);
        break;
      }
      case START_ARRAY: {
        ArrayList<Integer> offsets = new ArrayList<>();
        int start = writePos;
        while (parser.nextToken() != JsonToken.END_ARRAY) {
          int offset = writePos - start;
          offsets.add(offset);
          buildJson(parser);
        }
        int dataSize = writePos - start;
        int size = offsets.size();
        boolean largeSize = size > U8_MAX;
        int sizeBytes = largeSize ? U24_SIZE : 1;
        int offsetSize = getIntegerSize(dataSize);
        // The space for header byte, object size, and offset list.
        int headerSize = 1 + sizeBytes + (size + 1) * offsetSize;
        checkCapacity(headerSize);
        // Shift the just-written field data to make room for the header section.
        System.arraycopy(value, start, value, start + headerSize, dataSize);
        writePos += headerSize;
        value[start] = arrayHeader(largeSize, offsetSize);
        writeLong(value, start + 1, size, sizeBytes);
        int offsetStart = start + 1 + sizeBytes;
        for (int i = 0; i < size; ++i) {
          writeLong(value, offsetStart + i * offsetSize, offsets.get(i), offsetSize);
        }
        writeLong(value, offsetStart + size * offsetSize, dataSize, offsetSize);
        break;
      }
      case VALUE_STRING:
        byte[] text = parser.getText().getBytes(StandardCharsets.UTF_8);
        boolean longStr = text.length > MAX_SHORT_STR_SIZE;
        int strSize = (longStr ? 1 + U24_SIZE : 1) + text.length;
        checkCapacity(strSize);
        if (longStr) {
          value[writePos] = atomicHeader(LONG_STR);
          writeLong(value, writePos + 1, text.length, U24_SIZE);
          System.arraycopy(text, 0, value, writePos + 1 + U24_SIZE, text.length);
        } else {
          value[writePos] = shortStrHeader(text.length);
          System.arraycopy(text, 0, value, writePos + 1, text.length);
        }
        writePos += strSize;
        break;
      case VALUE_NUMBER_INT:
        try {
          long l = parser.getLongValue();
          checkCapacity(1 + 8);
          if (l == (byte) l) {
            value[writePos] = atomicHeader(INT1);
            writeLong(value, writePos + 1, l, 1);
            writePos += 1 + 1;
          } else if (l == (short) l) {
            value[writePos] = atomicHeader(INT2);
            writeLong(value, writePos + 1, l, 2);
            writePos += 1 + 2;
          } else if (l == (int) l) {
            value[writePos] = atomicHeader(INT4);
            writeLong(value, writePos + 1, l, 4);
            writePos += 1 + 4;
          } else {
            value[writePos] = atomicHeader(INT8);
            writeLong(value, writePos + 1, l, 8);
            writePos += 1 + 8;
          }
        } catch (Exception ignored) {
          // If the value doesn't fit any integer type, parse it as decimal or floating instead.
          parseFloatingPoint(parser);
        }
        break;
      case VALUE_NUMBER_FLOAT:
        parseFloatingPoint(parser);
        break;
      case VALUE_TRUE:
        checkCapacity(1);
        value[writePos++] = atomicHeader(TRUE);
        break;
      case VALUE_FALSE:
        checkCapacity(1);
        value[writePos++] = atomicHeader(FALSE);
        break;
      case VALUE_NULL:
        checkCapacity(1);
        value[writePos++] = atomicHeader(NULL);
        break;
      default:
        throw new JsonParseException(parser, "Unexpected token " + token);
    }
  }

  // Choose the smallest unsigned integer type that can store `value`. It must be within
  // `[0, U24_MAX]`.
  private int getIntegerSize(int value) {
    assert value >= 0 && value <= U24_MAX;
    if (value <= U8_MAX) {
      return 1;
    } else if (value <= U16_MAX) {
      return 2;
    } else {
      return U24_SIZE;
    }
  }

  private void parseFloatingPoint(JsonParser parser) throws IOException {
    if (!tryParseDecimal(parser.getText())) {
      checkCapacity(1 + 8);
      value[writePos] = atomicHeader(DOUBLE);
      writeLong(value, writePos + 1, Double.doubleToLongBits(parser.getDoubleValue()), 8);
      writePos += 1 + 8;
    }
  }

  // Try to parse a JSON number as a decimal. Return whether the parsing succeeds. The input must
  // only use the decimal format (an integer value with an optional '.' in it) and must not use
  // scientific notation. It also must fit into the precision limitation of decimal types.
  private boolean tryParseDecimal(String input) {
    for (int i = 0; i < input.length(); ++i) {
      char ch = input.charAt(i);
      if (ch != '-' && ch != '.' && !(ch >= '0' && ch <= '9')) {
        return false;
      }
    }
    BigDecimal d = new BigDecimal(input);
    checkCapacity(2 + 16);
    BigInteger unscaled = d.unscaledValue();
    if (d.scale() <= MAX_DECIMAL4_PRECISION && d.precision() <= MAX_DECIMAL4_PRECISION) {
      value[writePos] = atomicHeader(DECIMAL4);
      value[writePos + 1] = (byte)d.scale();
      writeLong(value, writePos + 2, unscaled.intValueExact(), 4);
      writePos += 2 + 4;
    } else if (d.scale() <= MAX_DECIMAL8_PRECISION && d.precision() <= MAX_DECIMAL8_PRECISION) {
      value[writePos] = atomicHeader(DECIMAL8);
      value[writePos + 1] = (byte)d.scale();
      writeLong(value, writePos + 2, unscaled.longValueExact(), 8);
      writePos += 2 + 8;
    } else if (d.scale() <= MAX_DECIMAL16_PRECISION && d.precision() <= MAX_DECIMAL16_PRECISION) {
      value[writePos] = atomicHeader(DECIMAL16);
      value[writePos + 1] = (byte)d.scale();
      // `toByteArray` returns a big-endian representation. We need to copy it reversely and sign
      // extend it to 16 bytes.
      byte[] bytes = unscaled.toByteArray();
      for (int i = 0; i < bytes.length; ++i) {
        value[writePos + 2 + i] = bytes[bytes.length - 1 - i];
      }
      byte sign = (byte) (bytes[0] < 0 ? -1 : 0);
      for (int i = bytes.length; i < 16; ++i) {
        value[writePos + 2 + i] = sign;
      }
      writePos += 2 + 16;
    } else {
      return false;
    }
    return true;
  }

  // The write buffer in building the variant value. Its first `writePos` bytes has been written.
  private byte[] value = new byte[128];
  private int writePos = 0;
  // Map keys to a monotonically increasing id.
  private final HashMap<String, Integer> dictionary = new HashMap<>();
  // Store all keys in `dictionary` in the order of id.
  private final ArrayList<byte[]> dictionaryKeys = new ArrayList<>();
}

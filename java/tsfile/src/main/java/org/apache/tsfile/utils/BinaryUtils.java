/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tsfile.utils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class BinaryUtils {
  public static String parseBinaryToString(Binary input) {
    return BytesUtils.parseBlobByteArrayToString(input.values, 0, input.getLength());
  }

  public static void serializeBytes(OutputStream stream, Binary binary) throws IOException {
    stream.write(binary.values, 0, binary.getLength());
  }

  public static void serializeBytes(ByteBuffer buffer, Binary binary) {
    buffer.put(binary.values, 0, binary.getLength());
  }

  public static ByteBuffer wrapToByteBuffer(Binary binary) {
    return ByteBuffer.wrap(binary.values, 0, binary.getLength());
  }

  public static InputStream wrapToByteStream(Binary binary) {
    return new ByteArrayInputStream(binary.values, 0, binary.getLength());
  }

  public static boolean binaryToBool(Binary binary) {
    if (binary.getLength() < 1) {
      throw new IllegalArgumentException("Invalid input: binary.getLength() < 1");
    }
    return BytesUtils.bytesToBool(binary.values);
  }

  public static boolean binaryToBool(Binary binary, int offset) {
    if (binary.getLength() - offset < 1) {
      throw new IllegalArgumentException("Invalid input: binary.getLength() - offset < 1");
    }
    return BytesUtils.bytesToBool(binary.values, offset);
  }

  public static int binaryToInt(Binary binary) {
    if (binary.getLength() < 4) {
      throw new IllegalArgumentException("Invalid input: binary.getLength() < 4");
    }
    return BytesUtils.bytesToInt(binary.values);
  }

  public static int binaryToInt(Binary binary, int offset) {
    if (binary.getLength() - offset < 4) {
      throw new IllegalArgumentException("Invalid input: binary.getLength() - offset < 4");
    }
    return BytesUtils.bytesToInt(binary.values, offset);
  }

  public static long binaryToLong(Binary binary, int numBytes) {
    if (binary.getLength() < numBytes) {
      throw new IllegalArgumentException("Invalid input: binary.getLength() < nnumBytes");
    }
    return BytesUtils.bytesToLong(binary.values, numBytes);
  }

  public static long binaryToLongFromOffset(Binary binary, int numBytes, int offset) {
    if (binary.getLength() - offset < numBytes) {
      throw new IllegalArgumentException("Invalid input: binary.getLength() - offset < numBytes");
    }
    return BytesUtils.bytesToLong(binary.values, numBytes, offset);
  }

  public static float binaryToFloat(Binary binary) {
    if (binary.getLength() < 4) {
      throw new IllegalArgumentException("Invalid input: binary.getLength() < 4");
    }
    return BytesUtils.bytesToFloat(binary.values);
  }

  public static float binaryToFloat(Binary binary, int offset) {
    if (binary.getLength() - offset < 4) {
      throw new IllegalArgumentException("Invalid input: binary.getLength() - offset < 4");
    }
    return BytesUtils.bytesToFloat(binary.values, offset);
  }

  public static double binaryToDouble(Binary binary) {
    if (binary.getLength() < 8) {
      throw new IllegalArgumentException("Invalid input: binary.getLength() < 8");
    }
    return BytesUtils.bytesToDouble(binary.values);
  }

  public static double binaryToDouble(Binary binary, int offset) {
    if (binary.getLength() - offset < 8) {
      throw new IllegalArgumentException("Invalid input: binary.getLength() - offset < 8");
    }
    return BytesUtils.bytesToDouble(binary.values, offset);
  }

  public static Binary subBinary(Binary binary, int start, int length) {
    if ((start + length) > binary.getLength()) {
      return Binary.EMPTY_VALUE;
    }
    if (length <= 0) {
      return Binary.EMPTY_VALUE;
    }

    byte[] subBytes = new byte[length];
    System.arraycopy(binary.values, start, subBytes, 0, length);
    return new Binary(subBytes);
  }
}

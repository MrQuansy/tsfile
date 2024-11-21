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

import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static org.apache.tsfile.utils.RamUsageEstimator.shallowSizeOfInstance;

/**
 * This class represents a pooled binary object for application layer. It is designed to improve
 * allocation performance and reduce GC overhead by reusing binary objects from a pool instead of
 * creating new instances each time. WARNING: The actual length of the binary may not equal to the
 * length of the underlying byte array. Always use getLength() instead of getValue().length to get
 * the correct length.
 */
public class PooledBinary implements Comparable<PooledBinary>, Serializable, Accountable {

  private static final long INSTANCE_SIZE = shallowSizeOfInstance(PooledBinary.class);
  private static final long serialVersionUID = 6394197743397020735L;
  public static final PooledBinary EMPTY_VALUE = new PooledBinary(new byte[0]);

  private Binary values;

  private int length;

  private int arenaIndex = -1;

  /** if the bytes v is modified, the modification is visible to this binary. */
  public PooledBinary(byte[] v) {
    this.values = new Binary(v);
    this.length = values.getLength();
  }

  public PooledBinary(String s, Charset charset) {
    this.values = new Binary(s, charset);
    this.length = values.getLength();
  }

  public PooledBinary(byte[] v, int length, int arenaIndex) {
    this.values = new Binary(v);
    this.length = length;
    this.arenaIndex = arenaIndex;
  }

  @Override
  public int compareTo(PooledBinary other) {
    if (other == null) {
      if (this.values == null) {
        return 0;
      } else {
        return 1;
      }
    }

    // copied from StringLatin1.compareT0
    int len1 = getLength();
    int len2 = other.getLength();
    int lim = Math.min(len1, len2);
    byte[] v0 = this.values.getValues();
    byte[] v1 = other.values.getValues();
    for (int k = 0; k < lim; k++) {
      if (v0[k] != v1[k]) {
        return getChar(v0, k) - getChar(v1, k);
      }
    }
    return len1 - len2;
  }

  // avoid overflow
  private char getChar(byte[] val, int index) {
    return (char) (val[index] & 0xff);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PooledBinary binary = (PooledBinary) o;

    if (length != binary.getLength()) {
      return false;
    }
    for (int i = 0; i < length; i++) {
      if (this.values.getValues()[i] != binary.values.getValues()[i]) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    if (values.getValues() == null) {
      return 0;
    } else {
      int result = 1;
      byte[] var2 = values.getValues();
      int var3 = length;

      for (int var4 = 0; var4 < var3; ++var4) {
        byte element = var2[var4];
        result = 31 * result + element;
      }

      return result;
    }
  }

  /**
   * get length.
   *
   * @return length
   */
  public int getLength() {
    return this.length;
  }

  public String getStringValue(Charset charset) {
    return new String(this.values.getValues(), 0, length, charset);
  }

  @Override
  public String toString() {
    // use UTF_8 by default since toString do not provide parameter
    return getStringValue(StandardCharsets.UTF_8);
  }

  public byte[] getValues() {
    return values.getValues();
  }

  public void setValues(byte[] values) {
    this.values.setValues(values);
    this.length = this.values.getLength();
  }

  public void setValues(byte[] values, int length) {
    this.values.setValues(values);
    this.length = length;
  }

  public int getArenaIndex() {
    return this.arenaIndex;
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE + values.ramBytesUsed();
  }

  public Binary toBinary() {
    return values;
  }
}

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
package org.apache.tsfile.write.record.datapoint;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * a subclass for Double data type extends DataPoint.
 *
 * @see DataPoint DataPoint
 */
public class DoubleDataPoint extends DataPoint {

  private static final Logger LOG = LoggerFactory.getLogger(DoubleDataPoint.class);

  /** actual value. */
  private double value;

  /** constructor of DoubleDataPoint, the value type will be set automatically. */
  public DoubleDataPoint(String measurementId, double v) {
    super(TSDataType.DOUBLE, measurementId);
    this.value = v;
  }

  @Override
  public void writeTo(long time, ChunkWriterImpl writer) {
    if (writer == null) {
      LOG.warn("given IChunkWriter is null, do nothing and return");
      return;
    }
    writer.write(time, value);
  }

  @Override
  public Object getValue() {
    return value;
  }

  @Override
  public void setDouble(double value) {
    this.value = value;
  }
}

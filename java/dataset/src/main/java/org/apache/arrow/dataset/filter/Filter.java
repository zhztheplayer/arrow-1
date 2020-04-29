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

package org.apache.arrow.dataset.filter;

import org.apache.arrow.dataset.DatasetTypes;

/**
 * Datasets filter.
 *
 * EXPERIMENTAL: Datasets filter definition should ideally keep up with C++ implementation. In next several release
 * versions The API may vary a lot without respect to backward compatibility.
 */
public class Filter {
  public static final Filter EMPTY = new Filter(DatasetTypes.Condition.newBuilder()
      .setRoot(
          DatasetTypes.TreeNode.newBuilder()
              .setBooleanNode(
                  DatasetTypes.BooleanNode.newBuilder()
                      .setValue(true)
                      .build())
              .build())
      .build());

  private final DatasetTypes.Condition condition;

  public Filter(DatasetTypes.Condition condition) {
    this.condition = condition;
  }

  public byte[] toByteArray() {
    return condition.toByteArray();
  }
}

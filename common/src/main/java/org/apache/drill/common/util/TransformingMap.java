/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.common.util;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

public class TransformingMap<K, V> extends AbstractMap<K, V> {

  private final Map<K, V> delegate;
  private final Function<K, K> transformation;

  public final static Function<String, String> STRING_LOWERCASE = new Function<String, String>() {
    @Override
    public String apply(String input) {
      return Preconditions.checkNotNull(input).toLowerCase();
    }
  };

  public final static Function<String, String> STRING_UPPERCASE = new Function<String, String>() {
    @Override
    public String apply(String input) {
      return Preconditions.checkNotNull(input).toUpperCase();
    }
  };

  public TransformingMap(Map<K, V> delegate, Function<K, K> transformation) {
    this.delegate = Preconditions.checkNotNull(delegate);
    this.transformation = Preconditions.checkNotNull(transformation);
  }

  @Override
  public V get(Object key) {
    final K transformedKey = transformation.apply((K)key);
    return super.get(transformedKey);
  }

  @Override
  public V put(K key, V value) {
    final K transformedKey = transformation.apply(key);
    return delegate.put(transformedKey, value);
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    return delegate.entrySet();
  }
}

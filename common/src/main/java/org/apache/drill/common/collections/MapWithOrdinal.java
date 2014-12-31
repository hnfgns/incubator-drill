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
package org.apache.drill.common.collections;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapWithOrdinal<K, V> implements Map<K, V> {
  private final static Logger logger = LoggerFactory.getLogger(MapWithOrdinal.class);

  private final Map<K, Entry<Integer, V>> primary = Maps.newLinkedHashMap();
  private final IntObjectHashMap<V> secondary = new IntObjectHashMap<>();

  private final Map<K, V> delegate = new Map<K, V>() {
    @Override
    public boolean isEmpty() {
      return size() == 0;
    }

    @Override
    public int size() {
      return primary.size();
    }

    @Override
    public boolean containsKey(Object key) {
      return primary.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
      return primary.containsValue(value);
    }

    @Override
    public V get(Object key) {
      Entry<Integer, V> pair = primary.get(key);
      if (pair != null) {
        return pair.getValue();
      }
      return null;
    }

    @Override
    public V put(K key, V value) {
      final Entry<Integer, V> oldPair = primary.get(key);
      // if key exists try replacing otherwise, assign a new ordinal identifier
      final int ordinal = oldPair == null ? primary.size():oldPair.getKey();
      primary.put(key, new AbstractMap.SimpleImmutableEntry<>(ordinal, value));
      secondary.put(ordinal, value);
      return oldPair==null ? null:oldPair.getValue();
    }

    @Override
    public V remove(Object key) {
      final Entry<Integer, V> oldPair = primary.remove(key);
      if (oldPair!=null) {
        final int lastOrdinal = secondary.size();
        final V last = secondary.get(lastOrdinal);
        // normalize mappings so that all numbers until primary.size() is assigned
        // swap the last element with the deleted one
        secondary.put(oldPair.getKey(), last);
        primary.put((K) key, new AbstractMap.SimpleImmutableEntry<>(oldPair.getKey(), last));
      }
      return oldPair==null ? null:oldPair.getValue();
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
      primary.clear();
      secondary.clear();
    }

    @Override
    public Set<K> keySet() {
      return primary.keySet();
    }

    @Override
    public Collection<V> values() {
      return Lists.newArrayList(Iterables.transform(secondary.entries(), new Function<IntObjectMap.Entry<V>, V>() {
        @Override
        public V apply(IntObjectMap.Entry<V> entry) {
          return Preconditions.checkNotNull(entry).value();
        }
      }));
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
      final Collection<Entry<K, Entry<Integer, V>>> entries = Ordering.natural()
          .onResultOf(new Function<Entry<K,Entry<Integer, V>>, Comparable>() {
            @Override
            public Comparable apply(Entry<K, Entry<Integer, V>> entry) {
              return Preconditions.checkNotNull(entry).getValue().getKey();
            }
          })
          .immutableSortedCopy(primary.entrySet());

      return Sets.newLinkedHashSet(Iterables.transform(primary.entrySet(), new Function<Entry<K, Entry<Integer, V>>, Entry<K, V>>() {
        @Override
        public Entry<K, V> apply(Entry<K, Entry<Integer, V>> entry) {
          return new AbstractMap.SimpleImmutableEntry<>(entry.getKey(), entry.getValue().getValue());
        }
      }));
    }
  };

  public V getByOrdinal(int id) {
    return secondary.get(id);
  }

  public int getOrdinal(K key) {
    Entry<Integer, V> pair = primary.get(key);
    if (pair != null) {
      return pair.getKey();
    }
    return -1;
  }

  @Override
  public int size() {
    return delegate.size();
  }

  @Override
  public boolean isEmpty() {
    return delegate.isEmpty();
  }

  @Override
  public V get(Object key) {
    return delegate.get(key);
  }

  @Override
  public V put(K key, V value) {
    return delegate.put(key, value);
  }

  @Override
  public Collection<V> values() {
    return delegate.values();
  }

  @Override
  public boolean containsKey(Object key) {
    return delegate.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return delegate.containsValue(value);
  }

  @Override
  public V remove(Object key) {
    return delegate.remove(key);
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    delegate.putAll(m);
  }

  @Override
  public void clear() {
    delegate.clear();
  }

  @Override
  public Set<K> keySet() {
    return delegate.keySet();
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    return delegate.entrySet();
  }
}

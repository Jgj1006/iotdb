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

package org.apache.iotdb.commons.pipe.datastructure;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ConcurrentIterableLinkedQueue<E> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConcurrentIterableLinkedQueue.class);

  private static class LinkedListNode<E> {

    private E data;
    private LinkedListNode<E> next;

    public LinkedListNode(E data) {
      this.data = data;
      this.next = null;
    }
  }

  private final LinkedListNode<E> dummyNode = new LinkedListNode<>(null);
  private LinkedListNode<E> firstNode;
  private LinkedListNode<E> lastNode;

  // The indexes of elements are (firstIndex, firstIndex + 1, ...., lastIndex - 1)
  private volatile long firstIndex = 0;
  private volatile long lastIndex = 0;

  private final ReentrantLock lock = new ReentrantLock();
  private final Condition hasNextCondition = lock.newCondition();

  public ConcurrentIterableLinkedQueue() {
    firstNode = dummyNode;
    lastNode = dummyNode;
  }

  public boolean isEmpty() {
    lock.lock();
    try {
      return firstNode == dummyNode;
    } finally {
      lock.unlock();
    }
  }

  public void add(E e) {
    if (e == null) {
      throw new IllegalArgumentException("The element to be added cannot be null");
    }

    final LinkedListNode<E> newNode = new LinkedListNode<>(e);

    lock.lock();
    try {
      if (firstNode == dummyNode) {
        firstNode = newNode;
      }

      lastNode.next = newNode;
      lastNode = newNode;

      ++lastIndex;

      hasNextCondition.signalAll();
    } finally {
      lock.unlock();
    }
  }

  public void removeBefore(long newFirstIndex) {
    lock.lock();
    try {
      newFirstIndex = Math.min(newFirstIndex, lastIndex);
      if (newFirstIndex <= firstIndex) {
        return;
      }
      // assert firstIndex < newFirstIndex

      LinkedListNode<E> currentNode = firstNode;

      for (long i = firstIndex; i < newFirstIndex; ++i) {
        final LinkedListNode<E> nextNode = currentNode.next;
        currentNode.data = null;
        currentNode.next = null;
        currentNode = nextNode;
      }

      firstNode = currentNode;
      firstIndex = newFirstIndex;

      if (firstIndex == lastIndex) {
        firstNode = dummyNode;
        lastNode = dummyNode;
      }

      hasNextCondition.signalAll();
    } finally {
      lock.unlock();
    }
  }

  public void clear() {
    removeBefore(lastIndex);
  }

  public long getFirstIndex() {
    lock.lock();
    try {
      return firstIndex;
    } finally {
      lock.unlock();
    }
  }

  public long getLastIndex() {
    lock.lock();
    try {
      return lastIndex;
    } finally {
      lock.unlock();
    }
  }

  public Iterator iterateFromEarliest() {
    lock.lock();
    try {
      return iterateFrom(firstIndex);
    } finally {
      lock.unlock();
    }
  }

  public Iterator iterateFromLatest() {
    lock.lock();
    try {
      return iterateFrom(lastIndex);
    } finally {
      lock.unlock();
    }
  }

  public Iterator iterateFrom(long index) {
    lock.lock();
    try {
      return new Iterator(index);
    } finally {
      lock.unlock();
    }
  }

  /** NOTE: not thread-safe. */
  public class Iterator implements java.util.Iterator<E> {

    private LinkedListNode<E> currentNode;
    private long currentIndex;

    private Iterator(long index) {
      seek(index);
    }

    /**
     * Seek the {@link Iterator#currentIndex} to the closest position allowed to the given index.
     * Note that one can seek to {@link ConcurrentIterableLinkedQueue#lastIndex} to subscribe the
     * next incoming element.
     *
     * @param index the attempt index
     * @return the actual new index
     */
    public long seek(long index) {
      lock.lock();
      try {
        currentNode = firstNode;
        currentIndex = firstIndex;

        final long targetIndex = Math.max(firstIndex, Math.min(index, lastIndex));
        for (long i = 0; i < targetIndex - firstIndex; ++i) {
          moveToNext();
        }

        return currentIndex;
      } finally {
        lock.unlock();
      }
    }

    private void moveToNext() {
      if (currentNode.next != null) {
        currentNode = currentNode.next;
        ++currentIndex;
      }
    }

    @Override
    public boolean hasNext() {
      lock.lock();
      try {
        return currentNode.next != null;
      } finally {
        lock.unlock();
      }
    }

    @Override
    public E next() {
      return next(Long.MAX_VALUE);
    }

    public E next(long waitTimeMillis) {
      lock.lock();
      try {
        while (!hasNext()) {
          if (!hasNextCondition.await(waitTimeMillis, TimeUnit.MILLISECONDS)) {
            return null;
          }
        }

        moveToNext();

        return currentNode.data;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return null;
      } finally {
        lock.unlock();
      }
    }

    public long getCurrentIndex() {
      lock.lock();
      try {
        return currentIndex;
      } finally {
        lock.unlock();
      }
    }
  }
}

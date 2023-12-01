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

    private LinkedListNode(E data) {
      this.data = data;
      this.next = null;
    }
  }

  private final LinkedListNode<E> dummyNode = new LinkedListNode<>(null);
  private volatile LinkedListNode<E> firstNode;
  private volatile LinkedListNode<E> lastNode;
  private volatile long firstIndex = -1;
  private volatile long lastIndex = -1;

  private final ReentrantLock lock = new ReentrantLock();
  private final Condition hasNextCondition = lock.newCondition();

  public ConcurrentIterableLinkedQueue() {
    firstNode = dummyNode;
    lastNode = dummyNode;
  }

  public boolean isEmpty() {
    lock.lock();
    try {
      return firstIndex == lastIndex;
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
      ++lastIndex;

      if (firstNode == dummyNode) {
        firstIndex = lastIndex;
        firstNode = newNode;
      }

      lastNode.next = newNode;
      lastNode = newNode;

      hasNextCondition.signalAll();
    } finally {
      lock.unlock();
    }
  }

  public void removeBefore(long newFirstIndex) {
    if (newFirstIndex <= firstIndex) {
      throw new IllegalArgumentException("New first index must be greater than the current first index.");
    }

    lock.lock();
    try {
      newFirstIndex = Math.min(newFirstIndex, lastIndex);
      if (newFirstIndex <= firstIndex) {
        return;
      }

      LinkedListNode<E> currentNode = firstNode;
      for (long i = firstIndex; i < newFirstIndex; ++i) {
        LinkedListNode<E> nextNode = currentNode.next;
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
    return new Iterator();
  }

  public Iterator iterateFromLatest() {
    return iterateFrom(lastIndex);
  }

  public Iterator iterateFrom(long index) {
    return new Iterator(index);
  }

  public class Iterator implements java.util.Iterator<E> {

    private LinkedListNode<E> currentNode;
    private long currentIndex;

    private Iterator() {
      lock.lock();
      try {
        currentNode = dummyNode;
        currentIndex = firstIndex - 1;
      } finally {
        lock.unlock();
      }
    }

    private Iterator(long index) {
      lock.lock();
      try {
        seek(index);
      } finally {
        lock.unlock();
      }
    }

    public long seek(long index) {
      lock.lock();
      try {
        currentNode = firstNode;
        currentIndex = firstIndex;

        index = Math.max(firstIndex, Math.min(index, lastIndex));
        while (currentIndex < index && currentNode.next != null) {
          currentNode = currentNode.next;
          currentIndex++;
        }

        return currentIndex;
      } finally {
        lock.unlock();
      }
    }

    @Override
    public boolean hasNext() {
      lock.lock();
      try {
        return currentNode != null && currentNode.next != null;
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
        while (currentNode.next == null) {
          if (!hasNextCondition.await(waitTimeMillis, TimeUnit.MILLISECONDS)) {
            return null;
          }
        }

        currentNode = currentNode.next;
        currentIndex++;

        return currentNode.data;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return null;
      } finally {
        lock.unlock();
      }
    }

    public E getCurrent() {
      lock.lock();
      try {
        return currentNode.data;
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

// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_WORKQUEUE_H
#define CEPH_WORKQUEUE_H

#include "Mutex.h"
#include "Cond.h"
#include "Thread.h"
#include "common/config_obs.h"
#include "common/HeartbeatMap.h"

class CephContext;

/// Pool of threads that share work submitted to multiple work queues.
class ThreadPool : public md_config_obs_t {
  CephContext *cct;
  string name;
  string lockname;
  Mutex _lock;
  Cond _cond;
  bool _stop;
  int _pause;
  int _draining;
  Cond _wait_cond;
  int ioprio_class, ioprio_priority;


private:

  /// Basic interface to a work queue used by the worker threads.
  struct WorkQueue_ {
    string name;
    time_t timeout_interval, suicide_interval;
    WorkQueue_(string n, time_t ti, time_t sti)
      : name(n), timeout_interval(ti), suicide_interval(sti)
    { }
    virtual ~WorkQueue_() {}
    /// Remove all work items from the queue.
    virtual void _clear() = 0;
    /// Check whether there is anything to do.
    virtual bool _empty() = 0;
    /// Get the next work item to process.
    virtual void *_void_dequeue() = 0;
    /** @brief Process the work item.
     * This function will be called several times in parallel
     * and must therefore be thread-safe. */
    virtual void _void_process(void *item, HBHandle &handle) = 0;
    /** @brief Synchronously finish processing a work item.
     * This function is called after _void_process with the global thread pool lock held,
     * so at most one copy will execute simultaneously for a given thread pool.
     * It can be used for non-thread-safe finalization. */
    virtual void _void_process_finish(void *) = 0;
  };

  // track thread pool size changes
  unsigned _num_threads;
  string _thread_num_option;
  const char **_conf_keys;

  const char **get_tracked_conf_keys() const {
    return _conf_keys;
  }
  void handle_conf_change(const struct md_config_t *conf,
			  const std::set <std::string> &changed);

public:
  /** @brief Work queue that processes several submitted items at once.
   * The queue will automatically add itself to the thread pool on construction
   * and remove itself on destruction. */
  template<class T>
  class BatchWorkQueue : public WorkQueue_ {
    ThreadPool *pool;

    virtual bool _enqueue(T *) = 0;
    virtual void _dequeue(T *) = 0;
    virtual void _dequeue(list<T*> *) = 0;
    virtual void _process_finish(const list<T*> &) {}

    // virtual methods from WorkQueue_ below
    void *_void_dequeue() {
      list<T*> *out(new list<T*>);
      _dequeue(out);
      if (!out->empty()) {
	return (void *)out;
      } else {
	delete out;
	return 0;
      }
    }
    void _void_process(void *p, HBHandle &handle) {
      _process(*((list<T*>*)p), handle);
    }
    void _void_process_finish(void *p) {
      _process_finish(*(list<T*>*)p);
      delete (list<T*> *)p;
    }

  protected:
    virtual void _process(const list<T*> &) { assert(0); }
    virtual void _process(const list<T*> &items, HBHandle &handle) {
      _process(items);
    }

  public:
    BatchWorkQueue(string n, time_t ti, time_t sti, ThreadPool* p)
      : WorkQueue_(n, ti, sti), pool(p) {
      pool->add_work_queue(this);
    }
    ~BatchWorkQueue() {
      pool->remove_work_queue(this);
    }

    bool queue(T *item) {
      pool->_lock.Lock();
      bool r = _enqueue(item);
      pool->_cond.SignalOne();
      pool->_lock.Unlock();
      return r;
    }
    void dequeue(T *item) {
      pool->_lock.Lock();
      _dequeue(item);
      pool->_lock.Unlock();
    }
    void clear() {
      pool->_lock.Lock();
      _clear();
      pool->_lock.Unlock();
    }

    void lock() {
      pool->lock();
    }
    void unlock() {
      pool->unlock();
    }
    void wake() {
      pool->wake();
    }
    void _wake() {
      pool->_wake();
    }
    void drain() {
      pool->drain(this);
    }

  };

  /** @brief Templated by-value work queue.
   * Skeleton implementation of a queue that processes items submitted by value.
   * This is useful if the items are single primitive values or very small objects
   * (a few bytes). The queue will automatically add itself to the thread pool on
   * construction and remove itself on destruction. */
  template<typename T, typename U = T>
  class WorkQueueVal : public WorkQueue_ {
    Mutex _lock;
    ThreadPool *pool;
    list<U> to_process;
    list<U> to_finish;
    virtual void _enqueue(T) = 0;
    virtual void _enqueue_front(T) = 0;
    virtual bool _empty() = 0;
    virtual U _dequeue() = 0;
    virtual void _process_finish(U) {}

    void *_void_dequeue() {
      {
	Mutex::Locker l(_lock);
	if (_empty())
	  return 0;
	U u = _dequeue();
	to_process.push_back(u);
      }
      return ((void*)1); // Not used
    }
    void _void_process(void *, HBHandle &handle) {
      _lock.Lock();
      assert(!to_process.empty());
      U u = to_process.front();
      to_process.pop_front();
      _lock.Unlock();

      _process(u, handle);

      _lock.Lock();
      to_finish.push_back(u);
      _lock.Unlock();
    }

    void _void_process_finish(void *) {
      _lock.Lock();
      assert(!to_finish.empty());
      U u = to_finish.front();
      to_finish.pop_front();
      _lock.Unlock();

      _process_finish(u);
    }

    void _clear() {}

  public:
    WorkQueueVal(string n, time_t ti, time_t sti, ThreadPool *p)
      : WorkQueue_(n, ti, sti), _lock("WorkQueueVal::lock"), pool(p) {
      pool->add_work_queue(this);
    }
    ~WorkQueueVal() {
      pool->remove_work_queue(this);
    }
    void queue(T item) {
      Mutex::Locker l(pool->_lock);
      _enqueue(item);
      pool->_cond.SignalOne();
    }
    void queue_front(T item) {
      Mutex::Locker l(pool->_lock);
      _enqueue_front(item);
      pool->_cond.SignalOne();
    }
    void drain() {
      pool->drain(this);
    }
  protected:
    void lock() {
      pool->lock();
    }
    void unlock() {
      pool->unlock();
    }
    virtual void _process(U) { assert(0); }
    virtual void _process(U u, HBHandle &) {
      _process(u);
    }
  };

  /** @brief Template by-pointer work queue.
   * Skeleton implementation of a queue that processes items of a given type submitted as pointers.
   * This is useful when the work item are large or include dynamically allocated memory. The queue
   * will automatically add itself to the thread pool on construction and remove itself on
   * destruction. */
  template<class T>
  class WorkQueue : public WorkQueue_ {
    ThreadPool *pool;
    
    /// Add a work item to the queue.
    virtual bool _enqueue(T *) = 0;
    /// Dequeue a previously submitted work item.
    virtual void _dequeue(T *) = 0;
    /// Dequeue a work item and return the original submitted pointer.
    virtual T *_dequeue() = 0;
    virtual void _process_finish(T *) {}

    // implementation of virtual methods from WorkQueue_
    void *_void_dequeue() {
      return (void *)_dequeue();
    }
    void _void_process(void *p, HBHandle &handle) {
      _process(static_cast<T *>(p), handle);
    }
    void _void_process_finish(void *p) {
      _process_finish(static_cast<T *>(p));
    }

  protected:
    /// Process a work item. Called from the worker threads.
    virtual void _process(T *t) { assert(0); }
    virtual void _process(T *t, HBHandle &) {
      _process(t);
    }

  public:
    WorkQueue(string n, time_t ti, time_t sti, ThreadPool* p) : WorkQueue_(n, ti, sti), pool(p) {
      pool->add_work_queue(this);
    }
    ~WorkQueue() {
      pool->remove_work_queue(this);
    }
    
    bool queue(T *item) {
      pool->_lock.Lock();
      bool r = _enqueue(item);
      pool->_cond.SignalOne();
      pool->_lock.Unlock();
      return r;
    }
    void dequeue(T *item) {
      pool->_lock.Lock();
      _dequeue(item);
      pool->_lock.Unlock();
    }
    void clear() {
      pool->_lock.Lock();
      _clear();
      pool->_lock.Unlock();
    }

    Mutex &get_lock() {
      return pool->_lock;
    }

    void lock() {
      pool->lock();
    }
    void unlock() {
      pool->unlock();
    }
    /// wake up the thread pool (without lock held)
    void wake() {
      pool->wake();
    }
    /// wake up the thread pool (with lock already held)
    void _wake() {
      pool->_wake();
    }
    void _wait() {
      pool->_wait();
    }
    void drain() {
      pool->drain(this);
    }

  };

private:
  vector<WorkQueue_*> work_queues;
  int last_work_queue;
 

  // threads
  struct WorkThread : public Thread {
    ThreadPool *pool;
    WorkThread(ThreadPool *p) : pool(p) {}
    void *entry() {
      pool->worker(this);
      return 0;
    }
  };
  
  set<WorkThread*> _threads;
  list<WorkThread*> _old_threads;  ///< need to be joined
  int processing;

  void start_threads();
  void join_old_threads();
  void worker(WorkThread *wt);

public:
  ThreadPool(CephContext *cct_, string nm, int n, const char *option = NULL);
  virtual ~ThreadPool();

  /// return number of threads currently running
  int get_num_threads() {
    Mutex::Locker l(_lock);
    return _num_threads;
  }
  
  /// assign a work queue to this thread pool
  void add_work_queue(WorkQueue_* wq) {
    Mutex::Locker l(_lock);
    work_queues.push_back(wq);
  }
  /// remove a work queue from this thread pool
  void remove_work_queue(WorkQueue_* wq) {
    Mutex::Locker l(_lock);
    unsigned i = 0;
    while (work_queues[i] != wq)
      i++;
    for (i++; i < work_queues.size(); i++) 
      work_queues[i-1] = work_queues[i];
    assert(i == work_queues.size());
    work_queues.resize(i-1);
  }

  /// take thread pool lock
  void lock() {
    _lock.Lock();
  }
  /// release thread pool lock
  void unlock() {
    _lock.Unlock();
  }

  /// wait for a kick on this thread pool
  void wait(Cond &c) {
    c.Wait(_lock);
  }

  /// wake up a waiter (with lock already held)
  void _wake() {
    _cond.Signal();
  }
  /// wake up a waiter (without lock held)
  void wake() {
    Mutex::Locker l(_lock);
    _cond.Signal();
  }
  void _wait() {
    _cond.Wait(_lock);
  }

  /// start thread pool thread
  void start();
  /// stop thread pool thread
  void stop(bool clear_after=true);
  /// pause thread pool (if it not already paused)
  void pause();
  /// pause initiation of new work
  void pause_new();
  /// resume work in thread pool.  must match each pause() call 1:1 to resume.
  void unpause();
  /** @brief Wait until work completes.
   * If the parameter is NULL, blocks until all threads are idle.
   * If it is not NULL, blocks until the given work queue does not have
   * any items left to process. */
  void drain(WorkQueue_* wq = 0);

  /// set io priority
  void set_ioprio(int cls, int priority);
};

class GenContextWQ :
  public ThreadPool::WorkQueueVal<GenContext<HBHandle&>*> {
  list<GenContext<HBHandle&>*> _queue;
public:
  GenContextWQ(const string &name, time_t ti, ThreadPool *tp)
    : ThreadPool::WorkQueueVal<
      GenContext<HBHandle&>*>(name, ti, ti*10, tp) {}
  
  void _enqueue(GenContext<HBHandle&> *c) {
    _queue.push_back(c);
  }
  void _enqueue_front(GenContext<HBHandle&> *c) {
    _queue.push_front(c);
  }
  bool _empty() {
    return _queue.empty();
  }
  GenContext<HBHandle&> *_dequeue() {
    assert(!_queue.empty());
    GenContext<HBHandle&> *c = _queue.front();
    _queue.pop_front();
    return c;
  }
  using ThreadPool::WorkQueueVal<GenContext<HBHandle&>*>::_process;
  void _process(GenContext<HBHandle&> *c, HBHandle &tp) {
    c->complete(tp);
  }
};

class C_QueueInWQ : public Context {
  GenContextWQ *wq;
  GenContext<HBHandle&> *c;
public:
  C_QueueInWQ(GenContextWQ *wq, GenContext<HBHandle &> *c)
    : wq(wq), c(c) {}
  void finish(int) {
    wq->queue(c);
  }
};

/// Work queue that asynchronously completes contexts (executes callbacks).
/// @see Finisher
class ContextWQ : public ThreadPool::WorkQueueVal<std::pair<Context *, int> > {
public:
  ContextWQ(const string &name, time_t ti, ThreadPool *tp)
    : ThreadPool::WorkQueueVal<std::pair<Context *, int> >(name, ti, 0, tp) {}

  void queue(Context *ctx, int result = 0) {
    ThreadPool::WorkQueueVal<std::pair<Context *, int> >::queue(
      std::make_pair(ctx, result));
  }

protected:
  virtual void _enqueue(std::pair<Context *, int> item) {
    _queue.push_back(item);
  }
  virtual void _enqueue_front(std::pair<Context *, int> item) {
    _queue.push_front(item);
  }
  virtual bool _empty() {
    return _queue.empty();
  }
  virtual std::pair<Context *, int> _dequeue() {
    std::pair<Context *, int> item = _queue.front();
    _queue.pop_front();
    return item;
  }
  virtual void _process(std::pair<Context *, int> item) {
    item.first->complete(item.second);
  }
  using ThreadPool::WorkQueueVal<std::pair<Context *, int> >::_process;
private:
  list<std::pair<Context *, int> > _queue;
};

#endif

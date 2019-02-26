#pragma once
#include <optional>
#include <set>
#include "common/spin_latch.h"
#include "transaction/transaction_defs.h"

namespace terrier::transaction {
/**
 * A TransactionThreadContext encapsulates information about the thread on which the transaction is started
 * (and presumably will finish). While this is not essential to our concurrency control algorithm, having
 * this information tagged with each transaction helps with various performance optimizations.
 */
class TransactionThreadContext {
 public:
  /**
   * Constructs a new TransactionThreadContext with the given worker_id
   * @param worker_id the worker_id of the thread
   */
  explicit TransactionThreadContext(worker_id_t worker_id, bool gc_enabled = false)
      : worker_id_(worker_id), gc_enabled_(gc_enabled) {}

  /**
   * @return worker id of the thread
   */
  worker_id_t GetWorkerId() const { return worker_id_; }

  /**
   * Begins a transaction.
   * @param timestamp timestamp at the beginning of the transaction
   */
  void BeginTransaction(timestamp_t timestamp);

  /**
   * Commits a transaction, making all of its changes visible to others.
   * @param txn the transaction to commit
   */
  void Commit(TransactionContext *txn);

  /**
   * Aborts a transaction, rolling back its changes (if any).
   * @param txn the transaction to abort.
   */
  void Abort(TransactionContext *txn);

  /**
   * Get the oldest transaction alive in the system at this time. Because of concurrent operations, it
   * is not guaranteed that upon return the txn is still alive. However, it is guaranteed that the return
   * timestamp is older than any transactions live.
   * @return timestamp that is older than any transactions alive or None if there are no
   *         running transactions in this worker thread.
   */
  std::optional<timestamp_t> OldestTransactionStartTime() const;

  /**
   * Submit all completed transactions done by the worker thread to the transaction manager
   * @return A TransactionQueue containing all completed transactions done by the worker thread
   */
  TransactionQueue SubmitCompletedTransactions();

 private:
  // id of the worker thread on which the transaction start and finish.
  worker_id_t worker_id_;

  bool gc_enabled_ = false;

  // Use the same data structure as transaction manager to reduce contention
  std::set<timestamp_t> curr_running_txns_;
  mutable common::SpinLatch curr_running_txns_latch_;
  TransactionQueue worker_completed_txns_;
};
}  // namespace terrier::transaction

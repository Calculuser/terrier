#include "transaction/transaction_thread_context.h"
#include "transaction/transaction_context.h"
#include <algorithm>
#include <utility>

namespace terrier::transaction {
void TransactionThreadContext::BeginTransaction(timestamp_t timestamp) {
  common::SpinLatch::ScopedSpinLatch guard(&curr_running_txns_latch_);
  curr_running_txns_.emplace(timestamp);
}

void TransactionThreadContext::Commit(TransactionContext *txn) {
  // In a critical section, remove this transaction from the table of running transactions
  common::SpinLatch::ScopedSpinLatch guard(&curr_running_txns_latch_);
  const timestamp_t start_time = txn->StartTime();
  const size_t ret UNUSED_ATTRIBUTE = curr_running_txns_.erase(start_time);
  TERRIER_ASSERT(ret == 1, "Committed transaction did not exist in global transactions table");
  if (gc_enabled_) worker_completed_txns_.push_front(txn);
}

void TransactionThreadContext::Abort(TransactionContext *txn) {
  // In a critical section, remove this transaction from the table of running transactions
  common::SpinLatch::ScopedSpinLatch guard(&curr_running_txns_latch_);
  const timestamp_t start_time = txn->StartTime();
  const size_t ret UNUSED_ATTRIBUTE = curr_running_txns_.erase(start_time);
  TERRIER_ASSERT(ret == 1, "Aborted transaction did not exist in global transactions table");
  if (gc_enabled_) worker_completed_txns_.push_front(txn);
}

std::optional<timestamp_t> TransactionThreadContext::OldestTransactionStartTime() const {
  common::SpinLatch::ScopedSpinLatch guard(&curr_running_txns_latch_);
  // If there is no running transactions, return None.
  if (curr_running_txns_.empty()) return std::optional<timestamp_t>();
  return std::optional<timestamp_t>(*curr_running_txns_.begin());
}

TransactionQueue TransactionThreadContext::SubmitCompletedTransactions() {
  common::SpinLatch::ScopedSpinLatch guard(&curr_running_txns_latch_);
  return std::move(worker_completed_txns_);
}
}  // namespace terrier::transaction

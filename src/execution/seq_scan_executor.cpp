//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_{plan} {}

void SeqScanExecutor::Init() {
  Catalog *catalog = exec_ctx_->GetCatalog();
  table_oid_t table_oid = plan_->GetTableOid();
  TableInfo *table_info = catalog->GetTable(table_oid);

  iterator_ = new TableIterator(table_info->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  for (; !iterator_->IsEnd(); iterator_->operator++()) {
    std::pair<TupleMeta, Tuple> t = iterator_->GetTuple();
    if (t.first.is_deleted_) {
      continue;
    }
    if (plan_->filter_predicate_) {
      auto value = plan_->filter_predicate_->Evaluate(&(t.second), plan_->OutputSchema());
      if (!value.IsNull() && value.GetAs<bool>()) {
        *tuple = t.second;
        *rid = t.second.GetRid();
        iterator_->operator++();
        return true;
      }
    } else {
      *tuple = t.second;
      *rid = t.second.GetRid();
      iterator_->operator++();
      return true;
    }
  }

  return false;
}

}  // namespace bustub

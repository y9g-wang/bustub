//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  executed_once_ = false;
}

void IndexScanExecutor::Init() {}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (executed_once_) {
    return false;
  }
  executed_once_ = true;

  Catalog *catalog = exec_ctx_->GetCatalog();
  index_oid_t index_oid = plan_->GetIndexOid();
  IndexInfo *index_info = catalog->GetIndex(index_oid);

  auto htable = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info->index_.get());

  std::vector<Value> values{};
  std::vector<RID> result{};
  values.reserve(plan_->pred_keys_.size());
  values.emplace_back(plan_->pred_keys_[0]->Evaluate(nullptr, plan_->OutputSchema()));
  htable->ScanKey(Tuple{values, htable->GetKeySchema()}, &result, exec_ctx_->GetTransaction());
  if (result.empty()) {
    return false;
  }
  // assumption is that there will not be duplicate entries
  BUSTUB_ENSURE(result.size() == 1, "[IndexScanExecutor::Next] result has multiple elements");

  table_oid_t table_oid = plan_->table_oid_;
  TableInfo *table_info = catalog->GetTable(table_oid);
  std::pair<TupleMeta, Tuple> get_tuple_result = table_info->table_->GetTuple(result[0]);
  if (get_tuple_result.first.is_deleted_) {
    return false;
  }
  *tuple = get_tuple_result.second;
  *rid = result[0];

  return true;
}

}  // namespace bustub

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  executed_once_ = false;
}

void InsertExecutor::Init() { child_executor_->Init(); }

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (executed_once_) {
    return false;
  }
  executed_once_ = true;
  Catalog *catalog = exec_ctx_->GetCatalog();
  table_oid_t table_oid = plan_->GetTableOid();
  TableInfo *table_info = catalog->GetTable(table_oid);

  int32_t num_tuples_inserted = 0;
  Tuple tuple_to_insert;
  RID rid_to_insert;
  while (child_executor_->Next(&tuple_to_insert, &rid_to_insert)) {
    auto tuple_meta = TupleMeta{INVALID_TXN_ID, false};
    std::optional<RID> optional_record_id = table_info->table_->InsertTuple(
        tuple_meta, tuple_to_insert, exec_ctx_->GetLockManager(), exec_ctx_->GetTransaction(), table_oid);
    BUSTUB_ENSURE(optional_record_id.has_value(), "[InsertExecutor::Next] failed to insert record into table");
    RID record_id = optional_record_id.value();

    for (auto index_info : catalog->GetTableIndexes(table_info->name_)) {
      Tuple index_key_to_insert =
          tuple_to_insert.KeyFromTuple(table_info->schema_, index_info->key_schema_,
                                       {table_info->schema_.GetColIdx(index_info->key_schema_.GetColumn(0).GetName())});
      bool inserted = index_info->index_->InsertEntry(index_key_to_insert, record_id, exec_ctx_->GetTransaction());
      BUSTUB_ENSURE(inserted, "[InsertExecutor::Next] failed to insert record into index");
    }
    num_tuples_inserted++;
  }

  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(TypeId::INTEGER, num_tuples_inserted);
  *tuple = Tuple{values, &GetOutputSchema()};

  return true;
}

}  // namespace bustub

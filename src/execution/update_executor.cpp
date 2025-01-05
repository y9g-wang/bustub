//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() { child_executor_->Init(); }

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (executed_once_) {
    return false;
  }
  executed_once_ = true;
  Catalog *catalog = exec_ctx_->GetCatalog();
  table_oid_t table_oid = plan_->GetTableOid();
  TableInfo *table_info = catalog->GetTable(table_oid);

  int32_t num_tuples_updated = 0;
  Tuple tuple_to_update;
  RID rid_to_update;

  // Update operations is carried out by first deleting then inserting the tuple
  while (child_executor_->Next(&tuple_to_update, &rid_to_update)) {
    // Deletion part
    TupleMeta tuple_meta_to_delete = table_info->table_->GetTupleMeta(rid_to_update);
    tuple_meta_to_delete.is_deleted_ = true;
    table_info->table_->UpdateTupleMeta(tuple_meta_to_delete, rid_to_update);
    for (auto index_info : catalog->GetTableIndexes(table_info->name_)) {
      Tuple index_key_to_update =
          tuple_to_update.KeyFromTuple(table_info->schema_, index_info->key_schema_,
                                       {table_info->schema_.GetColIdx(index_info->key_schema_.GetColumn(0).GetName())});
      index_info->index_->DeleteEntry(index_key_to_update, rid_to_update, exec_ctx_->GetTransaction());
    }

    std::vector<Value> values{};
    values.reserve(plan_->target_expressions_.size());
    for (const auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(&tuple_to_update, child_executor_->GetOutputSchema()));
    }
    Tuple tuple_to_insert = Tuple{values, &child_executor_->GetOutputSchema()};

    // Insertion part
    auto tuple_meta_to_insert = TupleMeta{INVALID_TXN_ID, false};
    std::optional<RID> optional_record_id = table_info->table_->InsertTuple(
        tuple_meta_to_insert, tuple_to_insert, exec_ctx_->GetLockManager(), exec_ctx_->GetTransaction(), table_oid);
    BUSTUB_ENSURE(optional_record_id.has_value(), "[UpdateExecutor::Next] failed to insert record into table");
    RID record_id = optional_record_id.value();
    for (auto index_info : catalog->GetTableIndexes(table_info->name_)) {
      Tuple index_key_to_insert =
          tuple_to_insert.KeyFromTuple(table_info->schema_, index_info->key_schema_,
                                       {table_info->schema_.GetColIdx(index_info->key_schema_.GetColumn(0).GetName())});
      bool inserted = index_info->index_->InsertEntry(index_key_to_insert, record_id, exec_ctx_->GetTransaction());
      BUSTUB_ENSURE(inserted, "[UpdateExecutor::Next] failed to insert record into index");
    }

    num_tuples_updated++;
  }

  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(TypeId::INTEGER, num_tuples_updated);
  *tuple = Tuple{values, &GetOutputSchema()};

  return true;
}

}  // namespace bustub

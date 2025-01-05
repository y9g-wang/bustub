//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (executed_once_) {
    return false;
  }
  executed_once_ = true;

  Catalog *catalog = exec_ctx_->GetCatalog();
  table_oid_t table_oid = plan_->GetTableOid();
  TableInfo *table_info = catalog->GetTable(table_oid);

  int32_t num_tuples_deleted = 0;
  Tuple tuple_to_delete;
  RID rid_to_delete;

  while (child_executor_->Next(&tuple_to_delete, &rid_to_delete)) {
    TupleMeta tuple_meta_to_delete = table_info->table_->GetTupleMeta(rid_to_delete);
    tuple_meta_to_delete.is_deleted_ = true;
    table_info->table_->UpdateTupleMeta(tuple_meta_to_delete, rid_to_delete);
    for (auto index_info : catalog->GetTableIndexes(table_info->name_)) {
      Tuple index_key_to_delete =
          tuple_to_delete.KeyFromTuple(table_info->schema_, index_info->key_schema_,
                                       {table_info->schema_.GetColIdx(index_info->key_schema_.GetColumn(0).GetName())});
      index_info->index_->DeleteEntry(index_key_to_delete, rid_to_delete, exec_ctx_->GetTransaction());
    }
    num_tuples_deleted++;
  }

  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(TypeId::INTEGER, num_tuples_deleted);
  *tuple = Tuple{values, &GetOutputSchema()};

  return true;
}

}  // namespace bustub

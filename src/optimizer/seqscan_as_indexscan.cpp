#include <execution/plans/filter_plan.h>
#include <execution/plans/index_scan_plan.h>
#include <execution/plans/seq_scan_plan.h>

#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const AbstractPlanNodeRef &child : plan->GetChildren()) {
    children.emplace_back(OptimizeMergeFilterScan(child));
  }

  std::unique_ptr<AbstractPlanNode> optimized_plan = plan->CloneWithChildren(std::move(children));

  if (plan->GetType() == PlanType::SeqScan) {
    const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);

    if (seq_scan_plan.filter_predicate_) {
      // TODO(yaofeng.wang): support OR expressions e.g. select * from t1 where v1 = 1 or v1 = 2;
      Column predicate_column = seq_scan_plan.filter_predicate_->GetChildAt(0)->GetReturnType();
      std::vector<IndexInfo *> index_infos = catalog_.GetTableIndexes(seq_scan_plan.table_name_);
      const IndexInfo *index_info = nullptr;
      for (const IndexInfo *index : index_infos) {
        if (predicate_column.GetName().find(index->key_schema_.GetColumn(0).GetName()) != std::string::npos) {
          index_info = index;
          break;
        }
      }
      if (index_info) {
        index_oid_t index_oid = index_info->index_oid_;
        std::vector<AbstractExpressionRef> pred_keys = {seq_scan_plan.filter_predicate_->GetChildAt(1)};
        return std::make_shared<IndexScanPlanNode>(seq_scan_plan.output_schema_, seq_scan_plan.table_oid_, index_oid,
                                                   seq_scan_plan.filter_predicate_, pred_keys);
      }
    }
  }

  return optimized_plan;
}

}  // namespace bustub

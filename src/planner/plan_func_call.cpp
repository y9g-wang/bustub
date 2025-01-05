#include <memory>
#include <tuple>
#include "binder/bound_expression.h"
#include "binder/bound_statement.h"
#include "binder/expressions/bound_agg_call.h"
#include "binder/expressions/bound_alias.h"
#include "binder/expressions/bound_binary_op.h"
#include "binder/expressions/bound_column_ref.h"
#include "binder/expressions/bound_constant.h"
#include "binder/expressions/bound_func_call.h"
#include "binder/expressions/bound_unary_op.h"
#include "binder/statement/select_statement.h"
#include "common/exception.h"
#include "common/macros.h"
#include "common/util/string_util.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/string_expression.h"
#include "execution/plans/abstract_plan.h"
#include "fmt/format.h"
#include "planner/planner.h"

namespace bustub {

// NOLINTNEXTLINE
auto Planner::GetFuncCallFromFactory(const std::string &func_name, std::vector<AbstractExpressionRef> args)
    -> AbstractExpressionRef {
  // 1. check if the parsed function name is "lower" or "upper".
  // 2. verify the number of args (should be 1), refer to the test cases for when you should throw an `Exception`.
  // 3. return a `StringExpression` std::shared_ptr.
  // throw Exception(fmt::format("func call {} not supported in planner yet", func_name));


  if (args.size() != 1) {
    throw Exception(fmt::format("args size is {}, is not 1", args.size()));
  }

  if (func_name == "lower") {
    // std::cout  << "func_name: " << func_name << std::endl;
    StringExpression se =  StringExpression {args[0], StringExpressionType::Lower};
    return std::make_shared<StringExpression>(se);
  } else if (func_name == "upper") {
    // std::cout  << "func_name: " << func_name << std::endl;
    StringExpression se =  StringExpression {args[0], StringExpressionType::Upper};
    return std::make_shared<StringExpression>(se);
  }

  throw Exception(fmt::format("func call {} not supported in planner yet", func_name));

}

}  // namespace bustub

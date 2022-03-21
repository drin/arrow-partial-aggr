// ------------------------------
// Dependencies

#include "simple.hpp"
#include "ops.hpp"


// ------------------------------
// Functions

shared_ptr<ChunkedArray>
VecAdd(shared_ptr<ChunkedArray> left_op, shared_ptr<ChunkedArray> right_op) {
    if (left_op == nullptr or right_op == nullptr) { return nullptr; }

    Result<Datum> op_result = Add(left_op, right_op);
    if (not op_result.ok()) { return nullptr; }

    return std::make_shared<ChunkedArray>(
        std::move(op_result.ValueOrDie()).chunks()
    );
}


shared_ptr<ChunkedArray>
VecSub(shared_ptr<ChunkedArray> left_op, shared_ptr<ChunkedArray> right_op) {
    if (left_op == nullptr or right_op == nullptr) { return nullptr; }

    Result<Datum> op_result = Subtract(left_op, right_op);
    if (not op_result.ok()) { return nullptr; }

    return std::make_shared<ChunkedArray>(
        std::move(op_result.ValueOrDie()).chunks()
    );
}


shared_ptr<ChunkedArray>
VecDiv(shared_ptr<ChunkedArray> left_op, shared_ptr<ChunkedArray> right_op) {
    if (left_op == nullptr or right_op == nullptr) { return nullptr; }

    Result<Datum> op_result = Divide(left_op, right_op);
    if (not op_result.ok()) { return nullptr; }

    return std::make_shared<ChunkedArray>(
        std::move(op_result.ValueOrDie()).chunks()
    );
}


shared_ptr<ChunkedArray>
VecDiv(shared_ptr<ChunkedArray> left_op, Datum right_op) {
    if (left_op == nullptr) { return nullptr; }

    Result<Datum> op_result = Divide(left_op, right_op);
    if (not op_result.ok()) { return nullptr; }

    return std::make_shared<ChunkedArray>(
        std::move(op_result.ValueOrDie()).chunks()
    );
}


shared_ptr<ChunkedArray>
VecMul(shared_ptr<ChunkedArray> left_op, shared_ptr<ChunkedArray> right_op) {
    if (left_op == nullptr or right_op == nullptr) { return nullptr; }

    Result<Datum> op_result = Multiply(left_op, right_op);
    if (not op_result.ok()) { return nullptr; }

    return std::make_shared<ChunkedArray>(
        std::move(op_result.ValueOrDie()).chunks()
    );
}


shared_ptr<ChunkedArray>
VecMul(shared_ptr<ChunkedArray> left_op, Datum right_op) {
    if (left_op == nullptr) { return nullptr; }

    Result<Datum> op_result = Multiply(left_op, right_op);
    if (not op_result.ok()) { return nullptr; }

    return std::make_shared<ChunkedArray>(
        std::move(op_result.ValueOrDie()).chunks()
    );
}


shared_ptr<ChunkedArray>
VecPow(shared_ptr<ChunkedArray> left_op, Datum right_op) {
    if (left_op == nullptr) { return nullptr; }

    Result<Datum> op_result = Power(left_op, right_op);
    if (not op_result.ok()) { return nullptr; }

    return std::make_shared<ChunkedArray>(
        std::move(op_result.ValueOrDie()).chunks()
    );
}


shared_ptr<ChunkedArray>
VecAbs(shared_ptr<ChunkedArray> unary_op) {
    if (unary_op == nullptr) { return nullptr; }

    Result<Datum> op_result = AbsoluteValue(unary_op);
    if (not op_result.ok()) { return nullptr; }

    return std::make_shared<ChunkedArray>(
        std::move(op_result.ValueOrDie()).chunks()
    );
}


// ------------------------------
// Aggr implementation

Aggr::~Aggr() {
    means.reset();
    variances.reset();
}

Aggr::Aggr() {
    count     = 0;
    means     = std::shared_ptr<ChunkedArray>(nullptr);
    variances = std::shared_ptr<ChunkedArray>(nullptr);
}

shared_ptr<Table>
Aggr::TakeResult() {
    auto result_schema = arrow::schema({
          arrow::field("mean"    , arrow::float64())
         ,arrow::field("variance", arrow::float64())
     });

    return Table::Make(result_schema, { this->means, this->variances });
}

Status
Aggr::Initialize(shared_ptr<ChunkedArray> initial_vals) {
    arrow::DoubleBuilder initial_vars;

    ARROW_RETURN_NOT_OK(initial_vars.AppendEmptyValues(initial_vals->length()));
    ARROW_ASSIGN_OR_RAISE(shared_ptr<Array> built_arr, initial_vars.Finish());

    variances   = std::make_shared<ChunkedArray>(built_arr);
    means       = initial_vals;
    this->count = 1;

    return Status::OK();
}


/**
  * Per Wikipedipa; Moment2 (variance):
  *     fn(count, mean, M2, newValue):
  *         (count, mean, M2)  = existingAggregate
  *         count             += 1
  *
  *         delta              = newValue - mean
  *         mean              += delta / count
  *
  *         delta2             = newValue - mean
  *         M2                += delta * delta2
  *
  *         return (count, mean, M2)
 */
Status
Aggr::Accumulate(shared_ptr<Table> new_vals, int64_t col_ndx) {
    shared_ptr<ChunkedArray> delta_mean;
    shared_ptr<ChunkedArray> delta_var;

    if (this->count == 0) {
        // std::cout << "Initializing new values..." << std::endl;
        this->Initialize(new_vals->column(col_ndx));
        this->count  = 1;
        col_ndx     += 1;
    }

    for (; col_ndx < new_vals->num_columns(); col_ndx++) {
        // use a result just to make call chains easier
        auto col_vals = new_vals->column(col_ndx);

        this->count += 1;
        delta_mean  = VecAbs(VecSub(col_vals, this->means));
        this->means = VecAdd(
             this->means
            ,VecDiv(delta_mean, Datum(this->count))
        );

        delta_var       = VecAbs(VecSub(col_vals, this->means));
        this->variances = VecAdd(
             this->variances
            ,VecMul(delta_var, delta_mean)
        );
    }

    return Status::OK();
}

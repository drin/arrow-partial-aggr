#include "simple.hpp"

struct Aggr {
    uint64_t                 count;
    shared_ptr<ChunkedArray> means;
    shared_ptr<ChunkedArray> variances;

    Aggr();
    ~Aggr();

    shared_ptr<Table> TakeResult();
    Status            Initialize(shared_ptr<ChunkedArray> initial_vals);
    Status            Accumulate( shared_ptr<Table> new_vals
                                 ,int64_t           col_startndx=1
                                 ,int64_t           col_stopndx =0);
};

// just for readability here
using ChunkedArrayPtr = shared_ptr<ChunkedArray>;

ChunkedArrayPtr VecAdd(ChunkedArrayPtr left_op, ChunkedArrayPtr right_op);
ChunkedArrayPtr VecSub(ChunkedArrayPtr left_op, ChunkedArrayPtr right_op);
ChunkedArrayPtr VecDiv(ChunkedArrayPtr left_op, ChunkedArrayPtr right_op);
ChunkedArrayPtr VecDiv(ChunkedArrayPtr left_op, Datum           right_op);
ChunkedArrayPtr VecMul(ChunkedArrayPtr left_op, ChunkedArrayPtr right_op);
ChunkedArrayPtr VecPow(ChunkedArrayPtr left_op, Datum           right_op);
ChunkedArrayPtr VecAbs(ChunkedArrayPtr unary_op);

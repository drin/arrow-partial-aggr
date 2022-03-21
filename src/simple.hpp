// ------------------------------
// Dependencies

#include <string>
#include <iostream>
#include <vector>

#include <arrow/api.h>
#include <arrow/ipc/api.h>
#include <arrow/compute/api.h>
#include <arrow/filesystem/api.h>


// ------------------------------
// Type Aliases


// ----------
// From core
using std::shared_ptr;


// ----------
// From Arrow

// >> Types
//  |> General util/support
using arrow::Status;
using arrow::Result;
using arrow::Datum;

//  |> Data types
using arrow::Array;
using arrow::ChunkedArray;
using arrow::Table;
using arrow::RecordBatch;

//  |> For interfacing with file system
using arrow::io::RandomAccessFile;
using arrow::fs::FileSystem;
using arrow::ipc::RecordBatchStreamReader;
using arrow::ipc::IpcReadOptions;


// >> Functions
using arrow::compute::Add;
using arrow::compute::Subtract;
using arrow::compute::Divide;
using arrow::compute::Multiply;
using arrow::compute::Power;
using arrow::compute::AbsoluteValue;

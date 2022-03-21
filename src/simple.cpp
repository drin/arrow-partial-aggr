#include <iostream>
#include "simple.hpp"
#include "ops.hpp"

using arrow::ipc::feather::Reader;

static std::string help_message {
    "simple <absolute path to current dir> <'table' | 'vector'>"
};


// Result<shared_ptr<RecordBatchStreamReader>>
Result<shared_ptr<Reader>>
ReaderForIPCFile(const std::string &path_as_uri) {
    std::cout << "Reading arrow IPC-formatted file: " << path_as_uri << std::endl;
    std::string path_to_file;

    // get a `FileSystem` instance (local fs scheme is "file://")
    ARROW_ASSIGN_OR_RAISE(auto localfs, arrow::fs::FileSystemFromUri(path_as_uri, &path_to_file));

    // use the `FileSystem` instance to open a handle to the file
    ARROW_ASSIGN_OR_RAISE(auto input_file_stream, localfs->OpenInputFile(path_to_file));

    // read from the handle using `RecordBatchStreamReader`
    /*
    return RecordBatchStreamReader::Open(
         input_file_stream
        ,IpcReadOptions::Defaults()
    );
    */

    // return Reader::Open(input_file_stream, IpcReadOptions::Defaults());
    return Reader::Open(input_file_stream);
}


Result<shared_ptr<Table>>
ReadIPCFile(const std::string& path_to_file) {
    std::cout << "Parsing file: '" << path_to_file << "'" << std::endl;

    // Declares and initializes `batch_reader`
    // ARROW_ASSIGN_OR_RAISE(auto batch_reader, ReaderForIPCFile(path_to_file));
    // return arrow::Table::FromRecordBatchReader(batch_reader.get());

    shared_ptr<Table> data_table = shared_ptr<Table>(nullptr);

    ARROW_ASSIGN_OR_RAISE(auto feather_reader, ReaderForIPCFile(path_to_file));
    ARROW_RETURN_NOT_OK(feather_reader->Read(&data_table));

    return data_table;
}


int main(int argc, char **argv) {
    // ----------
    // Process CLI args for convenience
    if (argc != 3) {
        // we need this to make it easier to run on various systems
        std::cerr << "Please provide the absolute path to the current directory" << std::endl;
        return 1;
    }

    std::string work_dirpath     { argv[1] };
    std::string should_aggrtable { argv[2] };

    bool        aggr_table   { should_aggrtable == "table" };
    std::string test_fpath   {
          "file://"
        + work_dirpath
        + "/resources/E-GEOD-76312.48-2152.x565.feather"
    };

    std::cout << "Using directory  [" << work_dirpath << "]" << std::endl;
    std::cout << "Aggregating over [" << (aggr_table ? "table" : "vector") << "]" << std::endl;


    // ----------
    // Read sample data from file
    auto reader_result = ReadIPCFile(test_fpath);
    if (not reader_result.ok()) {
        std::cerr << "Couldn't read file:"                     << std::endl
                  << "\t" << reader_result.status().ToString() << std::endl
        ;

        return 1;
    }


    // ----------
    // Peek at the data
    auto data_table = *reader_result;
    auto pkey_col   = data_table->column(0);
    std::cout << "Table dimensions ["
              <<     data_table->num_rows() << ", " << data_table->num_columns()
              << "]"
              << std::endl
              << "\t" << "[" << pkey_col->num_chunks()       << "] chunks"
              << std::endl
              << "\t" << "[" << pkey_col->chunk(0)->length() << "] chunk_size"
              << std::endl
    ;


    // >> The computation to be benchmarked
    std::chrono::milliseconds aggr_ttime { 0 };

    //  |> case for applying computation to whole table
    if (aggr_table) {
        auto aggr_tstart = std::chrono::steady_clock::now();

        Aggr partial_aggr;
        partial_aggr.Accumulate(data_table, 1);
        auto aggr_result = partial_aggr.TakeResult();

        auto aggr_tstop  = std::chrono::steady_clock::now();
        aggr_ttime += std::chrono::duration_cast<std::chrono::milliseconds>(
            aggr_tstop - aggr_tstart
        );
    }

    //  |> case for applying computation to each table chunk (i.e. RecordBatch)
    else {
        shared_ptr<RecordBatch>        table_batch;
        std::vector<shared_ptr<Table>> aggrs_by_slice;

        aggrs_by_slice.reserve(pkey_col->num_chunks());

        size_t batch_ndx { 0 };
        auto   table_batcher = arrow::TableBatchReader(*data_table);
        auto   read_status   = table_batcher.ReadNext(&table_batch);
        while (read_status.ok() and table_batch != nullptr) {
            // convert batch to a Table
            auto convert_result = Table::FromRecordBatches({ table_batch });
            if (not convert_result.ok()) {
                std::cerr << "Error:"                                   << std::endl
                          << "\t" << convert_result.status().ToString() << std::endl
                ;

                return 2;
            }
            auto tslice = *convert_result;

            // Aggregate table
            // NOTE: we are *only* timing the computation on each slice
            auto aggr_tstart = std::chrono::steady_clock::now();

            Aggr partial_aggr;
            partial_aggr.Accumulate(tslice, 1);

            auto aggr_tstop  = std::chrono::steady_clock::now();

            aggr_ttime += std::chrono::duration_cast<std::chrono::milliseconds>(
                aggr_tstop - aggr_tstart
            );

            // Save aggr result
            aggrs_by_slice.push_back(partial_aggr.TakeResult());

            // Read next batch
            batch_ndx += 1;
            // std::cout << "Reading next slice" << std::endl;
            read_status = table_batcher.ReadNext(&table_batch);
        }

        auto concat_result = arrow::ConcatenateTables(aggrs_by_slice);
        if (not concat_result.ok()) {
            std::cerr << "Failed to concatenate partial aggr results" << std::endl
                      << "\t" << concat_result.status().ToString()    << std::endl
            ;

            return 3;
        }
    }

    std::cout << "Aggr Time:\t" << aggr_ttime.count() << "ms" << std::endl;
    return 0;
}

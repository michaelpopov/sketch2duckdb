#pragma once

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/types/string_type.hpp"

#include "sketch2.h"

namespace duckdb {

class ClientContext;
struct Sketch2ClientState;

struct Sketch2Dataset {
	Sketch2Dataset(string database_path_p, string dataset_name_p);
	~Sketch2Dataset();
	Sketch2Dataset(const Sketch2Dataset &) = delete;
	Sketch2Dataset &operator=(const Sketch2Dataset &) = delete;
	Sketch2Dataset(Sketch2Dataset &&) = delete;
	Sketch2Dataset &operator=(Sketch2Dataset &&) = delete;

	char version[64];
	string database_path;
	string dataset_name;
	sk_handle_t *handle = nullptr;
};

struct Sketch2Bitset {
	Sketch2Bitset(void *blob_p, idx_t blob_size_p);
	~Sketch2Bitset();

	void *blob = nullptr;
	idx_t blob_size = 0;
};

shared_ptr<Sketch2ClientState> GetOrCreateSketch2State(ClientContext &context);
shared_ptr<Sketch2Dataset> GetSketch2Dataset(ClientContext &context);
void SetSketch2Dataset(ClientContext &context, shared_ptr<Sketch2Dataset> dataset);
void ClearSketch2Bitsets(ClientContext &context);
int64_t StoreSketch2Bitset(ClientContext &context, shared_ptr<Sketch2Bitset> bitset);
optional_ptr<Sketch2Bitset> GetSketch2Bitset(ClientContext &context, int64_t handle);

} // namespace duckdb

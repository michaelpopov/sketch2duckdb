#pragma once

#include <mutex>

#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/types/string_type.hpp"

#include "sketch2.h"

namespace duckdb {

class ClientContext;
struct Sketch2ClientState;

struct Sketch2Dataset {
	Sketch2Dataset() = default;
	~Sketch2Dataset();
	Sketch2Dataset(const Sketch2Dataset &) = delete;
	Sketch2Dataset &operator=(const Sketch2Dataset &) = delete;
	Sketch2Dataset(Sketch2Dataset &&) = delete;
	Sketch2Dataset &operator=(Sketch2Dataset &&) = delete;

	std::mutex mutex;
	string dataset_name;
	sk_handle_t *handle = nullptr;
};

shared_ptr<Sketch2ClientState> GetOrCreateSketch2State(ClientContext &context);
shared_ptr<Sketch2Dataset> GetSketch2Dataset(ClientContext &context);

} // namespace duckdb

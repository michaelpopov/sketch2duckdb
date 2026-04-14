#include "sketch2_dataset.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_context_state.hpp"

#include <unordered_map>

namespace duckdb {

static constexpr const char *SKETCH2_CLIENT_STATE_KEY = "sketch2_client_state";

struct Sketch2ClientState : public ClientContextState {
	shared_ptr<Sketch2Dataset> dataset;
	std::unordered_map<int64_t, shared_ptr<Sketch2Bitset>> bitsets;
	int64_t next_bitset_handle = 1;
};

Sketch2Dataset::Sketch2Dataset(string database_path_p, string dataset_name_p)
    : database_path(std::move(database_path_p)), dataset_name(std::move(dataset_name_p)) {
}

Sketch2Dataset::~Sketch2Dataset() {
	if (handle) {
		sk_release_handle(handle);
	}
}

Sketch2Bitset::Sketch2Bitset(void *blob_p, idx_t blob_size_p) : blob(blob_p), blob_size(blob_size_p) {
}

Sketch2Bitset::~Sketch2Bitset() {
	if (blob) {
		sk_free(blob);
	}
}

shared_ptr<Sketch2ClientState> GetOrCreateSketch2State(ClientContext &context) {
	auto state = context.registered_state->GetOrCreate<Sketch2ClientState>(SKETCH2_CLIENT_STATE_KEY);
	if (!state->dataset) {
		state->dataset = make_shared_ptr<Sketch2Dataset>("", "");
	}
	return state;
}

shared_ptr<Sketch2Dataset> GetSketch2Dataset(ClientContext &context) {
	return GetOrCreateSketch2State(context)->dataset;
}

void SetSketch2Dataset(ClientContext &context, shared_ptr<Sketch2Dataset> dataset) {
	GetOrCreateSketch2State(context)->dataset = std::move(dataset);
}

void ClearSketch2Bitsets(ClientContext &context) {
	GetOrCreateSketch2State(context)->bitsets.clear();
}

int64_t StoreSketch2Bitset(ClientContext &context, shared_ptr<Sketch2Bitset> bitset) {
	auto state = GetOrCreateSketch2State(context);
	state->bitsets.clear();
	const auto handle = state->next_bitset_handle++;
	state->bitsets.emplace(handle, std::move(bitset));
	return handle;
}

optional_ptr<Sketch2Bitset> GetSketch2Bitset(ClientContext &context, int64_t handle) {
	auto state = GetOrCreateSketch2State(context);
	auto entry = state->bitsets.find(handle);
	if (entry == state->bitsets.end()) {
		return nullptr;
	}
	return entry->second.get();
}

} // namespace duckdb

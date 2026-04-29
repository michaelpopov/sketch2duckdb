#include "sketch2_dataset.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_context_state.hpp"

namespace duckdb {

static constexpr const char *SKETCH2_CLIENT_STATE_KEY = "sketch2_client_state";

struct Sketch2ClientState : public ClientContextState {
	shared_ptr<Sketch2Dataset> dataset;
};

Sketch2Dataset::~Sketch2Dataset() {
	std::lock_guard<std::mutex> lock(mutex);
	if (handle) {
		sk_release_handle(handle);
		handle = nullptr;
	}
}

shared_ptr<Sketch2ClientState> GetOrCreateSketch2State(ClientContext &context) {
	auto state = context.registered_state->GetOrCreate<Sketch2ClientState>(SKETCH2_CLIENT_STATE_KEY);
	if (!state->dataset) {
		state->dataset = make_shared_ptr<Sketch2Dataset>();
	}
	return state;
}

shared_ptr<Sketch2Dataset> GetSketch2Dataset(ClientContext &context) {
	return GetOrCreateSketch2State(context)->dataset;
}

} // namespace duckdb

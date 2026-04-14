#include "sketch2_bitset_filter.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <algorithm>
#include "sketch2_dataset.hpp"
#include "sketch2.h"

#include <vector>

namespace duckdb {

static constexpr idx_t SKETCH2_BITSET_FILTER_MAX_IDS = 1000000;

struct Sketch2BitsetFilterBindData : public FunctionData {
	explicit Sketch2BitsetFilterBindData(ClientContext &context_p) : context(context_p) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<Sketch2BitsetFilterBindData>(context);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<Sketch2BitsetFilterBindData>();
		return &context == &other.context;
	}

	ClientContext &context;
};

struct BitsetFilterState {
	std::vector<uint64_t> ids;
};

struct BitsetFilterOperation {
	template <class STATE>
	static void EnsureCapacityFor(STATE &state, idx_t additional_ids) {
		if (state.ids.size() + additional_ids > SKETCH2_BITSET_FILTER_MAX_IDS) {
			throw InvalidInputException("bitset_filter: too many ids, maximum supported count is %llu",
			                            static_cast<unsigned long long>(SKETCH2_BITSET_FILTER_MAX_IDS));
		}
	}

	template <class STATE>
	static void Initialize(STATE &state) {
		new (&state) STATE();
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &) {
		if (input < 0) {
			throw InvalidInputException("bitset_filter: id must be non-negative");
		}
		EnsureCapacityFor(state, 1);
		state.ids.push_back(static_cast<uint64_t>(input));
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &input_data, idx_t count) {
		EnsureCapacityFor(state, count);
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, input, input_data);
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		target.ids.reserve(target.ids.size() + source.ids.size());
		target.ids.insert(target.ids.end(), source.ids.begin(), source.ids.end());
	}

	template <class RESULT_TYPE, class STATE>
	static void Finalize(STATE &state, RESULT_TYPE &target, AggregateFinalizeData &finalize_data) {
		std::sort(state.ids.begin(), state.ids.end());

		void *blob = nullptr;
		size_t blob_size = 0;
		bool out_of_memory = false;
		const char *error_message = nullptr;
		auto *ids = state.ids.empty() ? nullptr : state.ids.data();

		if (sk_bitset_build(ids, state.ids.size(), &blob, &blob_size, &out_of_memory, &error_message) != 0) {
			if (out_of_memory) {
				throw OutOfMemoryException("bitset_filter: out of memory");
			}
			throw InvalidInputException("bitset_filter: %s",
			                            error_message ? error_message : "failed to build bitset");
		}

		auto &context = finalize_data.input.bind_data->Cast<Sketch2BitsetFilterBindData>().context;
		auto bitset = make_shared_ptr<Sketch2Bitset>(blob, blob_size);
		target = StoreSketch2Bitset(context, std::move(bitset));
	}

	static bool IgnoreNull() {
		return true;
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &) {
		state.~STATE();
	}
};

static unique_ptr<FunctionData> BindBitsetFilter(ClientContext &context, AggregateFunction &,
                                                 vector<unique_ptr<Expression>> &) {
	ClearSketch2Bitsets(context);
	return make_uniq<Sketch2BitsetFilterBindData>(context);
}

void RegisterSketch2BitsetFilterFunction(ExtensionLoader &loader) {
	auto function = AggregateFunction::UnaryAggregateDestructor<BitsetFilterState, int64_t, int64_t,
	                                                            BitsetFilterOperation,
	                                                            AggregateDestructorType::LEGACY>(
	    LogicalType::BIGINT, LogicalType::BIGINT);
	function.name = "bitset_filter";
	function.SetBindCallback(BindBitsetFilter);
	loader.RegisterFunction(function);
}

} // namespace duckdb

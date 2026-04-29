#include "sketch2_bitset_filter.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

#include "sketch2.h"

#include <algorithm>
#include <mutex>
#include <string>
#include <vector>

namespace duckdb {

// Per-thread tail buffer cap. When a worker's tail reaches this many ids it
// is sorted and submitted to the shared Sketch2 builder via
// sk_bitset_add_multiple_ids_name. Sized inside the 8K-16K window suggested
// by Sketch2 to amortize the builder's mutex and exploit CRoaring's
// range-coalescing bulk-add path.
static constexpr idx_t SKETCH2_BITSET_FILTER_TAIL_CAP = 8 * 1024;

// Mutable resource shared across all per-thread aggregate states for one
// sketch2_bitset_filter invocation. Owns the in-flight Sketch2 builder; the
// mutex serializes calls into Sketch2's not-thread-safe builder API.
struct BitsetFilterShared {
	explicit BitsetFilterShared(string name_p) : name(std::move(name_p)) {
	}

	~BitsetFilterShared() {
		for (auto *filter : finished_filters) {
			if (filter) {
				sk_bitset_delete(filter);
			}
		}
		Discard();
	}

	// Cancellation / poisoned-builder cleanup. sk_bitset_finish releases the
	// builder structure; we delete the resulting filter without exposing it.
	// A partial named filter may end up persisted on disk on hard failure;
	// this matches the prior behavior of the aggregate.
	void Discard() {
		if (!builder) {
			return;
		}
		void *discard = nullptr;
		bool oom = false;
		const char *err = nullptr;
		(void)sk_bitset_finish(&builder, &discard, &oom, &err);
		if (discard) {
			sk_bitset_delete(discard);
		}
		builder = nullptr;
	}

	std::mutex mu;
	void *builder = nullptr;     // protected by mu
	bool has_input_row = false;  // protected by mu
	std::vector<void *> finished_filters; // protected by mu; released when bind data dies
	string name;                 // immutable after construction
};

struct BitsetFilterState {
	std::vector<uint64_t> tail;
	bool has_input_row = false;
};

struct BitsetFilterBindData : public FunctionData {
	// Scalar bindings (load / drop / cache_remove) carry only a name.
	explicit BitsetFilterBindData(string name_p) : name(std::move(name_p)) {
	}

	// Aggregate binding additionally carries the shared builder.
	BitsetFilterBindData(string name_p, shared_ptr<BitsetFilterShared> shared_p)
	    : name(std::move(name_p)), shared(std::move(shared_p)) {
	}

	string name;
	shared_ptr<BitsetFilterShared> shared;  // null for scalar bindings

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<BitsetFilterBindData>(name, shared);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<BitsetFilterBindData>();
		return name == other.name;
	}
};

// Sort the tail and hand it to the shared Sketch2 builder under the mutex.
// Lazy-creates the builder on the first flush so an empty input never
// touches disk. On Sketch2 failure the builder is poisoned per the API
// contract; BitsetFilterShared::Discard() in the destructor cleans it up
// when bind data is destroyed.
static void FlushTailToShared(std::vector<uint64_t> &tail, BitsetFilterShared &shared) {
	if (tail.empty()) {
		return;
	}
	std::sort(tail.begin(), tail.end());

	std::lock_guard<std::mutex> lock(shared.mu);

	bool out_of_memory = false;
	const char *error_message = nullptr;

	if (!shared.builder) {
		if (sk_bitset_create_builder(&shared.builder, &out_of_memory, &error_message,
		                             shared.name.c_str()) != 0) {
			if (out_of_memory) {
				throw OutOfMemoryException("sketch2_bitset_filter: out of memory");
			}
			throw InvalidInputException("sketch2_bitset_filter: %s",
			                            error_message ? error_message
			                                          : "failed to create named bitset filter");
		}
	}

	if (sk_bitset_add_multiple_ids_name(&shared.builder, tail.data(),
	                                    static_cast<uint64_t>(tail.size()),
	                                    &out_of_memory, &error_message) != 0) {
		if (out_of_memory) {
			throw OutOfMemoryException("sketch2_bitset_filter: out of memory");
		}
		throw InvalidInputException("sketch2_bitset_filter: %s",
		                            error_message ? error_message
		                                          : "failed to add ids to bitset");
	}

	shared.has_input_row = true;
	tail.clear();
}

struct BitsetFilterOperation {
	template <class STATE>
	static void Initialize(STATE &state) {
		new (&state) STATE();
	}

	template <class ID_TYPE, class NAME_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const ID_TYPE &id, const NAME_TYPE &name, AggregateBinaryInput &input) {
		(void)name;
		state.has_input_row = true;
		const auto id_is_valid = input.left_mask.RowIsValid(input.lidx);
		if (!id_is_valid) {
			return;
		}
		if (id < 0) {
			throw InvalidInputException("sketch2_bitset_filter: id must be non-negative");
		}
		state.tail.push_back(static_cast<uint64_t>(id));
		if (state.tail.size() >= SKETCH2_BITSET_FILTER_TAIL_CAP) {
			auto &bind_data = input.input.bind_data->Cast<BitsetFilterBindData>();
			FlushTailToShared(state.tail, *bind_data.shared);
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &aggr_input_data) {
		target.has_input_row = target.has_input_row || source.has_input_row;
		if (source.tail.empty()) {
			return;
		}
		target.tail.insert(target.tail.end(), source.tail.begin(), source.tail.end());
		if (target.tail.size() >= SKETCH2_BITSET_FILTER_TAIL_CAP) {
			auto &bind_data = aggr_input_data.bind_data->Cast<BitsetFilterBindData>();
			FlushTailToShared(target.tail, *bind_data.shared);
		}
	}

	template <class RESULT_TYPE, class STATE>
	static void Finalize(STATE &state, RESULT_TYPE &target, AggregateFinalizeData &finalize_data) {
		auto &bind_data = finalize_data.input.bind_data->Cast<BitsetFilterBindData>();
		auto &shared = *bind_data.shared;

		if (!state.tail.empty()) {
			FlushTailToShared(state.tail, shared);
		}

		std::lock_guard<std::mutex> lock(shared.mu);

		if (!state.has_input_row && !shared.has_input_row) {
			finalize_data.ReturnNull();
			return;
		}

		bool out_of_memory = false;
		const char *error_message = nullptr;

		// All observed rows had NULL ids: produce an empty named filter to
		// honor the prior contract (input was non-empty, so we return the name).
		if (!shared.builder) {
			if (sk_bitset_create_builder(&shared.builder, &out_of_memory, &error_message,
			                             shared.name.c_str()) != 0) {
				if (out_of_memory) {
					throw OutOfMemoryException("sketch2_bitset_filter: out of memory");
				}
				throw InvalidInputException("sketch2_bitset_filter: %s",
				                            error_message ? error_message
				                                          : "failed to create named bitset filter");
			}
		}

		void *filter = nullptr;
		if (sk_bitset_finish(&shared.builder, &filter, &out_of_memory, &error_message) != 0) {
			shared.has_input_row = false;
			if (out_of_memory) {
				throw OutOfMemoryException("sketch2_bitset_filter: out of memory");
			}
			throw InvalidInputException("sketch2_bitset_filter: %s",
			                            error_message ? error_message
			                                          : "failed to finalize bitset");
		}
		// Keep the returned filter handle alive until bind-data teardown. For
		// named filters, releasing it immediately can make the just-published
		// filter unavailable to later operators in the same statement.
		shared.finished_filters.push_back(filter);
		// sk_bitset_finish set shared.builder to nullptr on success. Reset
		// has_input_row so a re-execution of this prepared statement starts fresh.
		shared.has_input_row = false;

		target = StringVector::AddString(finalize_data.result, bind_data.name);
	}

	static bool IgnoreNull() {
		return false;
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &) {
		state.~STATE();
	}
};

static unique_ptr<FunctionData> Sketch2BitsetFilterBind(ClientContext &context, AggregateFunction &,
                                                        vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(arguments.size() == 2);
	if (!arguments[1]->IsFoldable()) {
		throw BinderException("sketch2_bitset_filter: name must be a constant VARCHAR expression");
	}

	auto name_value = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);
	if (name_value.IsNull()) {
		throw BinderException("sketch2_bitset_filter: name must not be NULL");
	}

	auto name = name_value.GetValue<string>();
	if (name.empty()) {
		throw BinderException("sketch2_bitset_filter: name must be non-empty");
	}
	auto shared = make_shared_ptr<BitsetFilterShared>(name);
	return make_uniq<BitsetFilterBindData>(std::move(name), std::move(shared));
}

static unique_ptr<FunctionData> Sketch2BitsetNameScalarBind(ClientContext &context, ScalarFunction &bound_function,
                                                            vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(arguments.size() == 1);
	if (!arguments[0]->IsFoldable()) {
		throw BinderException("%s: name must be a constant VARCHAR expression", bound_function.name);
	}

	auto name_value = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
	if (name_value.IsNull()) {
		throw BinderException("%s: name must not be NULL", bound_function.name);
	}

	auto name = name_value.GetValue<string>();
	if (name.empty()) {
		throw BinderException("%s: name must be non-empty", bound_function.name);
	}
	return make_uniq<BitsetFilterBindData>(std::move(name));
}

static void Sketch2BitsetDropFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &bind_data = func_expr.bind_info->Cast<BitsetFilterBindData>();
	UnaryExecutor::Execute<string_t, int32_t>(args.data[0], result, args.size(), [&](string_t) {
		int removed = 0;
		bool out_of_memory = false;
		const char *error_message = nullptr;
		if (sk_bitset_drop(bind_data.name.c_str(), &removed, &out_of_memory, &error_message) != 0) {
			if (out_of_memory) {
				throw OutOfMemoryException("sketch2_bitset_drop: out of memory");
			}
			throw InvalidInputException("sketch2_bitset_drop: %s",
			                            error_message ? error_message : "failed to drop bitset filter");
		}
		return removed;
	});
}

// sketch2_bitset_load(name) does two things:
//   1. Validates that `name` resolves to a loadable filter on disk (errors otherwise).
//   2. Warms Sketch2's process-global named-filter cache as a side effect of sk_bitset_load,
//      so a subsequent sketch2_knn(..., name) call sees a cache hit and avoids re-reading
//      the file from disk.
// The BitsetFilterControl* returned from sk_bitset_load is just a handle into the cache;
// sk_bitset_delete drops the handle but leaves the cache entry intact. That is why we
// delete `filter` immediately and still get the cache-warming effect.
// Use sketch2_bitset_cache_remove(name) or sketch2_bitset_cache_clear() to evict.
static void Sketch2BitsetLoadFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &bind_data = func_expr.bind_info->Cast<BitsetFilterBindData>();
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t) {
		void *filter = nullptr;
		bool out_of_memory = false;
		const char *error_message = nullptr;
		if (sk_bitset_load(bind_data.name.c_str(), &filter, &out_of_memory, &error_message) != 0) {
			if (out_of_memory) {
				throw OutOfMemoryException("sketch2_bitset_load: out of memory");
			}
			throw InvalidInputException("sketch2_bitset_load: %s",
			                            error_message ? error_message : "failed to load bitset filter");
		}
		// Drop the handle; the cache entry populated by sk_bitset_load survives this delete.
		if (filter) {
			sk_bitset_delete(filter);
		}
		return StringVector::AddString(result, bind_data.name);
	});
}

static void Sketch2BitsetCacheRemoveFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &bind_data = func_expr.bind_info->Cast<BitsetFilterBindData>();
	UnaryExecutor::Execute<string_t, int32_t>(args.data[0], result, args.size(), [&](string_t) {
		int removed = 0;
		bool out_of_memory = false;
		const char *error_message = nullptr;
		if (sk_bitset_cache_remove(bind_data.name.c_str(), &removed, &out_of_memory, &error_message) != 0) {
			if (out_of_memory) {
				throw OutOfMemoryException("sketch2_bitset_cache_remove: out of memory");
			}
			throw InvalidInputException("sketch2_bitset_cache_remove: %s",
			                            error_message ? error_message : "failed to remove bitset cache entry");
		}
		return removed;
	});
}

static void Sketch2BitsetCacheClearFunction(DataChunk &args, ExpressionState &, Vector &result) {
	D_ASSERT(args.ColumnCount() == 0);
	bool out_of_memory = false;
	const char *error_message = nullptr;
	if (sk_bitset_cache_clear(&out_of_memory, &error_message) != 0) {
		if (out_of_memory) {
			throw OutOfMemoryException("sketch2_bitset_cache_clear: out of memory");
		}
		throw InvalidInputException("sketch2_bitset_cache_clear: %s",
		                            error_message ? error_message : "failed to clear bitset cache");
	}
	result.SetValue(0, Value::BOOLEAN(true));
}

void RegisterSketch2BitsetFilterFunction(ExtensionLoader &loader) {
	auto function = AggregateFunction::BinaryAggregate<BitsetFilterState, int64_t, string_t, string_t,
	                                                   BitsetFilterOperation, AggregateDestructorType::LEGACY>(
	    LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::VARCHAR);
	function.name = "sketch2_bitset_filter";
	function.stability = FunctionStability::VOLATILE;
	function.SetBindCallback(Sketch2BitsetFilterBind);
	function.SetStateDestructorCallback(AggregateFunction::StateDestroy<BitsetFilterState, BitsetFilterOperation>);
	loader.RegisterFunction(function);

	auto load_function = ScalarFunction("sketch2_bitset_load", {LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                                    Sketch2BitsetLoadFunction);
	load_function.stability = FunctionStability::VOLATILE;
	load_function.SetBindCallback(Sketch2BitsetNameScalarBind);
	loader.RegisterFunction(load_function);

	auto drop_function = ScalarFunction("sketch2_bitset_drop", {LogicalType::VARCHAR}, LogicalType::INTEGER,
	                                    Sketch2BitsetDropFunction);
	drop_function.stability = FunctionStability::VOLATILE;
	drop_function.SetBindCallback(Sketch2BitsetNameScalarBind);
	loader.RegisterFunction(drop_function);

	auto cache_remove_function =
	    ScalarFunction("sketch2_bitset_cache_remove", {LogicalType::VARCHAR}, LogicalType::INTEGER,
	                   Sketch2BitsetCacheRemoveFunction);
	cache_remove_function.stability = FunctionStability::VOLATILE;
	cache_remove_function.SetBindCallback(Sketch2BitsetNameScalarBind);
	loader.RegisterFunction(cache_remove_function);

	auto cache_clear_function =
	    ScalarFunction("sketch2_bitset_cache_clear", {}, LogicalType::BOOLEAN, Sketch2BitsetCacheClearFunction);
	cache_clear_function.stability = FunctionStability::VOLATILE;
	loader.RegisterFunction(cache_clear_function);
}

} // namespace duckdb

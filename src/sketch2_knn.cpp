#include "sketch2_knn.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include "sketch2_dataset.hpp"

#include <cstring>

namespace duckdb {

static constexpr int64_t SKETCH2_KNN_MAX_K = 1000000;

enum class Sketch2KnnQueryFormat : uint8_t {
	String,
	FloatVector
};

struct Sketch2KnnBindData : public FunctionData {
	Sketch2KnnBindData(Sketch2KnnQueryFormat query_format_p, string query_vector_text_p,
	                   shared_ptr<const vector<float>> query_vector_f32_p, uint32_t k_p, string bitset_filter_name_p)
	    : query_format(query_format_p), query_vector_text(std::move(query_vector_text_p)),
	      query_vector_f32(std::move(query_vector_f32_p)), k(k_p), bitset_filter_name(std::move(bitset_filter_name_p)) {
	}

	Sketch2KnnQueryFormat query_format;
	string query_vector_text;
	shared_ptr<const vector<float>> query_vector_f32;
	uint32_t k;
	string bitset_filter_name;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<Sketch2KnnBindData>(query_format, query_vector_text, query_vector_f32, k, bitset_filter_name);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<Sketch2KnnBindData>();
		return query_format == other.query_format && query_vector_text == other.query_vector_text &&
		       *query_vector_f32 == *other.query_vector_f32 && k == other.k &&
		       bitset_filter_name == other.bitset_filter_name;
	}
};

struct Sketch2KnnGlobalState : public GlobalTableFunctionState {
	vector<uint64_t> ids;
	vector<double> scores;
	idx_t offset = 0;
};

struct ScopedBitsetFilter {
	~ScopedBitsetFilter() {
		if (ptr) {
			sk_bitset_delete(ptr);
		}
	}

	void *ptr = nullptr;
};

template <class T>
struct ScopedSketch2Alloc {
	~ScopedSketch2Alloc() {
		sk_free(ptr);
	}

	T *ptr = nullptr;
};

static void Sketch2KnnBindCommon(TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<string> &names,
                                 uint32_t &k_value_out, string &bitset_filter_name_out) {
	if (input.inputs.size() != 3) {
		throw BinderException("sketch2_knn(query_vector, k, bitset_filter_name) expects exactly 3 arguments");
	}
	if (input.inputs[1].IsNull()) {
		throw BinderException("sketch2_knn k must not be NULL");
	}

	const auto k_value = input.inputs[1].GetValue<int64_t>();
	if (k_value <= 0) {
		throw BinderException("sketch2_knn k must be > 0");
	}
	if (k_value > SKETCH2_KNN_MAX_K) {
		throw BinderException("sketch2_knn k must be <= %lld", static_cast<long long>(SKETCH2_KNN_MAX_K));
	}

	string bitset_filter_name;
	if (!input.inputs[2].IsNull()) {
		bitset_filter_name = StringValue::Get(input.inputs[2]);
		if (bitset_filter_name.empty()) {
			throw BinderException("sketch2_knn bitset_filter_name must be non-empty when provided");
		}
	}

	names.emplace_back("id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("score");
	return_types.emplace_back(LogicalType::DOUBLE);

	k_value_out = static_cast<uint32_t>(k_value);
	bitset_filter_name_out = std::move(bitset_filter_name);
}

static unique_ptr<FunctionData> Sketch2KnnBindString(ClientContext &, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs[0].IsNull()) {
		throw BinderException("sketch2_knn query_vector must not be NULL");
	}

	auto query_vector = StringValue::Get(input.inputs[0]);
	if (query_vector.empty()) {
		throw BinderException("sketch2_knn query_vector must be a non-empty string");
	}

	uint32_t k_value;
	string bitset_filter_name;
	Sketch2KnnBindCommon(input, return_types, names, k_value, bitset_filter_name);

	return make_uniq<Sketch2KnnBindData>(Sketch2KnnQueryFormat::String, std::move(query_vector),
	                                     make_shared_ptr<vector<float>>(), k_value, std::move(bitset_filter_name));
}

static vector<float> GetQueryVectorData(const Value &query_vector_value) {
	vector<float> result;
	if (query_vector_value.type().id() == LogicalTypeId::ARRAY) {
		const auto &children = ArrayValue::GetChildren(query_vector_value);
		result.reserve(children.size());
		for (const auto &child : children) {
			result.push_back(child.GetValue<float>());
		}
		return result;
	}

	const auto &children = ListValue::GetChildren(query_vector_value);
	result.reserve(children.size());
	for (const auto &child : children) {
		result.push_back(child.GetValue<float>());
	}
	return result;
}

static unique_ptr<FunctionData> Sketch2KnnBindVector(ClientContext &, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs[0].IsNull()) {
		throw BinderException("sketch2_knn query_vector must not be NULL");
	}

	auto query_vector = GetQueryVectorData(input.inputs[0]);
	if (query_vector.empty()) {
		throw BinderException("sketch2_knn query_vector must be a non-empty float array");
	}

	uint32_t k_value;
	string bitset_filter_name;
	Sketch2KnnBindCommon(input, return_types, names, k_value, bitset_filter_name);

	return make_uniq<Sketch2KnnBindData>(Sketch2KnnQueryFormat::FloatVector, string(),
	                                     make_shared_ptr<vector<float>>(std::move(query_vector)), k_value,
	                                     std::move(bitset_filter_name));
}

static unique_ptr<NodeStatistics> Sketch2KnnCardinality(ClientContext &, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<Sketch2KnnBindData>();
	// The lower bound stays at 0 because an empty dataset or an optional bitset
	// filter can legitimately eliminate every row, even when k > 0.
	return make_uniq<NodeStatistics>(0, bind_data.k);
}

static unique_ptr<GlobalTableFunctionState> Sketch2KnnInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<Sketch2KnnBindData>();
	auto dataset = GetSketch2Dataset(context);
	std::lock_guard<std::mutex> lock(dataset->mutex);
	if (!dataset->handle) {
		throw InvalidInputException(
		    "sketch2_knn requires PRAGMA sketch2_open(database_path, dataset_name) to be called first");
	}

	ScopedBitsetFilter bitset_filter;
	if (!bind_data.bitset_filter_name.empty()) {
		bool out_of_memory = false;
		const char *error_message = nullptr;
		if (sk_bitset_load(bind_data.bitset_filter_name.c_str(), &bitset_filter.ptr, &out_of_memory, &error_message) != 0) {
			if (out_of_memory) {
				throw OutOfMemoryException("sketch2_knn: out of memory");
			}
			throw InvalidInputException("sketch2_knn bitset_filter_name %s was not found: %s",
			                            bind_data.bitset_filter_name.c_str(),
			                            error_message ? error_message : "failed to load bitset filter");
		}
	}

	ScopedSketch2Alloc<uint64_t> ids;
	ScopedSketch2Alloc<double> scores;
	size_t count = 0;
	int ret = 0;
	if (bind_data.query_format == Sketch2KnnQueryFormat::String) {
		ret = sk_knn_items_bitset_filter(dataset->handle, bind_data.query_vector_text.c_str(), bind_data.k, bitset_filter.ptr,
		                                 &ids.ptr, &scores.ptr, &count);
	} else {
		ret = sk_knn_vector_items_bitset_filter(dataset->handle, bind_data.query_vector_f32->data(),
		                                        static_cast<uint64_t>(bind_data.query_vector_f32->size()), bind_data.k,
		                                        bitset_filter.ptr, &ids.ptr, &scores.ptr, &count);
	}
	if (ret != 0) {
		const auto *message_ptr = sk_error_message(dataset->handle);
		const auto message = string(message_ptr ? message_ptr : "unknown error");
		throw InvalidInputException("sketch2_knn failed: %s", message);
	}
	if (count > bind_data.k) {
		throw InvalidInputException("sketch2_knn failed: returned %llu rows for k=%u",
		                            static_cast<unsigned long long>(count), static_cast<unsigned int>(bind_data.k));
	}

	auto result = make_uniq<Sketch2KnnGlobalState>();
	if (count > 0) {
		result->ids.assign(ids.ptr, ids.ptr + count);
		result->scores.assign(scores.ptr, scores.ptr + count);
	}
	return std::move(result);
}

static void Sketch2KnnFunction(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<Sketch2KnnGlobalState>();
	if (state.offset >= state.ids.size()) {
		return;
	}

	const auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, state.ids.size() - state.offset);
	auto ids_data = FlatVector::GetData<uint64_t>(output.data[0]);
	auto scores_data = FlatVector::GetData<double>(output.data[1]);
	std::memcpy(ids_data, state.ids.data() + state.offset, count * sizeof(uint64_t));
	std::memcpy(scores_data, state.scores.data() + state.offset, count * sizeof(double));
	output.SetCardinality(count);
	state.offset += count;
}

void RegisterSketch2KnnFunction(ExtensionLoader &loader) {
	TableFunctionSet sketch2_knn_functions("sketch2_knn");

	auto add = [&](vector<LogicalType> args, table_function_bind_t bind) {
		TableFunction function("sketch2_knn", std::move(args), Sketch2KnnFunction, bind, Sketch2KnnInit);
		function.cardinality = Sketch2KnnCardinality;
		sketch2_knn_functions.AddFunction(std::move(function));
	};

	add({LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::VARCHAR}, Sketch2KnnBindString);
	add({LogicalType::LIST(LogicalType::FLOAT), LogicalType::BIGINT, LogicalType::VARCHAR}, Sketch2KnnBindVector);
	add({LogicalType::LIST(LogicalType::DOUBLE), LogicalType::BIGINT, LogicalType::VARCHAR}, Sketch2KnnBindVector);
	add({LogicalType::ARRAY(LogicalType::FLOAT, optional_idx()), LogicalType::BIGINT, LogicalType::VARCHAR},
	    Sketch2KnnBindVector);
	add({LogicalType::ARRAY(LogicalType::DOUBLE, optional_idx()), LogicalType::BIGINT, LogicalType::VARCHAR},
	    Sketch2KnnBindVector);

	loader.RegisterFunction(sketch2_knn_functions);
}

} // namespace duckdb

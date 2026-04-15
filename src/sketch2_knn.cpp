#include "sketch2_knn.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include "sketch2_dataset.hpp"

#include <limits>

namespace duckdb {

enum class Sketch2KnnQueryFormat : uint8_t {
	String,
	FloatVector
};

struct Sketch2KnnBindData : public FunctionData {
	Sketch2KnnBindData(Sketch2KnnQueryFormat query_format_p, string query_vector_text_p, vector<float> query_vector_f32_p,
	                   uint32_t k_p, bool has_bitset_filter_p, int64_t bitset_filter_handle_p)
	    : query_format(query_format_p), query_vector_text(std::move(query_vector_text_p)),
	      query_vector_f32(std::move(query_vector_f32_p)), k(k_p), has_bitset_filter(has_bitset_filter_p),
	      bitset_filter_handle(bitset_filter_handle_p) {
	}

	Sketch2KnnQueryFormat query_format;
	string query_vector_text;
	vector<float> query_vector_f32;
	uint32_t k;
	bool has_bitset_filter;
	int64_t bitset_filter_handle;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<Sketch2KnnBindData>(query_format, query_vector_text, query_vector_f32, k, has_bitset_filter,
		                                     bitset_filter_handle);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<Sketch2KnnBindData>();
		return query_format == other.query_format && query_vector_text == other.query_vector_text &&
		       query_vector_f32 == other.query_vector_f32 && k == other.k &&
		       has_bitset_filter == other.has_bitset_filter && bitset_filter_handle == other.bitset_filter_handle;
	}
};

struct Sketch2KnnGlobalState : public GlobalTableFunctionState {
	vector<uint64_t> ids;
	vector<double> scores;
	idx_t offset = 0;
};

static void Sketch2KnnBindCommon(TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<string> &names,
                                 uint32_t &k_value_out, bool &has_bitset_filter_out,
                                 int64_t &bitset_filter_handle_out) {
	if (input.inputs.size() != 3) {
		throw BinderException("sketch2_knn(query_vector, k, bitset_filter_ref) expects exactly 3 arguments");
	}
	if (input.inputs[1].IsNull()) {
		throw BinderException("sketch2_knn k must not be NULL");
	}

	const auto k_value = input.inputs[1].GetValue<int64_t>();
	if (k_value <= 0) {
		throw BinderException("sketch2_knn k must be > 0");
	}
	if (k_value > static_cast<int64_t>(std::numeric_limits<uint32_t>::max())) {
		throw BinderException("sketch2_knn k is too large");
	}

	bool has_bitset_filter = false;
	int64_t bitset_filter_handle = 0;
	if (!input.inputs[2].IsNull()) {
		bitset_filter_handle = input.inputs[2].GetValue<int64_t>();
		if (bitset_filter_handle <= 0) {
			throw BinderException("sketch2_knn bitset_filter_ref must be > 0 when provided");
		}
		has_bitset_filter = true;
	}

	names.emplace_back("id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("score");
	return_types.emplace_back(LogicalType::DOUBLE);

	k_value_out = static_cast<uint32_t>(k_value);
	has_bitset_filter_out = has_bitset_filter;
	bitset_filter_handle_out = bitset_filter_handle;
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
	bool has_bitset_filter;
	int64_t bitset_filter_handle;
	Sketch2KnnBindCommon(input, return_types, names, k_value, has_bitset_filter, bitset_filter_handle);

	return make_uniq<Sketch2KnnBindData>(Sketch2KnnQueryFormat::String, std::move(query_vector), vector<float>(), k_value,
	                                     has_bitset_filter, bitset_filter_handle);
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
	bool has_bitset_filter;
	int64_t bitset_filter_handle;
	Sketch2KnnBindCommon(input, return_types, names, k_value, has_bitset_filter, bitset_filter_handle);

	return make_uniq<Sketch2KnnBindData>(Sketch2KnnQueryFormat::FloatVector, string(), std::move(query_vector), k_value,
	                                     has_bitset_filter, bitset_filter_handle);
}

static unique_ptr<NodeStatistics> Sketch2KnnCardinality(ClientContext &, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<Sketch2KnnBindData>();
	// The lower bound stays at 0 because an empty dataset or an optional bitset
	// filter can legitimately eliminate every row, even when k > 0.
	return make_uniq<NodeStatistics>(0, bind_data.k);
}

static unique_ptr<GlobalTableFunctionState> Sketch2KnnInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<Sketch2KnnBindData>();
	auto &dataset = *GetSketch2Dataset(context);
	if (!dataset.handle) {
		throw InvalidInputException("sketch2_knn requires sketch2_open(database_path, dataset_name) to be called first");
	}

	const void *bitset_blob = nullptr;
	size_t bitset_blob_size = 0;
	if (bind_data.has_bitset_filter) {
		auto bitset = GetSketch2Bitset(context, bind_data.bitset_filter_handle);
		if (!bitset) {
			throw InvalidInputException("sketch2_knn bitset_filter_ref %lld was not found",
			                            static_cast<long long>(bind_data.bitset_filter_handle));
		}
		bitset_blob = bitset->blob;
		bitset_blob_size = bitset->blob_size;
	}

	uint64_t *ids = nullptr;
	double *scores = nullptr;
	size_t count = 0;
	int ret = 0;
	if (bind_data.query_format == Sketch2KnnQueryFormat::String) {
		ret = sk_knn_items(dataset.handle, bind_data.query_vector_text.c_str(), bind_data.k, bitset_blob, bitset_blob_size,
		                   &ids, &scores, &count);
	} else {
		ret = sk_knn_vector_items(dataset.handle, bind_data.query_vector_f32.data(),
		                          static_cast<uint64_t>(bind_data.query_vector_f32.size()), bind_data.k, bitset_blob,
		                          bitset_blob_size, &ids, &scores, &count);
	}
	if (ret != 0) {
		const auto *message_ptr = sk_error_message(dataset.handle);
		const auto message = string(message_ptr ? message_ptr : "unknown error");
		// ids/scores start as nullptr, and sk_free delegates to std::free(),
		// so cleanup remains safe even when the C API fails before assigning.
		sk_free(ids);
		sk_free(scores);
		throw InvalidInputException("sketch2_knn failed: %s", message);
	}
	if (count > bind_data.k) {
		sk_free(ids);
		sk_free(scores);
		throw InvalidInputException("sketch2_knn failed: returned %llu rows for k=%u",
		                            static_cast<unsigned long long>(count), static_cast<unsigned int>(bind_data.k));
	}

	auto result = make_uniq<Sketch2KnnGlobalState>();
	if (count > 0) {
		result->ids.assign(ids, ids + count);
		result->scores.assign(scores, scores + count);
	}
	sk_free(ids);
	sk_free(scores);
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
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		const auto source_idx = state.offset + row_idx;
		ids_data[row_idx] = state.ids[source_idx];
		scores_data[row_idx] = state.scores[source_idx];
	}
	output.SetCardinality(count);
	state.offset += count;
}

void RegisterSketch2KnnFunction(ExtensionLoader &loader) {
	TableFunctionSet sketch2_knn_functions("sketch2_knn");

	TableFunction sketch2_knn_string("sketch2_knn", {LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT},
	                                 Sketch2KnnFunction, Sketch2KnnBindString, Sketch2KnnInit);
	sketch2_knn_string.cardinality = Sketch2KnnCardinality;
	sketch2_knn_functions.AddFunction(std::move(sketch2_knn_string));

	TableFunction sketch2_knn_list("sketch2_knn",
	                               {LogicalType::LIST(LogicalType::FLOAT), LogicalType::BIGINT, LogicalType::BIGINT},
	                               Sketch2KnnFunction, Sketch2KnnBindVector, Sketch2KnnInit);
	sketch2_knn_list.cardinality = Sketch2KnnCardinality;
	sketch2_knn_functions.AddFunction(std::move(sketch2_knn_list));

	TableFunction sketch2_knn_double_list(
	    "sketch2_knn", {LogicalType::LIST(LogicalType::DOUBLE), LogicalType::BIGINT, LogicalType::BIGINT},
	    Sketch2KnnFunction, Sketch2KnnBindVector, Sketch2KnnInit);
	sketch2_knn_double_list.cardinality = Sketch2KnnCardinality;
	sketch2_knn_functions.AddFunction(std::move(sketch2_knn_double_list));

	TableFunction sketch2_knn_array(
	    "sketch2_knn", {LogicalType::ARRAY(LogicalType::FLOAT, optional_idx()), LogicalType::BIGINT, LogicalType::BIGINT},
	    Sketch2KnnFunction, Sketch2KnnBindVector, Sketch2KnnInit);
	sketch2_knn_array.cardinality = Sketch2KnnCardinality;
	sketch2_knn_functions.AddFunction(std::move(sketch2_knn_array));

	TableFunction sketch2_knn_double_array(
	    "sketch2_knn", {LogicalType::ARRAY(LogicalType::DOUBLE, optional_idx()), LogicalType::BIGINT, LogicalType::BIGINT},
	    Sketch2KnnFunction, Sketch2KnnBindVector, Sketch2KnnInit);
	sketch2_knn_double_array.cardinality = Sketch2KnnCardinality;
	sketch2_knn_functions.AddFunction(std::move(sketch2_knn_double_array));

	loader.RegisterFunction(sketch2_knn_functions);
}

} // namespace duckdb

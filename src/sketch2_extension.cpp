#define DUCKDB_EXTENSION_MAIN

#include "sketch2_bitset_filter.hpp"
#include "sketch2_extension.hpp"
#include "sketch2_open.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include "sketch2_dataset.hpp"
#include "sketch2.h"

namespace duckdb {

inline string_t get_sketch2_info(ClientContext &context, string_t arg) {
	auto dataset = GetSketch2Dataset(context);

	if (arg.Empty() || arg == "version") {
		memset(dataset->version, 0, sizeof(dataset->version));
		sk_version(dataset->version, sizeof(dataset->version));
		return dataset->version;
	}

	if (arg == "dataset") {
		return dataset->dataset_name;
	}

	return "Unknown request";
}

inline void Sketch2ScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &arg_vector = args.data[0];
	auto &context = state.GetContext();
	UnaryExecutor::Execute<string_t, string_t>(arg_vector, result, args.size(), [&](string_t arg) {
		const auto value = get_sketch2_info(context, arg);
		return StringVector::AddString(result, value);
	});
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register a scalar function
	auto sketch2_scalar_function = ScalarFunction("sketch2", {LogicalType::VARCHAR}, LogicalType::VARCHAR, Sketch2ScalarFun);
	loader.RegisterFunction(sketch2_scalar_function);
	RegisterSketch2BitsetFilterFunction(loader);
	RegisterSketch2OpenFunction(loader);
}

void Sketch2Extension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string Sketch2Extension::Name() {
	return "sketch2";
}

std::string Sketch2Extension::Version() const {
#ifdef EXT_VERSION_SKETCH2
	return EXT_VERSION_SKETCH2;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(sketch2, loader) {
	duckdb::LoadInternal(loader);
}
}

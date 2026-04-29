#define DUCKDB_EXTENSION_MAIN

#include "sketch2_bitset_filter.hpp"
#include "sketch2_close.hpp"
#include "sketch2_extension.hpp"
#include "sketch2_knn.hpp"
#include "sketch2_open.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include "sketch2_dataset.hpp"
#include "sketch2.h"

namespace duckdb {

inline std::string GetSketch2Version() {
	char version[64] = {};
	sk_version(version, sizeof(version));
	return std::string(version);
}

inline std::string GetSketch2DatasetName(ClientContext &context) {
	auto dataset = GetSketch2Dataset(context);
	std::lock_guard<std::mutex> lock(dataset->mutex);
	if (!dataset->handle) {
		throw InvalidInputException("sketch2_dataset(): no dataset is open on this connection");
	}
	return dataset->dataset_name;
}

inline void Sketch2VersionScalarFun(DataChunk &args, ExpressionState &, Vector &result) {
	D_ASSERT(args.size() == 1);
	result.SetValue(0, Value(GetSketch2Version()));
}

inline void Sketch2DatasetScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.size() == 1);
	result.SetValue(0, Value(GetSketch2DatasetName(state.GetContext())));
}

static void LoadInternal(ExtensionLoader &loader) {
	auto sketch2_version_function = ScalarFunction("sketch2_version", {}, LogicalType::VARCHAR, Sketch2VersionScalarFun);
	loader.RegisterFunction(sketch2_version_function);
	auto sketch2_dataset_function = ScalarFunction("sketch2_dataset", {}, LogicalType::VARCHAR, Sketch2DatasetScalarFun);
	loader.RegisterFunction(sketch2_dataset_function);
	RegisterSketch2BitsetFilterFunction(loader);
	RegisterSketch2CloseFunction(loader);
	RegisterSketch2KnnFunction(loader);
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

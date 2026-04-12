#define DUCKDB_EXTENSION_MAIN

#include "sketch2_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include "sketch2.h"

namespace duckdb {

inline void Sketch2ScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	char version[64];
	memset(version, 0, sizeof(version));
	sk_version(version, sizeof(version));

	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		// return StringVector::AddString(result, "Sketch2 " + name.GetString() + " 🐥");
		return StringVector::AddString(result, "Sketch2 " + std::string(version));
	});
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register a scalar function
	auto sketch2_scalar_function = ScalarFunction("sketch2", {LogicalType::VARCHAR}, LogicalType::VARCHAR, Sketch2ScalarFun);
	loader.RegisterFunction(sketch2_scalar_function);
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

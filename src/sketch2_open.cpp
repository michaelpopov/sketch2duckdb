#include "duckdb.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include "sketch2_dataset.hpp"
#include "sketch2_open.hpp"

namespace duckdb {

static void Sketch2OpenImpl(ClientContext &context, const string &database_path, const string &dataset_name) {
	auto dataset = GetSketch2Dataset(context);

	sk_handle_t *handle = sk_new_handle(database_path.c_str());
	if (handle == nullptr) {
		throw InvalidInputException("sketch2_open failed: handle");
	}

	int ret = sk_open(handle, dataset_name.c_str());
	if (ret != 0) {
		const std::string msg = sk_error_message(handle);
		sk_release_handle(handle);
		throw InvalidInputException("sketch2_open failed: %s", msg);
	}

	{
		std::lock_guard<std::mutex> lock(dataset->mutex);
		if (dataset->handle) {
			sk_release_handle(dataset->handle);
		}

		dataset->dataset_name = dataset_name;
		dataset->handle = handle;
	}
}

static void Sketch2OpenPragma(ClientContext &context, const FunctionParameters &parameters) {
	D_ASSERT(parameters.values.size() == 2);
	Sketch2OpenImpl(context, StringValue::Get(parameters.values[0]), StringValue::Get(parameters.values[1]));
}

void RegisterSketch2OpenFunction(ExtensionLoader &loader) {
	auto sketch2_open = PragmaFunction::PragmaCall("sketch2_open", Sketch2OpenPragma,
	                                               {LogicalType::VARCHAR, LogicalType::VARCHAR});
	loader.RegisterFunction(sketch2_open);
}

} // namespace duckdb

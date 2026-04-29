#include "duckdb.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include "sketch2_close.hpp"
#include "sketch2_dataset.hpp"

namespace duckdb {

static void Sketch2ClosePragma(ClientContext &context, const FunctionParameters &parameters) {
	D_ASSERT(parameters.values.empty());
	auto dataset = GetSketch2Dataset(context);

	{
		std::lock_guard<std::mutex> lock(dataset->mutex);
		if (dataset->handle) {
			auto *handle = dataset->handle;
			dataset->handle = nullptr;
			dataset->dataset_name.clear();

			const auto close_result = sk_close(handle);
			const auto error_message = string(sk_error_message(handle) ? sk_error_message(handle) : "unknown error");
			sk_release_handle(handle);
			if (close_result != 0) {
				throw InvalidInputException("sketch2_close failed: %s", error_message);
			}
		}
	}
}

void RegisterSketch2CloseFunction(ExtensionLoader &loader) {
	auto close_function = PragmaFunction::PragmaStatement("sketch2_close", Sketch2ClosePragma);
	loader.RegisterFunction(close_function);
}

} // namespace duckdb

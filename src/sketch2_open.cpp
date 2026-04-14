#include "duckdb.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include "sketch2_dataset.hpp"
#include "sketch2_open.hpp"

namespace duckdb {

struct Sketch2OpenBindData : public FunctionData {
	explicit Sketch2OpenBindData(string database_path_p, string dataset_name_p)
	    : database_path(std::move(database_path_p)), dataset_name(std::move(dataset_name_p)) {
	}

	string database_path;
	string dataset_name;
	bool finished = false;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<Sketch2OpenBindData>(database_path, dataset_name);
		result->finished = finished;
		return result;
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<Sketch2OpenBindData>();
		return database_path == other.database_path && dataset_name == other.dataset_name;
	}
};

static unique_ptr<FunctionData> Sketch2OpenBind(ClientContext &, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.size() != 2) {
		throw BinderException("sketch2_open(database_path, dataset_name) expects exactly 2 arguments");
	}
	for (idx_t i = 0; i < input.inputs.size(); i++) {
		if (input.inputs[i].IsNull()) {
			throw BinderException("sketch2_open does not accept NULL arguments");
		}
	}

	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");

	return make_uniq<Sketch2OpenBindData>(StringValue::Get(input.inputs[0]), StringValue::Get(input.inputs[1]));
}

static unique_ptr<NodeStatistics> Sketch2OpenCardinality(ClientContext &, const FunctionData *) {
	return make_uniq<NodeStatistics>(1, 1);
}

static void Sketch2OpenFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->CastNoConst<Sketch2OpenBindData>();
	if (bind_data.finished) {
		return;
	}

	const std::string database_path = bind_data.database_path;
	const std::string dataset_name = bind_data.dataset_name;
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

	dataset->database_path = database_path;
	dataset->dataset_name = dataset_name;

	if (dataset->handle) {
		sk_release_handle(dataset->handle);
	}

	dataset->handle = handle;

	output.SetValue(0, 0, Value::BOOLEAN(true));
	output.SetCardinality(1);
	bind_data.finished = true;
}

void RegisterSketch2OpenFunction(ExtensionLoader &loader) {
	TableFunction sketch2_open_function("sketch2_open", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                    Sketch2OpenFunction, Sketch2OpenBind);
	sketch2_open_function.cardinality = Sketch2OpenCardinality;
	loader.RegisterFunction(sketch2_open_function);
}

} // namespace duckdb

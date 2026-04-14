#pragma once

namespace duckdb {

class ExtensionLoader;

void RegisterSketch2KnnFunction(ExtensionLoader &loader);

} // namespace duckdb

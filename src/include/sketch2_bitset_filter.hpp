#pragma once

namespace duckdb {

class ExtensionLoader;

void RegisterSketch2BitsetFilterFunction(ExtensionLoader &loader);

} // namespace duckdb

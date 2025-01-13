#define DUCKDB_EXTENSION_MAIN

#include "quack_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"

#include "get_entropy.cpp"
#include "lift.cpp"
#include "sum.cpp"
#include "sum_no_lift.cpp"

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

void registerLiftFunction(DuckDB &db) {
	auto liftFunc = ScalarFunction(
		"lift",
		{duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR)},
		duckdb::LogicalType::LIST(duckdb::LogicalType::UBIGINT),
		lift::liftFunction
	);
	ExtensionUtil::RegisterFunction(*db.instance, liftFunc);
}

void registerSumDictFunction(DuckDB &db) {
	auto arg_types = duckdb::vector<duckdb::LogicalType>{
		duckdb::LogicalType::LIST(duckdb::LogicalType::UBIGINT)
	};

	auto sumDictFunc = duckdb::AggregateFunction(
		"custom_sum",
		arg_types,
		duckdb::LogicalType::LIST(
			duckdb::LogicalType::MAP(duckdb::LogicalType::UBIGINT, duckdb::LogicalType::INTEGER)
		),
		duckdb::AggregateFunction::StateSize<customSum::CustomSumState>,
		duckdb::AggregateFunction::StateInitialize<customSum::CustomSumState, customSum::CustomSumFunction>,
		customSum::customSumUpdate,
		customSum::customSumCombine,
		customSum::customSumFinalize,
		nullptr,
		customSum::customSumBind,
		duckdb::AggregateFunction::StateDestroy<customSum::CustomSumState, customSum::CustomSumFunction>
	);

	ExtensionUtil::RegisterFunction(*db.instance, sumDictFunc);
}

void registerSumNoLiftFunction(DuckDB &db) {
	auto arg_types = {duckdb::LogicalType::LIST(duckdb::LogicalType::ANY)};

	auto sumNoLiftFunc = duckdb::AggregateFunction(
		"sum_no_lift",
		arg_types,
		duckdb::LogicalType::LIST(
			duckdb::LogicalType::MAP(duckdb::LogicalType::UBIGINT, duckdb::LogicalType::INTEGER)
		),
		duckdb::AggregateFunction::StateSize<sumNoLift::SumNoLiftState>,
		duckdb::AggregateFunction::StateInitialize<sumNoLift::SumNoLiftState, sumNoLift::SumNoLiftFunction>,
		sumNoLift::sumNoLiftUpdate,
		sumNoLift::sumNoLiftCombine,
		sumNoLift::sumNoLiftFinalize,
		nullptr,
		sumNoLift::sumNoLiftBind,
		duckdb::AggregateFunction::StateDestroy<sumNoLift::SumNoLiftState, sumNoLift::SumNoLiftFunction>
	);

	ExtensionUtil::RegisterFunction(*db.instance, sumNoLiftFunc);
}

void registerGetEntropyFunction(DuckDB &db) {
	auto getEntropyFunc = ScalarFunction(
		"get_entropy",
		{duckdb::LogicalType::LIST(
			duckdb::LogicalType::MAP(duckdb::LogicalType::UBIGINT, duckdb::LogicalType::INTEGER))},
		duckdb::LogicalType::MAP(duckdb::LogicalType::VARCHAR, duckdb::LogicalType::DOUBLE),
		getEntropy::getEntropyFunction
	);

	ExtensionUtil::RegisterFunction(*db.instance, getEntropyFunc);
}

void QuackExtension::Load(DuckDB &db) {
	registerLiftFunction(db);
	registerSumDictFunction(db);
	registerSumNoLiftFunction(db);
	registerGetEntropyFunction(db);
}

std::string QuackExtension::Name() {
	return "quack";
}

std::string QuackExtension::Version() const {
#ifdef EXT_VERSION_QUACK
	return EXT_VERSION_QUACK;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void quack_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::QuackExtension>();
}

DUCKDB_EXTENSION_API const char *quack_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif

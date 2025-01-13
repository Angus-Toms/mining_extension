#include "duckdb.hpp"

#include <map>
#include <vector>

namespace customSum {

using hash_t = uint64_t;

struct CustomSumState {
    std::vector<std::map<hash_t, int64_t>> maps;
};

struct CustomSumFunction {
    template <class STATE>
    static void Initialize(STATE &state) {
        state.maps = {};
    }

    template <class STATE>
    static void Destroy(STATE &state, duckdb::AggregateInputData &aggr_input_data) {
        return;
    }

    static bool IgnoreNull() {
        return true;
    }
};

static void customSumUpdate(duckdb::Vector inputs[], duckdb::AggregateInputData &, idx_t inputCount, duckdb::Vector &stateVector, idx_t count) {
    auto &tuples = inputs[0];
    duckdb::UnifiedVectorFormat tuplesData;
    duckdb::UnifiedVectorFormat sdata;

    tuples.ToUnifiedFormat(count, tuplesData);
    stateVector.ToUnifiedFormat(count, sdata);
    auto states = (CustomSumState **)sdata.data;

    // Assuming no GROUP BY clause is passed, therefore single state 
    auto &state = *states[sdata.sel->get_index(0)];

    // Create empty maps for all combinations
    auto mapCount = duckdb::ListValue::GetChildren(tuples.GetValue(0)).size();
    state.maps = std::vector<std::map<hash_t, int64_t>>(mapCount);

    for (idx_t i = 0; i < count; i++) { 
        // Iterate through rows 
        auto combinations = duckdb::ListValue::GetChildren(tuples.GetValue(i));
        for (int j = 0; j < mapCount; j++) {
            state.maps[j][combinations[j].GetValue<hash_t>()]++;
        }
    } 
}

static void customSumCombine(duckdb::Vector &stateVector, duckdb::Vector &combined, duckdb::AggregateInputData &, idx_t count) {    
    duckdb::UnifiedVectorFormat sdata;
    stateVector.ToUnifiedFormat(count, sdata);
    auto statePtr = (CustomSumState **)sdata.data;
    auto combinedPtr = duckdb::FlatVector::GetData<CustomSumState *>(combined);

    for (idx_t i = 0; i < count; i++) {
        auto &state = *statePtr[sdata.sel->get_index(i)];

        if (state.maps.empty()) {
            // State maps not initialized, skip
            continue;
        }

        if (combinedPtr[i]->maps.empty()) {
            // Combined maps not initialized, copy state maps
            combinedPtr[i]->maps = state.maps;
        } else {
            // Both state and combined maps are initialized, combine
            // Assume that maps are in the same order
            for (idx_t j = 0; j < state.maps.size(); j++) {
                for (const auto& [k, v] : state.maps[j]) {
                    combinedPtr[i]->maps[j][k] += v;
                }
            }
        }
    }
}

static void customSumFinalize(duckdb::Vector &stateVector, duckdb::AggregateInputData &, duckdb::Vector &result, idx_t count, idx_t offset) {    
    duckdb::UnifiedVectorFormat sdata;
    stateVector.ToUnifiedFormat(count, sdata);
    auto states = (CustomSumState **)sdata.data;
    auto &mask = duckdb::FlatVector::Validity(result);
    auto oldLen = duckdb::ListVector::GetListSize(result);

    // Assuming no GROUP BY clause is passed, therefore single state
    auto &state = *states[sdata.sel->get_index(0)];

    // Create result list 
    for (const auto& map : state.maps) {
        auto mapSize = map.size();
        duckdb::vector<duckdb::Value> keys(mapSize);
        duckdb::vector<duckdb::Value> values(mapSize);
        
        idx_t index = 0;
        for (const auto& [k, v] : map) {
            keys[index] = duckdb::Value::UBIGINT(k);
            values[index] = duckdb::Value::INTEGER(v);
            index++;
        }

        duckdb::Value combination_map = duckdb::Value::MAP(
            duckdb::LogicalType::UBIGINT, // key type 
            duckdb::LogicalType::INTEGER, // value type
            keys,
            values
        );
        duckdb::ListVector::PushBack(result, combination_map);
    }

    // Set result list size
    auto resultData = duckdb::ListVector::GetData(result);
    resultData[offset].length = duckdb::ListVector::GetListSize(result) - oldLen;
    resultData[offset].offset = oldLen;
    oldLen += resultData[offset].length;

    // Output will always be a single row?
    result.Verify(1);
}

duckdb::unique_ptr<duckdb::FunctionData> customSumBind(duckdb::ClientContext &context, duckdb::AggregateFunction &function, duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &arguments) {
    auto resType = duckdb::LogicalType::LIST(duckdb::LogicalType::MAP(duckdb::LogicalType::UBIGINT, duckdb::LogicalType::INTEGER));
    function.return_type = resType;
    return duckdb::make_uniq<duckdb::VariableReturnBindData>(function.return_type);
}

} // namespace customSum
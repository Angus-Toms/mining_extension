#include "duckdb.hpp"

#include <vector>
#include <string>
#include <functional>
#include <iostream>

namespace lift_exact {

using hash_t = uint64_t;

hash_t combineHashes(hash_t a, hash_t b) {
    return a ^ (b + 0x9e3779b9 + (a << 6) + (a >> 2));
}

duckdb::vector<duckdb::Value> getIndexCombinations(const std::vector<int>& indeces, int limit) {
    duckdb::vector<duckdb::Value> result;
    std::vector<int> stack;

    // Recursive lambda function to generate combinations of exactly 'limit' elements
    std::function<void(int, int)> generateCombinations = [&](int start, int depth) {
        if (depth == limit) {
            duckdb::vector<duckdb::Value> currCombination;
            for (const auto& idx : stack) {
                currCombination.push_back(duckdb::Value::INTEGER(idx));
            }
            result.push_back(duckdb::Value::LIST(currCombination));
            return;
        }

        for (int i = start; i <= indeces.size() - (limit - depth); i++) {
            stack.push_back(indeces[i]);
            generateCombinations(i + 1, depth + 1);
            stack.pop_back();
        }
    };

    generateCombinations(0, 0);
    return result;
}

duckdb::vector<duckdb::Value> getHashCombinations(const duckdb::vector<duckdb::Value>& values, int limit) {
    duckdb::vector<duckdb::Value> result;
    std::vector<hash_t> combination;

    // Recursive lambda function to generate combinations of exactly 'limit' elements
    std::function<void(int, int)> generateCombinations = [&](int start, int depth) {
        if (depth == limit) {
            hash_t hash = 0;
            for (const auto& h : combination) {
                hash = combineHashes(hash, h);
            }
            result.push_back(duckdb::Value::UBIGINT(hash));
            return;
        }

        for (int i = start; i <= values.size() - (limit - depth); i++) {
            combination.push_back(values[i].Hash());
            generateCombinations(i + 1, depth + 1);
            combination.pop_back();
        }
    };

    generateCombinations(0, 0);
    return result;
}

// std::vector<std::string> getStringCombinations(const duckdb::vector<duckdb::Value>& values, int limit) {
//     std::vector<std::string> result;
//     std::vector<std::string> combination;

//     std::function<void(int, int)> generateCombinations = [&](int start, int depth) {
//         if (depth == limit) {
//             std::string concatenated;
//             for (const auto& str : combination) {
//                 concatenated += str;
//             }
//             result.push_back(concatenated);
//             return;
//         }
//         for (int i = start; i <= values.size() - (limit - depth); i++) {
//             combination.push_back(values[i].ToString());
//             generateCombinations(i + 1, depth + 1);
//             combination.pop_back();
//         }
//     };

//     generateCombinations(0, 0);
//     return result;
// }

void liftExactFunction(duckdb::DataChunk &args, duckdb::ExpressionState &state, duckdb::Vector &result) {
    auto count = args.size();
    auto& atts = args.data[0]; // All cols wrapped in list
    int limit = args.data[1].GetValue(0).GetValue<int>();
    int attrCount = duckdb::ListValue::GetChildren(atts.GetValue(0)).size();
    duckdb::ListVector::Reserve(result, count);

    // Get attribute indeces
    std::vector<int> indeces(attrCount);
    for (int i = 0; i < attrCount; i++) {
        std::string attrName = state.child_states[0]->child_states[i]->expr.GetName();
        indeces[i] = std::stoi(attrName.substr(3));
    }

    // Generate limited result keys (attr combinations) - same for all tuples
    duckdb::vector<duckdb::Value> keys = getIndexCombinations(indeces, limit);

    for (int i = 0; i < count; i++) {
        auto tuple = duckdb::ListValue::GetChildren(atts.GetValue(i));
        duckdb::vector<duckdb::Value> values = getHashCombinations(tuple, limit);
        duckdb::Value tupleMap = duckdb::Value::MAP(
            duckdb::LogicalType::LIST(duckdb::LogicalType::UBIGINT), // key: attr set
            duckdb::LogicalType::UBIGINT, // value: hash
            keys,
            values
        );
        result.SetValue(i, tupleMap);
    }
}


} // namespace lift_exact
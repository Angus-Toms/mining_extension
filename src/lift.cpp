#include "duckdb.hpp"

#include <vector>
#include <string>
#include <functional>
#include <iostream>

namespace lift {

using hash_t = uint64_t;

hash_t combineHashes(hash_t a, hash_t b) {
    return a ^ (b + 0x9e3779b9 + (a << 6) + (a >> 2));
}

std::vector<std::string> getStringCombinations(const duckdb::vector<duckdb::Value>& values) {
    std::vector<std::string> result;
    std::vector<std::string> stack;

    std::function<void(int)> generateCombinations = [&](int start) {
        for (int i = start; i < values.size(); i++) {
            stack.push_back(values[i].ToString());
            result.push_back("");
            for (const auto& str : stack) {
                result.back() += str;
            }
            generateCombinations(i + 1);
            stack.pop_back();
        }
    };

    generateCombinations(0);
    return result;
}

duckdb::vector<duckdb::Value> getHashCombinations(const duckdb::vector<duckdb::Value>& values) {
    duckdb::vector<duckdb::Value> result;
    std::vector<hash_t> stack;

    std::function<void(int)> generateCombinations = [&](int start) {
        for (int i = start; i < values.size(); i++) {
            stack.push_back(values[i].Hash());
            hash_t hash = 0;
            for (const auto& h : stack) {
                hash = combineHashes(hash, h);
            }
            result.push_back(duckdb::Value::UBIGINT(hash));
            generateCombinations(i + 1);
            stack.pop_back();
        }
    };
    generateCombinations(0);
    return result;
}

void liftFunction(duckdb::DataChunk &args, duckdb::ExpressionState &state, duckdb::Vector &result) {
    auto count = args.size();
    auto& inData = args.data[0]; // All cols wrapped in list
    duckdb::ListVector::Reserve(result, count);

    for (int i = 0; i < count; i++) {
        std::cout << i << "\n";
        auto tuple = duckdb::ListValue::GetChildren(inData.GetValue(i));
        duckdb::vector<duckdb::Value> hashList = getHashCombinations(tuple);

        result.SetValue(i, duckdb::Value::LIST(hashList));
    }
}

} // namespace lift
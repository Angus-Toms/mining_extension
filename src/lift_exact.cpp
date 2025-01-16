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

duckdb::vector<duckdb::Value> getHashCombinations(const duckdb::vector<duckdb::Value>& values, int count) {
    duckdb::vector<duckdb::Value> result;
    std::vector<hash_t> combination;

    // Recursive lambda function to generate combinations of exactly 'count' elements
    std::function<void(int, int)> generateCombinations = [&](int start, int depth) {
        if (depth == count) {
            hash_t hash = 0;
            for (const auto& h : combination) {
                hash = combineHashes(hash, h);
            }
            result.push_back(duckdb::Value::UBIGINT(hash));
            return;
        }

        for (int i = start; i <= values.size() - (count - depth); i++) {
            combination.push_back(values[i].Hash());
            generateCombinations(i + 1, depth + 1);
            combination.pop_back();
        }
    };

    generateCombinations(0, 0);
    return result;
}

std::vector<std::string> getStringCombinations(const duckdb::vector<duckdb::Value>& values, int count) {
    std::vector<std::string> result;
    std::vector<std::string> combination;

    std::function<void(int, int)> generateCombinations = [&](int start, int depth) {
        if (depth == count) {
            std::string concatenated;
            for (const auto& str : combination) {
                concatenated += str;
            }
            result.push_back(concatenated);
            return;
        }
        for (int i = start; i <= values.size() - (count - depth); i++) {
            combination.push_back(values[i].ToString());
            generateCombinations(i + 1, depth + 1);
            combination.pop_back();
        }
    };

    generateCombinations(0, 0);
    return result;
}

void liftExactFunction(duckdb::DataChunk &args, duckdb::ExpressionState &state, duckdb::Vector &result) {
    auto rows = args.size();
    auto& atts = args.data[0]; // All cols wrapped in list
    int count = args.data[1].GetValue(0).GetValue<int>();

    duckdb::ListVector::Reserve(result, rows);
    for (int i = 0; i < rows; i++) {
        auto tuple = duckdb::ListValue::GetChildren(atts.GetValue(i));
        for (const auto& str : getStringCombinations(tuple, count)) {
            std::cout << str << " ";
        }
        std::cout << "\n";
        duckdb::vector<duckdb::Value> hashList = getHashCombinations(tuple, count);

        result.SetValue(i, duckdb::Value::LIST(hashList));
    }
}


} // namespace lift_exact
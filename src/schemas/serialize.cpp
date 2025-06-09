#include <flatbuffers/flatbuffers.h>

#include "schemas/serialize.hpp"
#include "schemas/vector_generated.h"

std::vector<uint8_t> schemas::serialize(const std::span<const double_t> & data) {
	flatbuffers::FlatBufferBuilder builder { sizeof(decltype(data)) + sizeof(double_t) * data.size() };
	const auto result_offset = builder.CreateVector(data.data(), data.size());

	builder.Finish(CreateVector(builder, result_offset));
	return { builder.GetBufferPointer(), builder.GetBufferPointer() + builder.GetSize() };
}

std::vector<double_t> schemas::deserialize(const void * data) {
	const auto vec = GetVector(data);
	return { vec->data()->begin(), vec->data()->end() };
}

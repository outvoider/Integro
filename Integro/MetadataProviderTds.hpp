#pragma once

#include <sstream>
#include <chrono>

#include "Mave.hpp"
#include "ConversionMilliseconds.hpp"

namespace Integro
{
	using namespace std;

	class MetadataProviderTds
	{
		string timeAttribute;

	public:
		typedef milliseconds Time;

		MetadataProviderTds(const string &timeAttribute)
			: timeAttribute(timeAttribute)
		{
		}

		milliseconds GetTime(const Mave &data)
		{
			return data.AsMap().count(timeAttribute) == 0 ? milliseconds::zero() : ConversionMilliseconds::FromUtc(data[timeAttribute].AsString());
		}
	};
}
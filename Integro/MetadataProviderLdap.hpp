#pragma once

#include <sstream>
#include <chrono>

#include "Mave.hpp"
#include "ConversionMilliseconds.hpp"

namespace Integro
{
	using namespace std;

	class MetadataProviderLdap
	{
		string timeAttribute;

	public:
		typedef milliseconds Time;

		MetadataProviderLdap(const string &timeAttribute)
			: timeAttribute(timeAttribute)
		{
		}

		milliseconds GetTime(const Mave &data)
		{
			return ConversionMilliseconds::FromLdapTime(data[timeAttribute].AsString());
		}
	};
}
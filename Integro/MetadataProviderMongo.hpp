#pragma once

#include <sstream>
#include <chrono>

#include "Mave.hpp"

namespace Integro
{
	using namespace std;

	class MetadataProviderMongo
	{
		string timeAttribute;

	public:
		typedef milliseconds Time;

		MetadataProviderMongo(const string &timeAttribute)
			: timeAttribute(timeAttribute)
		{
		}

		milliseconds GetTime(const Mave &data)
		{
			return data[timeAttribute].AsMilliseconds();
		}
	};
}
#pragma once

#include <sstream>
#include <vector>

#include "ClientElastic.hpp"

namespace Integro
{
	using namespace std;

	class DestinationStoreElastic
	{
		string url;
		string index;
		string type;

	public:
		DestinationStoreElastic(const string &url
			, const string &index
			, const string &type)
			: url(url)
			, index(index)
			, type(type)
		{
		}

		void SaveData(const vector<Mave> &data)
		{
			ClientElastic::Index(data, url, index, type);
		}
	};
}
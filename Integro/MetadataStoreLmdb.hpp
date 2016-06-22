#pragma once

#include <sstream>
#include <chrono>

#include "ClientLmdb.hpp"

namespace Integro
{
	using namespace std;
	using namespace chrono;

	class MetadataStoreLmdb
	{
	protected:
		string path;
		string startTimeKey;

	public:
		MetadataStoreLmdb(const string &path
			, const string &startTimeKey)
			: path(path)
			, startTimeKey(startTimeKey)
		{
		}

		milliseconds LoadStartTime()
		{
			return milliseconds(stoull("0" + ClientLmdb::GetOrDefault(path, startTimeKey)));
		}

		void SaveStartTime(const milliseconds value)
		{
			ClientLmdb::Set(path, startTimeKey, to_string(value.count()));
		}
	};
}

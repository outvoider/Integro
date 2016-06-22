#pragma once

#include <sstream>
#include <chrono>

#include "ClientLmdb.hpp"

#include "MetadataStoreLmdb.hpp"

#include "mongo/client/dbclient.h"

namespace Integro
{
	using namespace std;
	using namespace chrono;

	class MetadataStoreStreamLmdb : public MetadataStoreLmdb
	{
		string startIdKey;

	public:
		MetadataStoreStreamLmdb(const string &path
			, const string &startIdKey
			, const string &startTimeKey)
			: startIdKey(startIdKey)
			, MetadataStoreLmdb(path, startTimeKey)
		{
		}

		mongo::OID LoadStartId()
		{
			auto value = ClientLmdb::GetOrDefault(path, startIdKey);
			return value == "" ? mongo::OID() : mongo::OID(value);
		}

		void SaveStartId(const mongo::OID &value)
		{
			ClientLmdb::Set(path, startIdKey, value.toString());
		}
	};
}

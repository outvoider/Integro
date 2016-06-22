#pragma once

#include <sstream>
#include <vector>

#include "ClientMongo.hpp"

namespace Integro
{
	using namespace std;

	class DestinationStoreMongo
	{
		string url;
		string database;
		string collection;

	public:
		DestinationStoreMongo(const string &url
			, const string &database
			, const string &collection)
			: url(url)
			, database(database)
			, collection(collection)
		{
		}

		void SaveData(const vector<Mave> &data)
		{
			ClientMongo::Upsert(url, database, collection, data);
		}
	};
}
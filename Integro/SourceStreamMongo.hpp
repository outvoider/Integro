#pragma once

#include <sstream>

#include "Mave.hpp"
#include "ClientMongo.hpp"

namespace Integro
{
	using namespace std;

	class SourceStreamMongo
	{
		string url;
		string database;
		string collection;

	public:
		SourceStreamMongo(const string &url
			, const string &database
			, const string &collection)
			: url(url)
			, database(database)
			, collection(collection)
		{
		}

		void LoadData(const mongo::OID &startId, function<void(const Mave&)> OnData)
		{
			ClientMongo::QueryCapped(url, database, collection, startId, OnData);
		}
	};
}
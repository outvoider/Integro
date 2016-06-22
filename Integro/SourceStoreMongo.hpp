#pragma once

#include <sstream>
#include <chrono>

#include "Mave.hpp"
#include "ClientMongo.hpp"

namespace Integro
{
	using namespace std;
	using namespace chrono;

	class SourceStoreMongo
	{
		string url;
		string database;
		string collection;
		string timeAttribute;

	public:
		SourceStoreMongo(const string &url
			, const string &database
			, const string &collection
			, const string &timeAttribute)
			: url(url)
			, database(database)
			, collection(collection)
			, timeAttribute(timeAttribute)
		{
		}

		void LoadData(const milliseconds startTime, function<void(const Mave&)> OnData)
		{
			ClientMongo::Query(url, database, collection, timeAttribute, startTime, milliseconds::zero(), OnData);
		}
	};
}
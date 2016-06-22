#pragma once

#include <sstream>
#include <chrono>
#include <regex>

#include "Mave.hpp"
#include "ClientTds.hpp"
#include "ConversionMilliseconds.hpp"

namespace Integro
{
	using namespace std;
	using namespace chrono;

	class SourceStoreTds
	{
		string host;
		string user;
		string password;
		string database;
		string query;

	public:
		SourceStoreTds(const string &host
			, const string &user
			, const string &password
			, const string &database
			, const string &query)
			: host(host)
			, user(user)
			, password(password)
			, database(database)
			, query(query)
		{
		}

		void LoadData(const milliseconds startTime, function<void(const Mave&)> OnData)
		{
			auto q = regex_replace(query
				, regex("\\$\\(LAST_EXEC_TIME\\)")
				, startTime <= milliseconds(1000)
				? "CONVERT(datetime, '1970-01-01')"
				: "convert(datetime, '" + ConversionMilliseconds::ToUtc(startTime - milliseconds(1000), true) + "')");

			ClientTds::ExecuteQuery(host, user, password, database, q, OnData);
		}
	};
}
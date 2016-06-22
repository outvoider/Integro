#pragma once

#include <sstream>
#include <chrono>

#include "Mave.hpp"
#include "ClientLdap.hpp"

namespace Integro
{
	using namespace std;
	using namespace chrono;

	class SourceStoreLdap
	{
		string host;
		int port;
		string user;
		string password;
		string node;
		string filter;
		string idAttribute;
		string timeAttribute;
		function<void(const string&)> OnError;
		function<void(const string&)> OnEvent;

	public:
		SourceStoreLdap(const string &host
			, const int port
			, const string &user
			, const string &password
			, const string &node
			, const string &filter
			, const string &idAttribute
			, const string &timeAttribute
			, function<void(const string&)> OnError
			, function<void(const string&)> OnEvent)
			: host(host)
			, port(port)
			, user(user)
			, password(password)
			, node(node)
			, filter(filter)
			, idAttribute(idAttribute)
			, timeAttribute(timeAttribute)
			, OnError(OnError)
			, OnEvent(OnEvent)
		{
		}

		void LoadData(const milliseconds startTime, function<void(const Mave&)> OnData)
		{
			ClientLdap::Search(host, port, user, password, node, filter, idAttribute, timeAttribute, startTime, milliseconds::zero(), OnData, OnError, OnEvent);
		}
	};
}
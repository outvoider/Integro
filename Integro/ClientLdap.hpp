#pragma once

#include <sstream>
#include <functional>
#include <memory>
#include <cassert>
#include <algorithm>

#include "LDAPAsynConnection.h"
#include "LDAPSearchResult.h"
#include "LDAPResult.h"

#include "ConversionMilliseconds.hpp"
#include "ConversionMave.hpp"

namespace Integro
{
	using namespace std;
	using namespace chrono;
	using namespace json11;

	class ClientLdap
	{
		static int SearchSome(
			const string &host
			, const int port
			, const string &user
			, const string &password
			, const string &node
			, const string &filter
			, function<void(const Mave&)> OnEntry)
		{
			auto result = LDAPResult::SUCCESS;
			LDAPAsynConnection connection(host, port);
			unique_ptr<LDAPMessageQueue>(connection.bind(user, password))->getNext();

			auto queue = unique_ptr<LDAPMessageQueue>(connection.search(
				node
				//, LDAPAsynConnection::SEARCH_BASE
				//, LDAPAsynConnection::SEARCH_ONE
				, LDAPAsynConnection::SEARCH_SUB
				, filter));

			for (auto cont = true; cont;)
			{
				auto message = unique_ptr<LDAPMsg>(queue->getNext());
				auto type = message->getMessageType();

				switch (type)
				{
					case LDAPMsg::SEARCH_ENTRY:
						OnEntry(ConversionMave::FromLdapEntry(*((LDAPSearchResult*)message.get())->getEntry()));
						break;
					case LDAPMsg::SEARCH_REFERENCE:
						break;
					default:
						result = ((LDAPResult*)message.get())->getResultCode();
						cont = false;
						break;
				}
			}

			return result;
		}

	public:
		static void Search(
			const string &host
			, const int port
			, const string &user
			, const string &password
			, const string &node
			, const string &filter
			, const string &idAttribute
			, const string &timeAttribute
			, const milliseconds lowerBound
			, const milliseconds upperBound
			, function<void(const Mave&)> OnEntries
			, function<void(const string&)> OnError
			, function<void(const string&)> OnEvent)
		{
			vector<Mave> entries;
			stack<pair<milliseconds, milliseconds>> intervals; intervals.push({ lowerBound, upperBound });
			stringstream s;

			while (intervals.size() > 0)
			{
				auto i = intervals.top(); intervals.pop();

				assert(i.second <= milliseconds::zero() || i.first <= i.second);

				s.str("");
				s << "(&";
				s << filter;
				s << "(" << idAttribute << "=*)";
				s << "(" << timeAttribute << "=*)";
				if (i.first > milliseconds::zero()) s << "(" << timeAttribute << ">=" << ConversionMilliseconds::ToLdapTime(i.first) << ")";
				if (i.second > milliseconds::zero()) s << "(" << timeAttribute << "<=" << ConversionMilliseconds::ToLdapTime(i.second) << ")";
				s << ")";
				auto newFilter = s.str();

				s.str("");
				auto utcLower = i.first > milliseconds::zero() ? ConversionMilliseconds::ToUtc(i.first) : "";
				auto utcUpper = i.second > milliseconds::zero() ? ConversionMilliseconds::ToUtc(i.second) : "";
				s << "ClientLdap::Search(): searching [" << utcLower << ", " << utcUpper << "]";
				OnEvent(s.str());

				entries.clear();
				auto result = SearchSome(host, port, user, password, node, newFilter, [&](const Mave &entry)
				{
					entries.push_back(entry);
					auto &value = entries.back()[timeAttribute];
					value = ConversionMilliseconds::FromLdapTime(value.AsString());
				});

				sort(entries.begin(), entries.end(), [&](const Mave &left, const Mave &right)
				{
					return left[timeAttribute].AsMilliseconds() < right[timeAttribute].AsMilliseconds();
				});

				if (result == LDAPResult::SUCCESS)
				{
					for (auto &entry : entries)
					{
						entry[timeAttribute] = ConversionMilliseconds::ToLdapTime(entry[timeAttribute].AsMilliseconds());
						OnEntries(entry);
					}
				}
				else if (entries.size() > 0
					&& (result == LDAPResult::SIZE_LIMIT_EXCEEDED
					|| result == LDAPResult::TIME_LIMIT_EXCEEDED))
				{
					auto lastTime = entries.back()[timeAttribute].AsMilliseconds();

					for (auto e = entries.rbegin() + entries.size() / 2; e != entries.rend(); ++e)
					{
						auto time = (*e)[timeAttribute].AsMilliseconds();

						if (time != lastTime)
						{
							intervals.push({ time, i.second });
							intervals.push({ i.first, time });
							result = LDAPResult::SUCCESS;
							break;
						}
					}
				}

				if (result != LDAPResult::SUCCESS)
				{
					s.str("");
					auto utcLower = i.first > milliseconds::zero() ? ConversionMilliseconds::ToUtc(i.first) : "";
					auto utcUpper = i.second > milliseconds::zero() ? ConversionMilliseconds::ToUtc(i.second) : "";
					s << "ClientLdap::Search(): failed at [" << utcLower << ", " << utcUpper << "]";
					OnError(s.str());
				}
			}
		}
	};
}
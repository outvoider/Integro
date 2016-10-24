#pragma once

#include <sstream>
#include <vector>
#include <functional>
#include <memory>
#include <algorithm>

#include "LDAPAsynConnection.h"
#include "LDAPSearchResult.h"
#include "LDAPResult.h"

#include "Milliseconds.hpp"
#include "Mave/Mave.hpp"
#include "Mave/Ldap.hpp"

namespace Integro
{
	namespace Access
	{
		using std::string;
		using std::stringstream;
		using std::vector;
		using std::stack;
		using std::chrono::milliseconds;
		using std::exception;
		using std::function;
		using std::unique_ptr;
		using std::pair;
		using std::to_string;

		class LdapClient
		{
			static
				int
				SearchSome(
					const string &host
					, const int port
					, const string &user
					, const string &password
					, const string &node
					, const string &filter
					, function<void(Mave::Mave&)> OnEntry)
			{
				auto result = LDAPResult::SUCCESS;
				LDAPAsynConnection connection(host, port);
				auto queue = unique_ptr<LDAPMessageQueue>(connection.bind(user, password));

				if (queue.get() == nullptr)
				{
					throw exception("LdapClient::SearchSome(): bind has failed");
				}

				auto message = unique_ptr<LDAPMsg>(queue->getNext());

				if (message.get() == nullptr)
				{
					throw exception("LdapClient::SearchSome(): bind has failed");
				}

				queue = unique_ptr<LDAPMessageQueue>(connection.search(
					node
					//, LDAPAsynConnection::SEARCH_BASE
					//, LDAPAsynConnection::SEARCH_ONE
					, LDAPAsynConnection::SEARCH_SUB
					, filter));

				if (queue.get() == nullptr)
				{
					throw exception("LdapClient::SearchSome(): search has failed");
				}

				for (auto cont = true; cont;)
				{
					message = unique_ptr<LDAPMsg>(queue->getNext());

					if (message.get() == nullptr)
					{
						throw exception("LdapClient::SearchSome(): search has failed");
					}

					auto type = message->getMessageType();
					const LDAPEntry *entry = nullptr;

					switch (type)
					{
						case LDAPMsg::SEARCH_ENTRY:
							entry = ((LDAPSearchResult*)message.get())->getEntry();
							if (entry == nullptr)
							{
								throw exception("LdapClient::SearchSome(): search has failed");
							}
							OnEntry(Mave::FromLdap(*entry));
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
			static
				void
				Search(
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
					, function<void(Mave::Mave&)> OnEntry
					, function<void(const string&)> OnError
					, function<void(const string&)> OnEvent)
			{
				vector<Mave::Mave> entries;
				stack<pair<milliseconds, milliseconds>> intervals; intervals.push({ lowerBound, upperBound });
				stringstream s;

				while (intervals.size() > 0)
				{
					auto i = intervals.top(); intervals.pop();

					if (i.second > milliseconds::zero() && i.first > i.second)
					{
						throw exception(("LdapClient::Search(): bad interval ["
							+ to_string(i.first.count()) + ", " + to_string(i.second.count()) + "]").c_str());
					}

					s.str("");
					s << "(&";
					s << filter;
					s << "(" << idAttribute << "=*)";
					s << "(" << timeAttribute << "=*)";
					if (i.first > milliseconds::zero()) s << "(" << timeAttribute << ">=" << Milliseconds::ToLdapTime(i.first) << ")";
					if (i.second > milliseconds::zero()) s << "(" << timeAttribute << "<=" << Milliseconds::ToLdapTime(i.second) << ")";
					s << ")";
					auto newFilter = s.str();

					s.str("");
					auto utcLower = i.first > milliseconds::zero() ? Milliseconds::ToUtc(i.first) : "";
					auto utcUpper = i.second > milliseconds::zero() ? Milliseconds::ToUtc(i.second) : "";
					s << "LdapClient::Search(): searching [" << utcLower << ", " << utcUpper << "]";
					OnEvent(s.str());

					entries.clear();
					auto result = SearchSome(host, port, user, password, node, newFilter, [&](Mave::Mave &entry)
					{
						entries.push_back(entry);
						auto &value = entries.back()[timeAttribute];
						value = Milliseconds::FromLdapTime(value.AsString());
					});

					sort(entries.begin(), entries.end(), [&](Mave::Mave &left, Mave::Mave &right)
					{
						return left[timeAttribute].AsMilliseconds() < right[timeAttribute].AsMilliseconds();
					});

					if (result == LDAPResult::SUCCESS)
					{
						for (auto &entry : entries)
						{
							entry[timeAttribute] = Milliseconds::ToLdapTime(entry[timeAttribute].AsMilliseconds());
							OnEntry(entry);
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
						auto utcLower = i.first > milliseconds::zero() ? Milliseconds::ToUtc(i.first) : "";
						auto utcUpper = i.second > milliseconds::zero() ? Milliseconds::ToUtc(i.second) : "";
						s << "LdapClient::Search(): failed at [" << utcLower << ", " << utcUpper << "]";
						OnError(s.str());
					}
				}
			}
		};
	}
}
#pragma once

#include <sstream>
#include <thread>

#include "json11.hxx"

#include "mongo/client/dbclient.h"

#include "LoadProcessSave.hpp"

#include "MetadataStoreLmdb.hpp"
#include "MetadataStoreStreamLmdb.hpp"

#include "DestinationStoreMongo.hpp"
#include "DestinationStoreElastic.hpp"

#include "SourceStreamMongo.hpp"

#include "SourceStoreTds.hpp"
#include "SourceStoreLdap.hpp"
#include "SourceStoreMongo.hpp"

#include "DataServiceTds.hpp"
#include "DataServiceLdap.hpp"
#include "DataServiceLdapElastic.hpp"
#include "DataServiceMongo.hpp"

#include "MetadataProviderTds.hpp"
#include "MetadataProviderLdap.hpp"
#include "MetadataProviderMongo.hpp"
#include "MetadataProviderStreamMongo.hpp"

namespace Integro
{
	using namespace std;
	using namespace json11;

	class HostAction
	{
		function<void(const string&)> OnError;
		function<void(const string&)> OnEvent;
		vector<function<void()>> actions;

		void Skip(function<void()> action
			, function<void(const string&)> OnError)
		{
			try
			{
				action();
			}
			catch (const exception &ex)
			{
				stringstream s; s
					<< "action has failed, error message is '"
					<< ex.what()
					<< "'";
				OnError(s.str());
			}
			catch (...)
			{
				OnError("an unspecified exception has been caught");
			}
		}

		void Retry(function<void()> action
			, function<void(const string&)> OnError
			, const int errorTolerance = 0
			, const int attemptsCount = 10
			, const milliseconds pauseBetweenAttempts = milliseconds(1000))
		{
			for (int attempt = 1;; ++attempt)
			{
				try
				{
					action();
				}
				catch (const exception &ex)
				{
					stringstream s;

					if (errorTolerance < 1 && attempt == attemptsCount)
					{
						s
							<< "an action has failed, error message is '"
							<< ex.what()
							<< "'";
						OnError(s.str());
						throw;
					}

					s
						<< "an action has failed, attempt "
						<< attempt
						<< " out of "
						<< (errorTolerance > 0 ? "infinity" : to_string(attemptsCount))
						<< ", next attempt in "
						<< pauseBetweenAttempts.count()
						<< " milliseconds, error message is '"
						<< ex.what()
						<< "'";
					OnError(s.str());
					this_thread::sleep_for(pauseBetweenAttempts);
				}
				catch (...)
				{
					stringstream s;

					if (errorTolerance < 2 && attempt == attemptsCount)
					{
						OnError("an action has failed");
						throw;
					}

					s
						<< "an action has failed, attempt "
						<< attempt
						<< " out of "
						<< (errorTolerance > 1 ? "infinity" : to_string(attemptsCount))
						<< ", next attempt in "
						<< pauseBetweenAttempts.count()
						<< " milliseconds";
					OnError(s.str());
					this_thread::sleep_for(pauseBetweenAttempts);
				}
			}
		}

		string Concatenate(const Json &stringArray)
		{
			stringstream s;

			for (auto &i : stringArray.array_items())
			{
				s << i.string_value();
			}

			return s.str();
		}

		vector<string> ToStringVector(const Json &stringArray)
		{
			vector<string> strings;

			for (auto &i : stringArray.array_items())
			{
				strings.push_back(i.string_value());
			}

			return strings;
		}

	public:
		HostAction(const Json &configuration
			, const string &environment
			, const string &metadataPath
			, function<void(const string&)> OnError
			, function<void(const string&)> OnEvent)
			: OnError(OnError)
			, OnEvent(OnEvent)
		{
			auto &mongo = configuration["mongo"];
			auto &elastic = configuration["elastic"];
			auto &tds = configuration["tds"];
			auto &ldap = configuration["ldap"];

			auto &mongoConnectionOne = mongo["connections"]["one"][environment];
			auto &elasticConnectionOne = elastic["connections"]["one"][environment];

			for (auto &channel : tds["channels"].object_items())
			{
				auto &proxy = tds["proxies"][channel.first][environment];
				auto &connection = tds["connections"][channel.first][environment];

				for (auto &topic : channel.second.array_items())
				{
					MetadataStoreLmdb metadataStore(
						metadataPath
						, topic["name"].string_value());
					SourceStoreTds sourceStore(
						channel.first
						, connection["user"].string_value()
						, connection["pass"].string_value()
						, connection["database"].string_value()
						, Concatenate(topic["script"]));
					DestinationStoreMongo destinationStore(
						"mongodb://" + mongoConnectionOne["host"].string_value() + ":" + mongoConnectionOne["port"].string_value()
						, mongoConnectionOne["database"].string_value()
						, topic["name"].string_value());
					DataServiceTds dataService(
						channel.first
						, topic["modelName"].string_value()
						, topic["model"].string_value()
						, topic["name"].string_value()
						, ToStringVector(topic["targetStores"]));
					MetadataProviderTds metadataProvider(
						"start_time");

					actions.emplace_back([=]()
					{
						//LoadProcessSave::FromStoreToStore(metadataStore, sourceStore, destinationStore, dataService, metadataProvider);
						LoadProcessSave::FromStoreToStoreBatch(metadataStore, sourceStore, destinationStore, dataService, metadataProvider);
					});
				}
			}

			for (auto &channel : ldap["channels"].object_items())
			{
				auto &connection = ldap["connections"][channel.first][environment];

				for (auto &topic : channel.second.array_items())
				{
					MetadataStoreLmdb metadataStore(
						metadataPath
						, topic["name"].string_value());
					SourceStoreLdap sourceStore(
						connection["host"].string_value()
						, stoi(connection["port"].string_value())
						, connection["user"].string_value()
						, connection["pass"].string_value()
						, topic["node"].string_value()
						, topic["filter"].string_value()
						, topic["idAttribute"].string_value()
						, topic["timeAttribute"].string_value()
						, OnError
						, OnEvent);
					DestinationStoreMongo destinationStore(
						"mongodb://" + mongoConnectionOne["host"].string_value() + ":" + mongoConnectionOne["port"].string_value()
						, mongoConnectionOne["database"].string_value()
						, topic["name"].string_value());
					DataServiceLdap dataService(
						topic["idAttribute"].string_value()
						, channel.first
						, "ldap"
						, "ldap"
						, topic["name"].string_value());
					MetadataProviderLdap metadataProvider(
						topic["timeAttribute"].string_value());

					actions.emplace_back([=]()
					{
						LoadProcessSave::FromStoreToStore(metadataStore, sourceStore, destinationStore, dataService, metadataProvider);
					});
				}
			}

			for (auto &channel : tds["channels"].object_items())
			{
				auto &proxy = tds["proxies"][channel.first][environment];
				auto &connection = tds["connections"][channel.first][environment];

				for (auto &topic : channel.second.array_items())
				{
					MetadataStoreLmdb metadataStore(
						metadataPath
						, topic["name"].string_value() + ">elastic");
					SourceStoreTds sourceStore(
						channel.first
						, connection["user"].string_value()
						, connection["pass"].string_value()
						, connection["database"].string_value()
						, Concatenate(topic["script"]));
					DestinationStoreElastic destinationStore(
						elasticConnectionOne["host"].string_value() + ":" + elasticConnectionOne["port"].string_value()
						, elasticConnectionOne["index"].string_value()
						, topic["name"].string_value());
					DataServiceTds dataService(
						channel.first
						, topic["modelName"].string_value()
						, topic["model"].string_value()
						, topic["name"].string_value()
						, ToStringVector(topic["targetStores"]));
					MetadataProviderTds metadataProvider(
						"start_time");

					actions.emplace_back([=]()
					{
						//LoadProcessSave::FromStoreToStore(metadataStore, sourceStore, destinationStore, dataService, metadataProvider);
						LoadProcessSave::FromStoreToStoreBatch(metadataStore, sourceStore, destinationStore, dataService, metadataProvider);
					});
				}
			}

			for (auto &channel : ldap["channels"].object_items())
			{
				auto &connection = ldap["connections"][channel.first][environment];

				for (auto &topic : channel.second.array_items())
				{
					MetadataStoreLmdb metadataStore(
						metadataPath
						, topic["name"].string_value() + ">elastic");
					SourceStoreLdap sourceStore(
						connection["host"].string_value()
						, stoi(connection["port"].string_value())
						, connection["user"].string_value()
						, connection["pass"].string_value()
						, topic["node"].string_value()
						, topic["filter"].string_value()
						, topic["idAttribute"].string_value()
						, topic["timeAttribute"].string_value()
						, OnError
						, OnEvent);
					DestinationStoreElastic destinationStore(
						elasticConnectionOne["host"].string_value() + ":" + elasticConnectionOne["port"].string_value()
						, elasticConnectionOne["index"].string_value()
						, topic["name"].string_value());
					DataServiceLdapElastic dataService(
						topic["idAttribute"].string_value()
						, channel.first
						, "ldap"
						, "ldap"
						, topic["name"].string_value());
					MetadataProviderLdap metadataProvider(
						topic["timeAttribute"].string_value());

					actions.emplace_back([=]()
					{
						LoadProcessSave::FromStoreToStore(metadataStore, sourceStore, destinationStore, dataService, metadataProvider);
					});
				}
			}
		}

		void operator()()
		{
			while (true)
			{
				int i = 0;
				for (auto action : actions)
				{
					OnEvent("action #" + to_string(++i) + " is starting");
					Skip(action, OnError);
				}
			}
		}
	};
}
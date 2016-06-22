#pragma once

#include <sstream>
#include <regex>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "Mave.hpp"

#include "ConversionMilliseconds.hpp"

namespace Integro
{
	using namespace std;

	class DataServiceTds
	{
		string channelName;
		string modelName;
		string model;
		string action;
		vector<string> targetStores;

	public:
		typedef Mave Data;

		DataServiceTds(const string &channelName
			, const string &modelName
			, const string &model
			, const string &action
			, const vector<string> &targetStores)
			: channelName(channelName)
			, modelName(modelName)
			, model(model)
			, action(action)
			, targetStores(targetStores)
		{
		}

		void ProcessData(Mave &data)
		{
			auto &m = data.AsMap();
			auto uuid = boost::uuids::random_generator()();

			m["_id"] = boost::uuids::to_string(uuid);
			if (m.count("oid") > 0) m["_id"] = m["oid"].AsString();
			m["processed"] = 0ll;
			m["channel"] = channelName;
			m["modelName"] = modelName;
			m["model"] = model;
			m["action"] = action;
			m["start_time"] = m.count("start_time") == 0 ? milliseconds::zero() : ConversionMilliseconds::FromUtc(m["start_time"].AsString());

			if (m.count("forType") > 0)
			{
				m["modelName"] = m["forType"].AsString();
				m["model"] = regex_replace(m["forType"].AsString(), regex("[[:punct:]|[:space:]]"), "");
			}

			if (targetStores.size() > 0)
			{
				m["targetStores"] = targetStores;
			}
		}
	};
}
#pragma once

#include <sstream>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "Mave.hpp"

namespace Integro
{
	using namespace std;

	class DataServiceMongo
	{
		string channelName;
		string modelName;
		string model;
		string action;

	public:
		typedef Mave Data;

		DataServiceMongo(const string &channelName
			, const string &modelName
			, const string &model
			, const string &action)
			: channelName(channelName)
			, modelName(modelName)
			, model(model)
			, action(action)
		{
		}

		void ProcessData(Mave &data)
		{
			auto &m = data.AsMap();
			auto uuid = boost::uuids::random_generator()();

			//m["_id"] = boost::uuids::to_string(uuid);
			m["processed"] = 0ll;
			m["channel"] = channelName;
			m["modelName"] = modelName;
			m["model"] = model;
			m["action"] = action;
		}
	};
}
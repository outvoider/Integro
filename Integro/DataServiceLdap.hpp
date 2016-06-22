#pragma once

#include <sstream>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "Mave.hpp"

#include "ConversionMilliseconds.hpp"

namespace Integro
{
	using namespace std;

	class DataServiceLdap
	{
		string idAttribute;
		string channelName;
		string modelName;
		string model;
		string action;

	public:
		typedef Mave Data;

		DataServiceLdap(const string &idAttribute
			, const string &channelName
			, const string &modelName
			, const string &model
			, const string &action)
			: idAttribute(idAttribute)
			, channelName(channelName)
			, modelName(modelName)
			, model(model)
			, action(action)
		{
		}

		void ProcessData(Mave &data)
		{
			// DateTime
			{
				for (auto a :
				{
					"msExchWhenMailboxCreated"
					, "msTSExpireDate"
					, "whenChanged"
					, "whenCreated"
					, "dSCorePropagationData"
				})
				{
					if (data.AsMap().count(a) > 0)
					{
						auto &o = data[a];

						if (!o.IsVector())
						{
							o = max(milliseconds::zero(), ConversionMilliseconds::FromLdapTime(o.AsString()));
						}
						else
						{
							for (auto &i : o.AsVector())
							{
								i = max(milliseconds::zero(), ConversionMilliseconds::FromLdapTime(i.AsString()));
							}
						}
					}
				}
			}

			auto &m = data.AsMap();
			auto uuid = boost::uuids::random_generator()();

			m["_uid"] = m[idAttribute].AsString();
			//m["_id"] = boost::uuids::to_string(uuid);
			m["_id"] = m["_uid"].AsString();
			m["action"] = action;
			m["channel"] = channelName;
			m["model"] = model;
			m["modelName"] = modelName;
			m["processed"] = 0ll;
			m["start_time"] = m[m.count("whenChanged") > 0 ? "whenChanged" : "whenCreated"].AsMilliseconds();
		}
	};
}
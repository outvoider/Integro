#pragma once

#include "MetadataProviderMongo.hpp"

namespace Integro
{
	using namespace std;

	class MetadataProviderStreamMongo : public MetadataProviderMongo
	{
	public:
		typedef mongo::OID Id;

		MetadataProviderStreamMongo(const string &timeAttribute)
			: MetadataProviderMongo(timeAttribute)
		{
		}

		mongo::OID GetId(const Mave &data)
		{
			return data["_id"].AsBsonOid();
		}
	};
}
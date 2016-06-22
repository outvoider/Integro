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

	class DataServiceLdapElastic : private DataServiceLdap
	{
	public:
		typedef Mave Data;

		DataServiceLdapElastic(const string &idAttribute
			, const string &channelName
			, const string &modelName
			, const string &model
			, const string &action)
			: DataServiceLdap(idAttribute
			, channelName
			, modelName
			, model
			, action)
		{
		}

		void ProcessData(Mave &data)
		{
			DataServiceLdap::ProcessData(data);

			// Binary
			{
				for (auto a :
				{
					"msExchMailboxGuid"
					, "msExchMailboxSecurityDescriptor"
					, "objectGUID"
					, "objectSid"
					, "userParameters"
					, "userCertificate"
					, "msExchArchiveGUID"
					, "msExchBlockedSendersHash"
					, "msExchSafeSendersHash"
					, "securityProtocol"
					, "terminalServer"
					, "mSMQDigests"
					, "mSMQSignCertificates"
					, "msExchSafeRecipientsHash"
					, "msExchDisabledArchiveGUID"
					, "sIDHistory"
					, "replicationSignature"
					, "msExchMasterAccountSid"
					, "logonHours"
					, "thumbnailPhoto"
				})
				{
					if (data.AsMap().count(a) > 0)
					{
						auto &o = data[a];

						if (!o.IsVector())
						{
							o = "";
						}
						else
						{
							for (auto &i : o.AsVector())
							{
								i = "";
							}
						}
					}
				}
			}

			// Variant
			{
				for (auto a :
				{
					"extensionAttribute1"
					, "extensionAttribute2"
					, "extensionAttribute3"
					, "extensionAttribute4"
					, "extensionAttribute5"
					, "extensionAttribute6"
					, "extensionAttribute7"
					, "extensionAttribute8"
					, "extensionAttribute9"
					, "extensionAttribute10"
					, "extensionAttribute11"
					, "extensionAttribute12"
					, "extensionAttribute13"
					, "extensionAttribute14"
					, "extensionAttribute15"
				})
				{
					if (data.AsMap().count(a) > 0)
					{
						auto &o = data[a];

						if (!o.IsVector())
						{
							o = "[string] " + o.AsString();
						}
						else
						{
							for (auto &i : o.AsVector())
							{
								i = "[string] " + i.AsString();
							}
						}
					}
				}
			}
		}
	};
}
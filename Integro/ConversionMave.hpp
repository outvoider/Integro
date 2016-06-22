#pragma once

#include <sstream>

#include "json11.hxx"

#include "LDAPAsynConnection.h"
#include "LDAPSearchResult.h"
#include "LDAPResult.h"

#include "mongo/client/dbclient.h"

#include "ConversionMilliseconds.hpp"
#include "Mave.hpp"

namespace Integro
{
	using namespace std;
	using namespace json11;

	class ConversionMave
	{
	public:
		static Mave FromLdapEntry(const LDAPEntry &ldap)
		{
			map<string, Mave> mm;
			auto al = ldap.getAttributes();

			for (auto a = al->begin(); a != al->end(); ++a)
			{
				auto an = a->getName();
				auto vl = a->getValues();

				if (0 == vl.size())
				{
					mm.insert({ an, nullptr });
				}
				else if (1 == vl.size())
				{
					mm.insert({ an, *vl.begin() });
				}
				else
				{
					vector<Mave> mv;

					for (auto v = vl.begin(); v != vl.end(); ++v)
					{
						mv.push_back(*v);
					}

					mm.insert({ an, mv });
				}
			}

			return mm;
		}

		static Mave FromBson(const mongo::BSONObj &bson)
		{
			if (bson.couldBeArray())
			{
				vector<Mave> mv;

				for (auto i = bson.begin(); i.more();)
				{
					auto e = i.next();

					switch (e.type())
					{
						case mongo::BSONType::MinKey:
						case mongo::BSONType::EOO:
						case mongo::BSONType::MaxKey:
							break;
						case mongo::BSONType::jstNULL:
							mv.push_back(nullptr);
							break;
						case mongo::BSONType::Bool:
							mv.push_back(e.boolean());
							break;
						case mongo::BSONType::NumberDouble:
							mv.push_back(e.numberDouble());
						case mongo::BSONType::NumberLong:
						case mongo::BSONType::NumberInt:
							mv.push_back(e.numberLong());
							break;
						case mongo::BSONType::Date:
							mv.push_back(milliseconds(e.date().millis));
							break;
						case mongo::BSONType::String:
							mv.push_back(e.str());
							break;
						case mongo::BSONType::jstOID:
							mv.push_back(e.OID());
							break;
						case mongo::BSONType::Undefined:
						case mongo::BSONType::Timestamp:
						case mongo::BSONType::BinData:
						case mongo::BSONType::RegEx:
						case mongo::BSONType::Symbol:
						case mongo::BSONType::CodeWScope:
							mv.push_back(e.toString());
							break;
						case mongo::BSONType::Object:
						case mongo::BSONType::Array:
							mv.push_back(FromBson(e.Obj()));
							break;
						default:
							throw exception("ConversionMave::FromBson(): unsupported type encountered");
					}
				}

				return mv;
			}

			map<string, Mave> mm;

			for (auto i = bson.begin(); i.more();)
			{
				auto e = i.next();

				switch (e.type())
				{
					case mongo::BSONType::MinKey:
					case mongo::BSONType::EOO:
					case mongo::BSONType::MaxKey:
						break;
					case mongo::BSONType::jstNULL:
						mm.insert({ e.fieldName(), nullptr });
						break;
					case mongo::BSONType::Bool:
						mm.insert({ e.fieldName(), e.boolean() });
						break;
					case mongo::BSONType::NumberDouble:
						mm.insert({ e.fieldName(), e.numberDouble() });
					case mongo::BSONType::NumberLong:
					case mongo::BSONType::NumberInt:
						mm.insert({ e.fieldName(), e.numberLong() });
						break;
					case mongo::BSONType::Date:
						mm.insert({ e.fieldName(), milliseconds(e.date().millis) });
						break;
					case mongo::BSONType::String:
						mm.insert({ e.fieldName(), e.str() });
						break;
					case mongo::BSONType::jstOID:
						mm.insert({ e.fieldName(), e.OID() });
						break;
					case mongo::BSONType::Undefined:
					case mongo::BSONType::Timestamp:
					case mongo::BSONType::BinData:
					case mongo::BSONType::RegEx:
					case mongo::BSONType::Symbol:
					case mongo::BSONType::CodeWScope:
						mm.insert({ e.fieldName(), e.toString() });
						break;
					case mongo::BSONType::Object:
					case mongo::BSONType::Array:
						mm.insert({ e.fieldName(), FromBson(e.Obj()) });
						break;
					default:
						throw exception("ConversionMave::FromBson(): unsupported type encountered");
				}
			}

			return mm;
		}

		static mongo::BSONObj ToBson(const Mave &mave)
		{
			mongo::BSONObjBuilder bo;
			mongo::BSONArrayBuilder ba;

			switch (mave.GetType())
			{
				case Mave::Type::NULLPTR:
					ba.appendNull();
					break;
				case Mave::Type::BOOL:
					ba.appendBool(mave.AsBool());
					break;
				case Mave::Type::LONG:
					ba.append(mave.AsLong());
					break;
				case Mave::Type::DOUBLE:
					ba.append(mave.AsDouble());
					break;
				case Mave::Type::MILLISECONDS:
					ba.appendTimeT(ConversionMilliseconds::ToTimeT(mave.AsMilliseconds()));
					break;
				case Mave::Type::BSON_OID:
					ba.append(mave.AsBsonOid());
					break;
				case Mave::Type::STRING:
					ba.append(mave.AsString());
					break;
				case Mave::Type::MAP:
					for (auto &i : mave.AsMap())
					{
						switch (i.second.GetType())
						{
							case Mave::Type::NULLPTR:
								bo.appendNull(i.first);
								break;
							case Mave::Type::BOOL:
								bo.appendBool(i.first, i.second.AsBool());
								break;
							case Mave::Type::LONG:
								bo.appendNumber(i.first, i.second.AsLong());
								break;
							case Mave::Type::DOUBLE:
								bo.appendNumber(i.first, i.second.AsDouble());
								break;
							case Mave::Type::MILLISECONDS:
								bo.appendTimeT(i.first, ConversionMilliseconds::ToTimeT(i.second.AsMilliseconds()));
								break;
							case Mave::Type::BSON_OID:
								bo.append(i.first, i.second.AsBsonOid());
								break;
							case Mave::Type::STRING:
								bo.append(i.first, i.second.AsString());
								break;
							case Mave::Type::MAP:
							case Mave::Type::VECTOR:
								bo.append(i.first, ToBson(i.second));
								break;
							default:
								throw exception("ConversionMave::ToBson(): unsupported type encountered");
						}
					}
					return bo.obj();
				case Mave::Type::VECTOR:
					for (auto &i : mave.AsVector())
					{
						switch (i.GetType())
						{
							case Mave::Type::NULLPTR:
								ba.appendNull();
								break;
							case Mave::Type::BOOL:
								ba.appendBool(i.AsBool());
								break;
							case Mave::Type::LONG:
								ba.append(i.AsLong());
								break;
							case Mave::Type::DOUBLE:
								ba.append(i.AsDouble());
								break;
							case Mave::Type::MILLISECONDS:
								ba.appendTimeT(ConversionMilliseconds::ToTimeT(i.AsMilliseconds()));
								break;
							case Mave::Type::BSON_OID:
								ba.append(i.AsBsonOid());
								break;
							case Mave::Type::STRING:
								ba.append(i.AsString());
								break;
							case Mave::Type::MAP:
							case Mave::Type::VECTOR:
								ba.append(ToBson(i));
								break;
							default:
								throw exception("ConversionMave::ToBson(): unsupported type encountered");
						}
					}
					return ba.arr();
				default:
					throw exception("ConversionMave::ToBson(): unsupported type encountered");
			}

			return ba.obj();
		}

		static Mave FromJson(const Json &json)
		{
			map<string, Mave> mm;
			vector<Mave> mv;

			switch (json.type())
			{
				case Json::Type::NUL:
					return nullptr;
				case Json::Type::BOOL:
					return json.bool_value();
				case Json::Type::NUMBER:
					return json.number_value();
				case Json::Type::STRING:
					return json.string_value();
				case Json::Type::OBJECT:
					for (auto &i : json.object_items())
					{
						switch (i.second.type())
						{
							case Json::Type::NUL:
								mm.insert({ i.first, nullptr });
								break;
							case Json::Type::BOOL:
								mm.insert({ i.first, i.second.bool_value() });
								break;
							case Json::Type::NUMBER:
								mm.insert({ i.first, i.second.number_value() });
								break;
							case Json::Type::STRING:
								mm.insert({ i.first, i.second.string_value() });
								break;
							case Json::Type::OBJECT:
							case Json::Type::ARRAY:
								mm.insert({ i.first, FromJson(i.second) });
								break;
							default:
								throw exception("ConversionMave::FromJson(): unsupported type encountered");
						}
					}
					return mm;
				case Json::Type::ARRAY:
					for (auto &i : json.array_items())
					{
						switch (i.type())
						{
							case Json::Type::NUL:
								mv.push_back(nullptr);
								break;
							case Json::Type::BOOL:
								mv.push_back(i.bool_value());
								break;
							case Json::Type::NUMBER:
								mv.push_back(i.number_value());
								break;
							case Json::Type::STRING:
								mv.push_back(i.string_value());
								break;
							case Json::Type::OBJECT:
							case Json::Type::ARRAY:
								mv.push_back(FromJson(i));
								break;
							default:
								throw exception("ConversionMave::FromJson(): unsupported type encountered");
						}
					}
					return mv;
				default:
					throw exception("ConversionMave::FromJson(): unsupported type encountered");
			}

			throw exception("ConversionMave::FromJson(): failed");
		}

		static Json ToJson(const Mave &mave)
		{
			Json::object jo;
			Json::array ja;

			switch (mave.GetType())
			{
				case Mave::Type::NULLPTR:
					return nullptr;
				case Mave::Type::BOOL:
					return mave.AsBool();
				case Mave::Type::LONG:
					return (double)mave.AsLong();
				case Mave::Type::DOUBLE:
					return mave.AsDouble();
				case Mave::Type::MILLISECONDS:
					return ConversionMilliseconds::ToUtc(mave.AsMilliseconds());
				case Mave::Type::BSON_OID:
					return mave.AsBsonOid().toString();
				case Mave::Type::STRING:
					return mave.AsString();
				case Mave::Type::MAP:
					for (auto &i : mave.AsMap())
					{
						switch (i.second.GetType())
						{
							case Mave::Type::NULLPTR:
								jo.insert({ i.first, nullptr });
								break;
							case Mave::Type::BOOL:
								jo.insert({ i.first, i.second.AsBool() });
								break;
							case Mave::Type::LONG:
								jo.insert({ i.first, (double)i.second.AsLong() });
								break;
							case Mave::Type::DOUBLE:
								jo.insert({ i.first, i.second.AsDouble() });
								break;
							case Mave::Type::MILLISECONDS:
								jo.insert({ i.first, ConversionMilliseconds::ToUtc(i.second.AsMilliseconds()) });
								break;
							case Mave::Type::BSON_OID:
								jo.insert({ i.first, i.second.AsBsonOid().toString() });
								break;
							case Mave::Type::STRING:
								jo.insert({ i.first, i.second.AsString() });
								break;
							case Mave::Type::MAP:
							case Mave::Type::VECTOR:
								jo.insert({ i.first, ToJson(i.second) });
								break;
							default:
								throw exception("ConversionMave::ToJson(): unsupported type encountered");
						}
					}
					return jo;
				case Mave::Type::VECTOR:
					for (auto &i : mave.AsVector())
					{
						switch (i.GetType())
						{
							case Mave::Type::NULLPTR:
								ja.push_back(nullptr);
								break;
							case Mave::Type::BOOL:
								ja.push_back(i.AsBool());
								break;
							case Mave::Type::LONG:
								ja.push_back((double)i.AsLong());
								break;
							case Mave::Type::DOUBLE:
								ja.push_back(i.AsDouble());
								break;
							case Mave::Type::MILLISECONDS:
								ja.push_back(ConversionMilliseconds::ToUtc(i.AsMilliseconds()));
								break;
							case Mave::Type::BSON_OID:
								ja.push_back(i.AsBsonOid().toString());
								break;
							case Mave::Type::STRING:
								ja.push_back(i.AsString());
								break;
							case Mave::Type::MAP:
							case Mave::Type::VECTOR:
								ja.push_back(ToJson(i));
								break;
							default:
								throw exception("ConversionMave::ToJson(): unsupported type encountered");
						}
					}
					return ja;
				default:
					throw exception("ConversionMave::ToJson(): unsupported type encountered");
			}

			throw exception("ConversionMave::ToJson(): failed");
		}
	};
}
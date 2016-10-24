#pragma once

#include "Mave/Mave.hpp"

#include "bsoncxx/builder/core.hpp"
#include "bsoncxx/builder/stream/document.hpp"
#include <bsoncxx/builder/stream/array.hpp>
#include <bsoncxx/builder/stream/helpers.hpp>
#include "bsoncxx/builder/concatenate.hpp"
#include <bsoncxx/types.hpp>
#include <bsoncxx/json.hpp>

namespace Integro
{
	namespace Mave
	{
		auto BSON_OID = string_generator()("389da9dd-4e9f-4b80-984c-331fe6ab0df1");

		Mave FromBson(bsoncxx::types::value bson)
		{
			Mave result;
			vector<function<bool()>> continuations;
			auto root = bson;

			while (true)
			{
				switch (bson.type())
				{
					case bsoncxx::type::k_undefined:
					case bsoncxx::type::k_minkey:
					case bsoncxx::type::k_maxkey:
						break;
					case bsoncxx::type::k_null:
						result = nullptr;
						break;
					case bsoncxx::type::k_bool:
						result = bson.get_bool().value;
						break;
					case bsoncxx::type::k_int32:
						result = bson.get_int32().value;
						break;
					case bsoncxx::type::k_int64:
						result = bson.get_int64().value;
						break;
					case bsoncxx::type::k_double:
						result = bson.get_double().value;
						break;
					case bsoncxx::type::k_date:
						result = milliseconds(bson.get_date().value);
						break;
					case bsoncxx::type::k_utf8:
						result = bson.get_utf8().value.to_string();
						break;
					case bsoncxx::type::k_oid:
						result = make_pair(BSON_OID, bson.get_oid().value.to_string());
						break;
					case bsoncxx::type::k_dbpointer:
						result = make_pair(BSON_OID, bson.get_dbpointer().value.to_string());
						break;
					case bsoncxx::type::k_timestamp:
						auto t = bson.get_timestamp();
						result = (long)(((unsigned long long)t.timestamp << 32) | t.increment);
						break;
					case bsoncxx::type::k_binary:
						auto b = bson.get_binary();
						result = string((const char*)b.bytes, b.size);
						break;
					case bsoncxx::type::k_regex:
						result = bson.get_regex().regex.to_string();
						break;
					case bsoncxx::type::k_symbol:
						result = bson.get_symbol().symbol.to_string();
						break;
					case bsoncxx::type::k_code:
						result = bson.get_code().code.to_string();
						break;
					case bsoncxx::type::k_codewscope:
						result = bson.get_codewscope().code.to_string();
						break;
					case bsoncxx::type::k_document:
						continuations.push_back(
							[&
							, m = map<string, Mave>()
							, i = bson.get_document().value.cbegin()
							, e = bson.get_document().value.cend()
							, f = false]() mutable -> bool
						{
							if (f)
							{
								m.insert({ i->key().to_string(), result });
								++i;
							}
							if (i != e)
							{
								bson = i->get_value();
								return f = true;
							}
							result = move(m);
							return false;
						});
						break;
					case bsoncxx::type::k_array:
						continuations.push_back(
							[&
							, v = vector<Mave>()
							, i = bson.get_array().value.cbegin()
							, e = bson.get_array().value.cend()
							, f = false]() mutable -> bool
						{
							if (f)
							{
								v.push_back(result);
								++i;
							}
							if (i != e)
							{
								bson = i->get_value();
								return f = true;
							}
							result = move(v);
							return false;
						});
						break;
					default:
						throw exception("Mave::FromBson(): unsupported type encountered");
				}

				while (true)
				{
					if (continuations.size() == 0)
					{
						return result;
					}
					if (continuations.back()())
					{
						break;
					}
					continuations.pop_back();
				}
			}
		}

		Mave FromBson(bsoncxx::document::view bson)
		{
			return FromBson(bsoncxx::types::value{ bsoncxx::types::b_document{ bson } });
		}

		Mave FromBson(bsoncxx::array::view bson)
		{
			return FromBson(bsoncxx::types::value{ bsoncxx::types::b_array{ bson } });
		}

		// private function, use ToBsonArray or ToBsonDocument
		void ToBson(Mave mave, bsoncxx::builder::core &result)
		{
			vector<function<bool()>> continuations;
			auto f = 0;
			auto root = mave;

			while (true)
			{
				switch (mave.GetType())
				{
					case MAVE_NULL:
						result.append(bsoncxx::types::b_null());
						break;
					case MAVE_BOOL:
						result.append(mave.AsBool());
						break;
					case MAVE_INT:
						result.append(mave.AsInt());
						break;
					case MAVE_LONG:
						result.append(mave.AsLong());
						break;
					case MAVE_DOUBLE:
						result.append(mave.AsDouble());
						break;
					case MAVE_MILLISECONDS:
						result.append(bsoncxx::types::b_date(mave.AsMilliseconds().count()));
						break;
					case MAVE_STRING:
						result.append(mave.AsString());
						break;
					case MAVE_CUSTOM:
						if (mave.AsCustom().first == BSON_OID)
						{
							result.append(bsoncxx::oid(mave.AsCustom().second));
						}
						else
						{
							result.append(mave.AsCustom().second);
						}
						break;
					case MAVE_MAP:
						if (f++ > 0) result.open_document();
						continuations.push_back(
							[&
							, i = mave.AsMap().cbegin()
							, e = mave.AsMap().cend()]() mutable -> bool
						{
							if (i != e)
							{
								result.key_view(i->first);
								mave = i++->second;
								return true;
							}
							if (--f > 0) result.close_document();
							return false;
						});
						break;
					case MAVE_VECTOR:
						if (f++ > 0) result.open_array();
						continuations.push_back(
							[&
							, i = mave.AsVector().cbegin()
							, e = mave.AsVector().cend()]() mutable -> bool
						{
							if (i != e)
							{
								mave = *i++;
								return true;
							}
							if (--f > 0) result.close_array();
							return false;
						});
						break;
					default:
						throw exception("Mave::ToBson(): unsupported type encountered");
				}

				while (true)
				{
					if (continuations.size() == 0)
					{
						return;
					}
					if (continuations.back()())
					{
						break;
					}
					continuations.pop_back();
				}
			}
		}

		bsoncxx::array::value ToBsonArray(Mave mave)
		{
			if (!mave.IsVector())
			{
				throw exception("Mave::ToBsonArray(): unexpected type encountered");
			}
			bsoncxx::builder::core result{ true };
			ToBson(mave, result);
			return result.extract_array();
		}

		bsoncxx::document::value ToBsonDocument(Mave mave)
		{
			if (!mave.IsMap())
			{
				throw exception("Mave::ToBsonDocument(): unexpected type encountered");
			}
			bsoncxx::builder::core result{ false };
			ToBson(mave, result);
			return result.extract_document();
		}
	}
}
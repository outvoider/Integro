#pragma once

#include <memory>
#include <initializer_list>
#include <vector>
#include <map>
#include <sstream>
#include <chrono>
#include <functional>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/string_generator.hpp>

#include "Milliseconds.hpp"
#include "Hash.hpp"

namespace Integro
{
	namespace Mave
	{
		using std::string;
		using std::stringstream;
		using std::vector;
		using std::map;
		using std::pair;
		using std::shared_ptr;
		using std::make_shared;
		using std::exception;
		using std::initializer_list;
		using std::move;
		using std::chrono::milliseconds;
		using boost::uuids::uuid;
		using boost::uuids::string_generator;
		using std::function;

		enum MaveType
		{
			MAVE_NULL
			, MAVE_VECTOR
			, MAVE_MAP
			, MAVE_BOOL
			, MAVE_INT
			, MAVE_LONG
			, MAVE_DOUBLE
			, MAVE_STRING
			, MAVE_MILLISECONDS
			, MAVE_CUSTOM
		};

		class Mave final
		{
			struct Type
			{
				MaveType type_;
				inline Type(const MaveType type) : type_(type) {}
			};

			template <MaveType type, typename T>
			struct Value : public Type
			{
				T value_;
				inline Value(const T &value) : value_(value), Type(type) {}
				inline Value(const T &&value) : value_(move(value)), Type(type) {}
			};

			typedef Value<MAVE_NULL, nullptr_t> Null;
			typedef Value<MAVE_VECTOR, vector<Mave>> Vector;
			typedef Value<MAVE_MAP, map<string, Mave>> Map;
			typedef Value<MAVE_BOOL, bool> Bool;
			typedef Value<MAVE_INT, int> Int;
			typedef Value<MAVE_LONG, long long> Long;
			typedef Value<MAVE_DOUBLE, double> Double;
			typedef Value<MAVE_STRING, string> String;
			typedef Value<MAVE_MILLISECONDS, milliseconds> Milliseconds;
			typedef Value<MAVE_CUSTOM, pair<uuid, string>> Custom;

			static shared_ptr<Type> null_;
			static shared_ptr<Type> true_;
			static shared_ptr<Type> false_;
			shared_ptr<Type> value;

		public:
			MaveType GetType() const { return value->type_; }
			bool HasType(const MaveType type) const { return type == value->type_; }
			void Assert(const MaveType type) const {
				if (!HasType(type)) {
					throw exception("Mave::Assert(): invalid type");
				}
			}

			Mave() : value(null_) {}
			Mave(nullptr_t) : value(null_) {}
			bool IsNull() const { return HasType(MAVE_NULL); }
			nullptr_t AsNull() const { Assert(MAVE_NULL); return nullptr; }

			template <class V, typename std::enable_if<
				std::is_constructible<Mave, typename V::value_type>::value,
				int>::type = 0>
				Mave(const V &v) : Mave(vector<Mave>(v.begin(), v.end())) {}
			Mave(vector<Mave> &value) : value(make_shared<Vector>(value)) {}
			Mave(vector<Mave> &&value) : value(make_shared<Vector>(move(value))) {}
			bool IsVector() const { return HasType(MAVE_VECTOR); }
			vector<Mave>& AsVector() const { Assert(MAVE_VECTOR); return ((Vector*)value.get())->value_; }
			Mave& operator[](int index) const { return AsVector().at(index); }

			template <class M, typename std::enable_if<
				std::is_constructible<std::string, typename M::key_type>::value
				&& std::is_constructible<Mave, typename M::mapped_type>::value,
				int>::type = 0>
				Mave(const M &m) : Mave(map<string, Mave>(m.begin(), m.end())) {}
			Mave(map<string, Mave> &value) : value(make_shared<Map>(value)) {}
			Mave(map<string, Mave> &&value) : value(make_shared<Map>(move(value))) {}
			bool IsMap() const { return HasType(MAVE_MAP); }
			map<string, Mave>& AsMap() const { Assert(MAVE_MAP); return ((Map*)value.get())->value_; }
			Mave& operator[](const string &key) const { return AsMap().at(key); }

			Mave(void *) = delete;
			Mave(bool value) : value(value ? true_ : false_) {}
			bool IsBool() const { return HasType(MAVE_BOOL); }
			bool AsBool() const { Assert(MAVE_BOOL); return ((Bool*)value.get())->value_; }

			Mave(int value) : value(make_shared<Int>(value)) {}
			bool IsInt() const { return HasType(MAVE_INT); }
			int AsInt() const { Assert(MAVE_INT); return ((Int*)value.get())->value_; }

			Mave(long long value) : value(make_shared<Long>(value)) {}
			bool IsLong() const { return HasType(MAVE_LONG); }
			long long AsLong() const { Assert(MAVE_LONG); return ((Long*)value.get())->value_; }

			Mave(double value) : value(make_shared<Double>(value)) {}
			bool IsDouble() const { return HasType(MAVE_DOUBLE); }
			double AsDouble() const { Assert(MAVE_DOUBLE); return ((Double*)value.get())->value_; }

			Mave(const string &value) : value(make_shared<String>(value)) {}
			Mave(const string &&value) : value(make_shared<String>(move(value))) {}
			Mave(const char *value) : value(make_shared<String>(value)) {}
			bool IsString() const { return HasType(MAVE_STRING); }
			string& AsString() const { Assert(MAVE_STRING); return ((String*)value.get())->value_; }

			Mave(milliseconds value) : value(make_shared<Milliseconds>(value)) {}
			bool IsMilliseconds() const { return HasType(MAVE_MILLISECONDS); }
			milliseconds AsMilliseconds() const { Assert(MAVE_MILLISECONDS); return ((Milliseconds*)value.get())->value_; }

			Mave(const pair<uuid, string> &value) : value(make_shared<Custom>(value)) {}
			Mave(const pair<uuid, string> &&value) : value(make_shared<Custom>(move(value))) {}
			bool IsCustom() const { return HasType(MAVE_CUSTOM); }
			pair<uuid, string>& AsCustom() const { Assert(MAVE_CUSTOM); return ((Custom*)value.get())->value_; }
		};

		shared_ptr<Mave::Type> Mave::null_ = make_shared<Null>(nullptr);
		shared_ptr<Mave::Type> Mave::true_ = make_shared<Bool>(true);
		shared_ptr<Mave::Type> Mave::false_ = make_shared<Bool>(false);

		Mave Copy(Mave mave)
		{
			Mave result;
			vector<function<bool()>> continuations;
			auto root = mave;

			while (true)
			{
				switch (mave.GetType())
				{
					case MAVE_NULL:
						result = nullptr;
						break;
					case MAVE_BOOL:
						result = mave.AsBool();
						break;
					case MAVE_INT:
						result = mave.AsInt();
						break;
					case MAVE_LONG:
						result = mave.AsLong();
						break;
					case MAVE_DOUBLE:
						result = mave.AsDouble();
						break;
					case MAVE_MILLISECONDS:
						result = mave.AsMilliseconds();
						break;
					case MAVE_STRING:
						result = mave.AsString();
						break;
					case MAVE_CUSTOM:
						result = mave.AsCustom();
						break;
					case MAVE_MAP:
						continuations.push_back(
							[&
							, m = map<string, Mave>()
							, i = mave.AsMap().cbegin()
							, e = mave.AsMap().cend()
							, f = false]() mutable -> bool
						{
							if (f)
							{
								m.insert({ i->first, result });
								++i;
							}
							if (i != e)
							{
								mave = i->second;
								return f = true;
							}
							result = move(m);
							return false;
						});
						break;
					case MAVE_VECTOR:
						continuations.push_back(
							[&
							, v = vector<Mave>()
							, i = mave.AsVector().cbegin()
							, e = mave.AsVector().cend()
							, f = false]() mutable -> bool
						{
							if (f)
							{
								v.push_back(result);
								++i;
							}
							if (i != e)
							{
								mave = *i;
								return f = true;
							}
							result = move(v);
							return false;
						});
						break;
					default:
						throw exception("Mave::Copy(): unsupported type encountered");
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

		string ToString(Mave mave)
		{
			stringstream result;
			vector<function<bool()>> continuations;
			auto root = mave;

			while (true)
			{
				switch (mave.GetType())
				{
					case MAVE_NULL:
						result << "null";
						break;
					case MAVE_BOOL:
						result << (mave.AsBool() ? "true" : "false");
						break;
					case MAVE_INT:
						result << mave.AsInt();
						break;
					case MAVE_LONG:
						result << mave.AsLong() << "L";
						break;
					case MAVE_DOUBLE:
						result << mave.AsDouble() << "D";
						break;
					case MAVE_MILLISECONDS:
						result << Milliseconds::ToUtc(mave.AsMilliseconds(), true);
						break;
					case MAVE_STRING:
						result << "\"" << mave.AsString() << "\"";
						break;
					case MAVE_CUSTOM:
						result << "(\"" << mave.AsCustom().first << "\":\"" << mave.AsCustom().second << "\")";
						break;
					case MAVE_MAP:
						result << "{";
						continuations.push_back(
							[&
							, i = mave.AsMap().cbegin()
							, e = mave.AsMap().cend()
							, f = false]() mutable -> bool
						{
							if (i != e)
							{
								if (f) result << ",";
								result << "\"" << i->first << "\":";
								mave = i++->second;
								return f = true;
							}
							result << "}";
							return false;
						});
						break;
					case MAVE_VECTOR:
						result << "[";
						continuations.push_back(
							[&
							, i = mave.AsVector().cbegin()
							, e = mave.AsVector().cend()
							, f = false]() mutable -> bool
						{
							if (i != e)
							{
								if (f) result << ",";
								mave = *i++;
								return f = true;
							}
							result << "]";
							return false;
						});
						break;
					default:
						throw exception("Mave::ToString(): unsupported type encountered");
				}

				while (true)
				{
					if (continuations.size() == 0)
					{
						return result.str();
					}
					if (continuations.back()())
					{
						break;
					}
					continuations.pop_back();
				}
			}
		}

		int Hash(Mave mave)
		{
			return ::Integro::Hash(ToString(mave));
		}
	}
}
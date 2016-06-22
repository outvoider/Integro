#pragma once

#include <sstream>
#include <memory>

#include "client_http.hpp"

#include "ConversionMave.hpp"

namespace Integro
{
	using namespace std;

	class ClientElastic
	{
		typedef SimpleWeb::Client<SimpleWeb::HTTP> HttpClient;

		static Json MakeRequest(const string &url
			, const string &request
			, const string &path
			, stringstream &content)
		{
			HttpClient client(url);
			auto httpResponse = client.request(request, path, content);
			stringstream s; s << httpResponse->content.rdbuf();
			string error;
			auto response = Json::parse(s.str(), error);

			if (error != "")
			{
				throw exception(error.c_str());
			}

			if (!response["error"].is_null())
			{
				throw exception(response.dump().c_str());
			}

			if (response["errors"].is_bool() && response["errors"].bool_value())
			{
				auto &items = const_cast<Json::array&>(response["items"].array_items());
				auto end = remove_if(items.begin(), items.end(), [&](const Json &item)
				{
					return item["index"]["error"].is_null();
				});
				items.resize(end - items.begin());

				throw exception(response.dump().c_str());
			}

			return response;
		}

	public:
		static void Index(const vector<Mave> &objects
			, const string &url
			, const string &index
			, const string &type
			, const int maxBatchCount = 10000
			, const int maxBatchSize = 100000000)
		{
			string request = "PUT";
			string path = (index == "" ? "" : "/" + index + (type == "" ? "" : "/" + type)) + "/_bulk";
			stringstream content;
			int n = 0;

			for (auto o = objects.begin(); o != objects.end(); ++o)
			{
				auto json = ConversionMave::ToJson(*o);
				content << "{\"index\":{\"_id\":\"" << json["_id"].string_value() << "\"}}" << endl;
				content << json.dump() << endl;

				if (++n == maxBatchCount || 1 + o == objects.end() || content.tellp() >= maxBatchSize)
				{
					auto response = MakeRequest(url, request, path, content);
					content.str("");
					n = 0;
				}
			}
		}

		static void Delete(const vector<string> &ids
			, const string &url
			, const string &index
			, const string &type)
		{
			string request = "DELETE";
			string path = index == "" ? "" : "/" + index + (type == "" ? "" : "/" + type);
			stringstream content;

			if (ids.size() > 0)
			{
				content << "{\"ids\":[";

				for (auto i = ids.begin();;)
				{
					content << "\"" << *i << "\"";

					if (++i == ids.end())
					{
						break;
					}

					content << ",";
				}

				content << "]}";
			}

			auto response = MakeRequest(url, request, path, content);
		}

		static void DeleteIndex(const string &url
			, const string &index)
		{
			if (index == "")
			{
				throw exception("ClientElastic::DeleteIndex(): index name is required");
			}

			string request = "DELETE";
			string path = "/" + index;
			stringstream content;
			auto response = MakeRequest(url, request, path, content);
		}

		static int Count(const string &url
			, const string &index
			, const string &type)
		{
			string request = "GET";
			string path = (index == "" ? "" : "/" + index + (type == "" ? "" : "/" + type)) + "/_search?from=0&size=0";
			stringstream content;
			auto response = MakeRequest(url, request, path, content);
			return response["hits"]["total"].int_value();
		}

		static Mave Get(const string &url
			, const string &index
			, const string &type
			, const string &id)
		{
			string request = "GET";
			string path = (index == "" ? "" : "/" + index + (type == "" ? "" : "/" + type)) + id;
			stringstream content;
			auto response = MakeRequest(url, request, path, content);
			return ConversionMave::FromJson(response);
		}

		static void Get(function<void(const Mave&)> OnObject
			, const vector<string> &ids
			, const string &url
			, const string &index
			, const string &type)
		{
			if (ids.size() > 0)
			{
				string request = "GET";
				string path = (index == "" ? "" : "/" + index + (type == "" ? "" : "/" + type)) + "/_mget";
				stringstream content;

				content << "{\"ids\":[";

				for (auto i = ids.begin();;)
				{
					content << "\"" << *i << "\"";

					if (++i == ids.end())
					{
						break;
					}

					content << ",";
				}

				content << "]}";

				auto response = MakeRequest(url, request, path, content);

				for (auto &o : response["docs"].array_items())
				{
					if (!o["_source"].is_null())
					{
						OnObject(ConversionMave::FromJson(o["_source"]));
					}
				}
			}
		}

		static void Search(function<void(const Mave&)> OnObject
			, const string &url
			, const string &index
			, const string &type
			, const string &query = ""
			, const int from = 0
			, const int count = 1000000
			, const int batchCount = 10000)
		{
			string request = "GET";
			stringstream content;

			for (int i = from, n = min(count, batchCount); n != 0; i += n)
			{
				content.str(""); if (query != "") content << query << endl;
				auto path = (index == "" ? "" : "/" + index + (type == "" ? "" : "/" + type)) + "/_search?from=" + to_string(i) + "&size=" + to_string(n);
				auto response = MakeRequest(url, request, path, content);

				for (auto &o : response["hits"]["hits"].array_items())
				{
					OnObject(ConversionMave::FromJson(o["_source"]));
				}

				n = min(batchCount, min(from + count, response["hits"]["total"].int_value()) - i);
			}
		}
	};
}
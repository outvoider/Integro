#pragma once

#include <sstream>
#include <vector>
#include <functional>

#include "client_http.hpp"

#include "Milliseconds.hpp"
#include "Mave/Mave.hpp"
#include "Mave/Json.hpp"

namespace Integro
{
	namespace Access
	{
		using std::string;
		using std::stringstream;
		using std::to_string;
		using std::vector;
		using std::exception;
		using std::function;
		using std::endl;

		class ElasticClient
		{
			typedef SimpleWeb::Client<SimpleWeb::HTTP> HttpClient;

			static
				json11::Json
				MakeRequest(
					const string &url
					, const string &request
					, const string &path
					, stringstream &content)
			{
				HttpClient client(url);
				auto httpResponse = client.request(request, path, content);
				stringstream s; s << httpResponse->content.rdbuf();
				string error;
				auto response = json11::Json::parse(s.str(), error);

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
					auto &items = const_cast<json11::Json::array&>(response["items"].array_items());
					auto end = remove_if(items.begin(), items.end(), [&](const json11::Json &item)
					{
						return item["index"]["error"].is_null();
					});
					items.resize(end - items.begin());

					throw exception(response.dump().c_str());
				}

				return response;
			}

		public:
			static
				void
				Index(
					const vector<Mave::Mave> &objects
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
					auto json = ToJson(*o);
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

			static
				void
				Delete(
					const vector<string> &ids
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

			static
				void
				CreateIndex(
					const string &url
					, const string &index)
			{
				if (index == "")
				{
					throw exception("ElasticClient::CreateIndex(): index name is required");
				}

				string request = "PUT";
				string path = "/" + index;
				stringstream content;
				auto response = MakeRequest(url, request, path, content);
			}

			static
				void
				DeleteIndex(
					const string &url
					, const string &index)
			{
				if (index == "")
				{
					throw exception("ElasticClient::DeleteIndex(): index name is required");
				}

				string request = "DELETE";
				string path = "/" + index;
				stringstream content;
				auto response = MakeRequest(url, request, path, content);
			}

			static
				void
				DeleteType(
					const string &url
					, const string &index
					, const string &type)
			{
				if (index == "" || type == "")
				{
					throw exception("ElasticClient::DeleteType(): index and type names are required");
				}

				string request = "DELETE";
				string path = "/" + index + "/" + type;
				stringstream content;
				auto response = MakeRequest(url, request, path, content);
			}

			static
				int
				Count(
					const string &url
					, const string &index
					, const string &type)
			{
				string request = "GET";
				string path = (index == "" ? "" : "/" + index + (type == "" ? "" : "/" + type)) + "/_search?from=0&size=0";
				stringstream content;
				auto response = MakeRequest(url, request, path, content);
				return response["hits"]["total"].int_value();
			}

			static
				Mave::Mave
				Get(
					const string &url
					, const string &index
					, const string &type
					, const string &id)
			{
				string request = "GET";
				string path = (index == "" ? "" : "/" + index + (type == "" ? "" : "/" + type)) + id;
				stringstream content;
				auto response = MakeRequest(url, request, path, content);
				return Mave::FromJson(response);
			}

			static
				void
				Get(
					function<void(Mave::Mave&)> OnObject
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
							OnObject(Mave::FromJson(o["_source"]));
						}
					}
				}
			}

			static
				void
				Search(
					function<void(Mave::Mave&)> OnObject
					, const string &url
					, const string &index
					, const string &type
					, const string &attribute
					, vector<string> &values)
			{
				stringstream query;
				query << "{\"query\":{\"constant_score\":{\"filter\":{\"terms\":{\"" << attribute << "\":[";

				for (auto i = values.begin();;)
				{
					query << "\"" << *i << "\"";

					if (++i == values.end())
					{
						break;
					}

					query << ",";
				}

				query << "]}}}}}";

				ElasticClient::Search(OnObject, url, index, type, query.str());
			}

			static
				void
				Search(
					function<void(Mave::Mave&)> OnObject
					, const string &url
					, const string &index
					, const string &type
					, const string &query = ""
					, const long long from = 0
					, const long long count = -1
					, const long long batchCount = 10000)
			{
				string request = "GET";
				stringstream content;

				for (long long i = from, n = count < 0 ? batchCount : min(count, batchCount); n != 0; i += n)
				{
					content.str(""); if (query != "") content << query << endl;
					auto path = (index == "" ? "" : "/" + index + (type == "" ? "" : "/" + type)) + "/_search?from=" + to_string(i) + "&size=" + to_string(n);
					auto response = MakeRequest(url, request, path, content);

					for (auto &o : response["hits"]["hits"].array_items())
					{
						OnObject(Mave::FromJson(o["_source"]));
					}

					auto m = response["hits"]["total"].int_value();
					n = min(batchCount, (count < 0 ? m : min(from + count, m)) - i);
				}
			}
		};
	}
}
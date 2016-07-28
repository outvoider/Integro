#pragma once

#include <sstream>
#include <iomanip>
#include <chrono>

namespace Integro
{
	using namespace std;
	using namespace chrono;

	milliseconds
		TimeTToMilliseconds(
		const time_t t)
	{
		return duration_cast<milliseconds>(system_clock::from_time_t(t).time_since_epoch());
	}

	time_t
		MillisecondsToTimeT(
		const milliseconds m)
	{
		return system_clock::to_time_t(system_clock::time_point(m));
	}

	milliseconds
		LdapTimeToMilliseconds(
		const string &lt)
	{
		tm t;

		t.tm_isdst = -1;
		t.tm_year = stoi(lt.substr(0, 4)) - 1900;
		t.tm_mon = stoi(lt.substr(4, 2)) - 1;
		t.tm_mday = stoi(lt.substr(6, 2));
		t.tm_hour = stoi(lt.substr(8, 2));
		t.tm_min = stoi(lt.substr(10, 2));
		t.tm_sec = stoi(lt.substr(12, 2));

		return TimeTToMilliseconds(mktime(&t));
	}

	string
		MillisecondsToLdapTime(
		const milliseconds m)
	{
		stringstream s;
		auto t = MillisecondsToTimeT(m);
		auto gt = localtime(&t);

		s << gt->tm_year + 1900
			<< setfill('0') << setw(2) << gt->tm_mon + 1
			<< setfill('0') << setw(2) << gt->tm_mday
			<< setfill('0') << setw(2) << gt->tm_hour
			<< setfill('0') << setw(2) << gt->tm_min
			<< setfill('0') << setw(2) << gt->tm_sec
			<< ".0Z";

		return s.str();
	}

	milliseconds
		UtcToMilliseconds(
		const string &ut)
	{
		tm t;

		t.tm_isdst = -1;
		t.tm_year = stoi(ut.substr(0, 4)) - 1900;
		t.tm_mon = stoi(ut.substr(5, 2)) - 1;
		t.tm_mday = stoi(ut.substr(8, 2));
		t.tm_hour = stoi(ut.substr(11, 2));
		t.tm_min = stoi(ut.substr(14, 2));
		t.tm_sec = stoi(ut.substr(17, 2));

		return TimeTToMilliseconds(mktime(&t));
	}

	string
		MillisecondsToUtc(
		const milliseconds m
		, const bool hasT = false
		, const bool hasM = false
		, const bool hasZ = false)
	{
		stringstream s;
		auto t = MillisecondsToTimeT(m);
		auto gt = localtime(&t);

		s << gt->tm_year + 1900 << "-"
			<< setfill('0') << setw(2) << gt->tm_mon + 1 << "-"
			<< setfill('0') << setw(2) << gt->tm_mday << (hasT ? "T" : " ")
			<< setfill('0') << setw(2) << gt->tm_hour << ":"
			<< setfill('0') << setw(2) << gt->tm_min << ":"
			<< setfill('0') << setw(2) << gt->tm_sec;

		if (hasM)
		{
			s << ".00";
		}

		if (hasZ)
		{
			s << "Z";
		}

		return s.str();
	}
}
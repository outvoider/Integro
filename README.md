Integro documentation.

Author: Michael Ostapenko

Version: 2.3

Dependencies:

boost, openldap, ldapcpp, cyrus-sasl, open-ssl, mongo-client, simple-web-server, lmdb, spdlog, json11, freetds

Description:

Integro is both a lightweight framework and application to continuously copy arriving data.
Restarting Integro has a guarantee that no data is skipped or written multiple times.
Integro can copy data without unique identifiers discarding any duplicates.
This operation is not transactional, because most of the underlying data stores do not support transactions.
Consequently, copying the same data to one destination from multiple sources at the same time may well result in duplicates.
Currently, Integro supports tds, ldap, elasticsearch, mongodb and lmdb.

Files:

main.cpp			entry point to the program;
Integro.hpp			configuration and execution of the application;
Copy.hpp			copy routines and their dependencies;
Access.hpp			database access;
Mave.hpp			data representation and manipulation;
Milliseconds.hpp	time format conversions;
Synchronized.hpp	a synchronized (thread-safe) buffer;
Hash.hpp			string hashing;
Debug.hpp			debug routines and unit tests;

config.json			release configuration (may not be present);
debug.json			debug configuration (may not be present);

README.md			Integro's documentation;


main.cpp:

int
	main(
	int argc
	, wchar_t* argv[])

	Initializes mongodb driver and passes control to Intgro or Debug.


Integro.hpp:

	Integro methods.

Integro(
	int argc
	, wchar_t* argv[])

	argc		must be 3
	argv[1]		must be --env
	argv[2]		must be dev, staging or prod

	Creates log and metadata directories, initializes a logger and reads config.json.

void Run()

	Executes copy actions.

vector<function<void()>>
	CreateTdsActions()

vector<function<void()>>
	CreateLdapActions()

	Creates copy actions based on the configuration in config.json.

vector<string>
	ToStringVector(
	const Json &stringArray)

string
	Concatenate(
	const Json &stringArray)

	Creates a vector of/Concatenates strings from a JSON object representing an array of strings.

void
	Retry(
	function<void()> action
	, function<void(const string&)> OnError
	, const int errorTolerance = 0
	, const int attemptsCount = 10
	, const milliseconds pauseBetweenAttempts = milliseconds(1000))

	Executes an action, catches all its exceptions, logs their messages, pauses for some time and repeats or rethrows a caught exception.

void
	Proceed(
	function<void()> action
	, function<void(const string&)> OnError)

	action					an action to execute
	OnError					expected to log error messages
	errorTolerance			0 - rethrows on reaching attemptsCount (all exceptions count)
							1 - rethrows on reaching attemptsCount (typed exceptions count)
							>1 - ignores attemptsCount
	attemptsCount			number of caught exceptions before rethrowing
	pauseBetweenAttempts	number of milliseconds to wait in between action executions

	Executes an action, catches all its exceptions, logs their messages and returns.

static
	void
	OnEvent(
	const string &message)

static
	void
	OnError(
	const string &message)

	message		an informative or error message

	Logs the message.


Copy.hpp:

	Copy methods.

template <
	typename Datum
	, typename Time>
static
	void
	CopyDataInBulk(
	function<void(Time, function<void(Datum&)>)> LoadData
	, function<void(vector<Datum>&)> SaveData
	, function<Time()> LoadStartTime
	, function<void(Time)> SaveStartTime
	, function<Time(Datum&)> GetTime)

template <
	typename Datum
	, typename Time>
static
	void
	CopyDataInChunks(
	function<void(Time, function<void(Datum&)>)> LoadData
	, function<void(vector<Datum>&)> SaveData
	, function<Time()> LoadStartTime
	, function<void(Time)> SaveStartTime
	, function<Time(Datum&)> GetTime)

template <
	typename Datum
	, typename Time
	, typename Id>
static
	void
	CopyCappedDataInChunks(
	function<void(Id&, function<void(Datum&)>)> LoadCappedData
	, function<void(Time, function<void(Datum&)>)> LoadData
	, function<void(vector<Datum>&)> SaveData
	, function<Time()> LoadStartTime
	, function<Id()> LoadStartId
	, function<void(Time)> SaveStartTime
	, function<void(Id&)> SaveStartId
	, function<Time(Datum&)> GetTime
	, function<Id(Datum&)> GetId)

	LoadCappedData			expected to load data from a capped collection starting from startId
	LoadData				expected to load data from a data store starting from startTime
	SaveData				expected to save loaded data to a data store
	LoadStartTime			expected to load startTime
	LoadStartId				expected to load startId
	SaveStartTime			expected to save startTime
	SaveStartId				expected to save startId
	GetTime					expected to extract new startTime from a datum
	GetId					expected to extract new startId from a datum

	Copies data.
	In CopyDataInChunks and CopyCappedDataInChunks loading and saving run in parallel.
	CopyDataInChunks and CopyCappedDataInChunks require that incoming data be in non-decreasing order with respect to its startTime/startId values.
	In case of a failure, CopyDataInBulk will have to perform a full copy. CopyDataInChunks and CopyCappedDataInChunks will start from the saved startTime/startId.
	CopyCappedDataInChunks requires that data must have unique identifiers.
	If a capped collection does not contain a datum with startId, CopyCappedDataInChunks queries the missing data from a data store.

static
	function<void(milliseconds, function<void(Mave&)>)>
	LoadDataTds(
	const string &host
	, const string &user
	, const string &password
	, const string &database
	, const string &query)

	host		an address of a tds server to connect to; can be an ip address plus a port number pair, host name or server name from a freetds config file client.conf
	user		a name of a database user
	pasword		a password of a database user
	query		a sql query

	The returned function updates query's startTime with that that is provided.

static
	function<void(milliseconds, function<void(Mave&)>)>
	LoadDataLdap(
	const string &host
	, const int port
	, const string &user
	, const string &password
	, const string &node
	, const string &filter
	, const string &idAttribute
	, const string &timeAttribute
	, function<void(const string&)> OnError
	, function<void(const string&)> OnEvent)

	host			an address of an ldap server to connect to; can be an ip address or url
	port			a port number of a server
	user			a name of an ldap user
	pasword			a password of an ldap user
	node			a name of an ldap node
	filter			an ldap query; can be empty
	idAttribute		a name of an ldap id attribute, specific to the current ldap node
	timeAttribute	a name of an ldap time attribute, specific to the current ldap node
	OnError			expected to log error messages
	OnEvent			expected to log informative messages

static
	function<void(milliseconds, function<void(Mave&)>)>
	LoadDataMongo(
	const string &url
	, const string &database
	, const string &collection
	, const string &timeAttribute)

	Creates an index on timeAttribute.

static
	function<void(OID&, function<void(Mave&)>)>
	LoadCappedDataMongo(
	const string &url
	, const string &database
	, const string &collection)

	Retuns a function that loads data from a tds/ldap/mongodb data store/capped collection starting from startTime/startId.
	Loaded data is expected to be passed to CopyData... functions via a callback.

static
	function<void(vector<Mave>&)>
	SaveDataMongo(
	const string &url
	, const string &database
	, const string &collection)

	url				an address of a mongodb server to connect to; can be an ip address or url plus a port number pair prefixed with 'mongodb://'
	database		a name of a mongodb database
	collection		a name of a database collection
	timeAttribute	a name of a time attribute in a collection

static
	function<void(vector<Mave>&)>
	SaveDataElastic(
	const string &url
	, const string &index
	, const string &type)

	url			an address of an elasticsearch server to connect to; can be an ip address or url
	index		a name of an elasticsearch index
	type		a name of an index type

	Retuns a function that saves data to a mongodb/elasticsearch data store.
	Data to be saved is expected to be passed from CopyData... functions in a vector.

static
	function<void(vector<Mave>&)>
	ProcessDataTds(
	const string &channelName
	, const string &modelName
	, const string &model
	, const string &action
	, const vector<string> &targetStores)

static
	function<void(vector<Mave>&)>
	ProcessDataLdap(
	const string &idAttribute
	, const string &channelName
	, const string &modelName
	, const string &model
	, const string &action)

static
	function<void(vector<Mave>&)>
	ProcessDataLdapElastic()

static
	function<void(vector<Mave>&)>
	ProcessDataMongo(
	const string &channelName
	, const string &modelName
	, const string &model
	, const string &action)

	idAttribute		a name of an ldap id attribute
	channelName		a channel name (domain specific parameter)
	modelName		a model name (domain specific parameter)
	model			a model (domain specific parameter)
	action			an action (domain specific parameter)
	targetStores	names of target data stores; currently, used as a dummy

	Retuns a function that processes data.
	Data to be processed is expected to be passed from 'decorated' SaveData... functions.
	A 'decorated' SaveData... function is any function with the same sinature as that of the functions returned by SaveData... functions.
	A typical example of a 'decorated' SaveData... function is a lambda which is passed to CopyData... functions.
	Such a lambda may first process data, then remove duplicates from it and finally save the result to mongodb and elasticsearch data stores.

static
	function<milliseconds()>
	LoadStartTimeLmdb(
	const string &path
	, const string &key)

static
	function<OID()>
	LoadStartIdLmdb(
	const string &path
	, const string &key)

static
	function<void(milliseconds)>
	SaveStartTimeLmdb(
	const string &path
	, const string &key)

static
	function<void(OID&)>
	SaveStartIdLmdb(
	const string &path
	, const string &key)

	path	a path to an lmdb database
	key		a name of a startTime/startId key in a database

	Loads/Saves startTime/startId from/to an lmdb database

static
	function<milliseconds(Mave&)>
	GetTimeTds(
	const string &timeAttribute)

static
	function<milliseconds(Mave&)>
	GetTimeLdap(
	const string &timeAttribute)

static
	function<milliseconds(Mave&)>
	GetTimeMongo(
	const string &timeAttribute)

static
	function<OID(Mave&)>
	GetIdMongo(
	const string &idAttribute)

	timeAttribute	a name of a time attribute in a datum
	idAttribute		a name of an id attribute in a datum

	Retuns a function that extracts new startTime/startId from a datum.
	Datum from which startTime/startId to be extracted is expected to be passed from CopyData... functions.

static
	function<void(vector<Mave>&)>
	RemoveDuplicates(
	const string &descriptorAttribute
	, const string &sourceAttribute
	, function<void(const string&, vector<int>&, function<void(Mave&)>)> LoadData)

	descriptorAttribute		a name of a descriptor attribute in a datum
	sourceAttribute			a name of a source attribute in a datum
	LoadData				expected to load data with provided descriptor attribute's values

	Retuns a function that removes datums which are already present in a data store from data to be filtered.
	Data to be filtered is expected to be passed from the aforementioned 'decorated' SaveData... functions.
	A descriptorAttribute attribute is added to each datum.
	A descriptorAttribute attribute's value is a hash of a sourceAttribute attribute's value.
	RemoveDuplicates fetches all data with descriptorAttribute attribute's values of data to be filtered.
	Datums in the fetched data are removed from data to be filtered.

static
	function<void(const string&, vector<int>&, function<void(Mave&)>)>
	LoadDuplicateDataMongo(
	const string &url
	, const string &database
	, const string &collection
	, const string &descriptorAttribute)

	url						an address of a mongodb server to connect to; can be an ip address or url plus a port number pair prefixed with 'mongodb://'
	database				a name of a mongodb database
	collection				a name of a database collection
	descriptorAttribute		a name of a descriptor attribute in a collection

	Creates an index on descriptorAttribute.

static
	function<void(const string&, vector<int>&, function<void(Mave&)>)>
	LoadDuplicateDataElastic(
	const string &url
	, const string &index
	, const string &type)

	url			an address of an elasticsearch server to connect to; can be an ip address or url
	index		a name of an elasticsearch index
	type		a name of an index type

	Retuns a function that loads data with provided attribute's values from a mongodb/elasticsearch store.
	Loaded data is passed to a caller via a callback.


Access.hpp:

	ClientTds methods.

static
	void
	ExecuteCommand(const string &host
	, const string &user
	, const string &password
	, const string &database
	, const string &sql)

	Establishes a connection to a database, executes a sql command on it and closes the connection.

void
	ExecuteCommand(
	const string &database
	, const string &sql)

	Executes a sql command on a currently connected database.

ClientTds(
	const string &host
	, const string &user
	, const string &password)

	Establishes a connection to a database.

~ClientTds()

	Closes a connection to a database.

static
	void
	ExecuteQuery(
	const string &host
	, const string &user
	, const string &password
	, const string &database
	, const string &sql
	, function<void(Mave&)> OnRow)

	Establishes a connection to a database, executes a sql query on it and closes the connection.

void
	FetchResults(
	function<void(Mave&)> OnRow)

	Executes a sql query on a currently connected database.
	The fetched data is returned to a caller via a callback OnRow.

	host		an address of a tds server to connect to; can be an ip address plus a port number pair, host name or server name from a freetds config file client.conf
	user		a name of a database user
	pasword		a password of a database user
	database	a name of a database to connect to
	sql			a sql command/query
	OnRow		Expected to process the data fetched from a database

static
	int
	HandleError(
	DBPROCESS* dbproc
	, int severity
	, int dberr
	, int oserr
	, char* dberrstr
	, char* oserrstr)

static
	int
	HandleMessage(
	DBPROCESS* dbproc
	, DBINT msgno
	, int msgstate
	, int severity
	, char* msgtext
	, char* srvname
	, char* procname
	, int line)

	A private callback for a freetds dblib library that formats its error/informative messages and passes them to Infin's OnError/OnEvent.
	See dblib for the descriptions of parameters.

static
	string
	ToTrimmedString(
	char *begin
	, char *end)

	begin		a pointer to the beginning of a string
	end			a pointer to the end of a string

	Constructs a string from an input character array without leading and trailing spaces.

	Infin methods.

Infin(
	const string &configPath
	, const string &applicationName
	, function<void(const string&)> OnError
	, function<void(const string&)> OnEvent)
	: ConfigPath(configPath)
	, ApplicationName(applicationName)
	, OnError(OnError)
	, OnEvent(OnEvent)

~Infin()

	configPath			a path to a freetds configuration file client.conf
	applicationName		a name of an application; can be anything
	OnError				expected to log error messages
	OnEvent				expected to log informative messages

	Initializes/Deinitializes a freetds dblib library.
	ClientTds has a private static field of Infin, which must be initialized according to c++ rules.
	This design is intended to prevent subtle bugs while using a freetds library, which should be treated as one global instance.


	ClientLdap methods.

static
	int
	SearchSome(
	const string &host
	, const int port
	, const string &user
	, const string &password
	, const string &node
	, const string &filter
	, function<void(Mave&)> OnEntry)

static
	void
	Search(
	const string &host
	, const int port
	, const string &user
	, const string &password
	, const string &node
	, const string &filter
	, const string &idAttribute
	, const string &timeAttribute
	, const milliseconds lowerBound
	, const milliseconds upperBound
	, function<void(Mave&)> OnEntry
	, function<void(const string&)> OnError
	, function<void(const string&)> OnEvent)

	host			an address of an ldap server to connect to; can be an ip address or url
	port			a port number of a server
	user			a name of an ldap user
	pasword			a password of an ldap user
	node			a name of an ldap node
	filter			an ldap query; can be empty
	idAttribute		a name of an ldap id attribute, specific to the current ldap node
	timeAttribute	a name of an ldap time attribute, specific to the current ldap node
	lowerBound		a lower bound of timeAttribute attribute's value
	upperBound		an upper bound of timeAttribute attribute's value
	OnEntry			expected to process fetched data
	OnError			expected to log error messages
	OnEvent			expected to log informative messages

	Queries an ldap server for data with timeAttribute attribute's values in [lowerBound, upperBound].
	If lowerBound/upperBound equals to 0, the bound is ignored.
	Ldap imposes a limit on the number of records that may be received in one query.
	To fetch all records from a server, median search over time intervals is used.
	During search the intervals are gradually divided into smaller ones.
	The division stops when the intervals become small enough to allow fetching all their data in one query.
	Search fails if an interval contains more records with the same time than may be received in one query.
	On any failure, Search will report it and continue execution from the next interval.


	ClientMongo methods.

static
	DBClientBase*
	Create(
	const string &url)

	Establishes a connection to a mongodb server.

static
	unsigned long long
	Count(
	const string &url
	, const string &database
	, const string &collection)

	Fetches the number of records in a mongodb collection.

static
	void
	QueryCapped(
	const string &url
	, const string &database
	, const string &collection
	, const mongo::OID &lowerBound
	, function<void(Mave&)> OnObject)

	lowerBound		a lower bound for id attribute's values

	Queries a mongodb server for data with id attribute's values greater than or equal to lowerBound.

static
	void
	Query(
	const string &url
	, const string &database
	, const string &collection
	, const string &timeAttribute
	, const milliseconds lowerBound
	, const milliseconds upperBound
	, function<void(Mave&)> OnObject)

	timeAttribute	a name of a time attribute in a collection
	lowerBound		a lower bound for timeAttribute attribute's values
	upperBound		a upper bound for timeAttribute attribute's values

	Queries a mongodb server for data with timeAttribute attribute's values in [lowerBound, upperBound].

static
	void
	Query(
	const string &url
	, const string &database
	, const string &collection
	, mongo::Query &query
	, function<void(Mave&)> OnObject)

	query		an object describing a query to a mongodb database
	OnObject	expected to process fetched data

	Queries a mongodb server for data.
	Fetched data satisfies query.
	Fetched data is passed to a caller via a callback OnObject.

static
	bool
	Contains(
	const string &url
	, const string &database
	, const string &collection
	, const mongo::OID &id)

	id		a unique id of an object to find

	Queries a mongodb server for containment of an object with an id attribute's value equal to id.

static
	void
	Insert(
	const string &url
	, const string &database
	, const string &collection
	, const Mave &object)

	object		an object to be inserted into a database

	Inserts an object into a mongodb database.

static
	void
	Upsert(
	const string &url
	, const string &database
	, const string &collection
	, const vector<Mave> &objects)

	objects		objects to be inserted into a database

	Upserts objects into a mongodb database.

static
	void
	CreateIndex(
	const string &url
	, const string &database
	, const string &collection
	, const mongo::IndexSpec &descriptor)

	descriptor		a description of an index to be created

	Creates an index in a mongodb database.

static
	void
	CreateCollection(
	const string &url
	, const string &database
	, const string &collection
	, const bool isCapped = true
	, const int maxDocumentsCount = 5000
	, const long long maxCollectionSize = 5242880)

	isCapped				defines if a collection to be created is capped
	maxDocumentsCount		sets the maximum number of documents that a capped collection can hold
	maxCollectionSize		sets the maximum size of a capped collection in bytes

	Creates a collection in a mongodb database.

static
	void
	DropCollection(
	const string &url
	, const string &database
	, const string &collection)

	Drops a collection in a mongodb database.

static
	void
	DropDatabase(
	const string &url
	, const string &database)

	Drops a database on a mongodb server.

	url				an address of a mongodb server to connect to; can be an ip address or url plus a port number pair prefixed with 'mongodb://'
	database		a name of a mongodb database
	collection		a name of a database collection


	ClientElastic methods.

static
	Json
	MakeRequest(
	const string &url
	, const string &request
	, const string &path
	, stringstream &content)

	request		a type of an http request, that is GET, PUT, POST, DELETE
	path		a url part that combines an index, type and special elasticsearch endpoints.
	content		content of an elasticsearch request

	Makes a request to elasticsearch, hndles errors and returns the result.

static
	void
	Index(
	const vector<Mave> &objects
	, const string &url
	, const string &index
	, const string &type
	, const int maxBatchCount = 10000
	, const int maxBatchSize = 100000000)

	objects			objects to be inserted into an elasticsearch index
	maxBatchCount	maximum number of objects to be sent in one request
	maxBatchSize	maximum size of objects to be sent in one request

	Inserts objects into an elasticsearch index.

static
	void
	Delete(
	const vector<string> &ids
	, const string &url
	, const string &index
	, const string &type)

	ids		unique identifiers of objects to be deleted

	Deletes objects from an elasticsearch index.

static
	void
	CreateIndex(
	const string &url
	, const string &index)

	Creates an elasticsearch index.

static
	void
	DeleteIndex(
	const string &url
	, const string &index)

	Deletes an elasticsearch index.

static
	int
	Count(
	const string &url
	, const string &index
	, const string &type)

	Counts number of objects in an elasticsearch index.

static
	Mave
	Get(
	const string &url
	, const string &index
	, const string &type
	, const string &id)

	id		a unique identifier of an object

	Fetches an object from an elasticsearch index with an id attribute's value equal to id.

static
	void
	Get(
	function<void(Mave&)> OnObject
	, const vector<string> &ids
	, const string &url
	, const string &index
	, const string &type)

	ids		unique identifiers of objects

	Fetches objects from an elasticsearch index with id attribute's values in ids.

static
	void
	Search(
	function<void(Mave&)> OnObject
	, const string &url
	, const string &index
	, const string &type
	, const string &attribute
	, vector<string> &values)

	attribute		a name of an attribute
	values			values of an attribute

	Fetches objects from an elasticsearch index with attribute attribute's values in values.

static
	void
	Search(
	function<void(Mave&)> OnObject
	, const string &url
	, const string &index
	, const string &type
	, const string &query = ""
	, const long long from = 0
	, const long long count = -1
	, const long long batchCount = 10000)

	OnObject		expected to process fetched objects
	query			an elasticsearch query
	from			an index from which querying starts
	count			a number of objects to query
	batchCount		a maximum number of objects that a server will return

	Fetches no more than count objects from an elasticsearch index starting from from.
	If count equals to -1, it is ignored.
	Fetched objects satisfy query.
	Fetched objects are passed to a caller via a callback OnObject.

	url			an address of an elasticsearch server to connect to; can be an ip address or url
	index		a name of an elasticsearch index
	type		a name of an index type


	ClientLmdb methods.

static
	string
	Get(
	const string &path
	, const string &key)

static
	string
	GetOrDefault(
	const string &path
	, const string &key)

	If key does not exist, returns an empty string.

static
	bool
	TryGet(
	const string &path
	, const string &key
	, string &value)

	Fetches a key value from an lmdb database.
	If key does not exist, sets value to an empty string and returns false.

static
	void
	Set(
	const string &path
	, const string &key
	, const string &value)

	Saves a key value to an lmdb database.

static
	void
	Remove(
	const string &path
	, const string &key)

	Removes a key from an lmdb database.

static
	void
	Query(
	const string &path
	, function<void(const string &key, const string &value)> OnKyeValue)

	OnKyeValue		expected to process fetched key/value pairs
	
	Fetches all key/value pairs from an lmdb database.

	path		a path to an lmdb database
	key			a key in an lmdb database
	value		a key value in an lmdb database


Mave.hpp:


	Mave Types.

	NULL_T
	, VECTOR
	, MAP
	, BOOL
	, INT
	, LONG
	, DOUBLE
	, STRING
	, MILLISECONDS
	, BSON_OID


	Mave value classes.

class Value

	An abstract base class.
	Has GetType method, which returns the type of a value.

template <Type type>
	class EValue final : public Value

	Represents an empty value.
	Does not have GetValue method.

template <Type type, typename T>
	class VValue final : public Value

	Accepts and returns values by value.
	Has GetValue method, which returns a value.
	Expected to be used with simple types such as int, long long, etc.

template <Type type, typename T>
	class RValue final : public Value

	Accepts and returns values by reference.
	Has GetValue method, which returns a value.
	Expected to be used with complex types such as vector, map, etc.


	Mave methods.

Type
	GetType() const
	
	Returns the type of a mave.

bool
	HasType(
	const Type type) const

	Returns true if a mave is of type type.

void
	Assert(
	const Type type) const

	Throws an exception if a mave is not of type type.

	type		one of Mave's types
	
Mave()
Mave(nullptr_t)
template <class V, typename std::enable_if<std::is_constructible<Mave, typename V::value_type>::value, int>::type = 0> Mave(const V &v)
Mave(vector<Mave> &value)
Mave(vector<Mave> &&value)
template <class M, typename std::enable_if<std::is_constructible<std::string, typename M::key_type>::value && std::is_constructible<Mave, typename M::mapped_type>::value, int>::type = 0> Mave(const M &m)
Mave(map<string, Mave> &value)
Mave(map<string, Mave> &&value)
Mave(bool value)
Mave(int value)
Mave(long long value)
Mave(double value)
Mave(const string &value)
Mave(string &&value)
Mave(const char *value)
Mave(milliseconds value)
Mave(OID &value)
Mave(OID &&value)

	Constructs a mave from a provided value of corresponding type.

	value		a value to be stored in a mave

bool IsNullptr() const
bool IsVector() const
bool IsMap() const
bool IsBool() const
bool IsInt() const
bool IsLong() const
bool IsDouble() const
bool IsString() const
bool IsMilliseconds() const
bool IsBsonOid()

	Returns true if a mave is of corresponding type.

nullptr_t AsNullptr() const
vector<Mave>& AsVector() const
map<string, Mave>& AsMap() const
bool AsBool() const
int AsInt() const
long long AsLong()
double AsDouble() const
string& AsString() const
milliseconds AsMilliseconds() const
OID& AsBsonOid() const

	Returns a value contained in a mave if it is of corresponding type.
	Otherwise throws an exception.

Mave&
	operator[](
	int index) const

	index		index in a vector

Mave&
	operator[](
	const string &key) const

	key			key in a map

	Returns a mave at index/key.
	If index/key does not exist, throws an exception.


	Mave extension.

	To extend Mave with an additional type, it is required to
		
		add a new Mave type:

			MY_TYPE
		
		define a VValue/RValue type:

			typedef VValue<MY_TYPE, SomeType> MyType;

			or

			typedef RValue<MY_TYPE, SomeType> MyType;
		
		implement methods:

			Mave(SomeType value) or Mave(const SomeType &value) and Mave(SomeType &&value)
			bool IsMyType() const
			SomeType AsMyType() const


	Mave can also be extended by introducing new auxiliary functions or updating those that already exist with new type handlers.
	These are the auxiliary functions that are currently implemented.

Mave
	Copy(
	const Mave &mave)

	Makes a deep copy of a mave.

void
	ToStringStream(
	const Mave &mave
	, stringstream &buffer)

	buffer		a string stream
	
	Serializes a mave to a string stream.

string
	ToString(
	const Mave &mave)

	Serializes a mave to a string.

int
	Hash(
	const Mave &mave)

	Computes a mave's hash value.

Mave
	ToMave(
	const LDAPEntry &ldap)

	ldap		an ldap entry
	
	Converts an ldap entry to a mave.

Mave
	ToMave(
	const mongo::BSONObj &bson)

	bson		a BSON object

	Converts a BSON object to a mave.

mongo::BSONObj
	ToBson(
	const Mave &mave)

	Converts a mave to a BSON object.

Mave
	ToMave(
	const Json &json)

	json		a JSON object

	Converts a JSON object to a mave.

Json
	ToJson(
	const Mave &mave)

	Converts a mave to a JSON object.

	mave		a mave object


Milliseconds.hpp:

milliseconds
	TimeTToMilliseconds(
	const time_t t)

	t		time in time_t format

	Converts time from time_t to milliseconds format.

time_t
	MillisecondsToTimeT(
	const milliseconds m)

	Converts time from milliseconds to time_t format.

milliseconds
	LdapTimeToMilliseconds(
	const string &lt)

	lt		time in ldap format

	Converts time from ldap to milliseconds format.

string
	MillisecondsToLdapTime(
	const milliseconds m)

	Converts time from milliseconds to ldap format.

milliseconds
	UtcToMilliseconds(
	const string &ut)

	ut		time in UTC format

	Converts time from UTC to milliseconds format.

string
	MillisecondsToUtc(
	const milliseconds m
	, const bool hasT = false
	, const bool hasM = false
	, const bool hasZ = false)

	hasT		Adds 'T' between date and time if true
	hasM		Adds '.'milliseconds after seconds if true
	hasZ		Adds 'Z' at the end if true

	Converts time from milliseconds to UTC format.

	m		time in milliseconds format


Synchronized.hpp:


	SynchronizedBuffer methods.

SynchronizedBuffer()

	Constructs a SynchronizedBuffer object.

bool
	IsEmpty()

	Returns true if a buffer is empty.

int
	Size()

	Returns the size of a buffer.

void
	AddOne(
	const T &item)

void
	AddOne(
	const T &&item)

	item		an object to be added to a buffer

	Adds item to a buffer.

vector<T>
	GetAll()

	Returns all items in a buffer and clears it.
	
	All methods use a spin lock for thread-safety.


Hash.hpp:


inline
	int
	Hash(
	unsigned char data
	, int hash = HASH_SEED)

int
	Hash(
	const unsigned char *data
	, int count, int hash = HASH_SEED)

	count		the length of data
	hash		a seed hash value

int
	Hash(
	const string &data)

	data		data to be hashed

	Computes hash of data.


Debug.hpp:


	Debug methods (main).

Debug()

	Constructs and configures a Debug object using debug.json.

void
	Run()

	Entry point. Calls all debug routines.

void
	CopyCorrectnessTest()

void
	CopyGeneralTest()

void
	CopyCappedCorrectnessTest()

	A unit test for CopyDataInChunks/CopyDataInChunks/CopyCappedDataInChunks. Simulates failures in its dependencies.
	Dependencies are in-memory collections/real data stores/in-memory collections.

void
	CopyPerformanceTest()

	Populates a data store for a period of time with interruptions, and therefore duplicates.

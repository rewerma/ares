// Generated from /Users/rewerma/Develop/git_workspace/ares/ares-parser/src/main/java/com/github/ares/parser/antlr4/sparksql/SqlBaseParser.g4 by ANTLR 4.12.0
package com.github.ares.parser.antlr4.sparksql;
import com.github.ares.org.antlr.v4.runtime.atn.*;
import com.github.ares.org.antlr.v4.runtime.dfa.DFA;
import com.github.ares.org.antlr.v4.runtime.*;
import com.github.ares.org.antlr.v4.runtime.misc.*;
import com.github.ares.org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast", "CheckReturnValue"})
public class SqlBaseParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.12.0", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		SEMICOLON=1, LEFT_PAREN=2, RIGHT_PAREN=3, COMMA=4, DOT=5, LEFT_BRACKET=6, 
		RIGHT_BRACKET=7, ADD=8, AFTER=9, ALL=10, ALTER=11, ANALYZE=12, AND=13, 
		ANTI=14, ANY=15, ARCHIVE=16, ARRAY=17, AS=18, ASC=19, AT=20, AUTHORIZATION=21, 
		BETWEEN=22, BOTH=23, BUCKET=24, BUCKETS=25, BY=26, CACHE=27, CASCADE=28, 
		CASE=29, CAST=30, CATALOG=31, CATALOGS=32, CHANGE=33, CHECK=34, CLEAR=35, 
		CLUSTER=36, CLUSTERED=37, CODEGEN=38, COLLATE=39, COLLECTION=40, COLUMN=41, 
		COLUMNS=42, COMMENT=43, COMMIT=44, COMPACT=45, COMPACTIONS=46, COMPUTE=47, 
		CONCATENATE=48, CONSTRAINT=49, COST=50, CREATE=51, CROSS=52, CUBE=53, 
		CURRENT=54, CURRENT_DATE=55, CURRENT_TIME=56, CURRENT_TIMESTAMP=57, CURRENT_USER=58, 
		DAY=59, DAYOFYEAR=60, DATA=61, DATABASE=62, DATABASES=63, DATEADD=64, 
		DATEDIFF=65, DBPROPERTIES=66, DEFINED=67, DELETE=68, DELIMITED=69, DESC=70, 
		DESCRIBE=71, DFS=72, DIRECTORIES=73, DIRECTORY=74, DISTINCT=75, DISTRIBUTE=76, 
		DIV=77, DROP=78, ELSE=79, END=80, ESCAPE=81, ESCAPED=82, EXCEPT=83, EXCHANGE=84, 
		EXISTS=85, EXPLAIN=86, EXPORT=87, EXTENDED=88, EXTERNAL=89, EXTRACT=90, 
		FALSE=91, FETCH=92, FIELDS=93, FILTER=94, FILEFORMAT=95, FIRST=96, FOLLOWING=97, 
		FOR=98, FOREIGN=99, FORMAT=100, FORMATTED=101, FROM=102, FULL=103, FUNCTION=104, 
		FUNCTIONS=105, GLOBAL=106, GRANT=107, GROUP=108, GROUPING=109, HAVING=110, 
		HOUR=111, IF=112, IGNORE=113, IMPORT=114, IN=115, INDEX=116, INDEXES=117, 
		INNER=118, INPATH=119, INPUTFORMAT=120, INSERT=121, INTERSECT=122, INTERVAL=123, 
		INTO=124, IS=125, ITEMS=126, JOIN=127, KEYS=128, LAST=129, LATERAL=130, 
		LAZY=131, LEADING=132, LEFT=133, LIKE=134, ILIKE=135, LIMIT=136, LINES=137, 
		LIST=138, LOAD=139, LOCAL=140, LOCATION=141, LOCK=142, LOCKS=143, LOGICAL=144, 
		MACRO=145, MAP=146, MATCHED=147, MERGE=148, MICROSECOND=149, MILLISECOND=150, 
		MINUTE=151, MONTH=152, MSCK=153, NAMESPACE=154, NAMESPACES=155, NATURAL=156, 
		NO=157, NOT=158, NULL=159, NULLS=160, OF=161, ON=162, ONLY=163, OPTION=164, 
		OPTIONS=165, OR=166, ORDER=167, OUT=168, OUTER=169, OUTPUTFORMAT=170, 
		OVER=171, OVERLAPS=172, OVERLAY=173, OVERWRITE=174, PARTITION=175, PARTITIONED=176, 
		PARTITIONS=177, PERCENTILE_CONT=178, PERCENTILE_DISC=179, PERCENTLIT=180, 
		PIVOT=181, PLACING=182, POSITION=183, PRECEDING=184, PRIMARY=185, PRINCIPALS=186, 
		PROPERTIES=187, PURGE=188, QUARTER=189, QUERY=190, RANGE=191, RECORDREADER=192, 
		RECORDWRITER=193, RECOVER=194, REDUCE=195, REFERENCES=196, REFRESH=197, 
		RENAME=198, REPAIR=199, REPEATABLE=200, REPLACE=201, RESET=202, RESPECT=203, 
		RESTRICT=204, REVOKE=205, RIGHT=206, RLIKE=207, ROLE=208, ROLES=209, ROLLBACK=210, 
		ROLLUP=211, ROW=212, ROWS=213, SECOND=214, SCHEMA=215, SCHEMAS=216, SELECT=217, 
		SEMI=218, SEPARATED=219, SERDE=220, SERDEPROPERTIES=221, SESSION_USER=222, 
		SET=223, SETMINUS=224, SETS=225, SHOW=226, SKEWED=227, SOME=228, SORT=229, 
		SORTED=230, START=231, STATISTICS=232, STORED=233, STRATIFY=234, STRUCT=235, 
		SUBSTR=236, SUBSTRING=237, SYNC=238, SYSTEM_TIME=239, SYSTEM_VERSION=240, 
		TABLE=241, TABLES=242, TABLESAMPLE=243, TBLPROPERTIES=244, TEMPORARY=245, 
		TERMINATED=246, THEN=247, TIME=248, TIMESTAMP=249, TIMESTAMPADD=250, TIMESTAMPDIFF=251, 
		TO=252, TOUCH=253, TRAILING=254, TRANSACTION=255, TRANSACTIONS=256, TRANSFORM=257, 
		TRIM=258, TRUE=259, TRUNCATE=260, TRY_CAST=261, TYPE=262, UNARCHIVE=263, 
		UNBOUNDED=264, UNCACHE=265, UNION=266, UNIQUE=267, UNKNOWN=268, UNLOCK=269, 
		UNSET=270, UPDATE=271, USE=272, USER=273, USING=274, VALUES=275, VERSION=276, 
		VIEW=277, VIEWS=278, WEEK=279, WHEN=280, WHERE=281, WINDOW=282, WITH=283, 
		WITHIN=284, YEAR=285, ZONE=286, EQ=287, NSEQ=288, NEQ=289, NEQJ=290, LT=291, 
		LTE=292, GT=293, GTE=294, PLUS=295, MINUS=296, ASTERISK=297, SLASH=298, 
		PERCENT=299, TILDE=300, AMPERSAND=301, PIPE=302, CONCAT_PIPE=303, HAT=304, 
		COLON=305, ARROW=306, HENT_START=307, HENT_END=308, STRING=309, BIGINT_LITERAL=310, 
		SMALLINT_LITERAL=311, TINYINT_LITERAL=312, INTEGER_VALUE=313, EXPONENT_VALUE=314, 
		DECIMAL_VALUE=315, FLOAT_LITERAL=316, DOUBLE_LITERAL=317, BIGDECIMAL_LITERAL=318, 
		IDENTIFIER=319, BACKQUOTED_IDENTIFIER=320, SIMPLE_COMMENT=321, BRACKETED_COMMENT=322, 
		WS=323, UNRECOGNIZED=324;
	public static final int
		RULE_singleStatement = 0, RULE_singleExpression = 1, RULE_singleTableIdentifier = 2, 
		RULE_singleMultipartIdentifier = 3, RULE_singleFunctionIdentifier = 4, 
		RULE_singleDataType = 5, RULE_singleTableSchema = 6, RULE_statement = 7, 
		RULE_configKey = 8, RULE_configValue = 9, RULE_unsupportedHiveNativeCommands = 10, 
		RULE_createTableHeader = 11, RULE_replaceTableHeader = 12, RULE_bucketSpec = 13, 
		RULE_skewSpec = 14, RULE_locationSpec = 15, RULE_commentSpec = 16, RULE_query = 17, 
		RULE_insertInto = 18, RULE_partitionSpecLocation = 19, RULE_partitionSpec = 20, 
		RULE_partitionVal = 21, RULE_namespace = 22, RULE_namespaces = 23, RULE_describeFuncName = 24, 
		RULE_describeColName = 25, RULE_ctes = 26, RULE_namedQuery = 27, RULE_tableProvider = 28, 
		RULE_createTableClauses = 29, RULE_propertyList = 30, RULE_property = 31, 
		RULE_propertyKey = 32, RULE_propertyValue = 33, RULE_constantList = 34, 
		RULE_nestedConstantList = 35, RULE_createFileFormat = 36, RULE_fileFormat = 37, 
		RULE_storageHandler = 38, RULE_resource = 39, RULE_dmlStatementNoWith = 40, 
		RULE_queryOrganization = 41, RULE_multiInsertQueryBody = 42, RULE_queryTerm = 43, 
		RULE_queryPrimary = 44, RULE_sortItem = 45, RULE_fromStatement = 46, RULE_fromStatementBody = 47, 
		RULE_querySpecification = 48, RULE_transformClause = 49, RULE_selectClause = 50, 
		RULE_intoClause = 51, RULE_setClause = 52, RULE_matchedClause = 53, RULE_notMatchedClause = 54, 
		RULE_matchedAction = 55, RULE_notMatchedAction = 56, RULE_assignmentList = 57, 
		RULE_assignment = 58, RULE_whereClause = 59, RULE_havingClause = 60, RULE_hint = 61, 
		RULE_hintStatement = 62, RULE_fromClause = 63, RULE_temporalClause = 64, 
		RULE_aggregationClause = 65, RULE_groupByClause = 66, RULE_groupingAnalytics = 67, 
		RULE_groupingElement = 68, RULE_groupingSet = 69, RULE_pivotClause = 70, 
		RULE_pivotColumn = 71, RULE_pivotValue = 72, RULE_lateralView = 73, RULE_setQuantifier = 74, 
		RULE_relation = 75, RULE_joinRelation = 76, RULE_joinType = 77, RULE_joinCriteria = 78, 
		RULE_sample = 79, RULE_sampleMethod = 80, RULE_identifierList = 81, RULE_identifierSeq = 82, 
		RULE_orderedIdentifierList = 83, RULE_orderedIdentifier = 84, RULE_identifierCommentList = 85, 
		RULE_identifierComment = 86, RULE_relationPrimary = 87, RULE_inlineTable = 88, 
		RULE_functionTable = 89, RULE_tableAlias = 90, RULE_rowFormat = 91, RULE_multipartIdentifierList = 92, 
		RULE_multipartIdentifier = 93, RULE_multipartIdentifierPropertyList = 94, 
		RULE_multipartIdentifierProperty = 95, RULE_tableIdentifier = 96, RULE_functionIdentifier = 97, 
		RULE_namedExpression = 98, RULE_namedExpressionSeq = 99, RULE_partitionFieldList = 100, 
		RULE_partitionField = 101, RULE_transform = 102, RULE_transformArgument = 103, 
		RULE_expression = 104, RULE_expressionSeq = 105, RULE_booleanExpression = 106, 
		RULE_predicate = 107, RULE_valueExpression = 108, RULE_datetimeUnit = 109, 
		RULE_primaryExpression = 110, RULE_constant = 111, RULE_comparisonOperator = 112, 
		RULE_arithmeticOperator = 113, RULE_predicateOperator = 114, RULE_booleanValue = 115, 
		RULE_interval = 116, RULE_errorCapturingMultiUnitsInterval = 117, RULE_multiUnitsInterval = 118, 
		RULE_errorCapturingUnitToUnitInterval = 119, RULE_unitToUnitInterval = 120, 
		RULE_intervalValue = 121, RULE_colPosition = 122, RULE_dataType = 123, 
		RULE_qualifiedColTypeWithPositionList = 124, RULE_qualifiedColTypeWithPosition = 125, 
		RULE_colTypeList = 126, RULE_colType = 127, RULE_complexColTypeList = 128, 
		RULE_complexColType = 129, RULE_whenClause = 130, RULE_windowClause = 131, 
		RULE_namedWindow = 132, RULE_windowSpec = 133, RULE_windowFrame = 134, 
		RULE_frameBound = 135, RULE_qualifiedNameList = 136, RULE_functionName = 137, 
		RULE_qualifiedName = 138, RULE_errorCapturingIdentifier = 139, RULE_errorCapturingIdentifierExtra = 140, 
		RULE_identifier = 141, RULE_strictIdentifier = 142, RULE_quotedIdentifier = 143, 
		RULE_number = 144, RULE_alterColumnAction = 145, RULE_ansiNonReserved = 146, 
		RULE_strictNonReserved = 147, RULE_nonReserved = 148;
	private static String[] makeRuleNames() {
		return new String[] {
			"singleStatement", "singleExpression", "singleTableIdentifier", "singleMultipartIdentifier", 
			"singleFunctionIdentifier", "singleDataType", "singleTableSchema", "statement", 
			"configKey", "configValue", "unsupportedHiveNativeCommands", "createTableHeader", 
			"replaceTableHeader", "bucketSpec", "skewSpec", "locationSpec", "commentSpec", 
			"query", "insertInto", "partitionSpecLocation", "partitionSpec", "partitionVal", 
			"namespace", "namespaces", "describeFuncName", "describeColName", "ctes", 
			"namedQuery", "tableProvider", "createTableClauses", "propertyList", 
			"property", "propertyKey", "propertyValue", "constantList", "nestedConstantList", 
			"createFileFormat", "fileFormat", "storageHandler", "resource", "dmlStatementNoWith", 
			"queryOrganization", "multiInsertQueryBody", "queryTerm", "queryPrimary", 
			"sortItem", "fromStatement", "fromStatementBody", "querySpecification", 
			"transformClause", "selectClause", "intoClause", "setClause", "matchedClause", 
			"notMatchedClause", "matchedAction", "notMatchedAction", "assignmentList", 
			"assignment", "whereClause", "havingClause", "hint", "hintStatement", 
			"fromClause", "temporalClause", "aggregationClause", "groupByClause", 
			"groupingAnalytics", "groupingElement", "groupingSet", "pivotClause", 
			"pivotColumn", "pivotValue", "lateralView", "setQuantifier", "relation", 
			"joinRelation", "joinType", "joinCriteria", "sample", "sampleMethod", 
			"identifierList", "identifierSeq", "orderedIdentifierList", "orderedIdentifier", 
			"identifierCommentList", "identifierComment", "relationPrimary", "inlineTable", 
			"functionTable", "tableAlias", "rowFormat", "multipartIdentifierList", 
			"multipartIdentifier", "multipartIdentifierPropertyList", "multipartIdentifierProperty", 
			"tableIdentifier", "functionIdentifier", "namedExpression", "namedExpressionSeq", 
			"partitionFieldList", "partitionField", "transform", "transformArgument", 
			"expression", "expressionSeq", "booleanExpression", "predicate", "valueExpression", 
			"datetimeUnit", "primaryExpression", "constant", "comparisonOperator", 
			"arithmeticOperator", "predicateOperator", "booleanValue", "interval", 
			"errorCapturingMultiUnitsInterval", "multiUnitsInterval", "errorCapturingUnitToUnitInterval", 
			"unitToUnitInterval", "intervalValue", "colPosition", "dataType", "qualifiedColTypeWithPositionList", 
			"qualifiedColTypeWithPosition", "colTypeList", "colType", "complexColTypeList", 
			"complexColType", "whenClause", "windowClause", "namedWindow", "windowSpec", 
			"windowFrame", "frameBound", "qualifiedNameList", "functionName", "qualifiedName", 
			"errorCapturingIdentifier", "errorCapturingIdentifierExtra", "identifier", 
			"strictIdentifier", "quotedIdentifier", "number", "alterColumnAction", 
			"ansiNonReserved", "strictNonReserved", "nonReserved"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "';'", "'('", "')'", "','", "'.'", "'['", "']'", "'ADD'", "'AFTER'", 
			"'ALL'", "'ALTER'", "'ANALYZE'", "'AND'", "'ANTI'", "'ANY'", "'ARCHIVE'", 
			"'ARRAY'", "'AS'", "'ASC'", "'AT'", "'AUTHORIZATION'", "'BETWEEN'", "'BOTH'", 
			"'BUCKET'", "'BUCKETS'", "'BY'", "'CACHE'", "'CASCADE'", "'CASE'", "'CAST'", 
			"'CATALOG'", "'CATALOGS'", "'CHANGE'", "'CHECK'", "'CLEAR'", "'CLUSTER'", 
			"'CLUSTERED'", "'CODEGEN'", "'COLLATE'", "'COLLECTION'", "'COLUMN'", 
			"'COLUMNS'", "'COMMENT'", "'COMMIT'", "'COMPACT'", "'COMPACTIONS'", "'COMPUTE'", 
			"'CONCATENATE'", "'CONSTRAINT'", "'COST'", "'CREATE'", "'CROSS'", "'CUBE'", 
			"'CURRENT'", "'CURRENT_DATE'", "'CURRENT_TIME'", "'CURRENT_TIMESTAMP'", 
			"'CURRENT_USER'", "'DAY'", "'DAYOFYEAR'", "'DATA'", "'DATABASE'", "'DATABASES'", 
			"'DATEADD'", "'DATEDIFF'", "'DBPROPERTIES'", "'DEFINED'", "'DELETE'", 
			"'DELIMITED'", "'DESC'", "'DESCRIBE'", "'DFS'", "'DIRECTORIES'", "'DIRECTORY'", 
			"'DISTINCT'", "'DISTRIBUTE'", "'DIV'", "'DROP'", "'ELSE'", "'END'", "'ESCAPE'", 
			"'ESCAPED'", "'EXCEPT'", "'EXCHANGE'", "'EXISTS'", "'EXPLAIN'", "'EXPORT'", 
			"'EXTENDED'", "'EXTERNAL'", "'EXTRACT'", "'FALSE'", "'FETCH'", "'FIELDS'", 
			"'FILTER'", "'FILEFORMAT'", "'FIRST'", "'FOLLOWING'", "'FOR'", "'FOREIGN'", 
			"'FORMAT'", "'FORMATTED'", "'FROM'", "'FULL'", "'FUNCTION'", "'FUNCTIONS'", 
			"'GLOBAL'", "'GRANT'", "'GROUP'", "'GROUPING'", "'HAVING'", "'HOUR'", 
			"'IF'", "'IGNORE'", "'IMPORT'", "'IN'", "'INDEX'", "'INDEXES'", "'INNER'", 
			"'INPATH'", "'INPUTFORMAT'", "'INSERT'", "'INTERSECT'", "'INTERVAL'", 
			"'INTO'", "'IS'", "'ITEMS'", "'JOIN'", "'KEYS'", "'LAST'", "'LATERAL'", 
			"'LAZY'", "'LEADING'", "'LEFT'", "'LIKE'", "'ILIKE'", "'LIMIT'", "'LINES'", 
			"'LIST'", "'LOAD'", "'LOCAL'", "'LOCATION'", "'LOCK'", "'LOCKS'", "'LOGICAL'", 
			"'MACRO'", "'MAP'", "'MATCHED'", "'MERGE'", "'MICROSECOND'", "'MILLISECOND'", 
			"'MINUTE'", "'MONTH'", "'MSCK'", "'NAMESPACE'", "'NAMESPACES'", "'NATURAL'", 
			"'NO'", null, "'NULL'", "'NULLS'", "'OF'", "'ON'", "'ONLY'", "'OPTION'", 
			"'OPTIONS'", "'OR'", "'ORDER'", "'OUT'", "'OUTER'", "'OUTPUTFORMAT'", 
			"'OVER'", "'OVERLAPS'", "'OVERLAY'", "'OVERWRITE'", "'PARTITION'", "'PARTITIONED'", 
			"'PARTITIONS'", "'PERCENTILE_CONT'", "'PERCENTILE_DISC'", "'PERCENT'", 
			"'PIVOT'", "'PLACING'", "'POSITION'", "'PRECEDING'", "'PRIMARY'", "'PRINCIPALS'", 
			"'PROPERTIES'", "'PURGE'", "'QUARTER'", "'QUERY'", "'RANGE'", "'RECORDREADER'", 
			"'RECORDWRITER'", "'RECOVER'", "'REDUCE'", "'REFERENCES'", "'REFRESH'", 
			"'RENAME'", "'REPAIR'", "'REPEATABLE'", "'REPLACE'", "'RESET'", "'RESPECT'", 
			"'RESTRICT'", "'REVOKE'", "'RIGHT'", null, "'ROLE'", "'ROLES'", "'ROLLBACK'", 
			"'ROLLUP'", "'ROW'", "'ROWS'", "'SECOND'", "'SCHEMA'", "'SCHEMAS'", "'SELECT'", 
			"'SEMI'", "'SEPARATED'", "'SERDE'", "'SERDEPROPERTIES'", "'SESSION_USER'", 
			"'SET'", "'MINUS'", "'SETS'", "'SHOW'", "'SKEWED'", "'SOME'", "'SORT'", 
			"'SORTED'", "'START'", "'STATISTICS'", "'STORED'", "'STRATIFY'", "'STRUCT'", 
			"'SUBSTR'", "'SUBSTRING'", "'SYNC'", "'SYSTEM_TIME'", "'SYSTEM_VERSION'", 
			"'TABLE'", "'TABLES'", "'TABLESAMPLE'", "'TBLPROPERTIES'", null, "'TERMINATED'", 
			"'THEN'", "'TIME'", "'TIMESTAMP'", "'TIMESTAMPADD'", "'TIMESTAMPDIFF'", 
			"'TO'", "'TOUCH'", "'TRAILING'", "'TRANSACTION'", "'TRANSACTIONS'", "'TRANSFORM'", 
			"'TRIM'", "'TRUE'", "'TRUNCATE'", "'TRY_CAST'", "'TYPE'", "'UNARCHIVE'", 
			"'UNBOUNDED'", "'UNCACHE'", "'UNION'", "'UNIQUE'", "'UNKNOWN'", "'UNLOCK'", 
			"'UNSET'", "'UPDATE'", "'USE'", "'USER'", "'USING'", "'VALUES'", "'VERSION'", 
			"'VIEW'", "'VIEWS'", "'WEEK'", "'WHEN'", "'WHERE'", "'WINDOW'", "'WITH'", 
			"'WITHIN'", "'YEAR'", "'ZONE'", null, "'<=>'", "'<>'", "'!='", "'<'", 
			null, "'>'", null, "'+'", "'-'", "'*'", "'/'", "'%'", "'~'", "'&'", "'|'", 
			"'||'", "'^'", "':'", "'->'", "'/*+'", "'*/'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, "SEMICOLON", "LEFT_PAREN", "RIGHT_PAREN", "COMMA", "DOT", "LEFT_BRACKET", 
			"RIGHT_BRACKET", "ADD", "AFTER", "ALL", "ALTER", "ANALYZE", "AND", "ANTI", 
			"ANY", "ARCHIVE", "ARRAY", "AS", "ASC", "AT", "AUTHORIZATION", "BETWEEN", 
			"BOTH", "BUCKET", "BUCKETS", "BY", "CACHE", "CASCADE", "CASE", "CAST", 
			"CATALOG", "CATALOGS", "CHANGE", "CHECK", "CLEAR", "CLUSTER", "CLUSTERED", 
			"CODEGEN", "COLLATE", "COLLECTION", "COLUMN", "COLUMNS", "COMMENT", "COMMIT", 
			"COMPACT", "COMPACTIONS", "COMPUTE", "CONCATENATE", "CONSTRAINT", "COST", 
			"CREATE", "CROSS", "CUBE", "CURRENT", "CURRENT_DATE", "CURRENT_TIME", 
			"CURRENT_TIMESTAMP", "CURRENT_USER", "DAY", "DAYOFYEAR", "DATA", "DATABASE", 
			"DATABASES", "DATEADD", "DATEDIFF", "DBPROPERTIES", "DEFINED", "DELETE", 
			"DELIMITED", "DESC", "DESCRIBE", "DFS", "DIRECTORIES", "DIRECTORY", "DISTINCT", 
			"DISTRIBUTE", "DIV", "DROP", "ELSE", "END", "ESCAPE", "ESCAPED", "EXCEPT", 
			"EXCHANGE", "EXISTS", "EXPLAIN", "EXPORT", "EXTENDED", "EXTERNAL", "EXTRACT", 
			"FALSE", "FETCH", "FIELDS", "FILTER", "FILEFORMAT", "FIRST", "FOLLOWING", 
			"FOR", "FOREIGN", "FORMAT", "FORMATTED", "FROM", "FULL", "FUNCTION", 
			"FUNCTIONS", "GLOBAL", "GRANT", "GROUP", "GROUPING", "HAVING", "HOUR", 
			"IF", "IGNORE", "IMPORT", "IN", "INDEX", "INDEXES", "INNER", "INPATH", 
			"INPUTFORMAT", "INSERT", "INTERSECT", "INTERVAL", "INTO", "IS", "ITEMS", 
			"JOIN", "KEYS", "LAST", "LATERAL", "LAZY", "LEADING", "LEFT", "LIKE", 
			"ILIKE", "LIMIT", "LINES", "LIST", "LOAD", "LOCAL", "LOCATION", "LOCK", 
			"LOCKS", "LOGICAL", "MACRO", "MAP", "MATCHED", "MERGE", "MICROSECOND", 
			"MILLISECOND", "MINUTE", "MONTH", "MSCK", "NAMESPACE", "NAMESPACES", 
			"NATURAL", "NO", "NOT", "NULL", "NULLS", "OF", "ON", "ONLY", "OPTION", 
			"OPTIONS", "OR", "ORDER", "OUT", "OUTER", "OUTPUTFORMAT", "OVER", "OVERLAPS", 
			"OVERLAY", "OVERWRITE", "PARTITION", "PARTITIONED", "PARTITIONS", "PERCENTILE_CONT", 
			"PERCENTILE_DISC", "PERCENTLIT", "PIVOT", "PLACING", "POSITION", "PRECEDING", 
			"PRIMARY", "PRINCIPALS", "PROPERTIES", "PURGE", "QUARTER", "QUERY", "RANGE", 
			"RECORDREADER", "RECORDWRITER", "RECOVER", "REDUCE", "REFERENCES", "REFRESH", 
			"RENAME", "REPAIR", "REPEATABLE", "REPLACE", "RESET", "RESPECT", "RESTRICT", 
			"REVOKE", "RIGHT", "RLIKE", "ROLE", "ROLES", "ROLLBACK", "ROLLUP", "ROW", 
			"ROWS", "SECOND", "SCHEMA", "SCHEMAS", "SELECT", "SEMI", "SEPARATED", 
			"SERDE", "SERDEPROPERTIES", "SESSION_USER", "SET", "SETMINUS", "SETS", 
			"SHOW", "SKEWED", "SOME", "SORT", "SORTED", "START", "STATISTICS", "STORED", 
			"STRATIFY", "STRUCT", "SUBSTR", "SUBSTRING", "SYNC", "SYSTEM_TIME", "SYSTEM_VERSION", 
			"TABLE", "TABLES", "TABLESAMPLE", "TBLPROPERTIES", "TEMPORARY", "TERMINATED", 
			"THEN", "TIME", "TIMESTAMP", "TIMESTAMPADD", "TIMESTAMPDIFF", "TO", "TOUCH", 
			"TRAILING", "TRANSACTION", "TRANSACTIONS", "TRANSFORM", "TRIM", "TRUE", 
			"TRUNCATE", "TRY_CAST", "TYPE", "UNARCHIVE", "UNBOUNDED", "UNCACHE", 
			"UNION", "UNIQUE", "UNKNOWN", "UNLOCK", "UNSET", "UPDATE", "USE", "USER", 
			"USING", "VALUES", "VERSION", "VIEW", "VIEWS", "WEEK", "WHEN", "WHERE", 
			"WINDOW", "WITH", "WITHIN", "YEAR", "ZONE", "EQ", "NSEQ", "NEQ", "NEQJ", 
			"LT", "LTE", "GT", "GTE", "PLUS", "MINUS", "ASTERISK", "SLASH", "PERCENT", 
			"TILDE", "AMPERSAND", "PIPE", "CONCAT_PIPE", "HAT", "COLON", "ARROW", 
			"HENT_START", "HENT_END", "STRING", "BIGINT_LITERAL", "SMALLINT_LITERAL", 
			"TINYINT_LITERAL", "INTEGER_VALUE", "EXPONENT_VALUE", "DECIMAL_VALUE", 
			"FLOAT_LITERAL", "DOUBLE_LITERAL", "BIGDECIMAL_LITERAL", "IDENTIFIER", 
			"BACKQUOTED_IDENTIFIER", "SIMPLE_COMMENT", "BRACKETED_COMMENT", "WS", 
			"UNRECOGNIZED"
		};
	}
	private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "SqlBaseParser.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }


	  /**
	   * When false, INTERSECT is given the greater precedence over the other set
	   * operations (UNION, EXCEPT and MINUS) as per the SQL standard.
	   */
	  public boolean legacy_setops_precedence_enabled = false;

	  /**
	   * When false, a literal with an exponent would be converted into
	   * double type rather than decimal type.
	   */
	  public boolean legacy_exponent_literal_as_decimal_enabled = false;

	  /**
	   * When true, the behavior of keywords follows ANSI SQL standard.
	   */
	  public boolean SQL_standard_keyword_behavior = false;

	public SqlBaseParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SingleStatementContext extends ParserRuleContext {
		public StatementContext statement() {
			return getRuleContext(StatementContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public List<TerminalNode> SEMICOLON() { return getTokens(SqlBaseParser.SEMICOLON); }
		public TerminalNode SEMICOLON(int i) {
			return getToken(SqlBaseParser.SEMICOLON, i);
		}
		public SingleStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterSingleStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitSingleStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitSingleStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SingleStatementContext singleStatement() throws RecognitionException {
		SingleStatementContext _localctx = new SingleStatementContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_singleStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(298);
			statement();
			setState(302);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==SEMICOLON) {
				{
				{
				setState(299);
				match(SEMICOLON);
				}
				}
				setState(304);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(305);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SingleExpressionContext extends ParserRuleContext {
		public NamedExpressionContext namedExpression() {
			return getRuleContext(NamedExpressionContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public SingleExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterSingleExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitSingleExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitSingleExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SingleExpressionContext singleExpression() throws RecognitionException {
		SingleExpressionContext _localctx = new SingleExpressionContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_singleExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(307);
			namedExpression();
			setState(308);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SingleTableIdentifierContext extends ParserRuleContext {
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public SingleTableIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleTableIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterSingleTableIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitSingleTableIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitSingleTableIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SingleTableIdentifierContext singleTableIdentifier() throws RecognitionException {
		SingleTableIdentifierContext _localctx = new SingleTableIdentifierContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_singleTableIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(310);
			tableIdentifier();
			setState(311);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SingleMultipartIdentifierContext extends ParserRuleContext {
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public SingleMultipartIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleMultipartIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterSingleMultipartIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitSingleMultipartIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitSingleMultipartIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SingleMultipartIdentifierContext singleMultipartIdentifier() throws RecognitionException {
		SingleMultipartIdentifierContext _localctx = new SingleMultipartIdentifierContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_singleMultipartIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(313);
			multipartIdentifier();
			setState(314);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SingleFunctionIdentifierContext extends ParserRuleContext {
		public FunctionIdentifierContext functionIdentifier() {
			return getRuleContext(FunctionIdentifierContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public SingleFunctionIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleFunctionIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterSingleFunctionIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitSingleFunctionIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitSingleFunctionIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SingleFunctionIdentifierContext singleFunctionIdentifier() throws RecognitionException {
		SingleFunctionIdentifierContext _localctx = new SingleFunctionIdentifierContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_singleFunctionIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(316);
			functionIdentifier();
			setState(317);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SingleDataTypeContext extends ParserRuleContext {
		public DataTypeContext dataType() {
			return getRuleContext(DataTypeContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public SingleDataTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleDataType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterSingleDataType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitSingleDataType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitSingleDataType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SingleDataTypeContext singleDataType() throws RecognitionException {
		SingleDataTypeContext _localctx = new SingleDataTypeContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_singleDataType);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(319);
			dataType();
			setState(320);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SingleTableSchemaContext extends ParserRuleContext {
		public ColTypeListContext colTypeList() {
			return getRuleContext(ColTypeListContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public SingleTableSchemaContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleTableSchema; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterSingleTableSchema(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitSingleTableSchema(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitSingleTableSchema(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SingleTableSchemaContext singleTableSchema() throws RecognitionException {
		SingleTableSchemaContext _localctx = new SingleTableSchemaContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_singleTableSchema);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(322);
			colTypeList();
			setState(323);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class StatementContext extends ParserRuleContext {
		public StatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_statement; }
	 
		public StatementContext() { }
		public void copyFrom(StatementContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ExplainContext extends StatementContext {
		public TerminalNode EXPLAIN() { return getToken(SqlBaseParser.EXPLAIN, 0); }
		public StatementContext statement() {
			return getRuleContext(StatementContext.class,0);
		}
		public TerminalNode LOGICAL() { return getToken(SqlBaseParser.LOGICAL, 0); }
		public TerminalNode FORMATTED() { return getToken(SqlBaseParser.FORMATTED, 0); }
		public TerminalNode EXTENDED() { return getToken(SqlBaseParser.EXTENDED, 0); }
		public TerminalNode CODEGEN() { return getToken(SqlBaseParser.CODEGEN, 0); }
		public TerminalNode COST() { return getToken(SqlBaseParser.COST, 0); }
		public ExplainContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterExplain(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitExplain(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitExplain(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ResetConfigurationContext extends StatementContext {
		public TerminalNode RESET() { return getToken(SqlBaseParser.RESET, 0); }
		public ResetConfigurationContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterResetConfiguration(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitResetConfiguration(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitResetConfiguration(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AlterViewQueryContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public AlterViewQueryContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterAlterViewQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitAlterViewQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitAlterViewQuery(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class UseContext extends StatementContext {
		public TerminalNode USE() { return getToken(SqlBaseParser.USE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public UseContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterUse(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitUse(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitUse(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DropNamespaceContext extends StatementContext {
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public NamespaceContext namespace() {
			return getRuleContext(NamespaceContext.class,0);
		}
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode RESTRICT() { return getToken(SqlBaseParser.RESTRICT, 0); }
		public TerminalNode CASCADE() { return getToken(SqlBaseParser.CASCADE, 0); }
		public DropNamespaceContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterDropNamespace(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitDropNamespace(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitDropNamespace(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateTempViewUsingContext extends StatementContext {
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TableProviderContext tableProvider() {
			return getRuleContext(TableProviderContext.class,0);
		}
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode GLOBAL() { return getToken(SqlBaseParser.GLOBAL, 0); }
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public ColTypeListContext colTypeList() {
			return getRuleContext(ColTypeListContext.class,0);
		}
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public TerminalNode OPTIONS() { return getToken(SqlBaseParser.OPTIONS, 0); }
		public PropertyListContext propertyList() {
			return getRuleContext(PropertyListContext.class,0);
		}
		public CreateTempViewUsingContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterCreateTempViewUsing(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitCreateTempViewUsing(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitCreateTempViewUsing(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RenameTableContext extends StatementContext {
		public MultipartIdentifierContext from;
		public MultipartIdentifierContext to;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode RENAME() { return getToken(SqlBaseParser.RENAME, 0); }
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public List<MultipartIdentifierContext> multipartIdentifier() {
			return getRuleContexts(MultipartIdentifierContext.class);
		}
		public MultipartIdentifierContext multipartIdentifier(int i) {
			return getRuleContext(MultipartIdentifierContext.class,i);
		}
		public RenameTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterRenameTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitRenameTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitRenameTable(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class FailNativeCommandContext extends StatementContext {
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode ROLE() { return getToken(SqlBaseParser.ROLE, 0); }
		public UnsupportedHiveNativeCommandsContext unsupportedHiveNativeCommands() {
			return getRuleContext(UnsupportedHiveNativeCommandsContext.class,0);
		}
		public FailNativeCommandContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterFailNativeCommand(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitFailNativeCommand(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitFailNativeCommand(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetCatalogContext extends StatementContext {
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode CATALOG() { return getToken(SqlBaseParser.CATALOG, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public SetCatalogContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterSetCatalog(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitSetCatalog(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitSetCatalog(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ClearCacheContext extends StatementContext {
		public TerminalNode CLEAR() { return getToken(SqlBaseParser.CLEAR, 0); }
		public TerminalNode CACHE() { return getToken(SqlBaseParser.CACHE, 0); }
		public ClearCacheContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterClearCache(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitClearCache(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitClearCache(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DropViewContext extends StatementContext {
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public DropViewContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterDropView(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitDropView(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitDropView(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowTablesContext extends StatementContext {
		public Token pattern;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode TABLES() { return getToken(SqlBaseParser.TABLES, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public ShowTablesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterShowTables(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitShowTables(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitShowTables(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RecoverPartitionsContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode RECOVER() { return getToken(SqlBaseParser.RECOVER, 0); }
		public TerminalNode PARTITIONS() { return getToken(SqlBaseParser.PARTITIONS, 0); }
		public RecoverPartitionsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterRecoverPartitions(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitRecoverPartitions(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitRecoverPartitions(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DropIndexContext extends StatementContext {
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode INDEX() { return getToken(SqlBaseParser.INDEX, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public DropIndexContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterDropIndex(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitDropIndex(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitDropIndex(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowCatalogsContext extends StatementContext {
		public Token pattern;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode CATALOGS() { return getToken(SqlBaseParser.CATALOGS, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public ShowCatalogsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterShowCatalogs(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitShowCatalogs(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitShowCatalogs(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowCurrentNamespaceContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode CURRENT() { return getToken(SqlBaseParser.CURRENT, 0); }
		public NamespaceContext namespace() {
			return getRuleContext(NamespaceContext.class,0);
		}
		public ShowCurrentNamespaceContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterShowCurrentNamespace(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitShowCurrentNamespace(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitShowCurrentNamespace(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RenameTablePartitionContext extends StatementContext {
		public PartitionSpecContext from;
		public PartitionSpecContext to;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode RENAME() { return getToken(SqlBaseParser.RENAME, 0); }
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public List<PartitionSpecContext> partitionSpec() {
			return getRuleContexts(PartitionSpecContext.class);
		}
		public PartitionSpecContext partitionSpec(int i) {
			return getRuleContext(PartitionSpecContext.class,i);
		}
		public RenameTablePartitionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterRenameTablePartition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitRenameTablePartition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitRenameTablePartition(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RepairTableContext extends StatementContext {
		public Token option;
		public TerminalNode MSCK() { return getToken(SqlBaseParser.MSCK, 0); }
		public TerminalNode REPAIR() { return getToken(SqlBaseParser.REPAIR, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode PARTITIONS() { return getToken(SqlBaseParser.PARTITIONS, 0); }
		public TerminalNode ADD() { return getToken(SqlBaseParser.ADD, 0); }
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode SYNC() { return getToken(SqlBaseParser.SYNC, 0); }
		public RepairTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterRepairTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitRepairTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitRepairTable(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RefreshResourceContext extends StatementContext {
		public TerminalNode REFRESH() { return getToken(SqlBaseParser.REFRESH, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public RefreshResourceContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterRefreshResource(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitRefreshResource(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitRefreshResource(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowCreateTableContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TerminalNode SERDE() { return getToken(SqlBaseParser.SERDE, 0); }
		public ShowCreateTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterShowCreateTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitShowCreateTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitShowCreateTable(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowNamespacesContext extends StatementContext {
		public Token pattern;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public NamespacesContext namespaces() {
			return getRuleContext(NamespacesContext.class,0);
		}
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public ShowNamespacesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterShowNamespaces(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitShowNamespaces(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitShowNamespaces(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowColumnsContext extends StatementContext {
		public MultipartIdentifierContext table;
		public MultipartIdentifierContext ns;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public List<TerminalNode> FROM() { return getTokens(SqlBaseParser.FROM); }
		public TerminalNode FROM(int i) {
			return getToken(SqlBaseParser.FROM, i);
		}
		public List<TerminalNode> IN() { return getTokens(SqlBaseParser.IN); }
		public TerminalNode IN(int i) {
			return getToken(SqlBaseParser.IN, i);
		}
		public List<MultipartIdentifierContext> multipartIdentifier() {
			return getRuleContexts(MultipartIdentifierContext.class);
		}
		public MultipartIdentifierContext multipartIdentifier(int i) {
			return getRuleContext(MultipartIdentifierContext.class,i);
		}
		public ShowColumnsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterShowColumns(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitShowColumns(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitShowColumns(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ReplaceTableContext extends StatementContext {
		public ReplaceTableHeaderContext replaceTableHeader() {
			return getRuleContext(ReplaceTableHeaderContext.class,0);
		}
		public CreateTableClausesContext createTableClauses() {
			return getRuleContext(CreateTableClausesContext.class,0);
		}
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public ColTypeListContext colTypeList() {
			return getRuleContext(ColTypeListContext.class,0);
		}
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public TableProviderContext tableProvider() {
			return getRuleContext(TableProviderContext.class,0);
		}
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public ReplaceTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterReplaceTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitReplaceTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitReplaceTable(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AnalyzeTablesContext extends StatementContext {
		public TerminalNode ANALYZE() { return getToken(SqlBaseParser.ANALYZE, 0); }
		public TerminalNode TABLES() { return getToken(SqlBaseParser.TABLES, 0); }
		public TerminalNode COMPUTE() { return getToken(SqlBaseParser.COMPUTE, 0); }
		public TerminalNode STATISTICS() { return getToken(SqlBaseParser.STATISTICS, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public AnalyzeTablesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterAnalyzeTables(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitAnalyzeTables(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitAnalyzeTables(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AddTablePartitionContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode ADD() { return getToken(SqlBaseParser.ADD, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public List<PartitionSpecLocationContext> partitionSpecLocation() {
			return getRuleContexts(PartitionSpecLocationContext.class);
		}
		public PartitionSpecLocationContext partitionSpecLocation(int i) {
			return getRuleContext(PartitionSpecLocationContext.class,i);
		}
		public AddTablePartitionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterAddTablePartition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitAddTablePartition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitAddTablePartition(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetNamespaceLocationContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public NamespaceContext namespace() {
			return getRuleContext(NamespaceContext.class,0);
		}
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public LocationSpecContext locationSpec() {
			return getRuleContext(LocationSpecContext.class,0);
		}
		public SetNamespaceLocationContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterSetNamespaceLocation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitSetNamespaceLocation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitSetNamespaceLocation(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RefreshTableContext extends StatementContext {
		public TerminalNode REFRESH() { return getToken(SqlBaseParser.REFRESH, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public RefreshTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterRefreshTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitRefreshTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitRefreshTable(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetNamespacePropertiesContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public NamespaceContext namespace() {
			return getRuleContext(NamespaceContext.class,0);
		}
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public PropertyListContext propertyList() {
			return getRuleContext(PropertyListContext.class,0);
		}
		public TerminalNode DBPROPERTIES() { return getToken(SqlBaseParser.DBPROPERTIES, 0); }
		public TerminalNode PROPERTIES() { return getToken(SqlBaseParser.PROPERTIES, 0); }
		public SetNamespacePropertiesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterSetNamespaceProperties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitSetNamespaceProperties(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitSetNamespaceProperties(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ManageResourceContext extends StatementContext {
		public Token op;
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode ADD() { return getToken(SqlBaseParser.ADD, 0); }
		public TerminalNode LIST() { return getToken(SqlBaseParser.LIST, 0); }
		public ManageResourceContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterManageResource(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitManageResource(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitManageResource(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetQuotedConfigurationContext extends StatementContext {
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public ConfigKeyContext configKey() {
			return getRuleContext(ConfigKeyContext.class,0);
		}
		public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
		public ConfigValueContext configValue() {
			return getRuleContext(ConfigValueContext.class,0);
		}
		public SetQuotedConfigurationContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterSetQuotedConfiguration(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitSetQuotedConfiguration(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitSetQuotedConfiguration(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AnalyzeContext extends StatementContext {
		public TerminalNode ANALYZE() { return getToken(SqlBaseParser.ANALYZE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode COMPUTE() { return getToken(SqlBaseParser.COMPUTE, 0); }
		public TerminalNode STATISTICS() { return getToken(SqlBaseParser.STATISTICS, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode FOR() { return getToken(SqlBaseParser.FOR, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public IdentifierSeqContext identifierSeq() {
			return getRuleContext(IdentifierSeqContext.class,0);
		}
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public AnalyzeContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterAnalyze(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitAnalyze(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitAnalyze(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateFunctionContext extends StatementContext {
		public Token className;
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode FUNCTION() { return getToken(SqlBaseParser.FUNCTION, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public List<ResourceContext> resource() {
			return getRuleContexts(ResourceContext.class);
		}
		public ResourceContext resource(int i) {
			return getRuleContext(ResourceContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public CreateFunctionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterCreateFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitCreateFunction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitCreateFunction(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class HiveReplaceColumnsContext extends StatementContext {
		public MultipartIdentifierContext table;
		public QualifiedColTypeWithPositionListContext columns;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public QualifiedColTypeWithPositionListContext qualifiedColTypeWithPositionList() {
			return getRuleContext(QualifiedColTypeWithPositionListContext.class,0);
		}
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public HiveReplaceColumnsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterHiveReplaceColumns(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitHiveReplaceColumns(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitHiveReplaceColumns(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CommentNamespaceContext extends StatementContext {
		public Token comment;
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public NamespaceContext namespace() {
			return getRuleContext(NamespaceContext.class,0);
		}
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode IS() { return getToken(SqlBaseParser.IS, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public CommentNamespaceContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterCommentNamespace(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitCommentNamespace(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitCommentNamespace(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ResetQuotedConfigurationContext extends StatementContext {
		public TerminalNode RESET() { return getToken(SqlBaseParser.RESET, 0); }
		public ConfigKeyContext configKey() {
			return getRuleContext(ConfigKeyContext.class,0);
		}
		public ResetQuotedConfigurationContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterResetQuotedConfiguration(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitResetQuotedConfiguration(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitResetQuotedConfiguration(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateTableContext extends StatementContext {
		public CreateTableHeaderContext createTableHeader() {
			return getRuleContext(CreateTableHeaderContext.class,0);
		}
		public CreateTableClausesContext createTableClauses() {
			return getRuleContext(CreateTableClausesContext.class,0);
		}
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public ColTypeListContext colTypeList() {
			return getRuleContext(ColTypeListContext.class,0);
		}
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public TableProviderContext tableProvider() {
			return getRuleContext(TableProviderContext.class,0);
		}
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public CreateTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterCreateTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitCreateTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitCreateTable(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DmlStatementContext extends StatementContext {
		public DmlStatementNoWithContext dmlStatementNoWith() {
			return getRuleContext(DmlStatementNoWithContext.class,0);
		}
		public CtesContext ctes() {
			return getRuleContext(CtesContext.class,0);
		}
		public DmlStatementContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterDmlStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitDmlStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitDmlStatement(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateTableLikeContext extends StatementContext {
		public TableIdentifierContext target;
		public TableIdentifierContext source;
		public PropertyListContext tableProps;
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public List<TableIdentifierContext> tableIdentifier() {
			return getRuleContexts(TableIdentifierContext.class);
		}
		public TableIdentifierContext tableIdentifier(int i) {
			return getRuleContext(TableIdentifierContext.class,i);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public List<TableProviderContext> tableProvider() {
			return getRuleContexts(TableProviderContext.class);
		}
		public TableProviderContext tableProvider(int i) {
			return getRuleContext(TableProviderContext.class,i);
		}
		public List<RowFormatContext> rowFormat() {
			return getRuleContexts(RowFormatContext.class);
		}
		public RowFormatContext rowFormat(int i) {
			return getRuleContext(RowFormatContext.class,i);
		}
		public List<CreateFileFormatContext> createFileFormat() {
			return getRuleContexts(CreateFileFormatContext.class);
		}
		public CreateFileFormatContext createFileFormat(int i) {
			return getRuleContext(CreateFileFormatContext.class,i);
		}
		public List<LocationSpecContext> locationSpec() {
			return getRuleContexts(LocationSpecContext.class);
		}
		public LocationSpecContext locationSpec(int i) {
			return getRuleContext(LocationSpecContext.class,i);
		}
		public List<TerminalNode> TBLPROPERTIES() { return getTokens(SqlBaseParser.TBLPROPERTIES); }
		public TerminalNode TBLPROPERTIES(int i) {
			return getToken(SqlBaseParser.TBLPROPERTIES, i);
		}
		public List<PropertyListContext> propertyList() {
			return getRuleContexts(PropertyListContext.class);
		}
		public PropertyListContext propertyList(int i) {
			return getRuleContext(PropertyListContext.class,i);
		}
		public CreateTableLikeContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterCreateTableLike(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitCreateTableLike(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitCreateTableLike(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class UncacheTableContext extends StatementContext {
		public TerminalNode UNCACHE() { return getToken(SqlBaseParser.UNCACHE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public UncacheTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterUncacheTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitUncacheTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitUncacheTable(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DropFunctionContext extends StatementContext {
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode FUNCTION() { return getToken(SqlBaseParser.FUNCTION, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public DropFunctionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterDropFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitDropFunction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitDropFunction(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DescribeRelationContext extends StatementContext {
		public Token option;
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode DESCRIBE() { return getToken(SqlBaseParser.DESCRIBE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public DescribeColNameContext describeColName() {
			return getRuleContext(DescribeColNameContext.class,0);
		}
		public TerminalNode EXTENDED() { return getToken(SqlBaseParser.EXTENDED, 0); }
		public TerminalNode FORMATTED() { return getToken(SqlBaseParser.FORMATTED, 0); }
		public DescribeRelationContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterDescribeRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitDescribeRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitDescribeRelation(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LoadDataContext extends StatementContext {
		public Token path;
		public TerminalNode LOAD() { return getToken(SqlBaseParser.LOAD, 0); }
		public TerminalNode DATA() { return getToken(SqlBaseParser.DATA, 0); }
		public TerminalNode INPATH() { return getToken(SqlBaseParser.INPATH, 0); }
		public TerminalNode INTO() { return getToken(SqlBaseParser.INTO, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode LOCAL() { return getToken(SqlBaseParser.LOCAL, 0); }
		public TerminalNode OVERWRITE() { return getToken(SqlBaseParser.OVERWRITE, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public LoadDataContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterLoadData(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitLoadData(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitLoadData(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowPartitionsContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode PARTITIONS() { return getToken(SqlBaseParser.PARTITIONS, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public ShowPartitionsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterShowPartitions(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitShowPartitions(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitShowPartitions(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DescribeFunctionContext extends StatementContext {
		public TerminalNode FUNCTION() { return getToken(SqlBaseParser.FUNCTION, 0); }
		public DescribeFuncNameContext describeFuncName() {
			return getRuleContext(DescribeFuncNameContext.class,0);
		}
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode DESCRIBE() { return getToken(SqlBaseParser.DESCRIBE, 0); }
		public TerminalNode EXTENDED() { return getToken(SqlBaseParser.EXTENDED, 0); }
		public DescribeFunctionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterDescribeFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitDescribeFunction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitDescribeFunction(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RenameTableColumnContext extends StatementContext {
		public MultipartIdentifierContext table;
		public MultipartIdentifierContext from;
		public ErrorCapturingIdentifierContext to;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode RENAME() { return getToken(SqlBaseParser.RENAME, 0); }
		public TerminalNode COLUMN() { return getToken(SqlBaseParser.COLUMN, 0); }
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public List<MultipartIdentifierContext> multipartIdentifier() {
			return getRuleContexts(MultipartIdentifierContext.class);
		}
		public MultipartIdentifierContext multipartIdentifier(int i) {
			return getRuleContext(MultipartIdentifierContext.class,i);
		}
		public ErrorCapturingIdentifierContext errorCapturingIdentifier() {
			return getRuleContext(ErrorCapturingIdentifierContext.class,0);
		}
		public RenameTableColumnContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterRenameTableColumn(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitRenameTableColumn(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitRenameTableColumn(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class StatementDefaultContext extends StatementContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public StatementDefaultContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterStatementDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitStatementDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitStatementDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class HiveChangeColumnContext extends StatementContext {
		public MultipartIdentifierContext table;
		public MultipartIdentifierContext colName;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode CHANGE() { return getToken(SqlBaseParser.CHANGE, 0); }
		public ColTypeContext colType() {
			return getRuleContext(ColTypeContext.class,0);
		}
		public List<MultipartIdentifierContext> multipartIdentifier() {
			return getRuleContexts(MultipartIdentifierContext.class);
		}
		public MultipartIdentifierContext multipartIdentifier(int i) {
			return getRuleContext(MultipartIdentifierContext.class,i);
		}
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public TerminalNode COLUMN() { return getToken(SqlBaseParser.COLUMN, 0); }
		public ColPositionContext colPosition() {
			return getRuleContext(ColPositionContext.class,0);
		}
		public HiveChangeColumnContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterHiveChangeColumn(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitHiveChangeColumn(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitHiveChangeColumn(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetTimeZoneContext extends StatementContext {
		public Token timezone;
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode TIME() { return getToken(SqlBaseParser.TIME, 0); }
		public TerminalNode ZONE() { return getToken(SqlBaseParser.ZONE, 0); }
		public IntervalContext interval() {
			return getRuleContext(IntervalContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode LOCAL() { return getToken(SqlBaseParser.LOCAL, 0); }
		public SetTimeZoneContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterSetTimeZone(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitSetTimeZone(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitSetTimeZone(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DescribeQueryContext extends StatementContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode DESCRIBE() { return getToken(SqlBaseParser.DESCRIBE, 0); }
		public TerminalNode QUERY() { return getToken(SqlBaseParser.QUERY, 0); }
		public DescribeQueryContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterDescribeQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitDescribeQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitDescribeQuery(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TruncateTableContext extends StatementContext {
		public TerminalNode TRUNCATE() { return getToken(SqlBaseParser.TRUNCATE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public TruncateTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterTruncateTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitTruncateTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitTruncateTable(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetTableSerDeContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode SERDE() { return getToken(SqlBaseParser.SERDE, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode SERDEPROPERTIES() { return getToken(SqlBaseParser.SERDEPROPERTIES, 0); }
		public PropertyListContext propertyList() {
			return getRuleContext(PropertyListContext.class,0);
		}
		public SetTableSerDeContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterSetTableSerDe(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitSetTableSerDe(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitSetTableSerDe(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateViewContext extends StatementContext {
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public IdentifierCommentListContext identifierCommentList() {
			return getRuleContext(IdentifierCommentListContext.class,0);
		}
		public List<CommentSpecContext> commentSpec() {
			return getRuleContexts(CommentSpecContext.class);
		}
		public CommentSpecContext commentSpec(int i) {
			return getRuleContext(CommentSpecContext.class,i);
		}
		public List<TerminalNode> PARTITIONED() { return getTokens(SqlBaseParser.PARTITIONED); }
		public TerminalNode PARTITIONED(int i) {
			return getToken(SqlBaseParser.PARTITIONED, i);
		}
		public List<TerminalNode> ON() { return getTokens(SqlBaseParser.ON); }
		public TerminalNode ON(int i) {
			return getToken(SqlBaseParser.ON, i);
		}
		public List<IdentifierListContext> identifierList() {
			return getRuleContexts(IdentifierListContext.class);
		}
		public IdentifierListContext identifierList(int i) {
			return getRuleContext(IdentifierListContext.class,i);
		}
		public List<TerminalNode> TBLPROPERTIES() { return getTokens(SqlBaseParser.TBLPROPERTIES); }
		public TerminalNode TBLPROPERTIES(int i) {
			return getToken(SqlBaseParser.TBLPROPERTIES, i);
		}
		public List<PropertyListContext> propertyList() {
			return getRuleContexts(PropertyListContext.class);
		}
		public PropertyListContext propertyList(int i) {
			return getRuleContext(PropertyListContext.class,i);
		}
		public TerminalNode GLOBAL() { return getToken(SqlBaseParser.GLOBAL, 0); }
		public CreateViewContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterCreateView(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitCreateView(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitCreateView(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DropTablePartitionsContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public List<PartitionSpecContext> partitionSpec() {
			return getRuleContexts(PartitionSpecContext.class);
		}
		public PartitionSpecContext partitionSpec(int i) {
			return getRuleContext(PartitionSpecContext.class,i);
		}
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public TerminalNode PURGE() { return getToken(SqlBaseParser.PURGE, 0); }
		public DropTablePartitionsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterDropTablePartitions(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitDropTablePartitions(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitDropTablePartitions(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetConfigurationContext extends StatementContext {
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public ConfigKeyContext configKey() {
			return getRuleContext(ConfigKeyContext.class,0);
		}
		public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
		public SetConfigurationContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterSetConfiguration(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitSetConfiguration(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitSetConfiguration(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DropTableContext extends StatementContext {
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode PURGE() { return getToken(SqlBaseParser.PURGE, 0); }
		public DropTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterDropTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitDropTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitDropTable(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowTableExtendedContext extends StatementContext {
		public MultipartIdentifierContext ns;
		public Token pattern;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode EXTENDED() { return getToken(SqlBaseParser.EXTENDED, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public ShowTableExtendedContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterShowTableExtended(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitShowTableExtended(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitShowTableExtended(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DescribeNamespaceContext extends StatementContext {
		public NamespaceContext namespace() {
			return getRuleContext(NamespaceContext.class,0);
		}
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode DESCRIBE() { return getToken(SqlBaseParser.DESCRIBE, 0); }
		public TerminalNode EXTENDED() { return getToken(SqlBaseParser.EXTENDED, 0); }
		public DescribeNamespaceContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterDescribeNamespace(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitDescribeNamespace(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitDescribeNamespace(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AlterTableAlterColumnContext extends StatementContext {
		public MultipartIdentifierContext table;
		public MultipartIdentifierContext column;
		public List<TerminalNode> ALTER() { return getTokens(SqlBaseParser.ALTER); }
		public TerminalNode ALTER(int i) {
			return getToken(SqlBaseParser.ALTER, i);
		}
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public List<MultipartIdentifierContext> multipartIdentifier() {
			return getRuleContexts(MultipartIdentifierContext.class);
		}
		public MultipartIdentifierContext multipartIdentifier(int i) {
			return getRuleContext(MultipartIdentifierContext.class,i);
		}
		public TerminalNode CHANGE() { return getToken(SqlBaseParser.CHANGE, 0); }
		public TerminalNode COLUMN() { return getToken(SqlBaseParser.COLUMN, 0); }
		public AlterColumnActionContext alterColumnAction() {
			return getRuleContext(AlterColumnActionContext.class,0);
		}
		public AlterTableAlterColumnContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterAlterTableAlterColumn(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitAlterTableAlterColumn(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitAlterTableAlterColumn(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RefreshFunctionContext extends StatementContext {
		public TerminalNode REFRESH() { return getToken(SqlBaseParser.REFRESH, 0); }
		public TerminalNode FUNCTION() { return getToken(SqlBaseParser.FUNCTION, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public RefreshFunctionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterRefreshFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitRefreshFunction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitRefreshFunction(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CommentTableContext extends StatementContext {
		public Token comment;
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode IS() { return getToken(SqlBaseParser.IS, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public CommentTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterCommentTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitCommentTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitCommentTable(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateIndexContext extends StatementContext {
		public IdentifierContext indexType;
		public MultipartIdentifierPropertyListContext columns;
		public PropertyListContext options;
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode INDEX() { return getToken(SqlBaseParser.INDEX, 0); }
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public MultipartIdentifierPropertyListContext multipartIdentifierPropertyList() {
			return getRuleContext(MultipartIdentifierPropertyListContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public TerminalNode OPTIONS() { return getToken(SqlBaseParser.OPTIONS, 0); }
		public PropertyListContext propertyList() {
			return getRuleContext(PropertyListContext.class,0);
		}
		public CreateIndexContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterCreateIndex(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitCreateIndex(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitCreateIndex(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class UseNamespaceContext extends StatementContext {
		public TerminalNode USE() { return getToken(SqlBaseParser.USE, 0); }
		public NamespaceContext namespace() {
			return getRuleContext(NamespaceContext.class,0);
		}
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public UseNamespaceContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterUseNamespace(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitUseNamespace(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitUseNamespace(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CreateNamespaceContext extends StatementContext {
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public NamespaceContext namespace() {
			return getRuleContext(NamespaceContext.class,0);
		}
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public List<CommentSpecContext> commentSpec() {
			return getRuleContexts(CommentSpecContext.class);
		}
		public CommentSpecContext commentSpec(int i) {
			return getRuleContext(CommentSpecContext.class,i);
		}
		public List<LocationSpecContext> locationSpec() {
			return getRuleContexts(LocationSpecContext.class);
		}
		public LocationSpecContext locationSpec(int i) {
			return getRuleContext(LocationSpecContext.class,i);
		}
		public List<TerminalNode> WITH() { return getTokens(SqlBaseParser.WITH); }
		public TerminalNode WITH(int i) {
			return getToken(SqlBaseParser.WITH, i);
		}
		public List<PropertyListContext> propertyList() {
			return getRuleContexts(PropertyListContext.class);
		}
		public PropertyListContext propertyList(int i) {
			return getRuleContext(PropertyListContext.class,i);
		}
		public List<TerminalNode> DBPROPERTIES() { return getTokens(SqlBaseParser.DBPROPERTIES); }
		public TerminalNode DBPROPERTIES(int i) {
			return getToken(SqlBaseParser.DBPROPERTIES, i);
		}
		public List<TerminalNode> PROPERTIES() { return getTokens(SqlBaseParser.PROPERTIES); }
		public TerminalNode PROPERTIES(int i) {
			return getToken(SqlBaseParser.PROPERTIES, i);
		}
		public CreateNamespaceContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterCreateNamespace(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitCreateNamespace(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitCreateNamespace(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowTblPropertiesContext extends StatementContext {
		public MultipartIdentifierContext table;
		public PropertyKeyContext key;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode TBLPROPERTIES() { return getToken(SqlBaseParser.TBLPROPERTIES, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public PropertyKeyContext propertyKey() {
			return getRuleContext(PropertyKeyContext.class,0);
		}
		public ShowTblPropertiesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterShowTblProperties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitShowTblProperties(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitShowTblProperties(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class UnsetTablePropertiesContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode UNSET() { return getToken(SqlBaseParser.UNSET, 0); }
		public TerminalNode TBLPROPERTIES() { return getToken(SqlBaseParser.TBLPROPERTIES, 0); }
		public PropertyListContext propertyList() {
			return getRuleContext(PropertyListContext.class,0);
		}
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public UnsetTablePropertiesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterUnsetTableProperties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitUnsetTableProperties(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitUnsetTableProperties(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetTableLocationContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public LocationSpecContext locationSpec() {
			return getRuleContext(LocationSpecContext.class,0);
		}
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public SetTableLocationContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterSetTableLocation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitSetTableLocation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitSetTableLocation(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DropTableColumnsContext extends StatementContext {
		public MultipartIdentifierListContext columns;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public TerminalNode COLUMN() { return getToken(SqlBaseParser.COLUMN, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public MultipartIdentifierListContext multipartIdentifierList() {
			return getRuleContext(MultipartIdentifierListContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public DropTableColumnsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterDropTableColumns(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitDropTableColumns(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitDropTableColumns(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowViewsContext extends StatementContext {
		public Token pattern;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode VIEWS() { return getToken(SqlBaseParser.VIEWS, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public ShowViewsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterShowViews(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitShowViews(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitShowViews(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ShowFunctionsContext extends StatementContext {
		public MultipartIdentifierContext ns;
		public MultipartIdentifierContext legacy;
		public Token pattern;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode FUNCTIONS() { return getToken(SqlBaseParser.FUNCTIONS, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public List<MultipartIdentifierContext> multipartIdentifier() {
			return getRuleContexts(MultipartIdentifierContext.class);
		}
		public MultipartIdentifierContext multipartIdentifier(int i) {
			return getRuleContext(MultipartIdentifierContext.class,i);
		}
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public ShowFunctionsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterShowFunctions(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitShowFunctions(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitShowFunctions(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CacheTableContext extends StatementContext {
		public PropertyListContext options;
		public TerminalNode CACHE() { return getToken(SqlBaseParser.CACHE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode LAZY() { return getToken(SqlBaseParser.LAZY, 0); }
		public TerminalNode OPTIONS() { return getToken(SqlBaseParser.OPTIONS, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public PropertyListContext propertyList() {
			return getRuleContext(PropertyListContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public CacheTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterCacheTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitCacheTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitCacheTable(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AddTableColumnsContext extends StatementContext {
		public QualifiedColTypeWithPositionListContext columns;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode ADD() { return getToken(SqlBaseParser.ADD, 0); }
		public TerminalNode COLUMN() { return getToken(SqlBaseParser.COLUMN, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public QualifiedColTypeWithPositionListContext qualifiedColTypeWithPositionList() {
			return getRuleContext(QualifiedColTypeWithPositionListContext.class,0);
		}
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public AddTableColumnsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterAddTableColumns(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitAddTableColumns(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitAddTableColumns(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetTablePropertiesContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode TBLPROPERTIES() { return getToken(SqlBaseParser.TBLPROPERTIES, 0); }
		public PropertyListContext propertyList() {
			return getRuleContext(PropertyListContext.class,0);
		}
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public SetTablePropertiesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterSetTableProperties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitSetTableProperties(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitSetTableProperties(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StatementContext statement() throws RecognitionException {
		StatementContext _localctx = new StatementContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_statement);
		int _la;
		try {
			int _alt;
			setState(1126);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,120,_ctx) ) {
			case 1:
				_localctx = new StatementDefaultContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(325);
				query();
				}
				break;
			case 2:
				_localctx = new DmlStatementContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(327);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(326);
					ctes();
					}
				}

				setState(329);
				dmlStatementNoWith();
				}
				break;
			case 3:
				_localctx = new UseContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(330);
				match(USE);
				setState(331);
				multipartIdentifier();
				}
				break;
			case 4:
				_localctx = new UseNamespaceContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(332);
				match(USE);
				setState(333);
				namespace();
				setState(334);
				multipartIdentifier();
				}
				break;
			case 5:
				_localctx = new SetCatalogContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(336);
				match(SET);
				setState(337);
				match(CATALOG);
				setState(340);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
				case 1:
					{
					setState(338);
					identifier();
					}
					break;
				case 2:
					{
					setState(339);
					match(STRING);
					}
					break;
				}
				}
				break;
			case 6:
				_localctx = new CreateNamespaceContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(342);
				match(CREATE);
				setState(343);
				namespace();
				setState(347);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,3,_ctx) ) {
				case 1:
					{
					setState(344);
					match(IF);
					setState(345);
					match(NOT);
					setState(346);
					match(EXISTS);
					}
					break;
				}
				setState(349);
				multipartIdentifier();
				setState(357);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMENT || _la==LOCATION || _la==WITH) {
					{
					setState(355);
					_errHandler.sync(this);
					switch (_input.LA(1)) {
					case COMMENT:
						{
						setState(350);
						commentSpec();
						}
						break;
					case LOCATION:
						{
						setState(351);
						locationSpec();
						}
						break;
					case WITH:
						{
						{
						setState(352);
						match(WITH);
						setState(353);
						_la = _input.LA(1);
						if ( !(_la==DBPROPERTIES || _la==PROPERTIES) ) {
						_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(354);
						propertyList();
						}
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					}
					setState(359);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case 7:
				_localctx = new SetNamespacePropertiesContext(_localctx);
				enterOuterAlt(_localctx, 7);
				{
				setState(360);
				match(ALTER);
				setState(361);
				namespace();
				setState(362);
				multipartIdentifier();
				setState(363);
				match(SET);
				setState(364);
				_la = _input.LA(1);
				if ( !(_la==DBPROPERTIES || _la==PROPERTIES) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(365);
				propertyList();
				}
				break;
			case 8:
				_localctx = new SetNamespaceLocationContext(_localctx);
				enterOuterAlt(_localctx, 8);
				{
				setState(367);
				match(ALTER);
				setState(368);
				namespace();
				setState(369);
				multipartIdentifier();
				setState(370);
				match(SET);
				setState(371);
				locationSpec();
				}
				break;
			case 9:
				_localctx = new DropNamespaceContext(_localctx);
				enterOuterAlt(_localctx, 9);
				{
				setState(373);
				match(DROP);
				setState(374);
				namespace();
				setState(377);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,6,_ctx) ) {
				case 1:
					{
					setState(375);
					match(IF);
					setState(376);
					match(EXISTS);
					}
					break;
				}
				setState(379);
				multipartIdentifier();
				setState(381);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==CASCADE || _la==RESTRICT) {
					{
					setState(380);
					_la = _input.LA(1);
					if ( !(_la==CASCADE || _la==RESTRICT) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				}
				break;
			case 10:
				_localctx = new ShowNamespacesContext(_localctx);
				enterOuterAlt(_localctx, 10);
				{
				setState(383);
				match(SHOW);
				setState(384);
				namespaces();
				setState(387);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FROM || _la==IN) {
					{
					setState(385);
					_la = _input.LA(1);
					if ( !(_la==FROM || _la==IN) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(386);
					multipartIdentifier();
					}
				}

				setState(393);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LIKE || _la==STRING) {
					{
					setState(390);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==LIKE) {
						{
						setState(389);
						match(LIKE);
						}
					}

					setState(392);
					((ShowNamespacesContext)_localctx).pattern = match(STRING);
					}
				}

				}
				break;
			case 11:
				_localctx = new CreateTableContext(_localctx);
				enterOuterAlt(_localctx, 11);
				{
				setState(395);
				createTableHeader();
				setState(400);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,11,_ctx) ) {
				case 1:
					{
					setState(396);
					match(LEFT_PAREN);
					setState(397);
					colTypeList();
					setState(398);
					match(RIGHT_PAREN);
					}
					break;
				}
				setState(403);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==USING) {
					{
					setState(402);
					tableProvider();
					}
				}

				setState(405);
				createTableClauses();
				setState(410);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LEFT_PAREN || _la==AS || _la==FROM || _la==MAP || ((((_la - 195)) & ~0x3f) == 0 && ((1L << (_la - 195)) & 70368748371969L) != 0) || _la==VALUES || _la==WITH) {
					{
					setState(407);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==AS) {
						{
						setState(406);
						match(AS);
						}
					}

					setState(409);
					query();
					}
				}

				}
				break;
			case 12:
				_localctx = new CreateTableLikeContext(_localctx);
				enterOuterAlt(_localctx, 12);
				{
				setState(412);
				match(CREATE);
				setState(413);
				match(TABLE);
				setState(417);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,15,_ctx) ) {
				case 1:
					{
					setState(414);
					match(IF);
					setState(415);
					match(NOT);
					setState(416);
					match(EXISTS);
					}
					break;
				}
				setState(419);
				((CreateTableLikeContext)_localctx).target = tableIdentifier();
				setState(420);
				match(LIKE);
				setState(421);
				((CreateTableLikeContext)_localctx).source = tableIdentifier();
				setState(430);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==LOCATION || ((((_la - 212)) & ~0x3f) == 0 && ((1L << (_la - 212)) & 4611686022724452353L) != 0)) {
					{
					setState(428);
					_errHandler.sync(this);
					switch (_input.LA(1)) {
					case USING:
						{
						setState(422);
						tableProvider();
						}
						break;
					case ROW:
						{
						setState(423);
						rowFormat();
						}
						break;
					case STORED:
						{
						setState(424);
						createFileFormat();
						}
						break;
					case LOCATION:
						{
						setState(425);
						locationSpec();
						}
						break;
					case TBLPROPERTIES:
						{
						{
						setState(426);
						match(TBLPROPERTIES);
						setState(427);
						((CreateTableLikeContext)_localctx).tableProps = propertyList();
						}
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					}
					setState(432);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case 13:
				_localctx = new ReplaceTableContext(_localctx);
				enterOuterAlt(_localctx, 13);
				{
				setState(433);
				replaceTableHeader();
				setState(438);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,18,_ctx) ) {
				case 1:
					{
					setState(434);
					match(LEFT_PAREN);
					setState(435);
					colTypeList();
					setState(436);
					match(RIGHT_PAREN);
					}
					break;
				}
				setState(441);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==USING) {
					{
					setState(440);
					tableProvider();
					}
				}

				setState(443);
				createTableClauses();
				setState(448);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LEFT_PAREN || _la==AS || _la==FROM || _la==MAP || ((((_la - 195)) & ~0x3f) == 0 && ((1L << (_la - 195)) & 70368748371969L) != 0) || _la==VALUES || _la==WITH) {
					{
					setState(445);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==AS) {
						{
						setState(444);
						match(AS);
						}
					}

					setState(447);
					query();
					}
				}

				}
				break;
			case 14:
				_localctx = new AnalyzeContext(_localctx);
				enterOuterAlt(_localctx, 14);
				{
				setState(450);
				match(ANALYZE);
				setState(451);
				match(TABLE);
				setState(452);
				multipartIdentifier();
				setState(454);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(453);
					partitionSpec();
					}
				}

				setState(456);
				match(COMPUTE);
				setState(457);
				match(STATISTICS);
				setState(465);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,23,_ctx) ) {
				case 1:
					{
					setState(458);
					identifier();
					}
					break;
				case 2:
					{
					setState(459);
					match(FOR);
					setState(460);
					match(COLUMNS);
					setState(461);
					identifierSeq();
					}
					break;
				case 3:
					{
					setState(462);
					match(FOR);
					setState(463);
					match(ALL);
					setState(464);
					match(COLUMNS);
					}
					break;
				}
				}
				break;
			case 15:
				_localctx = new AnalyzeTablesContext(_localctx);
				enterOuterAlt(_localctx, 15);
				{
				setState(467);
				match(ANALYZE);
				setState(468);
				match(TABLES);
				setState(471);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FROM || _la==IN) {
					{
					setState(469);
					_la = _input.LA(1);
					if ( !(_la==FROM || _la==IN) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(470);
					multipartIdentifier();
					}
				}

				setState(473);
				match(COMPUTE);
				setState(474);
				match(STATISTICS);
				setState(476);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,25,_ctx) ) {
				case 1:
					{
					setState(475);
					identifier();
					}
					break;
				}
				}
				break;
			case 16:
				_localctx = new AddTableColumnsContext(_localctx);
				enterOuterAlt(_localctx, 16);
				{
				setState(478);
				match(ALTER);
				setState(479);
				match(TABLE);
				setState(480);
				multipartIdentifier();
				setState(481);
				match(ADD);
				setState(482);
				_la = _input.LA(1);
				if ( !(_la==COLUMN || _la==COLUMNS) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(483);
				((AddTableColumnsContext)_localctx).columns = qualifiedColTypeWithPositionList();
				}
				break;
			case 17:
				_localctx = new AddTableColumnsContext(_localctx);
				enterOuterAlt(_localctx, 17);
				{
				setState(485);
				match(ALTER);
				setState(486);
				match(TABLE);
				setState(487);
				multipartIdentifier();
				setState(488);
				match(ADD);
				setState(489);
				_la = _input.LA(1);
				if ( !(_la==COLUMN || _la==COLUMNS) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(490);
				match(LEFT_PAREN);
				setState(491);
				((AddTableColumnsContext)_localctx).columns = qualifiedColTypeWithPositionList();
				setState(492);
				match(RIGHT_PAREN);
				}
				break;
			case 18:
				_localctx = new RenameTableColumnContext(_localctx);
				enterOuterAlt(_localctx, 18);
				{
				setState(494);
				match(ALTER);
				setState(495);
				match(TABLE);
				setState(496);
				((RenameTableColumnContext)_localctx).table = multipartIdentifier();
				setState(497);
				match(RENAME);
				setState(498);
				match(COLUMN);
				setState(499);
				((RenameTableColumnContext)_localctx).from = multipartIdentifier();
				setState(500);
				match(TO);
				setState(501);
				((RenameTableColumnContext)_localctx).to = errorCapturingIdentifier();
				}
				break;
			case 19:
				_localctx = new DropTableColumnsContext(_localctx);
				enterOuterAlt(_localctx, 19);
				{
				setState(503);
				match(ALTER);
				setState(504);
				match(TABLE);
				setState(505);
				multipartIdentifier();
				setState(506);
				match(DROP);
				setState(507);
				_la = _input.LA(1);
				if ( !(_la==COLUMN || _la==COLUMNS) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(510);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IF) {
					{
					setState(508);
					match(IF);
					setState(509);
					match(EXISTS);
					}
				}

				setState(512);
				match(LEFT_PAREN);
				setState(513);
				((DropTableColumnsContext)_localctx).columns = multipartIdentifierList();
				setState(514);
				match(RIGHT_PAREN);
				}
				break;
			case 20:
				_localctx = new DropTableColumnsContext(_localctx);
				enterOuterAlt(_localctx, 20);
				{
				setState(516);
				match(ALTER);
				setState(517);
				match(TABLE);
				setState(518);
				multipartIdentifier();
				setState(519);
				match(DROP);
				setState(520);
				_la = _input.LA(1);
				if ( !(_la==COLUMN || _la==COLUMNS) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(523);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,27,_ctx) ) {
				case 1:
					{
					setState(521);
					match(IF);
					setState(522);
					match(EXISTS);
					}
					break;
				}
				setState(525);
				((DropTableColumnsContext)_localctx).columns = multipartIdentifierList();
				}
				break;
			case 21:
				_localctx = new RenameTableContext(_localctx);
				enterOuterAlt(_localctx, 21);
				{
				setState(527);
				match(ALTER);
				setState(528);
				_la = _input.LA(1);
				if ( !(_la==TABLE || _la==VIEW) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(529);
				((RenameTableContext)_localctx).from = multipartIdentifier();
				setState(530);
				match(RENAME);
				setState(531);
				match(TO);
				setState(532);
				((RenameTableContext)_localctx).to = multipartIdentifier();
				}
				break;
			case 22:
				_localctx = new SetTablePropertiesContext(_localctx);
				enterOuterAlt(_localctx, 22);
				{
				setState(534);
				match(ALTER);
				setState(535);
				_la = _input.LA(1);
				if ( !(_la==TABLE || _la==VIEW) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(536);
				multipartIdentifier();
				setState(537);
				match(SET);
				setState(538);
				match(TBLPROPERTIES);
				setState(539);
				propertyList();
				}
				break;
			case 23:
				_localctx = new UnsetTablePropertiesContext(_localctx);
				enterOuterAlt(_localctx, 23);
				{
				setState(541);
				match(ALTER);
				setState(542);
				_la = _input.LA(1);
				if ( !(_la==TABLE || _la==VIEW) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(543);
				multipartIdentifier();
				setState(544);
				match(UNSET);
				setState(545);
				match(TBLPROPERTIES);
				setState(548);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IF) {
					{
					setState(546);
					match(IF);
					setState(547);
					match(EXISTS);
					}
				}

				setState(550);
				propertyList();
				}
				break;
			case 24:
				_localctx = new AlterTableAlterColumnContext(_localctx);
				enterOuterAlt(_localctx, 24);
				{
				setState(552);
				match(ALTER);
				setState(553);
				match(TABLE);
				setState(554);
				((AlterTableAlterColumnContext)_localctx).table = multipartIdentifier();
				setState(555);
				_la = _input.LA(1);
				if ( !(_la==ALTER || _la==CHANGE) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(557);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,29,_ctx) ) {
				case 1:
					{
					setState(556);
					match(COLUMN);
					}
					break;
				}
				setState(559);
				((AlterTableAlterColumnContext)_localctx).column = multipartIdentifier();
				setState(561);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==AFTER || _la==COMMENT || _la==DROP || _la==FIRST || _la==SET || _la==TYPE) {
					{
					setState(560);
					alterColumnAction();
					}
				}

				}
				break;
			case 25:
				_localctx = new HiveChangeColumnContext(_localctx);
				enterOuterAlt(_localctx, 25);
				{
				setState(563);
				match(ALTER);
				setState(564);
				match(TABLE);
				setState(565);
				((HiveChangeColumnContext)_localctx).table = multipartIdentifier();
				setState(567);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(566);
					partitionSpec();
					}
				}

				setState(569);
				match(CHANGE);
				setState(571);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,32,_ctx) ) {
				case 1:
					{
					setState(570);
					match(COLUMN);
					}
					break;
				}
				setState(573);
				((HiveChangeColumnContext)_localctx).colName = multipartIdentifier();
				setState(574);
				colType();
				setState(576);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==AFTER || _la==FIRST) {
					{
					setState(575);
					colPosition();
					}
				}

				}
				break;
			case 26:
				_localctx = new HiveReplaceColumnsContext(_localctx);
				enterOuterAlt(_localctx, 26);
				{
				setState(578);
				match(ALTER);
				setState(579);
				match(TABLE);
				setState(580);
				((HiveReplaceColumnsContext)_localctx).table = multipartIdentifier();
				setState(582);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(581);
					partitionSpec();
					}
				}

				setState(584);
				match(REPLACE);
				setState(585);
				match(COLUMNS);
				setState(586);
				match(LEFT_PAREN);
				setState(587);
				((HiveReplaceColumnsContext)_localctx).columns = qualifiedColTypeWithPositionList();
				setState(588);
				match(RIGHT_PAREN);
				}
				break;
			case 27:
				_localctx = new SetTableSerDeContext(_localctx);
				enterOuterAlt(_localctx, 27);
				{
				setState(590);
				match(ALTER);
				setState(591);
				match(TABLE);
				setState(592);
				multipartIdentifier();
				setState(594);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(593);
					partitionSpec();
					}
				}

				setState(596);
				match(SET);
				setState(597);
				match(SERDE);
				setState(598);
				match(STRING);
				setState(602);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(599);
					match(WITH);
					setState(600);
					match(SERDEPROPERTIES);
					setState(601);
					propertyList();
					}
				}

				}
				break;
			case 28:
				_localctx = new SetTableSerDeContext(_localctx);
				enterOuterAlt(_localctx, 28);
				{
				setState(604);
				match(ALTER);
				setState(605);
				match(TABLE);
				setState(606);
				multipartIdentifier();
				setState(608);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(607);
					partitionSpec();
					}
				}

				setState(610);
				match(SET);
				setState(611);
				match(SERDEPROPERTIES);
				setState(612);
				propertyList();
				}
				break;
			case 29:
				_localctx = new AddTablePartitionContext(_localctx);
				enterOuterAlt(_localctx, 29);
				{
				setState(614);
				match(ALTER);
				setState(615);
				_la = _input.LA(1);
				if ( !(_la==TABLE || _la==VIEW) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(616);
				multipartIdentifier();
				setState(617);
				match(ADD);
				setState(621);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IF) {
					{
					setState(618);
					match(IF);
					setState(619);
					match(NOT);
					setState(620);
					match(EXISTS);
					}
				}

				setState(624); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(623);
					partitionSpecLocation();
					}
					}
					setState(626); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==PARTITION );
				}
				break;
			case 30:
				_localctx = new RenameTablePartitionContext(_localctx);
				enterOuterAlt(_localctx, 30);
				{
				setState(628);
				match(ALTER);
				setState(629);
				match(TABLE);
				setState(630);
				multipartIdentifier();
				setState(631);
				((RenameTablePartitionContext)_localctx).from = partitionSpec();
				setState(632);
				match(RENAME);
				setState(633);
				match(TO);
				setState(634);
				((RenameTablePartitionContext)_localctx).to = partitionSpec();
				}
				break;
			case 31:
				_localctx = new DropTablePartitionsContext(_localctx);
				enterOuterAlt(_localctx, 31);
				{
				setState(636);
				match(ALTER);
				setState(637);
				_la = _input.LA(1);
				if ( !(_la==TABLE || _la==VIEW) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(638);
				multipartIdentifier();
				setState(639);
				match(DROP);
				setState(642);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IF) {
					{
					setState(640);
					match(IF);
					setState(641);
					match(EXISTS);
					}
				}

				setState(644);
				partitionSpec();
				setState(649);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(645);
					match(COMMA);
					setState(646);
					partitionSpec();
					}
					}
					setState(651);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(653);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PURGE) {
					{
					setState(652);
					match(PURGE);
					}
				}

				}
				break;
			case 32:
				_localctx = new SetTableLocationContext(_localctx);
				enterOuterAlt(_localctx, 32);
				{
				setState(655);
				match(ALTER);
				setState(656);
				match(TABLE);
				setState(657);
				multipartIdentifier();
				setState(659);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(658);
					partitionSpec();
					}
				}

				setState(661);
				match(SET);
				setState(662);
				locationSpec();
				}
				break;
			case 33:
				_localctx = new RecoverPartitionsContext(_localctx);
				enterOuterAlt(_localctx, 33);
				{
				setState(664);
				match(ALTER);
				setState(665);
				match(TABLE);
				setState(666);
				multipartIdentifier();
				setState(667);
				match(RECOVER);
				setState(668);
				match(PARTITIONS);
				}
				break;
			case 34:
				_localctx = new DropTableContext(_localctx);
				enterOuterAlt(_localctx, 34);
				{
				setState(670);
				match(DROP);
				setState(671);
				match(TABLE);
				setState(674);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,44,_ctx) ) {
				case 1:
					{
					setState(672);
					match(IF);
					setState(673);
					match(EXISTS);
					}
					break;
				}
				setState(676);
				multipartIdentifier();
				setState(678);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PURGE) {
					{
					setState(677);
					match(PURGE);
					}
				}

				}
				break;
			case 35:
				_localctx = new DropViewContext(_localctx);
				enterOuterAlt(_localctx, 35);
				{
				setState(680);
				match(DROP);
				setState(681);
				match(VIEW);
				setState(684);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,46,_ctx) ) {
				case 1:
					{
					setState(682);
					match(IF);
					setState(683);
					match(EXISTS);
					}
					break;
				}
				setState(686);
				multipartIdentifier();
				}
				break;
			case 36:
				_localctx = new CreateViewContext(_localctx);
				enterOuterAlt(_localctx, 36);
				{
				setState(687);
				match(CREATE);
				setState(690);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OR) {
					{
					setState(688);
					match(OR);
					setState(689);
					match(REPLACE);
					}
				}

				setState(696);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==GLOBAL || _la==TEMPORARY) {
					{
					setState(693);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==GLOBAL) {
						{
						setState(692);
						match(GLOBAL);
						}
					}

					setState(695);
					match(TEMPORARY);
					}
				}

				setState(698);
				match(VIEW);
				setState(702);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,50,_ctx) ) {
				case 1:
					{
					setState(699);
					match(IF);
					setState(700);
					match(NOT);
					setState(701);
					match(EXISTS);
					}
					break;
				}
				setState(704);
				multipartIdentifier();
				setState(706);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LEFT_PAREN) {
					{
					setState(705);
					identifierCommentList();
					}
				}

				setState(716);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMENT || _la==PARTITIONED || _la==TBLPROPERTIES) {
					{
					setState(714);
					_errHandler.sync(this);
					switch (_input.LA(1)) {
					case COMMENT:
						{
						setState(708);
						commentSpec();
						}
						break;
					case PARTITIONED:
						{
						{
						setState(709);
						match(PARTITIONED);
						setState(710);
						match(ON);
						setState(711);
						identifierList();
						}
						}
						break;
					case TBLPROPERTIES:
						{
						{
						setState(712);
						match(TBLPROPERTIES);
						setState(713);
						propertyList();
						}
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					}
					setState(718);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(719);
				match(AS);
				setState(720);
				query();
				}
				break;
			case 37:
				_localctx = new CreateTempViewUsingContext(_localctx);
				enterOuterAlt(_localctx, 37);
				{
				setState(722);
				match(CREATE);
				setState(725);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OR) {
					{
					setState(723);
					match(OR);
					setState(724);
					match(REPLACE);
					}
				}

				setState(728);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==GLOBAL) {
					{
					setState(727);
					match(GLOBAL);
					}
				}

				setState(730);
				match(TEMPORARY);
				setState(731);
				match(VIEW);
				setState(732);
				tableIdentifier();
				setState(737);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LEFT_PAREN) {
					{
					setState(733);
					match(LEFT_PAREN);
					setState(734);
					colTypeList();
					setState(735);
					match(RIGHT_PAREN);
					}
				}

				setState(739);
				tableProvider();
				setState(742);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OPTIONS) {
					{
					setState(740);
					match(OPTIONS);
					setState(741);
					propertyList();
					}
				}

				}
				break;
			case 38:
				_localctx = new AlterViewQueryContext(_localctx);
				enterOuterAlt(_localctx, 38);
				{
				setState(744);
				match(ALTER);
				setState(745);
				match(VIEW);
				setState(746);
				multipartIdentifier();
				setState(748);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==AS) {
					{
					setState(747);
					match(AS);
					}
				}

				setState(750);
				query();
				}
				break;
			case 39:
				_localctx = new CreateFunctionContext(_localctx);
				enterOuterAlt(_localctx, 39);
				{
				setState(752);
				match(CREATE);
				setState(755);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OR) {
					{
					setState(753);
					match(OR);
					setState(754);
					match(REPLACE);
					}
				}

				setState(758);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==TEMPORARY) {
					{
					setState(757);
					match(TEMPORARY);
					}
				}

				setState(760);
				match(FUNCTION);
				setState(764);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,61,_ctx) ) {
				case 1:
					{
					setState(761);
					match(IF);
					setState(762);
					match(NOT);
					setState(763);
					match(EXISTS);
					}
					break;
				}
				setState(766);
				multipartIdentifier();
				setState(767);
				match(AS);
				setState(768);
				((CreateFunctionContext)_localctx).className = match(STRING);
				setState(778);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==USING) {
					{
					setState(769);
					match(USING);
					setState(770);
					resource();
					setState(775);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==COMMA) {
						{
						{
						setState(771);
						match(COMMA);
						setState(772);
						resource();
						}
						}
						setState(777);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				}
				break;
			case 40:
				_localctx = new DropFunctionContext(_localctx);
				enterOuterAlt(_localctx, 40);
				{
				setState(780);
				match(DROP);
				setState(782);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==TEMPORARY) {
					{
					setState(781);
					match(TEMPORARY);
					}
				}

				setState(784);
				match(FUNCTION);
				setState(787);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,65,_ctx) ) {
				case 1:
					{
					setState(785);
					match(IF);
					setState(786);
					match(EXISTS);
					}
					break;
				}
				setState(789);
				multipartIdentifier();
				}
				break;
			case 41:
				_localctx = new ExplainContext(_localctx);
				enterOuterAlt(_localctx, 41);
				{
				setState(790);
				match(EXPLAIN);
				setState(792);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==CODEGEN || _la==COST || ((((_la - 88)) & ~0x3f) == 0 && ((1L << (_la - 88)) & 72057594037936129L) != 0)) {
					{
					setState(791);
					_la = _input.LA(1);
					if ( !(_la==CODEGEN || _la==COST || ((((_la - 88)) & ~0x3f) == 0 && ((1L << (_la - 88)) & 72057594037936129L) != 0)) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				setState(794);
				statement();
				}
				break;
			case 42:
				_localctx = new ShowTablesContext(_localctx);
				enterOuterAlt(_localctx, 42);
				{
				setState(795);
				match(SHOW);
				setState(796);
				match(TABLES);
				setState(799);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FROM || _la==IN) {
					{
					setState(797);
					_la = _input.LA(1);
					if ( !(_la==FROM || _la==IN) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(798);
					multipartIdentifier();
					}
				}

				setState(805);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LIKE || _la==STRING) {
					{
					setState(802);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==LIKE) {
						{
						setState(801);
						match(LIKE);
						}
					}

					setState(804);
					((ShowTablesContext)_localctx).pattern = match(STRING);
					}
				}

				}
				break;
			case 43:
				_localctx = new ShowTableExtendedContext(_localctx);
				enterOuterAlt(_localctx, 43);
				{
				setState(807);
				match(SHOW);
				setState(808);
				match(TABLE);
				setState(809);
				match(EXTENDED);
				setState(812);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FROM || _la==IN) {
					{
					setState(810);
					_la = _input.LA(1);
					if ( !(_la==FROM || _la==IN) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(811);
					((ShowTableExtendedContext)_localctx).ns = multipartIdentifier();
					}
				}

				setState(814);
				match(LIKE);
				setState(815);
				((ShowTableExtendedContext)_localctx).pattern = match(STRING);
				setState(817);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(816);
					partitionSpec();
					}
				}

				}
				break;
			case 44:
				_localctx = new ShowTblPropertiesContext(_localctx);
				enterOuterAlt(_localctx, 44);
				{
				setState(819);
				match(SHOW);
				setState(820);
				match(TBLPROPERTIES);
				setState(821);
				((ShowTblPropertiesContext)_localctx).table = multipartIdentifier();
				setState(826);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LEFT_PAREN) {
					{
					setState(822);
					match(LEFT_PAREN);
					setState(823);
					((ShowTblPropertiesContext)_localctx).key = propertyKey();
					setState(824);
					match(RIGHT_PAREN);
					}
				}

				}
				break;
			case 45:
				_localctx = new ShowColumnsContext(_localctx);
				enterOuterAlt(_localctx, 45);
				{
				setState(828);
				match(SHOW);
				setState(829);
				match(COLUMNS);
				setState(830);
				_la = _input.LA(1);
				if ( !(_la==FROM || _la==IN) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(831);
				((ShowColumnsContext)_localctx).table = multipartIdentifier();
				setState(834);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FROM || _la==IN) {
					{
					setState(832);
					_la = _input.LA(1);
					if ( !(_la==FROM || _la==IN) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(833);
					((ShowColumnsContext)_localctx).ns = multipartIdentifier();
					}
				}

				}
				break;
			case 46:
				_localctx = new ShowViewsContext(_localctx);
				enterOuterAlt(_localctx, 46);
				{
				setState(836);
				match(SHOW);
				setState(837);
				match(VIEWS);
				setState(840);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FROM || _la==IN) {
					{
					setState(838);
					_la = _input.LA(1);
					if ( !(_la==FROM || _la==IN) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(839);
					multipartIdentifier();
					}
				}

				setState(846);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LIKE || _la==STRING) {
					{
					setState(843);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==LIKE) {
						{
						setState(842);
						match(LIKE);
						}
					}

					setState(845);
					((ShowViewsContext)_localctx).pattern = match(STRING);
					}
				}

				}
				break;
			case 47:
				_localctx = new ShowPartitionsContext(_localctx);
				enterOuterAlt(_localctx, 47);
				{
				setState(848);
				match(SHOW);
				setState(849);
				match(PARTITIONS);
				setState(850);
				multipartIdentifier();
				setState(852);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(851);
					partitionSpec();
					}
				}

				}
				break;
			case 48:
				_localctx = new ShowFunctionsContext(_localctx);
				enterOuterAlt(_localctx, 48);
				{
				setState(854);
				match(SHOW);
				setState(856);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,78,_ctx) ) {
				case 1:
					{
					setState(855);
					identifier();
					}
					break;
				}
				setState(858);
				match(FUNCTIONS);
				setState(861);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,79,_ctx) ) {
				case 1:
					{
					setState(859);
					_la = _input.LA(1);
					if ( !(_la==FROM || _la==IN) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(860);
					((ShowFunctionsContext)_localctx).ns = multipartIdentifier();
					}
					break;
				}
				setState(870);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,82,_ctx) ) {
				case 1:
					{
					setState(864);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,80,_ctx) ) {
					case 1:
						{
						setState(863);
						match(LIKE);
						}
						break;
					}
					setState(868);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,81,_ctx) ) {
					case 1:
						{
						setState(866);
						((ShowFunctionsContext)_localctx).legacy = multipartIdentifier();
						}
						break;
					case 2:
						{
						setState(867);
						((ShowFunctionsContext)_localctx).pattern = match(STRING);
						}
						break;
					}
					}
					break;
				}
				}
				break;
			case 49:
				_localctx = new ShowCreateTableContext(_localctx);
				enterOuterAlt(_localctx, 49);
				{
				setState(872);
				match(SHOW);
				setState(873);
				match(CREATE);
				setState(874);
				match(TABLE);
				setState(875);
				multipartIdentifier();
				setState(878);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==AS) {
					{
					setState(876);
					match(AS);
					setState(877);
					match(SERDE);
					}
				}

				}
				break;
			case 50:
				_localctx = new ShowCurrentNamespaceContext(_localctx);
				enterOuterAlt(_localctx, 50);
				{
				setState(880);
				match(SHOW);
				setState(881);
				match(CURRENT);
				setState(882);
				namespace();
				}
				break;
			case 51:
				_localctx = new ShowCatalogsContext(_localctx);
				enterOuterAlt(_localctx, 51);
				{
				setState(883);
				match(SHOW);
				setState(884);
				match(CATALOGS);
				setState(889);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LIKE || _la==STRING) {
					{
					setState(886);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==LIKE) {
						{
						setState(885);
						match(LIKE);
						}
					}

					setState(888);
					((ShowCatalogsContext)_localctx).pattern = match(STRING);
					}
				}

				}
				break;
			case 52:
				_localctx = new DescribeFunctionContext(_localctx);
				enterOuterAlt(_localctx, 52);
				{
				setState(891);
				_la = _input.LA(1);
				if ( !(_la==DESC || _la==DESCRIBE) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(892);
				match(FUNCTION);
				setState(894);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,86,_ctx) ) {
				case 1:
					{
					setState(893);
					match(EXTENDED);
					}
					break;
				}
				setState(896);
				describeFuncName();
				}
				break;
			case 53:
				_localctx = new DescribeNamespaceContext(_localctx);
				enterOuterAlt(_localctx, 53);
				{
				setState(897);
				_la = _input.LA(1);
				if ( !(_la==DESC || _la==DESCRIBE) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(898);
				namespace();
				setState(900);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,87,_ctx) ) {
				case 1:
					{
					setState(899);
					match(EXTENDED);
					}
					break;
				}
				setState(902);
				multipartIdentifier();
				}
				break;
			case 54:
				_localctx = new DescribeRelationContext(_localctx);
				enterOuterAlt(_localctx, 54);
				{
				setState(904);
				_la = _input.LA(1);
				if ( !(_la==DESC || _la==DESCRIBE) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(906);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,88,_ctx) ) {
				case 1:
					{
					setState(905);
					match(TABLE);
					}
					break;
				}
				setState(909);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,89,_ctx) ) {
				case 1:
					{
					setState(908);
					((DescribeRelationContext)_localctx).option = _input.LT(1);
					_la = _input.LA(1);
					if ( !(_la==EXTENDED || _la==FORMATTED) ) {
						((DescribeRelationContext)_localctx).option = (Token)_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
					break;
				}
				setState(911);
				multipartIdentifier();
				setState(913);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,90,_ctx) ) {
				case 1:
					{
					setState(912);
					partitionSpec();
					}
					break;
				}
				setState(916);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,91,_ctx) ) {
				case 1:
					{
					setState(915);
					describeColName();
					}
					break;
				}
				}
				break;
			case 55:
				_localctx = new DescribeQueryContext(_localctx);
				enterOuterAlt(_localctx, 55);
				{
				setState(918);
				_la = _input.LA(1);
				if ( !(_la==DESC || _la==DESCRIBE) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(920);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==QUERY) {
					{
					setState(919);
					match(QUERY);
					}
				}

				setState(922);
				query();
				}
				break;
			case 56:
				_localctx = new CommentNamespaceContext(_localctx);
				enterOuterAlt(_localctx, 56);
				{
				setState(923);
				match(COMMENT);
				setState(924);
				match(ON);
				setState(925);
				namespace();
				setState(926);
				multipartIdentifier();
				setState(927);
				match(IS);
				setState(928);
				((CommentNamespaceContext)_localctx).comment = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==NULL || _la==STRING) ) {
					((CommentNamespaceContext)_localctx).comment = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			case 57:
				_localctx = new CommentTableContext(_localctx);
				enterOuterAlt(_localctx, 57);
				{
				setState(930);
				match(COMMENT);
				setState(931);
				match(ON);
				setState(932);
				match(TABLE);
				setState(933);
				multipartIdentifier();
				setState(934);
				match(IS);
				setState(935);
				((CommentTableContext)_localctx).comment = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==NULL || _la==STRING) ) {
					((CommentTableContext)_localctx).comment = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			case 58:
				_localctx = new RefreshTableContext(_localctx);
				enterOuterAlt(_localctx, 58);
				{
				setState(937);
				match(REFRESH);
				setState(938);
				match(TABLE);
				setState(939);
				multipartIdentifier();
				}
				break;
			case 59:
				_localctx = new RefreshFunctionContext(_localctx);
				enterOuterAlt(_localctx, 59);
				{
				setState(940);
				match(REFRESH);
				setState(941);
				match(FUNCTION);
				setState(942);
				multipartIdentifier();
				}
				break;
			case 60:
				_localctx = new RefreshResourceContext(_localctx);
				enterOuterAlt(_localctx, 60);
				{
				setState(943);
				match(REFRESH);
				setState(951);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,94,_ctx) ) {
				case 1:
					{
					setState(944);
					match(STRING);
					}
					break;
				case 2:
					{
					setState(948);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,93,_ctx);
					while ( _alt!=1 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
						if ( _alt==1+1 ) {
							{
							{
							setState(945);
							matchWildcard();
							}
							} 
						}
						setState(950);
						_errHandler.sync(this);
						_alt = getInterpreter().adaptivePredict(_input,93,_ctx);
					}
					}
					break;
				}
				}
				break;
			case 61:
				_localctx = new CacheTableContext(_localctx);
				enterOuterAlt(_localctx, 61);
				{
				setState(953);
				match(CACHE);
				setState(955);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LAZY) {
					{
					setState(954);
					match(LAZY);
					}
				}

				setState(957);
				match(TABLE);
				setState(958);
				multipartIdentifier();
				setState(961);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OPTIONS) {
					{
					setState(959);
					match(OPTIONS);
					setState(960);
					((CacheTableContext)_localctx).options = propertyList();
					}
				}

				setState(967);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LEFT_PAREN || _la==AS || _la==FROM || _la==MAP || ((((_la - 195)) & ~0x3f) == 0 && ((1L << (_la - 195)) & 70368748371969L) != 0) || _la==VALUES || _la==WITH) {
					{
					setState(964);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==AS) {
						{
						setState(963);
						match(AS);
						}
					}

					setState(966);
					query();
					}
				}

				}
				break;
			case 62:
				_localctx = new UncacheTableContext(_localctx);
				enterOuterAlt(_localctx, 62);
				{
				setState(969);
				match(UNCACHE);
				setState(970);
				match(TABLE);
				setState(973);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,99,_ctx) ) {
				case 1:
					{
					setState(971);
					match(IF);
					setState(972);
					match(EXISTS);
					}
					break;
				}
				setState(975);
				multipartIdentifier();
				}
				break;
			case 63:
				_localctx = new ClearCacheContext(_localctx);
				enterOuterAlt(_localctx, 63);
				{
				setState(976);
				match(CLEAR);
				setState(977);
				match(CACHE);
				}
				break;
			case 64:
				_localctx = new LoadDataContext(_localctx);
				enterOuterAlt(_localctx, 64);
				{
				setState(978);
				match(LOAD);
				setState(979);
				match(DATA);
				setState(981);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LOCAL) {
					{
					setState(980);
					match(LOCAL);
					}
				}

				setState(983);
				match(INPATH);
				setState(984);
				((LoadDataContext)_localctx).path = match(STRING);
				setState(986);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OVERWRITE) {
					{
					setState(985);
					match(OVERWRITE);
					}
				}

				setState(988);
				match(INTO);
				setState(989);
				match(TABLE);
				setState(990);
				multipartIdentifier();
				setState(992);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(991);
					partitionSpec();
					}
				}

				}
				break;
			case 65:
				_localctx = new TruncateTableContext(_localctx);
				enterOuterAlt(_localctx, 65);
				{
				setState(994);
				match(TRUNCATE);
				setState(995);
				match(TABLE);
				setState(996);
				multipartIdentifier();
				setState(998);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(997);
					partitionSpec();
					}
				}

				}
				break;
			case 66:
				_localctx = new RepairTableContext(_localctx);
				enterOuterAlt(_localctx, 66);
				{
				setState(1000);
				match(MSCK);
				setState(1001);
				match(REPAIR);
				setState(1002);
				match(TABLE);
				setState(1003);
				multipartIdentifier();
				setState(1006);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ADD || _la==DROP || _la==SYNC) {
					{
					setState(1004);
					((RepairTableContext)_localctx).option = _input.LT(1);
					_la = _input.LA(1);
					if ( !(_la==ADD || _la==DROP || _la==SYNC) ) {
						((RepairTableContext)_localctx).option = (Token)_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(1005);
					match(PARTITIONS);
					}
				}

				}
				break;
			case 67:
				_localctx = new ManageResourceContext(_localctx);
				enterOuterAlt(_localctx, 67);
				{
				setState(1008);
				((ManageResourceContext)_localctx).op = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==ADD || _la==LIST) ) {
					((ManageResourceContext)_localctx).op = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(1009);
				identifier();
				setState(1013);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,105,_ctx);
				while ( _alt!=1 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1+1 ) {
						{
						{
						setState(1010);
						matchWildcard();
						}
						} 
					}
					setState(1015);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,105,_ctx);
				}
				}
				break;
			case 68:
				_localctx = new FailNativeCommandContext(_localctx);
				enterOuterAlt(_localctx, 68);
				{
				setState(1016);
				match(SET);
				setState(1017);
				match(ROLE);
				setState(1021);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,106,_ctx);
				while ( _alt!=1 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1+1 ) {
						{
						{
						setState(1018);
						matchWildcard();
						}
						} 
					}
					setState(1023);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,106,_ctx);
				}
				}
				break;
			case 69:
				_localctx = new SetTimeZoneContext(_localctx);
				enterOuterAlt(_localctx, 69);
				{
				setState(1024);
				match(SET);
				setState(1025);
				match(TIME);
				setState(1026);
				match(ZONE);
				setState(1027);
				interval();
				}
				break;
			case 70:
				_localctx = new SetTimeZoneContext(_localctx);
				enterOuterAlt(_localctx, 70);
				{
				setState(1028);
				match(SET);
				setState(1029);
				match(TIME);
				setState(1030);
				match(ZONE);
				setState(1031);
				((SetTimeZoneContext)_localctx).timezone = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==LOCAL || _la==STRING) ) {
					((SetTimeZoneContext)_localctx).timezone = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			case 71:
				_localctx = new SetTimeZoneContext(_localctx);
				enterOuterAlt(_localctx, 71);
				{
				setState(1032);
				match(SET);
				setState(1033);
				match(TIME);
				setState(1034);
				match(ZONE);
				setState(1038);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,107,_ctx);
				while ( _alt!=1 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1+1 ) {
						{
						{
						setState(1035);
						matchWildcard();
						}
						} 
					}
					setState(1040);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,107,_ctx);
				}
				}
				break;
			case 72:
				_localctx = new SetQuotedConfigurationContext(_localctx);
				enterOuterAlt(_localctx, 72);
				{
				setState(1041);
				match(SET);
				setState(1042);
				configKey();
				setState(1043);
				match(EQ);
				setState(1044);
				configValue();
				}
				break;
			case 73:
				_localctx = new SetConfigurationContext(_localctx);
				enterOuterAlt(_localctx, 73);
				{
				setState(1046);
				match(SET);
				setState(1047);
				configKey();
				setState(1055);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==EQ) {
					{
					setState(1048);
					match(EQ);
					setState(1052);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,108,_ctx);
					while ( _alt!=1 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
						if ( _alt==1+1 ) {
							{
							{
							setState(1049);
							matchWildcard();
							}
							} 
						}
						setState(1054);
						_errHandler.sync(this);
						_alt = getInterpreter().adaptivePredict(_input,108,_ctx);
					}
					}
				}

				}
				break;
			case 74:
				_localctx = new SetQuotedConfigurationContext(_localctx);
				enterOuterAlt(_localctx, 74);
				{
				setState(1057);
				match(SET);
				setState(1061);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,110,_ctx);
				while ( _alt!=1 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1+1 ) {
						{
						{
						setState(1058);
						matchWildcard();
						}
						} 
					}
					setState(1063);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,110,_ctx);
				}
				setState(1064);
				match(EQ);
				setState(1065);
				configValue();
				}
				break;
			case 75:
				_localctx = new SetConfigurationContext(_localctx);
				enterOuterAlt(_localctx, 75);
				{
				setState(1066);
				match(SET);
				setState(1070);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,111,_ctx);
				while ( _alt!=1 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1+1 ) {
						{
						{
						setState(1067);
						matchWildcard();
						}
						} 
					}
					setState(1072);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,111,_ctx);
				}
				}
				break;
			case 76:
				_localctx = new ResetQuotedConfigurationContext(_localctx);
				enterOuterAlt(_localctx, 76);
				{
				setState(1073);
				match(RESET);
				setState(1074);
				configKey();
				}
				break;
			case 77:
				_localctx = new ResetConfigurationContext(_localctx);
				enterOuterAlt(_localctx, 77);
				{
				setState(1075);
				match(RESET);
				setState(1079);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,112,_ctx);
				while ( _alt!=1 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1+1 ) {
						{
						{
						setState(1076);
						matchWildcard();
						}
						} 
					}
					setState(1081);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,112,_ctx);
				}
				}
				break;
			case 78:
				_localctx = new CreateIndexContext(_localctx);
				enterOuterAlt(_localctx, 78);
				{
				setState(1082);
				match(CREATE);
				setState(1083);
				match(INDEX);
				setState(1087);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,113,_ctx) ) {
				case 1:
					{
					setState(1084);
					match(IF);
					setState(1085);
					match(NOT);
					setState(1086);
					match(EXISTS);
					}
					break;
				}
				setState(1089);
				identifier();
				setState(1090);
				match(ON);
				setState(1092);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,114,_ctx) ) {
				case 1:
					{
					setState(1091);
					match(TABLE);
					}
					break;
				}
				setState(1094);
				multipartIdentifier();
				setState(1097);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==USING) {
					{
					setState(1095);
					match(USING);
					setState(1096);
					((CreateIndexContext)_localctx).indexType = identifier();
					}
				}

				setState(1099);
				match(LEFT_PAREN);
				setState(1100);
				((CreateIndexContext)_localctx).columns = multipartIdentifierPropertyList();
				setState(1101);
				match(RIGHT_PAREN);
				setState(1104);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OPTIONS) {
					{
					setState(1102);
					match(OPTIONS);
					setState(1103);
					((CreateIndexContext)_localctx).options = propertyList();
					}
				}

				}
				break;
			case 79:
				_localctx = new DropIndexContext(_localctx);
				enterOuterAlt(_localctx, 79);
				{
				setState(1106);
				match(DROP);
				setState(1107);
				match(INDEX);
				setState(1110);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,117,_ctx) ) {
				case 1:
					{
					setState(1108);
					match(IF);
					setState(1109);
					match(EXISTS);
					}
					break;
				}
				setState(1112);
				identifier();
				setState(1113);
				match(ON);
				setState(1115);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,118,_ctx) ) {
				case 1:
					{
					setState(1114);
					match(TABLE);
					}
					break;
				}
				setState(1117);
				multipartIdentifier();
				}
				break;
			case 80:
				_localctx = new FailNativeCommandContext(_localctx);
				enterOuterAlt(_localctx, 80);
				{
				setState(1119);
				unsupportedHiveNativeCommands();
				setState(1123);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,119,_ctx);
				while ( _alt!=1 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1+1 ) {
						{
						{
						setState(1120);
						matchWildcard();
						}
						} 
					}
					setState(1125);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,119,_ctx);
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ConfigKeyContext extends ParserRuleContext {
		public QuotedIdentifierContext quotedIdentifier() {
			return getRuleContext(QuotedIdentifierContext.class,0);
		}
		public ConfigKeyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_configKey; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterConfigKey(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitConfigKey(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitConfigKey(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ConfigKeyContext configKey() throws RecognitionException {
		ConfigKeyContext _localctx = new ConfigKeyContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_configKey);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1128);
			quotedIdentifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ConfigValueContext extends ParserRuleContext {
		public QuotedIdentifierContext quotedIdentifier() {
			return getRuleContext(QuotedIdentifierContext.class,0);
		}
		public ConfigValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_configValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterConfigValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitConfigValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitConfigValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ConfigValueContext configValue() throws RecognitionException {
		ConfigValueContext _localctx = new ConfigValueContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_configValue);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1130);
			quotedIdentifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class UnsupportedHiveNativeCommandsContext extends ParserRuleContext {
		public Token kw1;
		public Token kw2;
		public Token kw3;
		public Token kw4;
		public Token kw5;
		public Token kw6;
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode ROLE() { return getToken(SqlBaseParser.ROLE, 0); }
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode GRANT() { return getToken(SqlBaseParser.GRANT, 0); }
		public TerminalNode REVOKE() { return getToken(SqlBaseParser.REVOKE, 0); }
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode PRINCIPALS() { return getToken(SqlBaseParser.PRINCIPALS, 0); }
		public TerminalNode ROLES() { return getToken(SqlBaseParser.ROLES, 0); }
		public TerminalNode CURRENT() { return getToken(SqlBaseParser.CURRENT, 0); }
		public TerminalNode EXPORT() { return getToken(SqlBaseParser.EXPORT, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode IMPORT() { return getToken(SqlBaseParser.IMPORT, 0); }
		public TerminalNode COMPACTIONS() { return getToken(SqlBaseParser.COMPACTIONS, 0); }
		public TerminalNode TRANSACTIONS() { return getToken(SqlBaseParser.TRANSACTIONS, 0); }
		public TerminalNode INDEXES() { return getToken(SqlBaseParser.INDEXES, 0); }
		public TerminalNode LOCKS() { return getToken(SqlBaseParser.LOCKS, 0); }
		public TerminalNode INDEX() { return getToken(SqlBaseParser.INDEX, 0); }
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode LOCK() { return getToken(SqlBaseParser.LOCK, 0); }
		public TerminalNode DATABASE() { return getToken(SqlBaseParser.DATABASE, 0); }
		public TerminalNode UNLOCK() { return getToken(SqlBaseParser.UNLOCK, 0); }
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode MACRO() { return getToken(SqlBaseParser.MACRO, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode CLUSTERED() { return getToken(SqlBaseParser.CLUSTERED, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public TerminalNode SORTED() { return getToken(SqlBaseParser.SORTED, 0); }
		public TerminalNode SKEWED() { return getToken(SqlBaseParser.SKEWED, 0); }
		public TerminalNode STORED() { return getToken(SqlBaseParser.STORED, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TerminalNode DIRECTORIES() { return getToken(SqlBaseParser.DIRECTORIES, 0); }
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode LOCATION() { return getToken(SqlBaseParser.LOCATION, 0); }
		public TerminalNode EXCHANGE() { return getToken(SqlBaseParser.EXCHANGE, 0); }
		public TerminalNode PARTITION() { return getToken(SqlBaseParser.PARTITION, 0); }
		public TerminalNode ARCHIVE() { return getToken(SqlBaseParser.ARCHIVE, 0); }
		public TerminalNode UNARCHIVE() { return getToken(SqlBaseParser.UNARCHIVE, 0); }
		public TerminalNode TOUCH() { return getToken(SqlBaseParser.TOUCH, 0); }
		public TerminalNode COMPACT() { return getToken(SqlBaseParser.COMPACT, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public TerminalNode CONCATENATE() { return getToken(SqlBaseParser.CONCATENATE, 0); }
		public TerminalNode FILEFORMAT() { return getToken(SqlBaseParser.FILEFORMAT, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public TerminalNode START() { return getToken(SqlBaseParser.START, 0); }
		public TerminalNode TRANSACTION() { return getToken(SqlBaseParser.TRANSACTION, 0); }
		public TerminalNode COMMIT() { return getToken(SqlBaseParser.COMMIT, 0); }
		public TerminalNode ROLLBACK() { return getToken(SqlBaseParser.ROLLBACK, 0); }
		public TerminalNode DFS() { return getToken(SqlBaseParser.DFS, 0); }
		public UnsupportedHiveNativeCommandsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_unsupportedHiveNativeCommands; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterUnsupportedHiveNativeCommands(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitUnsupportedHiveNativeCommands(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitUnsupportedHiveNativeCommands(this);
			else return visitor.visitChildren(this);
		}
	}

	public final UnsupportedHiveNativeCommandsContext unsupportedHiveNativeCommands() throws RecognitionException {
		UnsupportedHiveNativeCommandsContext _localctx = new UnsupportedHiveNativeCommandsContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_unsupportedHiveNativeCommands);
		int _la;
		try {
			setState(1300);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,128,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1132);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(CREATE);
				setState(1133);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(ROLE);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1134);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(DROP);
				setState(1135);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(ROLE);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1136);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(GRANT);
				setState(1138);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,121,_ctx) ) {
				case 1:
					{
					setState(1137);
					((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(ROLE);
					}
					break;
				}
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(1140);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(REVOKE);
				setState(1142);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,122,_ctx) ) {
				case 1:
					{
					setState(1141);
					((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(ROLE);
					}
					break;
				}
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(1144);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(1145);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(GRANT);
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(1146);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(1147);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(ROLE);
				setState(1149);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,123,_ctx) ) {
				case 1:
					{
					setState(1148);
					((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(GRANT);
					}
					break;
				}
				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(1151);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(1152);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(PRINCIPALS);
				}
				break;
			case 8:
				enterOuterAlt(_localctx, 8);
				{
				setState(1153);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(1154);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(ROLES);
				}
				break;
			case 9:
				enterOuterAlt(_localctx, 9);
				{
				setState(1155);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(1156);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(CURRENT);
				setState(1157);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(ROLES);
				}
				break;
			case 10:
				enterOuterAlt(_localctx, 10);
				{
				setState(1158);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(EXPORT);
				setState(1159);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				}
				break;
			case 11:
				enterOuterAlt(_localctx, 11);
				{
				setState(1160);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(IMPORT);
				setState(1161);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				}
				break;
			case 12:
				enterOuterAlt(_localctx, 12);
				{
				setState(1162);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(1163);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(COMPACTIONS);
				}
				break;
			case 13:
				enterOuterAlt(_localctx, 13);
				{
				setState(1164);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(1165);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(CREATE);
				setState(1166);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(TABLE);
				}
				break;
			case 14:
				enterOuterAlt(_localctx, 14);
				{
				setState(1167);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(1168);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TRANSACTIONS);
				}
				break;
			case 15:
				enterOuterAlt(_localctx, 15);
				{
				setState(1169);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(1170);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(INDEXES);
				}
				break;
			case 16:
				enterOuterAlt(_localctx, 16);
				{
				setState(1171);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(1172);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(LOCKS);
				}
				break;
			case 17:
				enterOuterAlt(_localctx, 17);
				{
				setState(1173);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(CREATE);
				setState(1174);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(INDEX);
				}
				break;
			case 18:
				enterOuterAlt(_localctx, 18);
				{
				setState(1175);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(DROP);
				setState(1176);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(INDEX);
				}
				break;
			case 19:
				enterOuterAlt(_localctx, 19);
				{
				setState(1177);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1178);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(INDEX);
				}
				break;
			case 20:
				enterOuterAlt(_localctx, 20);
				{
				setState(1179);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(LOCK);
				setState(1180);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				}
				break;
			case 21:
				enterOuterAlt(_localctx, 21);
				{
				setState(1181);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(LOCK);
				setState(1182);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(DATABASE);
				}
				break;
			case 22:
				enterOuterAlt(_localctx, 22);
				{
				setState(1183);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(UNLOCK);
				setState(1184);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				}
				break;
			case 23:
				enterOuterAlt(_localctx, 23);
				{
				setState(1185);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(UNLOCK);
				setState(1186);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(DATABASE);
				}
				break;
			case 24:
				enterOuterAlt(_localctx, 24);
				{
				setState(1187);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(CREATE);
				setState(1188);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TEMPORARY);
				setState(1189);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(MACRO);
				}
				break;
			case 25:
				enterOuterAlt(_localctx, 25);
				{
				setState(1190);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(DROP);
				setState(1191);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TEMPORARY);
				setState(1192);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(MACRO);
				}
				break;
			case 26:
				enterOuterAlt(_localctx, 26);
				{
				setState(1193);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1194);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1195);
				tableIdentifier();
				setState(1196);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(NOT);
				setState(1197);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(CLUSTERED);
				}
				break;
			case 27:
				enterOuterAlt(_localctx, 27);
				{
				setState(1199);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1200);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1201);
				tableIdentifier();
				setState(1202);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(CLUSTERED);
				setState(1203);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(BY);
				}
				break;
			case 28:
				enterOuterAlt(_localctx, 28);
				{
				setState(1205);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1206);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1207);
				tableIdentifier();
				setState(1208);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(NOT);
				setState(1209);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(SORTED);
				}
				break;
			case 29:
				enterOuterAlt(_localctx, 29);
				{
				setState(1211);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1212);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1213);
				tableIdentifier();
				setState(1214);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(SKEWED);
				setState(1215);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(BY);
				}
				break;
			case 30:
				enterOuterAlt(_localctx, 30);
				{
				setState(1217);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1218);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1219);
				tableIdentifier();
				setState(1220);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(NOT);
				setState(1221);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(SKEWED);
				}
				break;
			case 31:
				enterOuterAlt(_localctx, 31);
				{
				setState(1223);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1224);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1225);
				tableIdentifier();
				setState(1226);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(NOT);
				setState(1227);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(STORED);
				setState(1228);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw5 = match(AS);
				setState(1229);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw6 = match(DIRECTORIES);
				}
				break;
			case 32:
				enterOuterAlt(_localctx, 32);
				{
				setState(1231);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1232);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1233);
				tableIdentifier();
				setState(1234);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(SET);
				setState(1235);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(SKEWED);
				setState(1236);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw5 = match(LOCATION);
				}
				break;
			case 33:
				enterOuterAlt(_localctx, 33);
				{
				setState(1238);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1239);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1240);
				tableIdentifier();
				setState(1241);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(EXCHANGE);
				setState(1242);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(PARTITION);
				}
				break;
			case 34:
				enterOuterAlt(_localctx, 34);
				{
				setState(1244);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1245);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1246);
				tableIdentifier();
				setState(1247);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(ARCHIVE);
				setState(1248);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(PARTITION);
				}
				break;
			case 35:
				enterOuterAlt(_localctx, 35);
				{
				setState(1250);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1251);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1252);
				tableIdentifier();
				setState(1253);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(UNARCHIVE);
				setState(1254);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(PARTITION);
				}
				break;
			case 36:
				enterOuterAlt(_localctx, 36);
				{
				setState(1256);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1257);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1258);
				tableIdentifier();
				setState(1259);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(TOUCH);
				}
				break;
			case 37:
				enterOuterAlt(_localctx, 37);
				{
				setState(1261);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1262);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1263);
				tableIdentifier();
				setState(1265);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(1264);
					partitionSpec();
					}
				}

				setState(1267);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(COMPACT);
				}
				break;
			case 38:
				enterOuterAlt(_localctx, 38);
				{
				setState(1269);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1270);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1271);
				tableIdentifier();
				setState(1273);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(1272);
					partitionSpec();
					}
				}

				setState(1275);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(CONCATENATE);
				}
				break;
			case 39:
				enterOuterAlt(_localctx, 39);
				{
				setState(1277);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1278);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1279);
				tableIdentifier();
				setState(1281);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(1280);
					partitionSpec();
					}
				}

				setState(1283);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(SET);
				setState(1284);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(FILEFORMAT);
				}
				break;
			case 40:
				enterOuterAlt(_localctx, 40);
				{
				setState(1286);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(1287);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(1288);
				tableIdentifier();
				setState(1290);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(1289);
					partitionSpec();
					}
				}

				setState(1292);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(REPLACE);
				setState(1293);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(COLUMNS);
				}
				break;
			case 41:
				enterOuterAlt(_localctx, 41);
				{
				setState(1295);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(START);
				setState(1296);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TRANSACTION);
				}
				break;
			case 42:
				enterOuterAlt(_localctx, 42);
				{
				setState(1297);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(COMMIT);
				}
				break;
			case 43:
				enterOuterAlt(_localctx, 43);
				{
				setState(1298);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ROLLBACK);
				}
				break;
			case 44:
				enterOuterAlt(_localctx, 44);
				{
				setState(1299);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(DFS);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class CreateTableHeaderContext extends ParserRuleContext {
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode EXTERNAL() { return getToken(SqlBaseParser.EXTERNAL, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public CreateTableHeaderContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createTableHeader; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterCreateTableHeader(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitCreateTableHeader(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitCreateTableHeader(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CreateTableHeaderContext createTableHeader() throws RecognitionException {
		CreateTableHeaderContext _localctx = new CreateTableHeaderContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_createTableHeader);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1302);
			match(CREATE);
			setState(1304);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==TEMPORARY) {
				{
				setState(1303);
				match(TEMPORARY);
				}
			}

			setState(1307);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==EXTERNAL) {
				{
				setState(1306);
				match(EXTERNAL);
				}
			}

			setState(1309);
			match(TABLE);
			setState(1313);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,131,_ctx) ) {
			case 1:
				{
				setState(1310);
				match(IF);
				setState(1311);
				match(NOT);
				setState(1312);
				match(EXISTS);
				}
				break;
			}
			setState(1315);
			multipartIdentifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ReplaceTableHeaderContext extends ParserRuleContext {
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public ReplaceTableHeaderContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_replaceTableHeader; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterReplaceTableHeader(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitReplaceTableHeader(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitReplaceTableHeader(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ReplaceTableHeaderContext replaceTableHeader() throws RecognitionException {
		ReplaceTableHeaderContext _localctx = new ReplaceTableHeaderContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_replaceTableHeader);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1319);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==CREATE) {
				{
				setState(1317);
				match(CREATE);
				setState(1318);
				match(OR);
				}
			}

			setState(1321);
			match(REPLACE);
			setState(1322);
			match(TABLE);
			setState(1323);
			multipartIdentifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class BucketSpecContext extends ParserRuleContext {
		public TerminalNode CLUSTERED() { return getToken(SqlBaseParser.CLUSTERED, 0); }
		public List<TerminalNode> BY() { return getTokens(SqlBaseParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlBaseParser.BY, i);
		}
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public TerminalNode INTO() { return getToken(SqlBaseParser.INTO, 0); }
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public TerminalNode BUCKETS() { return getToken(SqlBaseParser.BUCKETS, 0); }
		public TerminalNode SORTED() { return getToken(SqlBaseParser.SORTED, 0); }
		public OrderedIdentifierListContext orderedIdentifierList() {
			return getRuleContext(OrderedIdentifierListContext.class,0);
		}
		public BucketSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_bucketSpec; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterBucketSpec(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitBucketSpec(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitBucketSpec(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BucketSpecContext bucketSpec() throws RecognitionException {
		BucketSpecContext _localctx = new BucketSpecContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_bucketSpec);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1325);
			match(CLUSTERED);
			setState(1326);
			match(BY);
			setState(1327);
			identifierList();
			setState(1331);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SORTED) {
				{
				setState(1328);
				match(SORTED);
				setState(1329);
				match(BY);
				setState(1330);
				orderedIdentifierList();
				}
			}

			setState(1333);
			match(INTO);
			setState(1334);
			match(INTEGER_VALUE);
			setState(1335);
			match(BUCKETS);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SkewSpecContext extends ParserRuleContext {
		public TerminalNode SKEWED() { return getToken(SqlBaseParser.SKEWED, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public ConstantListContext constantList() {
			return getRuleContext(ConstantListContext.class,0);
		}
		public NestedConstantListContext nestedConstantList() {
			return getRuleContext(NestedConstantListContext.class,0);
		}
		public TerminalNode STORED() { return getToken(SqlBaseParser.STORED, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TerminalNode DIRECTORIES() { return getToken(SqlBaseParser.DIRECTORIES, 0); }
		public SkewSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_skewSpec; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterSkewSpec(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitSkewSpec(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitSkewSpec(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SkewSpecContext skewSpec() throws RecognitionException {
		SkewSpecContext _localctx = new SkewSpecContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_skewSpec);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1337);
			match(SKEWED);
			setState(1338);
			match(BY);
			setState(1339);
			identifierList();
			setState(1340);
			match(ON);
			setState(1343);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,134,_ctx) ) {
			case 1:
				{
				setState(1341);
				constantList();
				}
				break;
			case 2:
				{
				setState(1342);
				nestedConstantList();
				}
				break;
			}
			setState(1348);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,135,_ctx) ) {
			case 1:
				{
				setState(1345);
				match(STORED);
				setState(1346);
				match(AS);
				setState(1347);
				match(DIRECTORIES);
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class LocationSpecContext extends ParserRuleContext {
		public TerminalNode LOCATION() { return getToken(SqlBaseParser.LOCATION, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public LocationSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_locationSpec; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterLocationSpec(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitLocationSpec(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitLocationSpec(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LocationSpecContext locationSpec() throws RecognitionException {
		LocationSpecContext _localctx = new LocationSpecContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_locationSpec);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1350);
			match(LOCATION);
			setState(1351);
			match(STRING);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class CommentSpecContext extends ParserRuleContext {
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public CommentSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_commentSpec; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterCommentSpec(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitCommentSpec(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitCommentSpec(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CommentSpecContext commentSpec() throws RecognitionException {
		CommentSpecContext _localctx = new CommentSpecContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_commentSpec);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1353);
			match(COMMENT);
			setState(1354);
			match(STRING);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QueryContext extends ParserRuleContext {
		public QueryTermContext queryTerm() {
			return getRuleContext(QueryTermContext.class,0);
		}
		public QueryOrganizationContext queryOrganization() {
			return getRuleContext(QueryOrganizationContext.class,0);
		}
		public CtesContext ctes() {
			return getRuleContext(CtesContext.class,0);
		}
		public QueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_query; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryContext query() throws RecognitionException {
		QueryContext _localctx = new QueryContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_query);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1357);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITH) {
				{
				setState(1356);
				ctes();
				}
			}

			setState(1359);
			queryTerm(0);
			setState(1360);
			queryOrganization();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class InsertIntoContext extends ParserRuleContext {
		public InsertIntoContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_insertInto; }
	 
		public InsertIntoContext() { }
		public void copyFrom(InsertIntoContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class InsertOverwriteHiveDirContext extends InsertIntoContext {
		public Token path;
		public TerminalNode INSERT() { return getToken(SqlBaseParser.INSERT, 0); }
		public TerminalNode OVERWRITE() { return getToken(SqlBaseParser.OVERWRITE, 0); }
		public TerminalNode DIRECTORY() { return getToken(SqlBaseParser.DIRECTORY, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode LOCAL() { return getToken(SqlBaseParser.LOCAL, 0); }
		public RowFormatContext rowFormat() {
			return getRuleContext(RowFormatContext.class,0);
		}
		public CreateFileFormatContext createFileFormat() {
			return getRuleContext(CreateFileFormatContext.class,0);
		}
		public InsertOverwriteHiveDirContext(InsertIntoContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterInsertOverwriteHiveDir(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitInsertOverwriteHiveDir(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitInsertOverwriteHiveDir(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class InsertOverwriteDirContext extends InsertIntoContext {
		public Token path;
		public PropertyListContext options;
		public TerminalNode INSERT() { return getToken(SqlBaseParser.INSERT, 0); }
		public TerminalNode OVERWRITE() { return getToken(SqlBaseParser.OVERWRITE, 0); }
		public TerminalNode DIRECTORY() { return getToken(SqlBaseParser.DIRECTORY, 0); }
		public TableProviderContext tableProvider() {
			return getRuleContext(TableProviderContext.class,0);
		}
		public TerminalNode LOCAL() { return getToken(SqlBaseParser.LOCAL, 0); }
		public TerminalNode OPTIONS() { return getToken(SqlBaseParser.OPTIONS, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public PropertyListContext propertyList() {
			return getRuleContext(PropertyListContext.class,0);
		}
		public InsertOverwriteDirContext(InsertIntoContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterInsertOverwriteDir(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitInsertOverwriteDir(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitInsertOverwriteDir(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class InsertOverwriteTableContext extends InsertIntoContext {
		public TerminalNode INSERT() { return getToken(SqlBaseParser.INSERT, 0); }
		public TerminalNode OVERWRITE() { return getToken(SqlBaseParser.OVERWRITE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public InsertOverwriteTableContext(InsertIntoContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterInsertOverwriteTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitInsertOverwriteTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitInsertOverwriteTable(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class InsertIntoTableContext extends InsertIntoContext {
		public TerminalNode INSERT() { return getToken(SqlBaseParser.INSERT, 0); }
		public TerminalNode INTO() { return getToken(SqlBaseParser.INTO, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public InsertIntoTableContext(InsertIntoContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterInsertIntoTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitInsertIntoTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitInsertIntoTable(this);
			else return visitor.visitChildren(this);
		}
	}

	public final InsertIntoContext insertInto() throws RecognitionException {
		InsertIntoContext _localctx = new InsertIntoContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_insertInto);
		int _la;
		try {
			setState(1423);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,151,_ctx) ) {
			case 1:
				_localctx = new InsertOverwriteTableContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1362);
				match(INSERT);
				setState(1363);
				match(OVERWRITE);
				setState(1365);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,137,_ctx) ) {
				case 1:
					{
					setState(1364);
					match(TABLE);
					}
					break;
				}
				setState(1367);
				multipartIdentifier();
				setState(1374);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(1368);
					partitionSpec();
					setState(1372);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==IF) {
						{
						setState(1369);
						match(IF);
						setState(1370);
						match(NOT);
						setState(1371);
						match(EXISTS);
						}
					}

					}
				}

				setState(1377);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,140,_ctx) ) {
				case 1:
					{
					setState(1376);
					identifierList();
					}
					break;
				}
				}
				break;
			case 2:
				_localctx = new InsertIntoTableContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1379);
				match(INSERT);
				setState(1380);
				match(INTO);
				setState(1382);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,141,_ctx) ) {
				case 1:
					{
					setState(1381);
					match(TABLE);
					}
					break;
				}
				setState(1384);
				multipartIdentifier();
				setState(1386);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(1385);
					partitionSpec();
					}
				}

				setState(1391);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IF) {
					{
					setState(1388);
					match(IF);
					setState(1389);
					match(NOT);
					setState(1390);
					match(EXISTS);
					}
				}

				setState(1394);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,144,_ctx) ) {
				case 1:
					{
					setState(1393);
					identifierList();
					}
					break;
				}
				}
				break;
			case 3:
				_localctx = new InsertOverwriteHiveDirContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1396);
				match(INSERT);
				setState(1397);
				match(OVERWRITE);
				setState(1399);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LOCAL) {
					{
					setState(1398);
					match(LOCAL);
					}
				}

				setState(1401);
				match(DIRECTORY);
				setState(1402);
				((InsertOverwriteHiveDirContext)_localctx).path = match(STRING);
				setState(1404);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ROW) {
					{
					setState(1403);
					rowFormat();
					}
				}

				setState(1407);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==STORED) {
					{
					setState(1406);
					createFileFormat();
					}
				}

				}
				break;
			case 4:
				_localctx = new InsertOverwriteDirContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1409);
				match(INSERT);
				setState(1410);
				match(OVERWRITE);
				setState(1412);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LOCAL) {
					{
					setState(1411);
					match(LOCAL);
					}
				}

				setState(1414);
				match(DIRECTORY);
				setState(1416);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==STRING) {
					{
					setState(1415);
					((InsertOverwriteDirContext)_localctx).path = match(STRING);
					}
				}

				setState(1418);
				tableProvider();
				setState(1421);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OPTIONS) {
					{
					setState(1419);
					match(OPTIONS);
					setState(1420);
					((InsertOverwriteDirContext)_localctx).options = propertyList();
					}
				}

				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PartitionSpecLocationContext extends ParserRuleContext {
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public LocationSpecContext locationSpec() {
			return getRuleContext(LocationSpecContext.class,0);
		}
		public PartitionSpecLocationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_partitionSpecLocation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterPartitionSpecLocation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitPartitionSpecLocation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitPartitionSpecLocation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PartitionSpecLocationContext partitionSpecLocation() throws RecognitionException {
		PartitionSpecLocationContext _localctx = new PartitionSpecLocationContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_partitionSpecLocation);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1425);
			partitionSpec();
			setState(1427);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==LOCATION) {
				{
				setState(1426);
				locationSpec();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PartitionSpecContext extends ParserRuleContext {
		public TerminalNode PARTITION() { return getToken(SqlBaseParser.PARTITION, 0); }
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public List<PartitionValContext> partitionVal() {
			return getRuleContexts(PartitionValContext.class);
		}
		public PartitionValContext partitionVal(int i) {
			return getRuleContext(PartitionValContext.class,i);
		}
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public PartitionSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_partitionSpec; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterPartitionSpec(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitPartitionSpec(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitPartitionSpec(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PartitionSpecContext partitionSpec() throws RecognitionException {
		PartitionSpecContext _localctx = new PartitionSpecContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_partitionSpec);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1429);
			match(PARTITION);
			setState(1430);
			match(LEFT_PAREN);
			setState(1431);
			partitionVal();
			setState(1436);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1432);
				match(COMMA);
				setState(1433);
				partitionVal();
				}
				}
				setState(1438);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1439);
			match(RIGHT_PAREN);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PartitionValContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
		public ConstantContext constant() {
			return getRuleContext(ConstantContext.class,0);
		}
		public PartitionValContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_partitionVal; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterPartitionVal(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitPartitionVal(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitPartitionVal(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PartitionValContext partitionVal() throws RecognitionException {
		PartitionValContext _localctx = new PartitionValContext(_ctx, getState());
		enterRule(_localctx, 42, RULE_partitionVal);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1441);
			identifier();
			setState(1444);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==EQ) {
				{
				setState(1442);
				match(EQ);
				setState(1443);
				constant();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NamespaceContext extends ParserRuleContext {
		public TerminalNode NAMESPACE() { return getToken(SqlBaseParser.NAMESPACE, 0); }
		public TerminalNode DATABASE() { return getToken(SqlBaseParser.DATABASE, 0); }
		public TerminalNode SCHEMA() { return getToken(SqlBaseParser.SCHEMA, 0); }
		public NamespaceContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_namespace; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterNamespace(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitNamespace(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitNamespace(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NamespaceContext namespace() throws RecognitionException {
		NamespaceContext _localctx = new NamespaceContext(_ctx, getState());
		enterRule(_localctx, 44, RULE_namespace);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1446);
			_la = _input.LA(1);
			if ( !(_la==DATABASE || _la==NAMESPACE || _la==SCHEMA) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NamespacesContext extends ParserRuleContext {
		public TerminalNode NAMESPACES() { return getToken(SqlBaseParser.NAMESPACES, 0); }
		public TerminalNode DATABASES() { return getToken(SqlBaseParser.DATABASES, 0); }
		public TerminalNode SCHEMAS() { return getToken(SqlBaseParser.SCHEMAS, 0); }
		public NamespacesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_namespaces; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterNamespaces(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitNamespaces(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitNamespaces(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NamespacesContext namespaces() throws RecognitionException {
		NamespacesContext _localctx = new NamespacesContext(_ctx, getState());
		enterRule(_localctx, 46, RULE_namespaces);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1448);
			_la = _input.LA(1);
			if ( !(_la==DATABASES || _la==NAMESPACES || _la==SCHEMAS) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class DescribeFuncNameContext extends ParserRuleContext {
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public ComparisonOperatorContext comparisonOperator() {
			return getRuleContext(ComparisonOperatorContext.class,0);
		}
		public ArithmeticOperatorContext arithmeticOperator() {
			return getRuleContext(ArithmeticOperatorContext.class,0);
		}
		public PredicateOperatorContext predicateOperator() {
			return getRuleContext(PredicateOperatorContext.class,0);
		}
		public DescribeFuncNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_describeFuncName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterDescribeFuncName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitDescribeFuncName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitDescribeFuncName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DescribeFuncNameContext describeFuncName() throws RecognitionException {
		DescribeFuncNameContext _localctx = new DescribeFuncNameContext(_ctx, getState());
		enterRule(_localctx, 48, RULE_describeFuncName);
		try {
			setState(1455);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,155,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1450);
				qualifiedName();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1451);
				match(STRING);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1452);
				comparisonOperator();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(1453);
				arithmeticOperator();
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(1454);
				predicateOperator();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class DescribeColNameContext extends ParserRuleContext {
		public IdentifierContext identifier;
		public List<IdentifierContext> nameParts = new ArrayList<IdentifierContext>();
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public List<TerminalNode> DOT() { return getTokens(SqlBaseParser.DOT); }
		public TerminalNode DOT(int i) {
			return getToken(SqlBaseParser.DOT, i);
		}
		public DescribeColNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_describeColName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterDescribeColName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitDescribeColName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitDescribeColName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DescribeColNameContext describeColName() throws RecognitionException {
		DescribeColNameContext _localctx = new DescribeColNameContext(_ctx, getState());
		enterRule(_localctx, 50, RULE_describeColName);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1457);
			((DescribeColNameContext)_localctx).identifier = identifier();
			((DescribeColNameContext)_localctx).nameParts.add(((DescribeColNameContext)_localctx).identifier);
			setState(1462);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==DOT) {
				{
				{
				setState(1458);
				match(DOT);
				setState(1459);
				((DescribeColNameContext)_localctx).identifier = identifier();
				((DescribeColNameContext)_localctx).nameParts.add(((DescribeColNameContext)_localctx).identifier);
				}
				}
				setState(1464);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class CtesContext extends ParserRuleContext {
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public List<NamedQueryContext> namedQuery() {
			return getRuleContexts(NamedQueryContext.class);
		}
		public NamedQueryContext namedQuery(int i) {
			return getRuleContext(NamedQueryContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public CtesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ctes; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterCtes(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitCtes(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitCtes(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CtesContext ctes() throws RecognitionException {
		CtesContext _localctx = new CtesContext(_ctx, getState());
		enterRule(_localctx, 52, RULE_ctes);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1465);
			match(WITH);
			setState(1466);
			namedQuery();
			setState(1471);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1467);
				match(COMMA);
				setState(1468);
				namedQuery();
				}
				}
				setState(1473);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NamedQueryContext extends ParserRuleContext {
		public ErrorCapturingIdentifierContext name;
		public IdentifierListContext columnAliases;
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public ErrorCapturingIdentifierContext errorCapturingIdentifier() {
			return getRuleContext(ErrorCapturingIdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public NamedQueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_namedQuery; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterNamedQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitNamedQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitNamedQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NamedQueryContext namedQuery() throws RecognitionException {
		NamedQueryContext _localctx = new NamedQueryContext(_ctx, getState());
		enterRule(_localctx, 54, RULE_namedQuery);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1474);
			((NamedQueryContext)_localctx).name = errorCapturingIdentifier();
			setState(1476);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,158,_ctx) ) {
			case 1:
				{
				setState(1475);
				((NamedQueryContext)_localctx).columnAliases = identifierList();
				}
				break;
			}
			setState(1479);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==AS) {
				{
				setState(1478);
				match(AS);
				}
			}

			setState(1481);
			match(LEFT_PAREN);
			setState(1482);
			query();
			setState(1483);
			match(RIGHT_PAREN);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TableProviderContext extends ParserRuleContext {
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TableProviderContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableProvider; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterTableProvider(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitTableProvider(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitTableProvider(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TableProviderContext tableProvider() throws RecognitionException {
		TableProviderContext _localctx = new TableProviderContext(_ctx, getState());
		enterRule(_localctx, 56, RULE_tableProvider);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1485);
			match(USING);
			setState(1486);
			multipartIdentifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class CreateTableClausesContext extends ParserRuleContext {
		public PropertyListContext options;
		public PartitionFieldListContext partitioning;
		public PropertyListContext tableProps;
		public List<SkewSpecContext> skewSpec() {
			return getRuleContexts(SkewSpecContext.class);
		}
		public SkewSpecContext skewSpec(int i) {
			return getRuleContext(SkewSpecContext.class,i);
		}
		public List<BucketSpecContext> bucketSpec() {
			return getRuleContexts(BucketSpecContext.class);
		}
		public BucketSpecContext bucketSpec(int i) {
			return getRuleContext(BucketSpecContext.class,i);
		}
		public List<RowFormatContext> rowFormat() {
			return getRuleContexts(RowFormatContext.class);
		}
		public RowFormatContext rowFormat(int i) {
			return getRuleContext(RowFormatContext.class,i);
		}
		public List<CreateFileFormatContext> createFileFormat() {
			return getRuleContexts(CreateFileFormatContext.class);
		}
		public CreateFileFormatContext createFileFormat(int i) {
			return getRuleContext(CreateFileFormatContext.class,i);
		}
		public List<LocationSpecContext> locationSpec() {
			return getRuleContexts(LocationSpecContext.class);
		}
		public LocationSpecContext locationSpec(int i) {
			return getRuleContext(LocationSpecContext.class,i);
		}
		public List<CommentSpecContext> commentSpec() {
			return getRuleContexts(CommentSpecContext.class);
		}
		public CommentSpecContext commentSpec(int i) {
			return getRuleContext(CommentSpecContext.class,i);
		}
		public List<TerminalNode> OPTIONS() { return getTokens(SqlBaseParser.OPTIONS); }
		public TerminalNode OPTIONS(int i) {
			return getToken(SqlBaseParser.OPTIONS, i);
		}
		public List<TerminalNode> PARTITIONED() { return getTokens(SqlBaseParser.PARTITIONED); }
		public TerminalNode PARTITIONED(int i) {
			return getToken(SqlBaseParser.PARTITIONED, i);
		}
		public List<TerminalNode> BY() { return getTokens(SqlBaseParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlBaseParser.BY, i);
		}
		public List<TerminalNode> TBLPROPERTIES() { return getTokens(SqlBaseParser.TBLPROPERTIES); }
		public TerminalNode TBLPROPERTIES(int i) {
			return getToken(SqlBaseParser.TBLPROPERTIES, i);
		}
		public List<PropertyListContext> propertyList() {
			return getRuleContexts(PropertyListContext.class);
		}
		public PropertyListContext propertyList(int i) {
			return getRuleContext(PropertyListContext.class,i);
		}
		public List<PartitionFieldListContext> partitionFieldList() {
			return getRuleContexts(PartitionFieldListContext.class);
		}
		public PartitionFieldListContext partitionFieldList(int i) {
			return getRuleContext(PartitionFieldListContext.class,i);
		}
		public CreateTableClausesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createTableClauses; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterCreateTableClauses(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitCreateTableClauses(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitCreateTableClauses(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CreateTableClausesContext createTableClauses() throws RecognitionException {
		CreateTableClausesContext _localctx = new CreateTableClausesContext(_ctx, getState());
		enterRule(_localctx, 58, RULE_createTableClauses);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1503);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==CLUSTERED || _la==COMMENT || ((((_la - 141)) & ~0x3f) == 0 && ((1L << (_la - 141)) & 34376515585L) != 0) || ((((_la - 212)) & ~0x3f) == 0 && ((1L << (_la - 212)) & 4297097217L) != 0)) {
				{
				setState(1501);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case OPTIONS:
					{
					{
					setState(1488);
					match(OPTIONS);
					setState(1489);
					((CreateTableClausesContext)_localctx).options = propertyList();
					}
					}
					break;
				case PARTITIONED:
					{
					{
					setState(1490);
					match(PARTITIONED);
					setState(1491);
					match(BY);
					setState(1492);
					((CreateTableClausesContext)_localctx).partitioning = partitionFieldList();
					}
					}
					break;
				case SKEWED:
					{
					setState(1493);
					skewSpec();
					}
					break;
				case CLUSTERED:
					{
					setState(1494);
					bucketSpec();
					}
					break;
				case ROW:
					{
					setState(1495);
					rowFormat();
					}
					break;
				case STORED:
					{
					setState(1496);
					createFileFormat();
					}
					break;
				case LOCATION:
					{
					setState(1497);
					locationSpec();
					}
					break;
				case COMMENT:
					{
					setState(1498);
					commentSpec();
					}
					break;
				case TBLPROPERTIES:
					{
					{
					setState(1499);
					match(TBLPROPERTIES);
					setState(1500);
					((CreateTableClausesContext)_localctx).tableProps = propertyList();
					}
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				setState(1505);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PropertyListContext extends ParserRuleContext {
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public List<PropertyContext> property() {
			return getRuleContexts(PropertyContext.class);
		}
		public PropertyContext property(int i) {
			return getRuleContext(PropertyContext.class,i);
		}
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public PropertyListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_propertyList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterPropertyList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitPropertyList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitPropertyList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PropertyListContext propertyList() throws RecognitionException {
		PropertyListContext _localctx = new PropertyListContext(_ctx, getState());
		enterRule(_localctx, 60, RULE_propertyList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1506);
			match(LEFT_PAREN);
			setState(1507);
			property();
			setState(1512);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1508);
				match(COMMA);
				setState(1509);
				property();
				}
				}
				setState(1514);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1515);
			match(RIGHT_PAREN);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PropertyContext extends ParserRuleContext {
		public PropertyKeyContext key;
		public PropertyValueContext value;
		public PropertyKeyContext propertyKey() {
			return getRuleContext(PropertyKeyContext.class,0);
		}
		public PropertyValueContext propertyValue() {
			return getRuleContext(PropertyValueContext.class,0);
		}
		public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
		public PropertyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_property; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterProperty(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitProperty(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitProperty(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PropertyContext property() throws RecognitionException {
		PropertyContext _localctx = new PropertyContext(_ctx, getState());
		enterRule(_localctx, 62, RULE_property);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1517);
			((PropertyContext)_localctx).key = propertyKey();
			setState(1522);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==FALSE || ((((_la - 259)) & ~0x3f) == 0 && ((1L << (_la - 259)) & 91197892722688001L) != 0)) {
				{
				setState(1519);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==EQ) {
					{
					setState(1518);
					match(EQ);
					}
				}

				setState(1521);
				((PropertyContext)_localctx).value = propertyValue();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PropertyKeyContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public List<TerminalNode> DOT() { return getTokens(SqlBaseParser.DOT); }
		public TerminalNode DOT(int i) {
			return getToken(SqlBaseParser.DOT, i);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public PropertyKeyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_propertyKey; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterPropertyKey(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitPropertyKey(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitPropertyKey(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PropertyKeyContext propertyKey() throws RecognitionException {
		PropertyKeyContext _localctx = new PropertyKeyContext(_ctx, getState());
		enterRule(_localctx, 64, RULE_propertyKey);
		int _la;
		try {
			setState(1533);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,166,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1524);
				identifier();
				setState(1529);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==DOT) {
					{
					{
					setState(1525);
					match(DOT);
					setState(1526);
					identifier();
					}
					}
					setState(1531);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1532);
				match(STRING);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PropertyValueContext extends ParserRuleContext {
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public TerminalNode DECIMAL_VALUE() { return getToken(SqlBaseParser.DECIMAL_VALUE, 0); }
		public BooleanValueContext booleanValue() {
			return getRuleContext(BooleanValueContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public PropertyValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_propertyValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterPropertyValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitPropertyValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitPropertyValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PropertyValueContext propertyValue() throws RecognitionException {
		PropertyValueContext _localctx = new PropertyValueContext(_ctx, getState());
		enterRule(_localctx, 66, RULE_propertyValue);
		try {
			setState(1539);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case INTEGER_VALUE:
				enterOuterAlt(_localctx, 1);
				{
				setState(1535);
				match(INTEGER_VALUE);
				}
				break;
			case DECIMAL_VALUE:
				enterOuterAlt(_localctx, 2);
				{
				setState(1536);
				match(DECIMAL_VALUE);
				}
				break;
			case FALSE:
			case TRUE:
				enterOuterAlt(_localctx, 3);
				{
				setState(1537);
				booleanValue();
				}
				break;
			case STRING:
				enterOuterAlt(_localctx, 4);
				{
				setState(1538);
				match(STRING);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ConstantListContext extends ParserRuleContext {
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public List<ConstantContext> constant() {
			return getRuleContexts(ConstantContext.class);
		}
		public ConstantContext constant(int i) {
			return getRuleContext(ConstantContext.class,i);
		}
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public ConstantListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_constantList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterConstantList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitConstantList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitConstantList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ConstantListContext constantList() throws RecognitionException {
		ConstantListContext _localctx = new ConstantListContext(_ctx, getState());
		enterRule(_localctx, 68, RULE_constantList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1541);
			match(LEFT_PAREN);
			setState(1542);
			constant();
			setState(1547);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1543);
				match(COMMA);
				setState(1544);
				constant();
				}
				}
				setState(1549);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1550);
			match(RIGHT_PAREN);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NestedConstantListContext extends ParserRuleContext {
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public List<ConstantListContext> constantList() {
			return getRuleContexts(ConstantListContext.class);
		}
		public ConstantListContext constantList(int i) {
			return getRuleContext(ConstantListContext.class,i);
		}
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public NestedConstantListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nestedConstantList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterNestedConstantList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitNestedConstantList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitNestedConstantList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NestedConstantListContext nestedConstantList() throws RecognitionException {
		NestedConstantListContext _localctx = new NestedConstantListContext(_ctx, getState());
		enterRule(_localctx, 70, RULE_nestedConstantList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1552);
			match(LEFT_PAREN);
			setState(1553);
			constantList();
			setState(1558);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1554);
				match(COMMA);
				setState(1555);
				constantList();
				}
				}
				setState(1560);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1561);
			match(RIGHT_PAREN);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class CreateFileFormatContext extends ParserRuleContext {
		public TerminalNode STORED() { return getToken(SqlBaseParser.STORED, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public FileFormatContext fileFormat() {
			return getRuleContext(FileFormatContext.class,0);
		}
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public StorageHandlerContext storageHandler() {
			return getRuleContext(StorageHandlerContext.class,0);
		}
		public CreateFileFormatContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createFileFormat; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterCreateFileFormat(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitCreateFileFormat(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitCreateFileFormat(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CreateFileFormatContext createFileFormat() throws RecognitionException {
		CreateFileFormatContext _localctx = new CreateFileFormatContext(_ctx, getState());
		enterRule(_localctx, 72, RULE_createFileFormat);
		try {
			setState(1569);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,170,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1563);
				match(STORED);
				setState(1564);
				match(AS);
				setState(1565);
				fileFormat();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1566);
				match(STORED);
				setState(1567);
				match(BY);
				setState(1568);
				storageHandler();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class FileFormatContext extends ParserRuleContext {
		public FileFormatContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fileFormat; }
	 
		public FileFormatContext() { }
		public void copyFrom(FileFormatContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TableFileFormatContext extends FileFormatContext {
		public Token inFmt;
		public Token outFmt;
		public TerminalNode INPUTFORMAT() { return getToken(SqlBaseParser.INPUTFORMAT, 0); }
		public TerminalNode OUTPUTFORMAT() { return getToken(SqlBaseParser.OUTPUTFORMAT, 0); }
		public List<TerminalNode> STRING() { return getTokens(SqlBaseParser.STRING); }
		public TerminalNode STRING(int i) {
			return getToken(SqlBaseParser.STRING, i);
		}
		public TableFileFormatContext(FileFormatContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterTableFileFormat(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitTableFileFormat(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitTableFileFormat(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class GenericFileFormatContext extends FileFormatContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public GenericFileFormatContext(FileFormatContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterGenericFileFormat(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitGenericFileFormat(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitGenericFileFormat(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FileFormatContext fileFormat() throws RecognitionException {
		FileFormatContext _localctx = new FileFormatContext(_ctx, getState());
		enterRule(_localctx, 74, RULE_fileFormat);
		try {
			setState(1576);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,171,_ctx) ) {
			case 1:
				_localctx = new TableFileFormatContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1571);
				match(INPUTFORMAT);
				setState(1572);
				((TableFileFormatContext)_localctx).inFmt = match(STRING);
				setState(1573);
				match(OUTPUTFORMAT);
				setState(1574);
				((TableFileFormatContext)_localctx).outFmt = match(STRING);
				}
				break;
			case 2:
				_localctx = new GenericFileFormatContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1575);
				identifier();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class StorageHandlerContext extends ParserRuleContext {
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode SERDEPROPERTIES() { return getToken(SqlBaseParser.SERDEPROPERTIES, 0); }
		public PropertyListContext propertyList() {
			return getRuleContext(PropertyListContext.class,0);
		}
		public StorageHandlerContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_storageHandler; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterStorageHandler(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitStorageHandler(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitStorageHandler(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StorageHandlerContext storageHandler() throws RecognitionException {
		StorageHandlerContext _localctx = new StorageHandlerContext(_ctx, getState());
		enterRule(_localctx, 76, RULE_storageHandler);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1578);
			match(STRING);
			setState(1582);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,172,_ctx) ) {
			case 1:
				{
				setState(1579);
				match(WITH);
				setState(1580);
				match(SERDEPROPERTIES);
				setState(1581);
				propertyList();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ResourceContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public ResourceContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_resource; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterResource(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitResource(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitResource(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ResourceContext resource() throws RecognitionException {
		ResourceContext _localctx = new ResourceContext(_ctx, getState());
		enterRule(_localctx, 78, RULE_resource);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1584);
			identifier();
			setState(1585);
			match(STRING);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class DmlStatementNoWithContext extends ParserRuleContext {
		public DmlStatementNoWithContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dmlStatementNoWith; }
	 
		public DmlStatementNoWithContext() { }
		public void copyFrom(DmlStatementNoWithContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DeleteFromTableContext extends DmlStatementNoWithContext {
		public MultipartIdentifierContext source;
		public TableAliasContext sourceAlias;
		public QueryContext sourceQuery;
		public TerminalNode DELETE() { return getToken(SqlBaseParser.DELETE, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public List<MultipartIdentifierContext> multipartIdentifier() {
			return getRuleContexts(MultipartIdentifierContext.class);
		}
		public MultipartIdentifierContext multipartIdentifier(int i) {
			return getRuleContext(MultipartIdentifierContext.class,i);
		}
		public List<TableAliasContext> tableAlias() {
			return getRuleContexts(TableAliasContext.class);
		}
		public TableAliasContext tableAlias(int i) {
			return getRuleContext(TableAliasContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public WhereClauseContext whereClause() {
			return getRuleContext(WhereClauseContext.class,0);
		}
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public DeleteFromTableContext(DmlStatementNoWithContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterDeleteFromTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitDeleteFromTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitDeleteFromTable(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SingleInsertQueryContext extends DmlStatementNoWithContext {
		public InsertIntoContext insertInto() {
			return getRuleContext(InsertIntoContext.class,0);
		}
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public SingleInsertQueryContext(DmlStatementNoWithContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterSingleInsertQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitSingleInsertQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitSingleInsertQuery(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class MultiInsertQueryContext extends DmlStatementNoWithContext {
		public FromClauseContext fromClause() {
			return getRuleContext(FromClauseContext.class,0);
		}
		public List<MultiInsertQueryBodyContext> multiInsertQueryBody() {
			return getRuleContexts(MultiInsertQueryBodyContext.class);
		}
		public MultiInsertQueryBodyContext multiInsertQueryBody(int i) {
			return getRuleContext(MultiInsertQueryBodyContext.class,i);
		}
		public MultiInsertQueryContext(DmlStatementNoWithContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterMultiInsertQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitMultiInsertQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitMultiInsertQuery(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class UpdateTableContext extends DmlStatementNoWithContext {
		public MultipartIdentifierContext source;
		public TableAliasContext sourceAlias;
		public QueryContext sourceQuery;
		public TerminalNode UPDATE() { return getToken(SqlBaseParser.UPDATE, 0); }
		public List<MultipartIdentifierContext> multipartIdentifier() {
			return getRuleContexts(MultipartIdentifierContext.class);
		}
		public MultipartIdentifierContext multipartIdentifier(int i) {
			return getRuleContext(MultipartIdentifierContext.class,i);
		}
		public List<TableAliasContext> tableAlias() {
			return getRuleContexts(TableAliasContext.class);
		}
		public TableAliasContext tableAlias(int i) {
			return getRuleContext(TableAliasContext.class,i);
		}
		public SetClauseContext setClause() {
			return getRuleContext(SetClauseContext.class,0);
		}
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public WhereClauseContext whereClause() {
			return getRuleContext(WhereClauseContext.class,0);
		}
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public UpdateTableContext(DmlStatementNoWithContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterUpdateTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitUpdateTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitUpdateTable(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class MergeIntoTableContext extends DmlStatementNoWithContext {
		public MultipartIdentifierContext target;
		public TableAliasContext targetAlias;
		public MultipartIdentifierContext source;
		public QueryContext sourceQuery;
		public TableAliasContext sourceAlias;
		public BooleanExpressionContext mergeCondition;
		public TerminalNode MERGE() { return getToken(SqlBaseParser.MERGE, 0); }
		public TerminalNode INTO() { return getToken(SqlBaseParser.INTO, 0); }
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public List<MultipartIdentifierContext> multipartIdentifier() {
			return getRuleContexts(MultipartIdentifierContext.class);
		}
		public MultipartIdentifierContext multipartIdentifier(int i) {
			return getRuleContext(MultipartIdentifierContext.class,i);
		}
		public List<TableAliasContext> tableAlias() {
			return getRuleContexts(TableAliasContext.class);
		}
		public TableAliasContext tableAlias(int i) {
			return getRuleContext(TableAliasContext.class,i);
		}
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public List<MatchedClauseContext> matchedClause() {
			return getRuleContexts(MatchedClauseContext.class);
		}
		public MatchedClauseContext matchedClause(int i) {
			return getRuleContext(MatchedClauseContext.class,i);
		}
		public List<NotMatchedClauseContext> notMatchedClause() {
			return getRuleContexts(NotMatchedClauseContext.class);
		}
		public NotMatchedClauseContext notMatchedClause(int i) {
			return getRuleContext(NotMatchedClauseContext.class,i);
		}
		public MergeIntoTableContext(DmlStatementNoWithContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterMergeIntoTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitMergeIntoTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitMergeIntoTable(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DmlStatementNoWithContext dmlStatementNoWith() throws RecognitionException {
		DmlStatementNoWithContext _localctx = new DmlStatementNoWithContext(_ctx, getState());
		enterRule(_localctx, 80, RULE_dmlStatementNoWith);
		int _la;
		try {
			setState(1665);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case INSERT:
				_localctx = new SingleInsertQueryContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1587);
				insertInto();
				setState(1588);
				query();
				}
				break;
			case FROM:
				_localctx = new MultiInsertQueryContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1590);
				fromClause();
				setState(1592); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(1591);
					multiInsertQueryBody();
					}
					}
					setState(1594); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==INSERT );
				}
				break;
			case DELETE:
				_localctx = new DeleteFromTableContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1596);
				match(DELETE);
				setState(1597);
				match(FROM);
				setState(1598);
				multipartIdentifier();
				setState(1599);
				tableAlias();
				setState(1604);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,174,_ctx) ) {
				case 1:
					{
					setState(1600);
					match(COMMA);
					setState(1601);
					((DeleteFromTableContext)_localctx).source = multipartIdentifier();
					setState(1602);
					((DeleteFromTableContext)_localctx).sourceAlias = tableAlias();
					}
					break;
				}
				setState(1612);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMA) {
					{
					setState(1606);
					match(COMMA);
					setState(1607);
					match(LEFT_PAREN);
					setState(1608);
					((DeleteFromTableContext)_localctx).sourceQuery = query();
					setState(1609);
					match(RIGHT_PAREN);
					setState(1610);
					((DeleteFromTableContext)_localctx).sourceAlias = tableAlias();
					}
				}

				setState(1615);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WHERE) {
					{
					setState(1614);
					whereClause();
					}
				}

				}
				break;
			case UPDATE:
				_localctx = new UpdateTableContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1617);
				match(UPDATE);
				setState(1618);
				multipartIdentifier();
				setState(1619);
				tableAlias();
				setState(1624);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,177,_ctx) ) {
				case 1:
					{
					setState(1620);
					match(COMMA);
					setState(1621);
					((UpdateTableContext)_localctx).source = multipartIdentifier();
					setState(1622);
					((UpdateTableContext)_localctx).sourceAlias = tableAlias();
					}
					break;
				}
				setState(1632);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMA) {
					{
					setState(1626);
					match(COMMA);
					setState(1627);
					match(LEFT_PAREN);
					setState(1628);
					((UpdateTableContext)_localctx).sourceQuery = query();
					setState(1629);
					match(RIGHT_PAREN);
					setState(1630);
					((UpdateTableContext)_localctx).sourceAlias = tableAlias();
					}
				}

				setState(1634);
				setClause();
				setState(1636);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WHERE) {
					{
					setState(1635);
					whereClause();
					}
				}

				}
				break;
			case MERGE:
				_localctx = new MergeIntoTableContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(1638);
				match(MERGE);
				setState(1639);
				match(INTO);
				setState(1640);
				((MergeIntoTableContext)_localctx).target = multipartIdentifier();
				setState(1641);
				((MergeIntoTableContext)_localctx).targetAlias = tableAlias();
				setState(1642);
				match(USING);
				setState(1648);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,180,_ctx) ) {
				case 1:
					{
					setState(1643);
					((MergeIntoTableContext)_localctx).source = multipartIdentifier();
					}
					break;
				case 2:
					{
					setState(1644);
					match(LEFT_PAREN);
					setState(1645);
					((MergeIntoTableContext)_localctx).sourceQuery = query();
					setState(1646);
					match(RIGHT_PAREN);
					}
					break;
				}
				setState(1650);
				((MergeIntoTableContext)_localctx).sourceAlias = tableAlias();
				setState(1651);
				match(ON);
				setState(1652);
				((MergeIntoTableContext)_localctx).mergeCondition = booleanExpression(0);
				setState(1654);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,181,_ctx) ) {
				case 1:
					{
					setState(1653);
					matchedClause();
					}
					break;
				}
				setState(1657);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,182,_ctx) ) {
				case 1:
					{
					setState(1656);
					notMatchedClause();
					}
					break;
				}
				setState(1660);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,183,_ctx) ) {
				case 1:
					{
					setState(1659);
					matchedClause();
					}
					break;
				}
				setState(1663);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WHEN) {
					{
					setState(1662);
					notMatchedClause();
					}
				}

				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QueryOrganizationContext extends ParserRuleContext {
		public SortItemContext sortItem;
		public List<SortItemContext> order = new ArrayList<SortItemContext>();
		public ExpressionContext expression;
		public List<ExpressionContext> clusterBy = new ArrayList<ExpressionContext>();
		public List<ExpressionContext> distributeBy = new ArrayList<ExpressionContext>();
		public List<SortItemContext> sort = new ArrayList<SortItemContext>();
		public ExpressionContext limit;
		public TerminalNode ORDER() { return getToken(SqlBaseParser.ORDER, 0); }
		public List<TerminalNode> BY() { return getTokens(SqlBaseParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlBaseParser.BY, i);
		}
		public TerminalNode CLUSTER() { return getToken(SqlBaseParser.CLUSTER, 0); }
		public TerminalNode DISTRIBUTE() { return getToken(SqlBaseParser.DISTRIBUTE, 0); }
		public TerminalNode SORT() { return getToken(SqlBaseParser.SORT, 0); }
		public WindowClauseContext windowClause() {
			return getRuleContext(WindowClauseContext.class,0);
		}
		public TerminalNode LIMIT() { return getToken(SqlBaseParser.LIMIT, 0); }
		public List<SortItemContext> sortItem() {
			return getRuleContexts(SortItemContext.class);
		}
		public SortItemContext sortItem(int i) {
			return getRuleContext(SortItemContext.class,i);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public QueryOrganizationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryOrganization; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterQueryOrganization(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitQueryOrganization(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitQueryOrganization(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryOrganizationContext queryOrganization() throws RecognitionException {
		QueryOrganizationContext _localctx = new QueryOrganizationContext(_ctx, getState());
		enterRule(_localctx, 82, RULE_queryOrganization);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1677);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,187,_ctx) ) {
			case 1:
				{
				setState(1667);
				match(ORDER);
				setState(1668);
				match(BY);
				setState(1669);
				((QueryOrganizationContext)_localctx).sortItem = sortItem();
				((QueryOrganizationContext)_localctx).order.add(((QueryOrganizationContext)_localctx).sortItem);
				setState(1674);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,186,_ctx);
				while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1670);
						match(COMMA);
						setState(1671);
						((QueryOrganizationContext)_localctx).sortItem = sortItem();
						((QueryOrganizationContext)_localctx).order.add(((QueryOrganizationContext)_localctx).sortItem);
						}
						} 
					}
					setState(1676);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,186,_ctx);
				}
				}
				break;
			}
			setState(1689);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,189,_ctx) ) {
			case 1:
				{
				setState(1679);
				match(CLUSTER);
				setState(1680);
				match(BY);
				setState(1681);
				((QueryOrganizationContext)_localctx).expression = expression();
				((QueryOrganizationContext)_localctx).clusterBy.add(((QueryOrganizationContext)_localctx).expression);
				setState(1686);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,188,_ctx);
				while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1682);
						match(COMMA);
						setState(1683);
						((QueryOrganizationContext)_localctx).expression = expression();
						((QueryOrganizationContext)_localctx).clusterBy.add(((QueryOrganizationContext)_localctx).expression);
						}
						} 
					}
					setState(1688);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,188,_ctx);
				}
				}
				break;
			}
			setState(1701);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,191,_ctx) ) {
			case 1:
				{
				setState(1691);
				match(DISTRIBUTE);
				setState(1692);
				match(BY);
				setState(1693);
				((QueryOrganizationContext)_localctx).expression = expression();
				((QueryOrganizationContext)_localctx).distributeBy.add(((QueryOrganizationContext)_localctx).expression);
				setState(1698);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,190,_ctx);
				while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1694);
						match(COMMA);
						setState(1695);
						((QueryOrganizationContext)_localctx).expression = expression();
						((QueryOrganizationContext)_localctx).distributeBy.add(((QueryOrganizationContext)_localctx).expression);
						}
						} 
					}
					setState(1700);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,190,_ctx);
				}
				}
				break;
			}
			setState(1713);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,193,_ctx) ) {
			case 1:
				{
				setState(1703);
				match(SORT);
				setState(1704);
				match(BY);
				setState(1705);
				((QueryOrganizationContext)_localctx).sortItem = sortItem();
				((QueryOrganizationContext)_localctx).sort.add(((QueryOrganizationContext)_localctx).sortItem);
				setState(1710);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,192,_ctx);
				while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1706);
						match(COMMA);
						setState(1707);
						((QueryOrganizationContext)_localctx).sortItem = sortItem();
						((QueryOrganizationContext)_localctx).sort.add(((QueryOrganizationContext)_localctx).sortItem);
						}
						} 
					}
					setState(1712);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,192,_ctx);
				}
				}
				break;
			}
			setState(1716);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,194,_ctx) ) {
			case 1:
				{
				setState(1715);
				windowClause();
				}
				break;
			}
			setState(1723);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,196,_ctx) ) {
			case 1:
				{
				setState(1718);
				match(LIMIT);
				setState(1721);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,195,_ctx) ) {
				case 1:
					{
					setState(1719);
					match(ALL);
					}
					break;
				case 2:
					{
					setState(1720);
					((QueryOrganizationContext)_localctx).limit = expression();
					}
					break;
				}
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class MultiInsertQueryBodyContext extends ParserRuleContext {
		public InsertIntoContext insertInto() {
			return getRuleContext(InsertIntoContext.class,0);
		}
		public FromStatementBodyContext fromStatementBody() {
			return getRuleContext(FromStatementBodyContext.class,0);
		}
		public MultiInsertQueryBodyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_multiInsertQueryBody; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterMultiInsertQueryBody(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitMultiInsertQueryBody(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitMultiInsertQueryBody(this);
			else return visitor.visitChildren(this);
		}
	}

	public final MultiInsertQueryBodyContext multiInsertQueryBody() throws RecognitionException {
		MultiInsertQueryBodyContext _localctx = new MultiInsertQueryBodyContext(_ctx, getState());
		enterRule(_localctx, 84, RULE_multiInsertQueryBody);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1725);
			insertInto();
			setState(1726);
			fromStatementBody();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QueryTermContext extends ParserRuleContext {
		public QueryTermContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryTerm; }
	 
		public QueryTermContext() { }
		public void copyFrom(QueryTermContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class QueryTermDefaultContext extends QueryTermContext {
		public QueryPrimaryContext queryPrimary() {
			return getRuleContext(QueryPrimaryContext.class,0);
		}
		public QueryTermDefaultContext(QueryTermContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterQueryTermDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitQueryTermDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitQueryTermDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SetOperationContext extends QueryTermContext {
		public QueryTermContext left;
		public Token operator;
		public QueryTermContext right;
		public List<QueryTermContext> queryTerm() {
			return getRuleContexts(QueryTermContext.class);
		}
		public QueryTermContext queryTerm(int i) {
			return getRuleContext(QueryTermContext.class,i);
		}
		public TerminalNode INTERSECT() { return getToken(SqlBaseParser.INTERSECT, 0); }
		public TerminalNode UNION() { return getToken(SqlBaseParser.UNION, 0); }
		public TerminalNode EXCEPT() { return getToken(SqlBaseParser.EXCEPT, 0); }
		public TerminalNode SETMINUS() { return getToken(SqlBaseParser.SETMINUS, 0); }
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public SetOperationContext(QueryTermContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterSetOperation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitSetOperation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitSetOperation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryTermContext queryTerm() throws RecognitionException {
		return queryTerm(0);
	}

	private QueryTermContext queryTerm(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		QueryTermContext _localctx = new QueryTermContext(_ctx, _parentState);
		QueryTermContext _prevctx = _localctx;
		int _startState = 86;
		enterRecursionRule(_localctx, 86, RULE_queryTerm, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			{
			_localctx = new QueryTermDefaultContext(_localctx);
			_ctx = _localctx;
			_prevctx = _localctx;

			setState(1729);
			queryPrimary();
			}
			_ctx.stop = _input.LT(-1);
			setState(1754);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,201,_ctx);
			while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(1752);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,200,_ctx) ) {
					case 1:
						{
						_localctx = new SetOperationContext(new QueryTermContext(_parentctx, _parentState));
						((SetOperationContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_queryTerm);
						setState(1731);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(1732);
						if (!(legacy_setops_precedence_enabled)) throw new FailedPredicateException(this, "legacy_setops_precedence_enabled");
						setState(1733);
						((SetOperationContext)_localctx).operator = _input.LT(1);
						_la = _input.LA(1);
						if ( !(_la==EXCEPT || _la==INTERSECT || _la==SETMINUS || _la==UNION) ) {
							((SetOperationContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(1735);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==ALL || _la==DISTINCT) {
							{
							setState(1734);
							setQuantifier();
							}
						}

						setState(1737);
						((SetOperationContext)_localctx).right = queryTerm(4);
						}
						break;
					case 2:
						{
						_localctx = new SetOperationContext(new QueryTermContext(_parentctx, _parentState));
						((SetOperationContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_queryTerm);
						setState(1738);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(1739);
						if (!(!legacy_setops_precedence_enabled)) throw new FailedPredicateException(this, "!legacy_setops_precedence_enabled");
						setState(1740);
						((SetOperationContext)_localctx).operator = match(INTERSECT);
						setState(1742);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==ALL || _la==DISTINCT) {
							{
							setState(1741);
							setQuantifier();
							}
						}

						setState(1744);
						((SetOperationContext)_localctx).right = queryTerm(3);
						}
						break;
					case 3:
						{
						_localctx = new SetOperationContext(new QueryTermContext(_parentctx, _parentState));
						((SetOperationContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_queryTerm);
						setState(1745);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(1746);
						if (!(!legacy_setops_precedence_enabled)) throw new FailedPredicateException(this, "!legacy_setops_precedence_enabled");
						setState(1747);
						((SetOperationContext)_localctx).operator = _input.LT(1);
						_la = _input.LA(1);
						if ( !(_la==EXCEPT || _la==SETMINUS || _la==UNION) ) {
							((SetOperationContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(1749);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==ALL || _la==DISTINCT) {
							{
							setState(1748);
							setQuantifier();
							}
						}

						setState(1751);
						((SetOperationContext)_localctx).right = queryTerm(2);
						}
						break;
					}
					} 
				}
				setState(1756);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,201,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QueryPrimaryContext extends ParserRuleContext {
		public QueryPrimaryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryPrimary; }
	 
		public QueryPrimaryContext() { }
		public void copyFrom(QueryPrimaryContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SubqueryContext extends QueryPrimaryContext {
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public SubqueryContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterSubquery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitSubquery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitSubquery(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class QueryPrimaryDefaultContext extends QueryPrimaryContext {
		public QuerySpecificationContext querySpecification() {
			return getRuleContext(QuerySpecificationContext.class,0);
		}
		public QueryPrimaryDefaultContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterQueryPrimaryDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitQueryPrimaryDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitQueryPrimaryDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class InlineTableDefault1Context extends QueryPrimaryContext {
		public InlineTableContext inlineTable() {
			return getRuleContext(InlineTableContext.class,0);
		}
		public InlineTableDefault1Context(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterInlineTableDefault1(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitInlineTableDefault1(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitInlineTableDefault1(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class FromStmtContext extends QueryPrimaryContext {
		public FromStatementContext fromStatement() {
			return getRuleContext(FromStatementContext.class,0);
		}
		public FromStmtContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterFromStmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitFromStmt(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitFromStmt(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TableContext extends QueryPrimaryContext {
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TableContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitTable(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryPrimaryContext queryPrimary() throws RecognitionException {
		QueryPrimaryContext _localctx = new QueryPrimaryContext(_ctx, getState());
		enterRule(_localctx, 88, RULE_queryPrimary);
		try {
			setState(1766);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case MAP:
			case REDUCE:
			case SELECT:
				_localctx = new QueryPrimaryDefaultContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1757);
				querySpecification();
				}
				break;
			case FROM:
				_localctx = new FromStmtContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1758);
				fromStatement();
				}
				break;
			case TABLE:
				_localctx = new TableContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1759);
				match(TABLE);
				setState(1760);
				multipartIdentifier();
				}
				break;
			case VALUES:
				_localctx = new InlineTableDefault1Context(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1761);
				inlineTable();
				}
				break;
			case LEFT_PAREN:
				_localctx = new SubqueryContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(1762);
				match(LEFT_PAREN);
				setState(1763);
				query();
				setState(1764);
				match(RIGHT_PAREN);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SortItemContext extends ParserRuleContext {
		public Token ordering;
		public Token nullOrder;
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode NULLS() { return getToken(SqlBaseParser.NULLS, 0); }
		public TerminalNode ASC() { return getToken(SqlBaseParser.ASC, 0); }
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode LAST() { return getToken(SqlBaseParser.LAST, 0); }
		public TerminalNode FIRST() { return getToken(SqlBaseParser.FIRST, 0); }
		public SortItemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sortItem; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterSortItem(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitSortItem(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitSortItem(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SortItemContext sortItem() throws RecognitionException {
		SortItemContext _localctx = new SortItemContext(_ctx, getState());
		enterRule(_localctx, 90, RULE_sortItem);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1768);
			expression();
			setState(1770);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,203,_ctx) ) {
			case 1:
				{
				setState(1769);
				((SortItemContext)_localctx).ordering = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==ASC || _la==DESC) ) {
					((SortItemContext)_localctx).ordering = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			}
			setState(1774);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,204,_ctx) ) {
			case 1:
				{
				setState(1772);
				match(NULLS);
				setState(1773);
				((SortItemContext)_localctx).nullOrder = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==FIRST || _la==LAST) ) {
					((SortItemContext)_localctx).nullOrder = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class FromStatementContext extends ParserRuleContext {
		public FromClauseContext fromClause() {
			return getRuleContext(FromClauseContext.class,0);
		}
		public List<FromStatementBodyContext> fromStatementBody() {
			return getRuleContexts(FromStatementBodyContext.class);
		}
		public FromStatementBodyContext fromStatementBody(int i) {
			return getRuleContext(FromStatementBodyContext.class,i);
		}
		public FromStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fromStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterFromStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitFromStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitFromStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FromStatementContext fromStatement() throws RecognitionException {
		FromStatementContext _localctx = new FromStatementContext(_ctx, getState());
		enterRule(_localctx, 92, RULE_fromStatement);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1776);
			fromClause();
			setState(1778); 
			_errHandler.sync(this);
			_alt = 1;
			do {
				switch (_alt) {
				case 1:
					{
					{
					setState(1777);
					fromStatementBody();
					}
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(1780); 
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,205,_ctx);
			} while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class FromStatementBodyContext extends ParserRuleContext {
		public TransformClauseContext transformClause() {
			return getRuleContext(TransformClauseContext.class,0);
		}
		public QueryOrganizationContext queryOrganization() {
			return getRuleContext(QueryOrganizationContext.class,0);
		}
		public WhereClauseContext whereClause() {
			return getRuleContext(WhereClauseContext.class,0);
		}
		public SelectClauseContext selectClause() {
			return getRuleContext(SelectClauseContext.class,0);
		}
		public List<LateralViewContext> lateralView() {
			return getRuleContexts(LateralViewContext.class);
		}
		public LateralViewContext lateralView(int i) {
			return getRuleContext(LateralViewContext.class,i);
		}
		public AggregationClauseContext aggregationClause() {
			return getRuleContext(AggregationClauseContext.class,0);
		}
		public HavingClauseContext havingClause() {
			return getRuleContext(HavingClauseContext.class,0);
		}
		public WindowClauseContext windowClause() {
			return getRuleContext(WindowClauseContext.class,0);
		}
		public FromStatementBodyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fromStatementBody; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterFromStatementBody(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitFromStatementBody(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitFromStatementBody(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FromStatementBodyContext fromStatementBody() throws RecognitionException {
		FromStatementBodyContext _localctx = new FromStatementBodyContext(_ctx, getState());
		enterRule(_localctx, 94, RULE_fromStatementBody);
		try {
			int _alt;
			setState(1809);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,212,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1782);
				transformClause();
				setState(1784);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,206,_ctx) ) {
				case 1:
					{
					setState(1783);
					whereClause();
					}
					break;
				}
				setState(1786);
				queryOrganization();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1788);
				selectClause();
				setState(1792);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,207,_ctx);
				while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1789);
						lateralView();
						}
						} 
					}
					setState(1794);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,207,_ctx);
				}
				setState(1796);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,208,_ctx) ) {
				case 1:
					{
					setState(1795);
					whereClause();
					}
					break;
				}
				setState(1799);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,209,_ctx) ) {
				case 1:
					{
					setState(1798);
					aggregationClause();
					}
					break;
				}
				setState(1802);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,210,_ctx) ) {
				case 1:
					{
					setState(1801);
					havingClause();
					}
					break;
				}
				setState(1805);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,211,_ctx) ) {
				case 1:
					{
					setState(1804);
					windowClause();
					}
					break;
				}
				setState(1807);
				queryOrganization();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QuerySpecificationContext extends ParserRuleContext {
		public QuerySpecificationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_querySpecification; }
	 
		public QuerySpecificationContext() { }
		public void copyFrom(QuerySpecificationContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RegularQuerySpecificationContext extends QuerySpecificationContext {
		public SelectClauseContext selectClause() {
			return getRuleContext(SelectClauseContext.class,0);
		}
		public FromClauseContext fromClause() {
			return getRuleContext(FromClauseContext.class,0);
		}
		public List<LateralViewContext> lateralView() {
			return getRuleContexts(LateralViewContext.class);
		}
		public LateralViewContext lateralView(int i) {
			return getRuleContext(LateralViewContext.class,i);
		}
		public WhereClauseContext whereClause() {
			return getRuleContext(WhereClauseContext.class,0);
		}
		public AggregationClauseContext aggregationClause() {
			return getRuleContext(AggregationClauseContext.class,0);
		}
		public HavingClauseContext havingClause() {
			return getRuleContext(HavingClauseContext.class,0);
		}
		public WindowClauseContext windowClause() {
			return getRuleContext(WindowClauseContext.class,0);
		}
		public RegularQuerySpecificationContext(QuerySpecificationContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterRegularQuerySpecification(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitRegularQuerySpecification(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitRegularQuerySpecification(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TransformQuerySpecificationContext extends QuerySpecificationContext {
		public TransformClauseContext transformClause() {
			return getRuleContext(TransformClauseContext.class,0);
		}
		public FromClauseContext fromClause() {
			return getRuleContext(FromClauseContext.class,0);
		}
		public List<LateralViewContext> lateralView() {
			return getRuleContexts(LateralViewContext.class);
		}
		public LateralViewContext lateralView(int i) {
			return getRuleContext(LateralViewContext.class,i);
		}
		public WhereClauseContext whereClause() {
			return getRuleContext(WhereClauseContext.class,0);
		}
		public AggregationClauseContext aggregationClause() {
			return getRuleContext(AggregationClauseContext.class,0);
		}
		public HavingClauseContext havingClause() {
			return getRuleContext(HavingClauseContext.class,0);
		}
		public WindowClauseContext windowClause() {
			return getRuleContext(WindowClauseContext.class,0);
		}
		public TransformQuerySpecificationContext(QuerySpecificationContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterTransformQuerySpecification(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitTransformQuerySpecification(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitTransformQuerySpecification(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QuerySpecificationContext querySpecification() throws RecognitionException {
		QuerySpecificationContext _localctx = new QuerySpecificationContext(_ctx, getState());
		enterRule(_localctx, 96, RULE_querySpecification);
		try {
			int _alt;
			setState(1855);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,225,_ctx) ) {
			case 1:
				_localctx = new TransformQuerySpecificationContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1811);
				transformClause();
				setState(1813);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,213,_ctx) ) {
				case 1:
					{
					setState(1812);
					fromClause();
					}
					break;
				}
				setState(1818);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,214,_ctx);
				while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1815);
						lateralView();
						}
						} 
					}
					setState(1820);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,214,_ctx);
				}
				setState(1822);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,215,_ctx) ) {
				case 1:
					{
					setState(1821);
					whereClause();
					}
					break;
				}
				setState(1825);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,216,_ctx) ) {
				case 1:
					{
					setState(1824);
					aggregationClause();
					}
					break;
				}
				setState(1828);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,217,_ctx) ) {
				case 1:
					{
					setState(1827);
					havingClause();
					}
					break;
				}
				setState(1831);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,218,_ctx) ) {
				case 1:
					{
					setState(1830);
					windowClause();
					}
					break;
				}
				}
				break;
			case 2:
				_localctx = new RegularQuerySpecificationContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1833);
				selectClause();
				setState(1835);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,219,_ctx) ) {
				case 1:
					{
					setState(1834);
					fromClause();
					}
					break;
				}
				setState(1840);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,220,_ctx);
				while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1837);
						lateralView();
						}
						} 
					}
					setState(1842);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,220,_ctx);
				}
				setState(1844);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,221,_ctx) ) {
				case 1:
					{
					setState(1843);
					whereClause();
					}
					break;
				}
				setState(1847);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,222,_ctx) ) {
				case 1:
					{
					setState(1846);
					aggregationClause();
					}
					break;
				}
				setState(1850);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,223,_ctx) ) {
				case 1:
					{
					setState(1849);
					havingClause();
					}
					break;
				}
				setState(1853);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,224,_ctx) ) {
				case 1:
					{
					setState(1852);
					windowClause();
					}
					break;
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TransformClauseContext extends ParserRuleContext {
		public Token kind;
		public RowFormatContext inRowFormat;
		public Token recordWriter;
		public Token script;
		public RowFormatContext outRowFormat;
		public Token recordReader;
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public List<TerminalNode> STRING() { return getTokens(SqlBaseParser.STRING); }
		public TerminalNode STRING(int i) {
			return getToken(SqlBaseParser.STRING, i);
		}
		public TerminalNode SELECT() { return getToken(SqlBaseParser.SELECT, 0); }
		public List<TerminalNode> LEFT_PAREN() { return getTokens(SqlBaseParser.LEFT_PAREN); }
		public TerminalNode LEFT_PAREN(int i) {
			return getToken(SqlBaseParser.LEFT_PAREN, i);
		}
		public ExpressionSeqContext expressionSeq() {
			return getRuleContext(ExpressionSeqContext.class,0);
		}
		public List<TerminalNode> RIGHT_PAREN() { return getTokens(SqlBaseParser.RIGHT_PAREN); }
		public TerminalNode RIGHT_PAREN(int i) {
			return getToken(SqlBaseParser.RIGHT_PAREN, i);
		}
		public TerminalNode TRANSFORM() { return getToken(SqlBaseParser.TRANSFORM, 0); }
		public TerminalNode MAP() { return getToken(SqlBaseParser.MAP, 0); }
		public TerminalNode REDUCE() { return getToken(SqlBaseParser.REDUCE, 0); }
		public TerminalNode RECORDWRITER() { return getToken(SqlBaseParser.RECORDWRITER, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TerminalNode RECORDREADER() { return getToken(SqlBaseParser.RECORDREADER, 0); }
		public List<RowFormatContext> rowFormat() {
			return getRuleContexts(RowFormatContext.class);
		}
		public RowFormatContext rowFormat(int i) {
			return getRuleContext(RowFormatContext.class,i);
		}
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public IdentifierSeqContext identifierSeq() {
			return getRuleContext(IdentifierSeqContext.class,0);
		}
		public ColTypeListContext colTypeList() {
			return getRuleContext(ColTypeListContext.class,0);
		}
		public TransformClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_transformClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterTransformClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitTransformClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitTransformClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TransformClauseContext transformClause() throws RecognitionException {
		TransformClauseContext _localctx = new TransformClauseContext(_ctx, getState());
		enterRule(_localctx, 98, RULE_transformClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1876);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case SELECT:
				{
				setState(1857);
				match(SELECT);
				setState(1858);
				((TransformClauseContext)_localctx).kind = match(TRANSFORM);
				setState(1859);
				match(LEFT_PAREN);
				setState(1861);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,226,_ctx) ) {
				case 1:
					{
					setState(1860);
					setQuantifier();
					}
					break;
				}
				setState(1863);
				expressionSeq();
				setState(1864);
				match(RIGHT_PAREN);
				}
				break;
			case MAP:
				{
				setState(1866);
				((TransformClauseContext)_localctx).kind = match(MAP);
				setState(1868);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,227,_ctx) ) {
				case 1:
					{
					setState(1867);
					setQuantifier();
					}
					break;
				}
				setState(1870);
				expressionSeq();
				}
				break;
			case REDUCE:
				{
				setState(1871);
				((TransformClauseContext)_localctx).kind = match(REDUCE);
				setState(1873);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,228,_ctx) ) {
				case 1:
					{
					setState(1872);
					setQuantifier();
					}
					break;
				}
				setState(1875);
				expressionSeq();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(1879);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ROW) {
				{
				setState(1878);
				((TransformClauseContext)_localctx).inRowFormat = rowFormat();
				}
			}

			setState(1883);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==RECORDWRITER) {
				{
				setState(1881);
				match(RECORDWRITER);
				setState(1882);
				((TransformClauseContext)_localctx).recordWriter = match(STRING);
				}
			}

			setState(1885);
			match(USING);
			setState(1886);
			((TransformClauseContext)_localctx).script = match(STRING);
			setState(1899);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,234,_ctx) ) {
			case 1:
				{
				setState(1887);
				match(AS);
				setState(1897);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,233,_ctx) ) {
				case 1:
					{
					setState(1888);
					identifierSeq();
					}
					break;
				case 2:
					{
					setState(1889);
					colTypeList();
					}
					break;
				case 3:
					{
					{
					setState(1890);
					match(LEFT_PAREN);
					setState(1893);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,232,_ctx) ) {
					case 1:
						{
						setState(1891);
						identifierSeq();
						}
						break;
					case 2:
						{
						setState(1892);
						colTypeList();
						}
						break;
					}
					setState(1895);
					match(RIGHT_PAREN);
					}
					}
					break;
				}
				}
				break;
			}
			setState(1902);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,235,_ctx) ) {
			case 1:
				{
				setState(1901);
				((TransformClauseContext)_localctx).outRowFormat = rowFormat();
				}
				break;
			}
			setState(1906);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,236,_ctx) ) {
			case 1:
				{
				setState(1904);
				match(RECORDREADER);
				setState(1905);
				((TransformClauseContext)_localctx).recordReader = match(STRING);
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SelectClauseContext extends ParserRuleContext {
		public HintContext hint;
		public List<HintContext> hints = new ArrayList<HintContext>();
		public TerminalNode SELECT() { return getToken(SqlBaseParser.SELECT, 0); }
		public NamedExpressionSeqContext namedExpressionSeq() {
			return getRuleContext(NamedExpressionSeqContext.class,0);
		}
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public IntoClauseContext intoClause() {
			return getRuleContext(IntoClauseContext.class,0);
		}
		public List<HintContext> hint() {
			return getRuleContexts(HintContext.class);
		}
		public HintContext hint(int i) {
			return getRuleContext(HintContext.class,i);
		}
		public SelectClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_selectClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterSelectClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitSelectClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitSelectClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SelectClauseContext selectClause() throws RecognitionException {
		SelectClauseContext _localctx = new SelectClauseContext(_ctx, getState());
		enterRule(_localctx, 100, RULE_selectClause);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1908);
			match(SELECT);
			setState(1912);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,237,_ctx);
			while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1909);
					((SelectClauseContext)_localctx).hint = hint();
					((SelectClauseContext)_localctx).hints.add(((SelectClauseContext)_localctx).hint);
					}
					} 
				}
				setState(1914);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,237,_ctx);
			}
			setState(1916);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,238,_ctx) ) {
			case 1:
				{
				setState(1915);
				setQuantifier();
				}
				break;
			}
			setState(1918);
			namedExpressionSeq();
			setState(1920);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,239,_ctx) ) {
			case 1:
				{
				setState(1919);
				intoClause();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class IntoClauseContext extends ParserRuleContext {
		public TerminalNode INTO() { return getToken(SqlBaseParser.INTO, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public IntoClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_intoClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterIntoClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitIntoClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitIntoClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IntoClauseContext intoClause() throws RecognitionException {
		IntoClauseContext _localctx = new IntoClauseContext(_ctx, getState());
		enterRule(_localctx, 102, RULE_intoClause);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1922);
			match(INTO);
			setState(1923);
			expression();
			setState(1928);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,240,_ctx);
			while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1924);
					match(COMMA);
					setState(1925);
					expression();
					}
					} 
				}
				setState(1930);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,240,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SetClauseContext extends ParserRuleContext {
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public AssignmentListContext assignmentList() {
			return getRuleContext(AssignmentListContext.class,0);
		}
		public SetClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_setClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterSetClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitSetClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitSetClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SetClauseContext setClause() throws RecognitionException {
		SetClauseContext _localctx = new SetClauseContext(_ctx, getState());
		enterRule(_localctx, 104, RULE_setClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1931);
			match(SET);
			setState(1932);
			assignmentList();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class MatchedClauseContext extends ParserRuleContext {
		public BooleanExpressionContext matchedCond;
		public TerminalNode WHEN() { return getToken(SqlBaseParser.WHEN, 0); }
		public TerminalNode MATCHED() { return getToken(SqlBaseParser.MATCHED, 0); }
		public TerminalNode THEN() { return getToken(SqlBaseParser.THEN, 0); }
		public MatchedActionContext matchedAction() {
			return getRuleContext(MatchedActionContext.class,0);
		}
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public MatchedClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_matchedClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterMatchedClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitMatchedClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitMatchedClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final MatchedClauseContext matchedClause() throws RecognitionException {
		MatchedClauseContext _localctx = new MatchedClauseContext(_ctx, getState());
		enterRule(_localctx, 106, RULE_matchedClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1934);
			match(WHEN);
			setState(1935);
			match(MATCHED);
			setState(1938);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==AND) {
				{
				setState(1936);
				match(AND);
				setState(1937);
				((MatchedClauseContext)_localctx).matchedCond = booleanExpression(0);
				}
			}

			setState(1940);
			match(THEN);
			setState(1941);
			matchedAction();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NotMatchedClauseContext extends ParserRuleContext {
		public BooleanExpressionContext notMatchedCond;
		public TerminalNode WHEN() { return getToken(SqlBaseParser.WHEN, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode MATCHED() { return getToken(SqlBaseParser.MATCHED, 0); }
		public TerminalNode THEN() { return getToken(SqlBaseParser.THEN, 0); }
		public NotMatchedActionContext notMatchedAction() {
			return getRuleContext(NotMatchedActionContext.class,0);
		}
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public NotMatchedClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_notMatchedClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterNotMatchedClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitNotMatchedClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitNotMatchedClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NotMatchedClauseContext notMatchedClause() throws RecognitionException {
		NotMatchedClauseContext _localctx = new NotMatchedClauseContext(_ctx, getState());
		enterRule(_localctx, 108, RULE_notMatchedClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1943);
			match(WHEN);
			setState(1944);
			match(NOT);
			setState(1945);
			match(MATCHED);
			setState(1948);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==AND) {
				{
				setState(1946);
				match(AND);
				setState(1947);
				((NotMatchedClauseContext)_localctx).notMatchedCond = booleanExpression(0);
				}
			}

			setState(1950);
			match(THEN);
			setState(1951);
			notMatchedAction();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class MatchedActionContext extends ParserRuleContext {
		public BooleanExpressionContext updateCondition;
		public TerminalNode DELETE() { return getToken(SqlBaseParser.DELETE, 0); }
		public TerminalNode UPDATE() { return getToken(SqlBaseParser.UPDATE, 0); }
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
		public AssignmentListContext assignmentList() {
			return getRuleContext(AssignmentListContext.class,0);
		}
		public TerminalNode WHERE() { return getToken(SqlBaseParser.WHERE, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public MatchedActionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_matchedAction; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterMatchedAction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitMatchedAction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitMatchedAction(this);
			else return visitor.visitChildren(this);
		}
	}

	public final MatchedActionContext matchedAction() throws RecognitionException {
		MatchedActionContext _localctx = new MatchedActionContext(_ctx, getState());
		enterRule(_localctx, 110, RULE_matchedAction);
		int _la;
		try {
			setState(1964);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,244,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1953);
				match(DELETE);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1954);
				match(UPDATE);
				setState(1955);
				match(SET);
				setState(1956);
				match(ASTERISK);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1957);
				match(UPDATE);
				setState(1958);
				match(SET);
				setState(1959);
				assignmentList();
				setState(1962);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WHERE) {
					{
					setState(1960);
					match(WHERE);
					setState(1961);
					((MatchedActionContext)_localctx).updateCondition = booleanExpression(0);
					}
				}

				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NotMatchedActionContext extends ParserRuleContext {
		public MultipartIdentifierListContext columns;
		public TerminalNode INSERT() { return getToken(SqlBaseParser.INSERT, 0); }
		public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
		public List<TerminalNode> LEFT_PAREN() { return getTokens(SqlBaseParser.LEFT_PAREN); }
		public TerminalNode LEFT_PAREN(int i) {
			return getToken(SqlBaseParser.LEFT_PAREN, i);
		}
		public List<TerminalNode> RIGHT_PAREN() { return getTokens(SqlBaseParser.RIGHT_PAREN); }
		public TerminalNode RIGHT_PAREN(int i) {
			return getToken(SqlBaseParser.RIGHT_PAREN, i);
		}
		public TerminalNode VALUES() { return getToken(SqlBaseParser.VALUES, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public MultipartIdentifierListContext multipartIdentifierList() {
			return getRuleContext(MultipartIdentifierListContext.class,0);
		}
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public NotMatchedActionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_notMatchedAction; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterNotMatchedAction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitNotMatchedAction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitNotMatchedAction(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NotMatchedActionContext notMatchedAction() throws RecognitionException {
		NotMatchedActionContext _localctx = new NotMatchedActionContext(_ctx, getState());
		enterRule(_localctx, 112, RULE_notMatchedAction);
		int _la;
		try {
			setState(1984);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,246,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1966);
				match(INSERT);
				setState(1967);
				match(ASTERISK);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1968);
				match(INSERT);
				setState(1969);
				match(LEFT_PAREN);
				setState(1970);
				((NotMatchedActionContext)_localctx).columns = multipartIdentifierList();
				setState(1971);
				match(RIGHT_PAREN);
				setState(1972);
				match(VALUES);
				setState(1973);
				match(LEFT_PAREN);
				setState(1974);
				expression();
				setState(1979);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(1975);
					match(COMMA);
					setState(1976);
					expression();
					}
					}
					setState(1981);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1982);
				match(RIGHT_PAREN);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class AssignmentListContext extends ParserRuleContext {
		public List<AssignmentContext> assignment() {
			return getRuleContexts(AssignmentContext.class);
		}
		public AssignmentContext assignment(int i) {
			return getRuleContext(AssignmentContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public AssignmentListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_assignmentList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterAssignmentList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitAssignmentList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitAssignmentList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AssignmentListContext assignmentList() throws RecognitionException {
		AssignmentListContext _localctx = new AssignmentListContext(_ctx, getState());
		enterRule(_localctx, 114, RULE_assignmentList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1986);
			assignment();
			setState(1991);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1987);
				match(COMMA);
				setState(1988);
				assignment();
				}
				}
				setState(1993);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class AssignmentContext extends ParserRuleContext {
		public MultipartIdentifierContext key;
		public ExpressionContext value;
		public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public AssignmentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_assignment; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterAssignment(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitAssignment(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitAssignment(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AssignmentContext assignment() throws RecognitionException {
		AssignmentContext _localctx = new AssignmentContext(_ctx, getState());
		enterRule(_localctx, 116, RULE_assignment);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1994);
			((AssignmentContext)_localctx).key = multipartIdentifier();
			setState(1995);
			match(EQ);
			setState(1996);
			((AssignmentContext)_localctx).value = expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class WhereClauseContext extends ParserRuleContext {
		public TerminalNode WHERE() { return getToken(SqlBaseParser.WHERE, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public WhereClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_whereClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterWhereClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitWhereClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitWhereClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WhereClauseContext whereClause() throws RecognitionException {
		WhereClauseContext _localctx = new WhereClauseContext(_ctx, getState());
		enterRule(_localctx, 118, RULE_whereClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1998);
			match(WHERE);
			setState(1999);
			booleanExpression(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class HavingClauseContext extends ParserRuleContext {
		public TerminalNode HAVING() { return getToken(SqlBaseParser.HAVING, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public HavingClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_havingClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterHavingClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitHavingClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitHavingClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final HavingClauseContext havingClause() throws RecognitionException {
		HavingClauseContext _localctx = new HavingClauseContext(_ctx, getState());
		enterRule(_localctx, 120, RULE_havingClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2001);
			match(HAVING);
			setState(2002);
			booleanExpression(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class HintContext extends ParserRuleContext {
		public HintStatementContext hintStatement;
		public List<HintStatementContext> hintStatements = new ArrayList<HintStatementContext>();
		public TerminalNode HENT_START() { return getToken(SqlBaseParser.HENT_START, 0); }
		public TerminalNode HENT_END() { return getToken(SqlBaseParser.HENT_END, 0); }
		public List<HintStatementContext> hintStatement() {
			return getRuleContexts(HintStatementContext.class);
		}
		public HintStatementContext hintStatement(int i) {
			return getRuleContext(HintStatementContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public HintContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_hint; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterHint(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitHint(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitHint(this);
			else return visitor.visitChildren(this);
		}
	}

	public final HintContext hint() throws RecognitionException {
		HintContext _localctx = new HintContext(_ctx, getState());
		enterRule(_localctx, 122, RULE_hint);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2004);
			match(HENT_START);
			setState(2005);
			((HintContext)_localctx).hintStatement = hintStatement();
			((HintContext)_localctx).hintStatements.add(((HintContext)_localctx).hintStatement);
			setState(2012);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,249,_ctx);
			while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(2007);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,248,_ctx) ) {
					case 1:
						{
						setState(2006);
						match(COMMA);
						}
						break;
					}
					setState(2009);
					((HintContext)_localctx).hintStatement = hintStatement();
					((HintContext)_localctx).hintStatements.add(((HintContext)_localctx).hintStatement);
					}
					} 
				}
				setState(2014);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,249,_ctx);
			}
			setState(2015);
			match(HENT_END);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class HintStatementContext extends ParserRuleContext {
		public IdentifierContext hintName;
		public PrimaryExpressionContext primaryExpression;
		public List<PrimaryExpressionContext> parameters = new ArrayList<PrimaryExpressionContext>();
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public List<PrimaryExpressionContext> primaryExpression() {
			return getRuleContexts(PrimaryExpressionContext.class);
		}
		public PrimaryExpressionContext primaryExpression(int i) {
			return getRuleContext(PrimaryExpressionContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public HintStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_hintStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterHintStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitHintStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitHintStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final HintStatementContext hintStatement() throws RecognitionException {
		HintStatementContext _localctx = new HintStatementContext(_ctx, getState());
		enterRule(_localctx, 124, RULE_hintStatement);
		int _la;
		try {
			setState(2030);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,251,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2017);
				((HintStatementContext)_localctx).hintName = identifier();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2018);
				((HintStatementContext)_localctx).hintName = identifier();
				setState(2019);
				match(LEFT_PAREN);
				setState(2020);
				((HintStatementContext)_localctx).primaryExpression = primaryExpression(0);
				((HintStatementContext)_localctx).parameters.add(((HintStatementContext)_localctx).primaryExpression);
				setState(2025);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(2021);
					match(COMMA);
					setState(2022);
					((HintStatementContext)_localctx).primaryExpression = primaryExpression(0);
					((HintStatementContext)_localctx).parameters.add(((HintStatementContext)_localctx).primaryExpression);
					}
					}
					setState(2027);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(2028);
				match(RIGHT_PAREN);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class FromClauseContext extends ParserRuleContext {
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public List<RelationContext> relation() {
			return getRuleContexts(RelationContext.class);
		}
		public RelationContext relation(int i) {
			return getRuleContext(RelationContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public List<LateralViewContext> lateralView() {
			return getRuleContexts(LateralViewContext.class);
		}
		public LateralViewContext lateralView(int i) {
			return getRuleContext(LateralViewContext.class,i);
		}
		public PivotClauseContext pivotClause() {
			return getRuleContext(PivotClauseContext.class,0);
		}
		public FromClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fromClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterFromClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitFromClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitFromClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FromClauseContext fromClause() throws RecognitionException {
		FromClauseContext _localctx = new FromClauseContext(_ctx, getState());
		enterRule(_localctx, 126, RULE_fromClause);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2032);
			match(FROM);
			setState(2033);
			relation();
			setState(2038);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,252,_ctx);
			while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(2034);
					match(COMMA);
					setState(2035);
					relation();
					}
					} 
				}
				setState(2040);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,252,_ctx);
			}
			setState(2044);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,253,_ctx);
			while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(2041);
					lateralView();
					}
					} 
				}
				setState(2046);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,253,_ctx);
			}
			setState(2048);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,254,_ctx) ) {
			case 1:
				{
				setState(2047);
				pivotClause();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TemporalClauseContext extends ParserRuleContext {
		public Token version;
		public ValueExpressionContext timestamp;
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TerminalNode OF() { return getToken(SqlBaseParser.OF, 0); }
		public TerminalNode SYSTEM_VERSION() { return getToken(SqlBaseParser.SYSTEM_VERSION, 0); }
		public TerminalNode VERSION() { return getToken(SqlBaseParser.VERSION, 0); }
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode FOR() { return getToken(SqlBaseParser.FOR, 0); }
		public TerminalNode SYSTEM_TIME() { return getToken(SqlBaseParser.SYSTEM_TIME, 0); }
		public TerminalNode TIMESTAMP() { return getToken(SqlBaseParser.TIMESTAMP, 0); }
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public TemporalClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_temporalClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterTemporalClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitTemporalClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitTemporalClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TemporalClauseContext temporalClause() throws RecognitionException {
		TemporalClauseContext _localctx = new TemporalClauseContext(_ctx, getState());
		enterRule(_localctx, 128, RULE_temporalClause);
		int _la;
		try {
			setState(2064);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,257,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2051);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FOR) {
					{
					setState(2050);
					match(FOR);
					}
				}

				setState(2053);
				_la = _input.LA(1);
				if ( !(_la==SYSTEM_VERSION || _la==VERSION) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(2054);
				match(AS);
				setState(2055);
				match(OF);
				setState(2056);
				((TemporalClauseContext)_localctx).version = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==STRING || _la==INTEGER_VALUE) ) {
					((TemporalClauseContext)_localctx).version = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2058);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FOR) {
					{
					setState(2057);
					match(FOR);
					}
				}

				setState(2060);
				_la = _input.LA(1);
				if ( !(_la==SYSTEM_TIME || _la==TIMESTAMP) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(2061);
				match(AS);
				setState(2062);
				match(OF);
				setState(2063);
				((TemporalClauseContext)_localctx).timestamp = valueExpression(0);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class AggregationClauseContext extends ParserRuleContext {
		public GroupByClauseContext groupByClause;
		public List<GroupByClauseContext> groupingExpressionsWithGroupingAnalytics = new ArrayList<GroupByClauseContext>();
		public ExpressionContext expression;
		public List<ExpressionContext> groupingExpressions = new ArrayList<ExpressionContext>();
		public Token kind;
		public TerminalNode GROUP() { return getToken(SqlBaseParser.GROUP, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public List<GroupByClauseContext> groupByClause() {
			return getRuleContexts(GroupByClauseContext.class);
		}
		public GroupByClauseContext groupByClause(int i) {
			return getRuleContext(GroupByClauseContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode SETS() { return getToken(SqlBaseParser.SETS, 0); }
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public List<GroupingSetContext> groupingSet() {
			return getRuleContexts(GroupingSetContext.class);
		}
		public GroupingSetContext groupingSet(int i) {
			return getRuleContext(GroupingSetContext.class,i);
		}
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public TerminalNode ROLLUP() { return getToken(SqlBaseParser.ROLLUP, 0); }
		public TerminalNode CUBE() { return getToken(SqlBaseParser.CUBE, 0); }
		public TerminalNode GROUPING() { return getToken(SqlBaseParser.GROUPING, 0); }
		public AggregationClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_aggregationClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterAggregationClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitAggregationClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitAggregationClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AggregationClauseContext aggregationClause() throws RecognitionException {
		AggregationClauseContext _localctx = new AggregationClauseContext(_ctx, getState());
		enterRule(_localctx, 130, RULE_aggregationClause);
		int _la;
		try {
			int _alt;
			setState(2105);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,262,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2066);
				match(GROUP);
				setState(2067);
				match(BY);
				setState(2068);
				((AggregationClauseContext)_localctx).groupByClause = groupByClause();
				((AggregationClauseContext)_localctx).groupingExpressionsWithGroupingAnalytics.add(((AggregationClauseContext)_localctx).groupByClause);
				setState(2073);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,258,_ctx);
				while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(2069);
						match(COMMA);
						setState(2070);
						((AggregationClauseContext)_localctx).groupByClause = groupByClause();
						((AggregationClauseContext)_localctx).groupingExpressionsWithGroupingAnalytics.add(((AggregationClauseContext)_localctx).groupByClause);
						}
						} 
					}
					setState(2075);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,258,_ctx);
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2076);
				match(GROUP);
				setState(2077);
				match(BY);
				setState(2078);
				((AggregationClauseContext)_localctx).expression = expression();
				((AggregationClauseContext)_localctx).groupingExpressions.add(((AggregationClauseContext)_localctx).expression);
				setState(2083);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,259,_ctx);
				while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(2079);
						match(COMMA);
						setState(2080);
						((AggregationClauseContext)_localctx).expression = expression();
						((AggregationClauseContext)_localctx).groupingExpressions.add(((AggregationClauseContext)_localctx).expression);
						}
						} 
					}
					setState(2085);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,259,_ctx);
				}
				setState(2103);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,261,_ctx) ) {
				case 1:
					{
					setState(2086);
					match(WITH);
					setState(2087);
					((AggregationClauseContext)_localctx).kind = match(ROLLUP);
					}
					break;
				case 2:
					{
					setState(2088);
					match(WITH);
					setState(2089);
					((AggregationClauseContext)_localctx).kind = match(CUBE);
					}
					break;
				case 3:
					{
					setState(2090);
					((AggregationClauseContext)_localctx).kind = match(GROUPING);
					setState(2091);
					match(SETS);
					setState(2092);
					match(LEFT_PAREN);
					setState(2093);
					groupingSet();
					setState(2098);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==COMMA) {
						{
						{
						setState(2094);
						match(COMMA);
						setState(2095);
						groupingSet();
						}
						}
						setState(2100);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(2101);
					match(RIGHT_PAREN);
					}
					break;
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class GroupByClauseContext extends ParserRuleContext {
		public GroupingAnalyticsContext groupingAnalytics() {
			return getRuleContext(GroupingAnalyticsContext.class,0);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public GroupByClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupByClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterGroupByClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitGroupByClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitGroupByClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GroupByClauseContext groupByClause() throws RecognitionException {
		GroupByClauseContext _localctx = new GroupByClauseContext(_ctx, getState());
		enterRule(_localctx, 132, RULE_groupByClause);
		try {
			setState(2109);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,263,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2107);
				groupingAnalytics();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2108);
				expression();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class GroupingAnalyticsContext extends ParserRuleContext {
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public List<GroupingSetContext> groupingSet() {
			return getRuleContexts(GroupingSetContext.class);
		}
		public GroupingSetContext groupingSet(int i) {
			return getRuleContext(GroupingSetContext.class,i);
		}
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public TerminalNode ROLLUP() { return getToken(SqlBaseParser.ROLLUP, 0); }
		public TerminalNode CUBE() { return getToken(SqlBaseParser.CUBE, 0); }
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public TerminalNode GROUPING() { return getToken(SqlBaseParser.GROUPING, 0); }
		public TerminalNode SETS() { return getToken(SqlBaseParser.SETS, 0); }
		public List<GroupingElementContext> groupingElement() {
			return getRuleContexts(GroupingElementContext.class);
		}
		public GroupingElementContext groupingElement(int i) {
			return getRuleContext(GroupingElementContext.class,i);
		}
		public GroupingAnalyticsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupingAnalytics; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterGroupingAnalytics(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitGroupingAnalytics(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitGroupingAnalytics(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GroupingAnalyticsContext groupingAnalytics() throws RecognitionException {
		GroupingAnalyticsContext _localctx = new GroupingAnalyticsContext(_ctx, getState());
		enterRule(_localctx, 134, RULE_groupingAnalytics);
		int _la;
		try {
			setState(2136);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case CUBE:
			case ROLLUP:
				enterOuterAlt(_localctx, 1);
				{
				setState(2111);
				_la = _input.LA(1);
				if ( !(_la==CUBE || _la==ROLLUP) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(2112);
				match(LEFT_PAREN);
				setState(2113);
				groupingSet();
				setState(2118);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(2114);
					match(COMMA);
					setState(2115);
					groupingSet();
					}
					}
					setState(2120);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(2121);
				match(RIGHT_PAREN);
				}
				break;
			case GROUPING:
				enterOuterAlt(_localctx, 2);
				{
				setState(2123);
				match(GROUPING);
				setState(2124);
				match(SETS);
				setState(2125);
				match(LEFT_PAREN);
				setState(2126);
				groupingElement();
				setState(2131);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(2127);
					match(COMMA);
					setState(2128);
					groupingElement();
					}
					}
					setState(2133);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(2134);
				match(RIGHT_PAREN);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class GroupingElementContext extends ParserRuleContext {
		public GroupingAnalyticsContext groupingAnalytics() {
			return getRuleContext(GroupingAnalyticsContext.class,0);
		}
		public GroupingSetContext groupingSet() {
			return getRuleContext(GroupingSetContext.class,0);
		}
		public GroupingElementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupingElement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterGroupingElement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitGroupingElement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitGroupingElement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GroupingElementContext groupingElement() throws RecognitionException {
		GroupingElementContext _localctx = new GroupingElementContext(_ctx, getState());
		enterRule(_localctx, 136, RULE_groupingElement);
		try {
			setState(2140);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,267,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2138);
				groupingAnalytics();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2139);
				groupingSet();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class GroupingSetContext extends ParserRuleContext {
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public GroupingSetContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupingSet; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterGroupingSet(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitGroupingSet(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitGroupingSet(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GroupingSetContext groupingSet() throws RecognitionException {
		GroupingSetContext _localctx = new GroupingSetContext(_ctx, getState());
		enterRule(_localctx, 138, RULE_groupingSet);
		int _la;
		try {
			setState(2155);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,270,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2142);
				match(LEFT_PAREN);
				setState(2151);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,269,_ctx) ) {
				case 1:
					{
					setState(2143);
					expression();
					setState(2148);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==COMMA) {
						{
						{
						setState(2144);
						match(COMMA);
						setState(2145);
						expression();
						}
						}
						setState(2150);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
					break;
				}
				setState(2153);
				match(RIGHT_PAREN);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2154);
				expression();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PivotClauseContext extends ParserRuleContext {
		public NamedExpressionSeqContext aggregates;
		public PivotValueContext pivotValue;
		public List<PivotValueContext> pivotValues = new ArrayList<PivotValueContext>();
		public TerminalNode PIVOT() { return getToken(SqlBaseParser.PIVOT, 0); }
		public List<TerminalNode> LEFT_PAREN() { return getTokens(SqlBaseParser.LEFT_PAREN); }
		public TerminalNode LEFT_PAREN(int i) {
			return getToken(SqlBaseParser.LEFT_PAREN, i);
		}
		public TerminalNode FOR() { return getToken(SqlBaseParser.FOR, 0); }
		public PivotColumnContext pivotColumn() {
			return getRuleContext(PivotColumnContext.class,0);
		}
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public List<TerminalNode> RIGHT_PAREN() { return getTokens(SqlBaseParser.RIGHT_PAREN); }
		public TerminalNode RIGHT_PAREN(int i) {
			return getToken(SqlBaseParser.RIGHT_PAREN, i);
		}
		public NamedExpressionSeqContext namedExpressionSeq() {
			return getRuleContext(NamedExpressionSeqContext.class,0);
		}
		public List<PivotValueContext> pivotValue() {
			return getRuleContexts(PivotValueContext.class);
		}
		public PivotValueContext pivotValue(int i) {
			return getRuleContext(PivotValueContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public PivotClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pivotClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterPivotClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitPivotClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitPivotClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PivotClauseContext pivotClause() throws RecognitionException {
		PivotClauseContext _localctx = new PivotClauseContext(_ctx, getState());
		enterRule(_localctx, 140, RULE_pivotClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2157);
			match(PIVOT);
			setState(2158);
			match(LEFT_PAREN);
			setState(2159);
			((PivotClauseContext)_localctx).aggregates = namedExpressionSeq();
			setState(2160);
			match(FOR);
			setState(2161);
			pivotColumn();
			setState(2162);
			match(IN);
			setState(2163);
			match(LEFT_PAREN);
			setState(2164);
			((PivotClauseContext)_localctx).pivotValue = pivotValue();
			((PivotClauseContext)_localctx).pivotValues.add(((PivotClauseContext)_localctx).pivotValue);
			setState(2169);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(2165);
				match(COMMA);
				setState(2166);
				((PivotClauseContext)_localctx).pivotValue = pivotValue();
				((PivotClauseContext)_localctx).pivotValues.add(((PivotClauseContext)_localctx).pivotValue);
				}
				}
				setState(2171);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(2172);
			match(RIGHT_PAREN);
			setState(2173);
			match(RIGHT_PAREN);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PivotColumnContext extends ParserRuleContext {
		public IdentifierContext identifier;
		public List<IdentifierContext> identifiers = new ArrayList<IdentifierContext>();
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public PivotColumnContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pivotColumn; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterPivotColumn(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitPivotColumn(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitPivotColumn(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PivotColumnContext pivotColumn() throws RecognitionException {
		PivotColumnContext _localctx = new PivotColumnContext(_ctx, getState());
		enterRule(_localctx, 142, RULE_pivotColumn);
		int _la;
		try {
			setState(2187);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,273,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2175);
				((PivotColumnContext)_localctx).identifier = identifier();
				((PivotColumnContext)_localctx).identifiers.add(((PivotColumnContext)_localctx).identifier);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2176);
				match(LEFT_PAREN);
				setState(2177);
				((PivotColumnContext)_localctx).identifier = identifier();
				((PivotColumnContext)_localctx).identifiers.add(((PivotColumnContext)_localctx).identifier);
				setState(2182);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(2178);
					match(COMMA);
					setState(2179);
					((PivotColumnContext)_localctx).identifier = identifier();
					((PivotColumnContext)_localctx).identifiers.add(((PivotColumnContext)_localctx).identifier);
					}
					}
					setState(2184);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(2185);
				match(RIGHT_PAREN);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PivotValueContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public PivotValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pivotValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterPivotValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitPivotValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitPivotValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PivotValueContext pivotValue() throws RecognitionException {
		PivotValueContext _localctx = new PivotValueContext(_ctx, getState());
		enterRule(_localctx, 144, RULE_pivotValue);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2189);
			expression();
			setState(2194);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,275,_ctx) ) {
			case 1:
				{
				setState(2191);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,274,_ctx) ) {
				case 1:
					{
					setState(2190);
					match(AS);
					}
					break;
				}
				setState(2193);
				identifier();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class LateralViewContext extends ParserRuleContext {
		public IdentifierContext tblName;
		public IdentifierContext identifier;
		public List<IdentifierContext> colName = new ArrayList<IdentifierContext>();
		public TerminalNode LATERAL() { return getToken(SqlBaseParser.LATERAL, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public TerminalNode OUTER() { return getToken(SqlBaseParser.OUTER, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public LateralViewContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_lateralView; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterLateralView(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitLateralView(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitLateralView(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LateralViewContext lateralView() throws RecognitionException {
		LateralViewContext _localctx = new LateralViewContext(_ctx, getState());
		enterRule(_localctx, 146, RULE_lateralView);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2196);
			match(LATERAL);
			setState(2197);
			match(VIEW);
			setState(2199);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,276,_ctx) ) {
			case 1:
				{
				setState(2198);
				match(OUTER);
				}
				break;
			}
			setState(2201);
			qualifiedName();
			setState(2202);
			match(LEFT_PAREN);
			setState(2211);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,278,_ctx) ) {
			case 1:
				{
				setState(2203);
				expression();
				setState(2208);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(2204);
					match(COMMA);
					setState(2205);
					expression();
					}
					}
					setState(2210);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			}
			setState(2213);
			match(RIGHT_PAREN);
			setState(2214);
			((LateralViewContext)_localctx).tblName = identifier();
			setState(2226);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,281,_ctx) ) {
			case 1:
				{
				setState(2216);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,279,_ctx) ) {
				case 1:
					{
					setState(2215);
					match(AS);
					}
					break;
				}
				setState(2218);
				((LateralViewContext)_localctx).identifier = identifier();
				((LateralViewContext)_localctx).colName.add(((LateralViewContext)_localctx).identifier);
				setState(2223);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,280,_ctx);
				while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(2219);
						match(COMMA);
						setState(2220);
						((LateralViewContext)_localctx).identifier = identifier();
						((LateralViewContext)_localctx).colName.add(((LateralViewContext)_localctx).identifier);
						}
						} 
					}
					setState(2225);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,280,_ctx);
				}
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SetQuantifierContext extends ParserRuleContext {
		public TerminalNode DISTINCT() { return getToken(SqlBaseParser.DISTINCT, 0); }
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public SetQuantifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_setQuantifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterSetQuantifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitSetQuantifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitSetQuantifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SetQuantifierContext setQuantifier() throws RecognitionException {
		SetQuantifierContext _localctx = new SetQuantifierContext(_ctx, getState());
		enterRule(_localctx, 148, RULE_setQuantifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2228);
			_la = _input.LA(1);
			if ( !(_la==ALL || _la==DISTINCT) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class RelationContext extends ParserRuleContext {
		public RelationPrimaryContext relationPrimary() {
			return getRuleContext(RelationPrimaryContext.class,0);
		}
		public TerminalNode LATERAL() { return getToken(SqlBaseParser.LATERAL, 0); }
		public List<JoinRelationContext> joinRelation() {
			return getRuleContexts(JoinRelationContext.class);
		}
		public JoinRelationContext joinRelation(int i) {
			return getRuleContext(JoinRelationContext.class,i);
		}
		public RelationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitRelation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RelationContext relation() throws RecognitionException {
		RelationContext _localctx = new RelationContext(_ctx, getState());
		enterRule(_localctx, 150, RULE_relation);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2231);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,282,_ctx) ) {
			case 1:
				{
				setState(2230);
				match(LATERAL);
				}
				break;
			}
			setState(2233);
			relationPrimary();
			setState(2237);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,283,_ctx);
			while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(2234);
					joinRelation();
					}
					} 
				}
				setState(2239);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,283,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class JoinRelationContext extends ParserRuleContext {
		public RelationPrimaryContext right;
		public TerminalNode JOIN() { return getToken(SqlBaseParser.JOIN, 0); }
		public RelationPrimaryContext relationPrimary() {
			return getRuleContext(RelationPrimaryContext.class,0);
		}
		public JoinTypeContext joinType() {
			return getRuleContext(JoinTypeContext.class,0);
		}
		public TerminalNode LATERAL() { return getToken(SqlBaseParser.LATERAL, 0); }
		public JoinCriteriaContext joinCriteria() {
			return getRuleContext(JoinCriteriaContext.class,0);
		}
		public TerminalNode NATURAL() { return getToken(SqlBaseParser.NATURAL, 0); }
		public JoinRelationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinRelation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterJoinRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitJoinRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitJoinRelation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinRelationContext joinRelation() throws RecognitionException {
		JoinRelationContext _localctx = new JoinRelationContext(_ctx, getState());
		enterRule(_localctx, 152, RULE_joinRelation);
		try {
			setState(2257);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ANTI:
			case CROSS:
			case FULL:
			case INNER:
			case JOIN:
			case LEFT:
			case RIGHT:
			case SEMI:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(2240);
				joinType();
				}
				setState(2241);
				match(JOIN);
				setState(2243);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,284,_ctx) ) {
				case 1:
					{
					setState(2242);
					match(LATERAL);
					}
					break;
				}
				setState(2245);
				((JoinRelationContext)_localctx).right = relationPrimary();
				setState(2247);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,285,_ctx) ) {
				case 1:
					{
					setState(2246);
					joinCriteria();
					}
					break;
				}
				}
				break;
			case NATURAL:
				enterOuterAlt(_localctx, 2);
				{
				setState(2249);
				match(NATURAL);
				setState(2250);
				joinType();
				setState(2251);
				match(JOIN);
				setState(2253);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,286,_ctx) ) {
				case 1:
					{
					setState(2252);
					match(LATERAL);
					}
					break;
				}
				setState(2255);
				((JoinRelationContext)_localctx).right = relationPrimary();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class JoinTypeContext extends ParserRuleContext {
		public TerminalNode INNER() { return getToken(SqlBaseParser.INNER, 0); }
		public TerminalNode CROSS() { return getToken(SqlBaseParser.CROSS, 0); }
		public TerminalNode LEFT() { return getToken(SqlBaseParser.LEFT, 0); }
		public TerminalNode OUTER() { return getToken(SqlBaseParser.OUTER, 0); }
		public TerminalNode SEMI() { return getToken(SqlBaseParser.SEMI, 0); }
		public TerminalNode RIGHT() { return getToken(SqlBaseParser.RIGHT, 0); }
		public TerminalNode FULL() { return getToken(SqlBaseParser.FULL, 0); }
		public TerminalNode ANTI() { return getToken(SqlBaseParser.ANTI, 0); }
		public JoinTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterJoinType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitJoinType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitJoinType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinTypeContext joinType() throws RecognitionException {
		JoinTypeContext _localctx = new JoinTypeContext(_ctx, getState());
		enterRule(_localctx, 154, RULE_joinType);
		int _la;
		try {
			setState(2283);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,294,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2260);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==INNER) {
					{
					setState(2259);
					match(INNER);
					}
				}

				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2262);
				match(CROSS);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(2263);
				match(LEFT);
				setState(2265);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OUTER) {
					{
					setState(2264);
					match(OUTER);
					}
				}

				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(2268);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LEFT) {
					{
					setState(2267);
					match(LEFT);
					}
				}

				setState(2270);
				match(SEMI);
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(2271);
				match(RIGHT);
				setState(2273);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OUTER) {
					{
					setState(2272);
					match(OUTER);
					}
				}

				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(2275);
				match(FULL);
				setState(2277);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OUTER) {
					{
					setState(2276);
					match(OUTER);
					}
				}

				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(2280);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LEFT) {
					{
					setState(2279);
					match(LEFT);
					}
				}

				setState(2282);
				match(ANTI);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class JoinCriteriaContext extends ParserRuleContext {
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public JoinCriteriaContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinCriteria; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterJoinCriteria(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitJoinCriteria(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitJoinCriteria(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinCriteriaContext joinCriteria() throws RecognitionException {
		JoinCriteriaContext _localctx = new JoinCriteriaContext(_ctx, getState());
		enterRule(_localctx, 156, RULE_joinCriteria);
		try {
			setState(2289);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ON:
				enterOuterAlt(_localctx, 1);
				{
				setState(2285);
				match(ON);
				setState(2286);
				booleanExpression(0);
				}
				break;
			case USING:
				enterOuterAlt(_localctx, 2);
				{
				setState(2287);
				match(USING);
				setState(2288);
				identifierList();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SampleContext extends ParserRuleContext {
		public Token seed;
		public TerminalNode TABLESAMPLE() { return getToken(SqlBaseParser.TABLESAMPLE, 0); }
		public List<TerminalNode> LEFT_PAREN() { return getTokens(SqlBaseParser.LEFT_PAREN); }
		public TerminalNode LEFT_PAREN(int i) {
			return getToken(SqlBaseParser.LEFT_PAREN, i);
		}
		public List<TerminalNode> RIGHT_PAREN() { return getTokens(SqlBaseParser.RIGHT_PAREN); }
		public TerminalNode RIGHT_PAREN(int i) {
			return getToken(SqlBaseParser.RIGHT_PAREN, i);
		}
		public SampleMethodContext sampleMethod() {
			return getRuleContext(SampleMethodContext.class,0);
		}
		public TerminalNode REPEATABLE() { return getToken(SqlBaseParser.REPEATABLE, 0); }
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public SampleContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sample; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterSample(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitSample(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitSample(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SampleContext sample() throws RecognitionException {
		SampleContext _localctx = new SampleContext(_ctx, getState());
		enterRule(_localctx, 158, RULE_sample);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2291);
			match(TABLESAMPLE);
			setState(2292);
			match(LEFT_PAREN);
			setState(2294);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,296,_ctx) ) {
			case 1:
				{
				setState(2293);
				sampleMethod();
				}
				break;
			}
			setState(2296);
			match(RIGHT_PAREN);
			setState(2301);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,297,_ctx) ) {
			case 1:
				{
				setState(2297);
				match(REPEATABLE);
				setState(2298);
				match(LEFT_PAREN);
				setState(2299);
				((SampleContext)_localctx).seed = match(INTEGER_VALUE);
				setState(2300);
				match(RIGHT_PAREN);
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SampleMethodContext extends ParserRuleContext {
		public SampleMethodContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sampleMethod; }
	 
		public SampleMethodContext() { }
		public void copyFrom(SampleMethodContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SampleByRowsContext extends SampleMethodContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode ROWS() { return getToken(SqlBaseParser.ROWS, 0); }
		public SampleByRowsContext(SampleMethodContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterSampleByRows(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitSampleByRows(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitSampleByRows(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SampleByPercentileContext extends SampleMethodContext {
		public Token negativeSign;
		public Token percentage;
		public TerminalNode PERCENTLIT() { return getToken(SqlBaseParser.PERCENTLIT, 0); }
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public TerminalNode DECIMAL_VALUE() { return getToken(SqlBaseParser.DECIMAL_VALUE, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public SampleByPercentileContext(SampleMethodContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterSampleByPercentile(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitSampleByPercentile(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitSampleByPercentile(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SampleByBucketContext extends SampleMethodContext {
		public Token sampleType;
		public Token numerator;
		public Token denominator;
		public TerminalNode OUT() { return getToken(SqlBaseParser.OUT, 0); }
		public TerminalNode OF() { return getToken(SqlBaseParser.OF, 0); }
		public TerminalNode BUCKET() { return getToken(SqlBaseParser.BUCKET, 0); }
		public List<TerminalNode> INTEGER_VALUE() { return getTokens(SqlBaseParser.INTEGER_VALUE); }
		public TerminalNode INTEGER_VALUE(int i) {
			return getToken(SqlBaseParser.INTEGER_VALUE, i);
		}
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public SampleByBucketContext(SampleMethodContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterSampleByBucket(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitSampleByBucket(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitSampleByBucket(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SampleByBytesContext extends SampleMethodContext {
		public ExpressionContext bytes;
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public SampleByBytesContext(SampleMethodContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterSampleByBytes(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitSampleByBytes(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitSampleByBytes(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SampleMethodContext sampleMethod() throws RecognitionException {
		SampleMethodContext _localctx = new SampleMethodContext(_ctx, getState());
		enterRule(_localctx, 160, RULE_sampleMethod);
		int _la;
		try {
			setState(2327);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,301,_ctx) ) {
			case 1:
				_localctx = new SampleByPercentileContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2304);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(2303);
					((SampleByPercentileContext)_localctx).negativeSign = match(MINUS);
					}
				}

				setState(2306);
				((SampleByPercentileContext)_localctx).percentage = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==INTEGER_VALUE || _la==DECIMAL_VALUE) ) {
					((SampleByPercentileContext)_localctx).percentage = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(2307);
				match(PERCENTLIT);
				}
				break;
			case 2:
				_localctx = new SampleByRowsContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2308);
				expression();
				setState(2309);
				match(ROWS);
				}
				break;
			case 3:
				_localctx = new SampleByBucketContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(2311);
				((SampleByBucketContext)_localctx).sampleType = match(BUCKET);
				setState(2312);
				((SampleByBucketContext)_localctx).numerator = match(INTEGER_VALUE);
				setState(2313);
				match(OUT);
				setState(2314);
				match(OF);
				setState(2315);
				((SampleByBucketContext)_localctx).denominator = match(INTEGER_VALUE);
				setState(2324);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ON) {
					{
					setState(2316);
					match(ON);
					setState(2322);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,299,_ctx) ) {
					case 1:
						{
						setState(2317);
						identifier();
						}
						break;
					case 2:
						{
						setState(2318);
						qualifiedName();
						setState(2319);
						match(LEFT_PAREN);
						setState(2320);
						match(RIGHT_PAREN);
						}
						break;
					}
					}
				}

				}
				break;
			case 4:
				_localctx = new SampleByBytesContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(2326);
				((SampleByBytesContext)_localctx).bytes = expression();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class IdentifierListContext extends ParserRuleContext {
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public IdentifierSeqContext identifierSeq() {
			return getRuleContext(IdentifierSeqContext.class,0);
		}
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public IdentifierListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifierList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterIdentifierList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitIdentifierList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitIdentifierList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierListContext identifierList() throws RecognitionException {
		IdentifierListContext _localctx = new IdentifierListContext(_ctx, getState());
		enterRule(_localctx, 162, RULE_identifierList);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2329);
			match(LEFT_PAREN);
			setState(2330);
			identifierSeq();
			setState(2331);
			match(RIGHT_PAREN);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class IdentifierSeqContext extends ParserRuleContext {
		public ErrorCapturingIdentifierContext errorCapturingIdentifier;
		public List<ErrorCapturingIdentifierContext> ident = new ArrayList<ErrorCapturingIdentifierContext>();
		public List<ErrorCapturingIdentifierContext> errorCapturingIdentifier() {
			return getRuleContexts(ErrorCapturingIdentifierContext.class);
		}
		public ErrorCapturingIdentifierContext errorCapturingIdentifier(int i) {
			return getRuleContext(ErrorCapturingIdentifierContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public IdentifierSeqContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifierSeq; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterIdentifierSeq(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitIdentifierSeq(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitIdentifierSeq(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierSeqContext identifierSeq() throws RecognitionException {
		IdentifierSeqContext _localctx = new IdentifierSeqContext(_ctx, getState());
		enterRule(_localctx, 164, RULE_identifierSeq);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2333);
			((IdentifierSeqContext)_localctx).errorCapturingIdentifier = errorCapturingIdentifier();
			((IdentifierSeqContext)_localctx).ident.add(((IdentifierSeqContext)_localctx).errorCapturingIdentifier);
			setState(2338);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,302,_ctx);
			while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(2334);
					match(COMMA);
					setState(2335);
					((IdentifierSeqContext)_localctx).errorCapturingIdentifier = errorCapturingIdentifier();
					((IdentifierSeqContext)_localctx).ident.add(((IdentifierSeqContext)_localctx).errorCapturingIdentifier);
					}
					} 
				}
				setState(2340);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,302,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class OrderedIdentifierListContext extends ParserRuleContext {
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public List<OrderedIdentifierContext> orderedIdentifier() {
			return getRuleContexts(OrderedIdentifierContext.class);
		}
		public OrderedIdentifierContext orderedIdentifier(int i) {
			return getRuleContext(OrderedIdentifierContext.class,i);
		}
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public OrderedIdentifierListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_orderedIdentifierList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterOrderedIdentifierList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitOrderedIdentifierList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitOrderedIdentifierList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OrderedIdentifierListContext orderedIdentifierList() throws RecognitionException {
		OrderedIdentifierListContext _localctx = new OrderedIdentifierListContext(_ctx, getState());
		enterRule(_localctx, 166, RULE_orderedIdentifierList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2341);
			match(LEFT_PAREN);
			setState(2342);
			orderedIdentifier();
			setState(2347);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(2343);
				match(COMMA);
				setState(2344);
				orderedIdentifier();
				}
				}
				setState(2349);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(2350);
			match(RIGHT_PAREN);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class OrderedIdentifierContext extends ParserRuleContext {
		public ErrorCapturingIdentifierContext ident;
		public Token ordering;
		public ErrorCapturingIdentifierContext errorCapturingIdentifier() {
			return getRuleContext(ErrorCapturingIdentifierContext.class,0);
		}
		public TerminalNode ASC() { return getToken(SqlBaseParser.ASC, 0); }
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public OrderedIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_orderedIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterOrderedIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitOrderedIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitOrderedIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OrderedIdentifierContext orderedIdentifier() throws RecognitionException {
		OrderedIdentifierContext _localctx = new OrderedIdentifierContext(_ctx, getState());
		enterRule(_localctx, 168, RULE_orderedIdentifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2352);
			((OrderedIdentifierContext)_localctx).ident = errorCapturingIdentifier();
			setState(2354);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ASC || _la==DESC) {
				{
				setState(2353);
				((OrderedIdentifierContext)_localctx).ordering = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==ASC || _la==DESC) ) {
					((OrderedIdentifierContext)_localctx).ordering = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class IdentifierCommentListContext extends ParserRuleContext {
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public List<IdentifierCommentContext> identifierComment() {
			return getRuleContexts(IdentifierCommentContext.class);
		}
		public IdentifierCommentContext identifierComment(int i) {
			return getRuleContext(IdentifierCommentContext.class,i);
		}
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public IdentifierCommentListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifierCommentList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterIdentifierCommentList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitIdentifierCommentList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitIdentifierCommentList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierCommentListContext identifierCommentList() throws RecognitionException {
		IdentifierCommentListContext _localctx = new IdentifierCommentListContext(_ctx, getState());
		enterRule(_localctx, 170, RULE_identifierCommentList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2356);
			match(LEFT_PAREN);
			setState(2357);
			identifierComment();
			setState(2362);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(2358);
				match(COMMA);
				setState(2359);
				identifierComment();
				}
				}
				setState(2364);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(2365);
			match(RIGHT_PAREN);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class IdentifierCommentContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public CommentSpecContext commentSpec() {
			return getRuleContext(CommentSpecContext.class,0);
		}
		public IdentifierCommentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifierComment; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterIdentifierComment(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitIdentifierComment(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitIdentifierComment(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierCommentContext identifierComment() throws RecognitionException {
		IdentifierCommentContext _localctx = new IdentifierCommentContext(_ctx, getState());
		enterRule(_localctx, 172, RULE_identifierComment);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2367);
			identifier();
			setState(2369);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COMMENT) {
				{
				setState(2368);
				commentSpec();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class RelationPrimaryContext extends ParserRuleContext {
		public RelationPrimaryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relationPrimary; }
	 
		public RelationPrimaryContext() { }
		public void copyFrom(RelationPrimaryContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TableValuedFunctionContext extends RelationPrimaryContext {
		public FunctionTableContext functionTable() {
			return getRuleContext(FunctionTableContext.class,0);
		}
		public TableValuedFunctionContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterTableValuedFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitTableValuedFunction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitTableValuedFunction(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class InlineTableDefault2Context extends RelationPrimaryContext {
		public InlineTableContext inlineTable() {
			return getRuleContext(InlineTableContext.class,0);
		}
		public InlineTableDefault2Context(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterInlineTableDefault2(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitInlineTableDefault2(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitInlineTableDefault2(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AliasedRelationContext extends RelationPrimaryContext {
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public RelationContext relation() {
			return getRuleContext(RelationContext.class,0);
		}
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public TableAliasContext tableAlias() {
			return getRuleContext(TableAliasContext.class,0);
		}
		public SampleContext sample() {
			return getRuleContext(SampleContext.class,0);
		}
		public AliasedRelationContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterAliasedRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitAliasedRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitAliasedRelation(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AliasedQueryContext extends RelationPrimaryContext {
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public TableAliasContext tableAlias() {
			return getRuleContext(TableAliasContext.class,0);
		}
		public SampleContext sample() {
			return getRuleContext(SampleContext.class,0);
		}
		public AliasedQueryContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterAliasedQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitAliasedQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitAliasedQuery(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TableNameContext extends RelationPrimaryContext {
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TableAliasContext tableAlias() {
			return getRuleContext(TableAliasContext.class,0);
		}
		public TemporalClauseContext temporalClause() {
			return getRuleContext(TemporalClauseContext.class,0);
		}
		public SampleContext sample() {
			return getRuleContext(SampleContext.class,0);
		}
		public TableNameContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterTableName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitTableName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitTableName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RelationPrimaryContext relationPrimary() throws RecognitionException {
		RelationPrimaryContext _localctx = new RelationPrimaryContext(_ctx, getState());
		enterRule(_localctx, 174, RULE_relationPrimary);
		try {
			setState(2398);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,311,_ctx) ) {
			case 1:
				_localctx = new TableNameContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2371);
				multipartIdentifier();
				setState(2373);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,307,_ctx) ) {
				case 1:
					{
					setState(2372);
					temporalClause();
					}
					break;
				}
				setState(2376);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,308,_ctx) ) {
				case 1:
					{
					setState(2375);
					sample();
					}
					break;
				}
				setState(2378);
				tableAlias();
				}
				break;
			case 2:
				_localctx = new AliasedQueryContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2380);
				match(LEFT_PAREN);
				setState(2381);
				query();
				setState(2382);
				match(RIGHT_PAREN);
				setState(2384);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,309,_ctx) ) {
				case 1:
					{
					setState(2383);
					sample();
					}
					break;
				}
				setState(2386);
				tableAlias();
				}
				break;
			case 3:
				_localctx = new AliasedRelationContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(2388);
				match(LEFT_PAREN);
				setState(2389);
				relation();
				setState(2390);
				match(RIGHT_PAREN);
				setState(2392);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,310,_ctx) ) {
				case 1:
					{
					setState(2391);
					sample();
					}
					break;
				}
				setState(2394);
				tableAlias();
				}
				break;
			case 4:
				_localctx = new InlineTableDefault2Context(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(2396);
				inlineTable();
				}
				break;
			case 5:
				_localctx = new TableValuedFunctionContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(2397);
				functionTable();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class InlineTableContext extends ParserRuleContext {
		public TerminalNode VALUES() { return getToken(SqlBaseParser.VALUES, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TableAliasContext tableAlias() {
			return getRuleContext(TableAliasContext.class,0);
		}
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public InlineTableContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_inlineTable; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterInlineTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitInlineTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitInlineTable(this);
			else return visitor.visitChildren(this);
		}
	}

	public final InlineTableContext inlineTable() throws RecognitionException {
		InlineTableContext _localctx = new InlineTableContext(_ctx, getState());
		enterRule(_localctx, 176, RULE_inlineTable);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2400);
			match(VALUES);
			setState(2401);
			expression();
			setState(2406);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,312,_ctx);
			while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(2402);
					match(COMMA);
					setState(2403);
					expression();
					}
					} 
				}
				setState(2408);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,312,_ctx);
			}
			setState(2409);
			tableAlias();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class FunctionTableContext extends ParserRuleContext {
		public FunctionNameContext funcName;
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public TableAliasContext tableAlias() {
			return getRuleContext(TableAliasContext.class,0);
		}
		public FunctionNameContext functionName() {
			return getRuleContext(FunctionNameContext.class,0);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public FunctionTableContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_functionTable; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterFunctionTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitFunctionTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitFunctionTable(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FunctionTableContext functionTable() throws RecognitionException {
		FunctionTableContext _localctx = new FunctionTableContext(_ctx, getState());
		enterRule(_localctx, 178, RULE_functionTable);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2411);
			((FunctionTableContext)_localctx).funcName = functionName();
			setState(2412);
			match(LEFT_PAREN);
			setState(2421);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,314,_ctx) ) {
			case 1:
				{
				setState(2413);
				expression();
				setState(2418);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(2414);
					match(COMMA);
					setState(2415);
					expression();
					}
					}
					setState(2420);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			}
			setState(2423);
			match(RIGHT_PAREN);
			setState(2424);
			tableAlias();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TableAliasContext extends ParserRuleContext {
		public StrictIdentifierContext strictIdentifier() {
			return getRuleContext(StrictIdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public TableAliasContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableAlias; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterTableAlias(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitTableAlias(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitTableAlias(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TableAliasContext tableAlias() throws RecognitionException {
		TableAliasContext _localctx = new TableAliasContext(_ctx, getState());
		enterRule(_localctx, 180, RULE_tableAlias);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2433);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,317,_ctx) ) {
			case 1:
				{
				setState(2427);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,315,_ctx) ) {
				case 1:
					{
					setState(2426);
					match(AS);
					}
					break;
				}
				setState(2429);
				strictIdentifier();
				setState(2431);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,316,_ctx) ) {
				case 1:
					{
					setState(2430);
					identifierList();
					}
					break;
				}
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class RowFormatContext extends ParserRuleContext {
		public RowFormatContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rowFormat; }
	 
		public RowFormatContext() { }
		public void copyFrom(RowFormatContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RowFormatSerdeContext extends RowFormatContext {
		public Token name;
		public PropertyListContext props;
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public TerminalNode FORMAT() { return getToken(SqlBaseParser.FORMAT, 0); }
		public TerminalNode SERDE() { return getToken(SqlBaseParser.SERDE, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode SERDEPROPERTIES() { return getToken(SqlBaseParser.SERDEPROPERTIES, 0); }
		public PropertyListContext propertyList() {
			return getRuleContext(PropertyListContext.class,0);
		}
		public RowFormatSerdeContext(RowFormatContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterRowFormatSerde(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitRowFormatSerde(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitRowFormatSerde(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RowFormatDelimitedContext extends RowFormatContext {
		public Token fieldsTerminatedBy;
		public Token escapedBy;
		public Token collectionItemsTerminatedBy;
		public Token keysTerminatedBy;
		public Token linesSeparatedBy;
		public Token nullDefinedAs;
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public TerminalNode FORMAT() { return getToken(SqlBaseParser.FORMAT, 0); }
		public TerminalNode DELIMITED() { return getToken(SqlBaseParser.DELIMITED, 0); }
		public TerminalNode FIELDS() { return getToken(SqlBaseParser.FIELDS, 0); }
		public List<TerminalNode> TERMINATED() { return getTokens(SqlBaseParser.TERMINATED); }
		public TerminalNode TERMINATED(int i) {
			return getToken(SqlBaseParser.TERMINATED, i);
		}
		public List<TerminalNode> BY() { return getTokens(SqlBaseParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlBaseParser.BY, i);
		}
		public TerminalNode COLLECTION() { return getToken(SqlBaseParser.COLLECTION, 0); }
		public TerminalNode ITEMS() { return getToken(SqlBaseParser.ITEMS, 0); }
		public TerminalNode MAP() { return getToken(SqlBaseParser.MAP, 0); }
		public TerminalNode KEYS() { return getToken(SqlBaseParser.KEYS, 0); }
		public TerminalNode LINES() { return getToken(SqlBaseParser.LINES, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public TerminalNode DEFINED() { return getToken(SqlBaseParser.DEFINED, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public List<TerminalNode> STRING() { return getTokens(SqlBaseParser.STRING); }
		public TerminalNode STRING(int i) {
			return getToken(SqlBaseParser.STRING, i);
		}
		public TerminalNode ESCAPED() { return getToken(SqlBaseParser.ESCAPED, 0); }
		public RowFormatDelimitedContext(RowFormatContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterRowFormatDelimited(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitRowFormatDelimited(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitRowFormatDelimited(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RowFormatContext rowFormat() throws RecognitionException {
		RowFormatContext _localctx = new RowFormatContext(_ctx, getState());
		enterRule(_localctx, 182, RULE_rowFormat);
		try {
			setState(2484);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,325,_ctx) ) {
			case 1:
				_localctx = new RowFormatSerdeContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2435);
				match(ROW);
				setState(2436);
				match(FORMAT);
				setState(2437);
				match(SERDE);
				setState(2438);
				((RowFormatSerdeContext)_localctx).name = match(STRING);
				setState(2442);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,318,_ctx) ) {
				case 1:
					{
					setState(2439);
					match(WITH);
					setState(2440);
					match(SERDEPROPERTIES);
					setState(2441);
					((RowFormatSerdeContext)_localctx).props = propertyList();
					}
					break;
				}
				}
				break;
			case 2:
				_localctx = new RowFormatDelimitedContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2444);
				match(ROW);
				setState(2445);
				match(FORMAT);
				setState(2446);
				match(DELIMITED);
				setState(2456);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,320,_ctx) ) {
				case 1:
					{
					setState(2447);
					match(FIELDS);
					setState(2448);
					match(TERMINATED);
					setState(2449);
					match(BY);
					setState(2450);
					((RowFormatDelimitedContext)_localctx).fieldsTerminatedBy = match(STRING);
					setState(2454);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,319,_ctx) ) {
					case 1:
						{
						setState(2451);
						match(ESCAPED);
						setState(2452);
						match(BY);
						setState(2453);
						((RowFormatDelimitedContext)_localctx).escapedBy = match(STRING);
						}
						break;
					}
					}
					break;
				}
				setState(2463);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,321,_ctx) ) {
				case 1:
					{
					setState(2458);
					match(COLLECTION);
					setState(2459);
					match(ITEMS);
					setState(2460);
					match(TERMINATED);
					setState(2461);
					match(BY);
					setState(2462);
					((RowFormatDelimitedContext)_localctx).collectionItemsTerminatedBy = match(STRING);
					}
					break;
				}
				setState(2470);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,322,_ctx) ) {
				case 1:
					{
					setState(2465);
					match(MAP);
					setState(2466);
					match(KEYS);
					setState(2467);
					match(TERMINATED);
					setState(2468);
					match(BY);
					setState(2469);
					((RowFormatDelimitedContext)_localctx).keysTerminatedBy = match(STRING);
					}
					break;
				}
				setState(2476);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,323,_ctx) ) {
				case 1:
					{
					setState(2472);
					match(LINES);
					setState(2473);
					match(TERMINATED);
					setState(2474);
					match(BY);
					setState(2475);
					((RowFormatDelimitedContext)_localctx).linesSeparatedBy = match(STRING);
					}
					break;
				}
				setState(2482);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,324,_ctx) ) {
				case 1:
					{
					setState(2478);
					match(NULL);
					setState(2479);
					match(DEFINED);
					setState(2480);
					match(AS);
					setState(2481);
					((RowFormatDelimitedContext)_localctx).nullDefinedAs = match(STRING);
					}
					break;
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class MultipartIdentifierListContext extends ParserRuleContext {
		public List<MultipartIdentifierContext> multipartIdentifier() {
			return getRuleContexts(MultipartIdentifierContext.class);
		}
		public MultipartIdentifierContext multipartIdentifier(int i) {
			return getRuleContext(MultipartIdentifierContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public MultipartIdentifierListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_multipartIdentifierList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterMultipartIdentifierList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitMultipartIdentifierList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitMultipartIdentifierList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final MultipartIdentifierListContext multipartIdentifierList() throws RecognitionException {
		MultipartIdentifierListContext _localctx = new MultipartIdentifierListContext(_ctx, getState());
		enterRule(_localctx, 184, RULE_multipartIdentifierList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2486);
			multipartIdentifier();
			setState(2491);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(2487);
				match(COMMA);
				setState(2488);
				multipartIdentifier();
				}
				}
				setState(2493);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class MultipartIdentifierContext extends ParserRuleContext {
		public ErrorCapturingIdentifierContext errorCapturingIdentifier;
		public List<ErrorCapturingIdentifierContext> parts = new ArrayList<ErrorCapturingIdentifierContext>();
		public List<ErrorCapturingIdentifierContext> errorCapturingIdentifier() {
			return getRuleContexts(ErrorCapturingIdentifierContext.class);
		}
		public ErrorCapturingIdentifierContext errorCapturingIdentifier(int i) {
			return getRuleContext(ErrorCapturingIdentifierContext.class,i);
		}
		public List<TerminalNode> DOT() { return getTokens(SqlBaseParser.DOT); }
		public TerminalNode DOT(int i) {
			return getToken(SqlBaseParser.DOT, i);
		}
		public MultipartIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_multipartIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterMultipartIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitMultipartIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitMultipartIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final MultipartIdentifierContext multipartIdentifier() throws RecognitionException {
		MultipartIdentifierContext _localctx = new MultipartIdentifierContext(_ctx, getState());
		enterRule(_localctx, 186, RULE_multipartIdentifier);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2494);
			((MultipartIdentifierContext)_localctx).errorCapturingIdentifier = errorCapturingIdentifier();
			((MultipartIdentifierContext)_localctx).parts.add(((MultipartIdentifierContext)_localctx).errorCapturingIdentifier);
			setState(2499);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,327,_ctx);
			while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(2495);
					match(DOT);
					setState(2496);
					((MultipartIdentifierContext)_localctx).errorCapturingIdentifier = errorCapturingIdentifier();
					((MultipartIdentifierContext)_localctx).parts.add(((MultipartIdentifierContext)_localctx).errorCapturingIdentifier);
					}
					} 
				}
				setState(2501);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,327,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class MultipartIdentifierPropertyListContext extends ParserRuleContext {
		public List<MultipartIdentifierPropertyContext> multipartIdentifierProperty() {
			return getRuleContexts(MultipartIdentifierPropertyContext.class);
		}
		public MultipartIdentifierPropertyContext multipartIdentifierProperty(int i) {
			return getRuleContext(MultipartIdentifierPropertyContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public MultipartIdentifierPropertyListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_multipartIdentifierPropertyList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterMultipartIdentifierPropertyList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitMultipartIdentifierPropertyList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitMultipartIdentifierPropertyList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final MultipartIdentifierPropertyListContext multipartIdentifierPropertyList() throws RecognitionException {
		MultipartIdentifierPropertyListContext _localctx = new MultipartIdentifierPropertyListContext(_ctx, getState());
		enterRule(_localctx, 188, RULE_multipartIdentifierPropertyList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2502);
			multipartIdentifierProperty();
			setState(2507);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(2503);
				match(COMMA);
				setState(2504);
				multipartIdentifierProperty();
				}
				}
				setState(2509);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class MultipartIdentifierPropertyContext extends ParserRuleContext {
		public PropertyListContext options;
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode OPTIONS() { return getToken(SqlBaseParser.OPTIONS, 0); }
		public PropertyListContext propertyList() {
			return getRuleContext(PropertyListContext.class,0);
		}
		public MultipartIdentifierPropertyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_multipartIdentifierProperty; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterMultipartIdentifierProperty(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitMultipartIdentifierProperty(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitMultipartIdentifierProperty(this);
			else return visitor.visitChildren(this);
		}
	}

	public final MultipartIdentifierPropertyContext multipartIdentifierProperty() throws RecognitionException {
		MultipartIdentifierPropertyContext _localctx = new MultipartIdentifierPropertyContext(_ctx, getState());
		enterRule(_localctx, 190, RULE_multipartIdentifierProperty);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2510);
			multipartIdentifier();
			setState(2513);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==OPTIONS) {
				{
				setState(2511);
				match(OPTIONS);
				setState(2512);
				((MultipartIdentifierPropertyContext)_localctx).options = propertyList();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TableIdentifierContext extends ParserRuleContext {
		public ErrorCapturingIdentifierContext db;
		public ErrorCapturingIdentifierContext table;
		public List<ErrorCapturingIdentifierContext> errorCapturingIdentifier() {
			return getRuleContexts(ErrorCapturingIdentifierContext.class);
		}
		public ErrorCapturingIdentifierContext errorCapturingIdentifier(int i) {
			return getRuleContext(ErrorCapturingIdentifierContext.class,i);
		}
		public TerminalNode DOT() { return getToken(SqlBaseParser.DOT, 0); }
		public TableIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterTableIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitTableIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitTableIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TableIdentifierContext tableIdentifier() throws RecognitionException {
		TableIdentifierContext _localctx = new TableIdentifierContext(_ctx, getState());
		enterRule(_localctx, 192, RULE_tableIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2518);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,330,_ctx) ) {
			case 1:
				{
				setState(2515);
				((TableIdentifierContext)_localctx).db = errorCapturingIdentifier();
				setState(2516);
				match(DOT);
				}
				break;
			}
			setState(2520);
			((TableIdentifierContext)_localctx).table = errorCapturingIdentifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class FunctionIdentifierContext extends ParserRuleContext {
		public ErrorCapturingIdentifierContext db;
		public ErrorCapturingIdentifierContext function;
		public List<ErrorCapturingIdentifierContext> errorCapturingIdentifier() {
			return getRuleContexts(ErrorCapturingIdentifierContext.class);
		}
		public ErrorCapturingIdentifierContext errorCapturingIdentifier(int i) {
			return getRuleContext(ErrorCapturingIdentifierContext.class,i);
		}
		public TerminalNode DOT() { return getToken(SqlBaseParser.DOT, 0); }
		public FunctionIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_functionIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterFunctionIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitFunctionIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitFunctionIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FunctionIdentifierContext functionIdentifier() throws RecognitionException {
		FunctionIdentifierContext _localctx = new FunctionIdentifierContext(_ctx, getState());
		enterRule(_localctx, 194, RULE_functionIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2525);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,331,_ctx) ) {
			case 1:
				{
				setState(2522);
				((FunctionIdentifierContext)_localctx).db = errorCapturingIdentifier();
				setState(2523);
				match(DOT);
				}
				break;
			}
			setState(2527);
			((FunctionIdentifierContext)_localctx).function = errorCapturingIdentifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NamedExpressionContext extends ParserRuleContext {
		public ErrorCapturingIdentifierContext name;
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public ErrorCapturingIdentifierContext errorCapturingIdentifier() {
			return getRuleContext(ErrorCapturingIdentifierContext.class,0);
		}
		public NamedExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_namedExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterNamedExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitNamedExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitNamedExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NamedExpressionContext namedExpression() throws RecognitionException {
		NamedExpressionContext _localctx = new NamedExpressionContext(_ctx, getState());
		enterRule(_localctx, 196, RULE_namedExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2529);
			expression();
			setState(2537);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,334,_ctx) ) {
			case 1:
				{
				setState(2531);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,332,_ctx) ) {
				case 1:
					{
					setState(2530);
					match(AS);
					}
					break;
				}
				setState(2535);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,333,_ctx) ) {
				case 1:
					{
					setState(2533);
					((NamedExpressionContext)_localctx).name = errorCapturingIdentifier();
					}
					break;
				case 2:
					{
					setState(2534);
					identifierList();
					}
					break;
				}
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NamedExpressionSeqContext extends ParserRuleContext {
		public List<NamedExpressionContext> namedExpression() {
			return getRuleContexts(NamedExpressionContext.class);
		}
		public NamedExpressionContext namedExpression(int i) {
			return getRuleContext(NamedExpressionContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public NamedExpressionSeqContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_namedExpressionSeq; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterNamedExpressionSeq(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitNamedExpressionSeq(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitNamedExpressionSeq(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NamedExpressionSeqContext namedExpressionSeq() throws RecognitionException {
		NamedExpressionSeqContext _localctx = new NamedExpressionSeqContext(_ctx, getState());
		enterRule(_localctx, 198, RULE_namedExpressionSeq);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2539);
			namedExpression();
			setState(2544);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,335,_ctx);
			while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(2540);
					match(COMMA);
					setState(2541);
					namedExpression();
					}
					} 
				}
				setState(2546);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,335,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PartitionFieldListContext extends ParserRuleContext {
		public PartitionFieldContext partitionField;
		public List<PartitionFieldContext> fields = new ArrayList<PartitionFieldContext>();
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public List<PartitionFieldContext> partitionField() {
			return getRuleContexts(PartitionFieldContext.class);
		}
		public PartitionFieldContext partitionField(int i) {
			return getRuleContext(PartitionFieldContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public PartitionFieldListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_partitionFieldList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterPartitionFieldList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitPartitionFieldList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitPartitionFieldList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PartitionFieldListContext partitionFieldList() throws RecognitionException {
		PartitionFieldListContext _localctx = new PartitionFieldListContext(_ctx, getState());
		enterRule(_localctx, 200, RULE_partitionFieldList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2547);
			match(LEFT_PAREN);
			setState(2548);
			((PartitionFieldListContext)_localctx).partitionField = partitionField();
			((PartitionFieldListContext)_localctx).fields.add(((PartitionFieldListContext)_localctx).partitionField);
			setState(2553);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(2549);
				match(COMMA);
				setState(2550);
				((PartitionFieldListContext)_localctx).partitionField = partitionField();
				((PartitionFieldListContext)_localctx).fields.add(((PartitionFieldListContext)_localctx).partitionField);
				}
				}
				setState(2555);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(2556);
			match(RIGHT_PAREN);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PartitionFieldContext extends ParserRuleContext {
		public PartitionFieldContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_partitionField; }
	 
		public PartitionFieldContext() { }
		public void copyFrom(PartitionFieldContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class PartitionColumnContext extends PartitionFieldContext {
		public ColTypeContext colType() {
			return getRuleContext(ColTypeContext.class,0);
		}
		public PartitionColumnContext(PartitionFieldContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterPartitionColumn(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitPartitionColumn(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitPartitionColumn(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class PartitionTransformContext extends PartitionFieldContext {
		public TransformContext transform() {
			return getRuleContext(TransformContext.class,0);
		}
		public PartitionTransformContext(PartitionFieldContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterPartitionTransform(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitPartitionTransform(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitPartitionTransform(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PartitionFieldContext partitionField() throws RecognitionException {
		PartitionFieldContext _localctx = new PartitionFieldContext(_ctx, getState());
		enterRule(_localctx, 202, RULE_partitionField);
		try {
			setState(2560);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,337,_ctx) ) {
			case 1:
				_localctx = new PartitionTransformContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2558);
				transform();
				}
				break;
			case 2:
				_localctx = new PartitionColumnContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2559);
				colType();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TransformContext extends ParserRuleContext {
		public TransformContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_transform; }
	 
		public TransformContext() { }
		public void copyFrom(TransformContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class IdentityTransformContext extends TransformContext {
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public IdentityTransformContext(TransformContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterIdentityTransform(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitIdentityTransform(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitIdentityTransform(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ApplyTransformContext extends TransformContext {
		public IdentifierContext transformName;
		public TransformArgumentContext transformArgument;
		public List<TransformArgumentContext> argument = new ArrayList<TransformArgumentContext>();
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public List<TransformArgumentContext> transformArgument() {
			return getRuleContexts(TransformArgumentContext.class);
		}
		public TransformArgumentContext transformArgument(int i) {
			return getRuleContext(TransformArgumentContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public ApplyTransformContext(TransformContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterApplyTransform(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitApplyTransform(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitApplyTransform(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TransformContext transform() throws RecognitionException {
		TransformContext _localctx = new TransformContext(_ctx, getState());
		enterRule(_localctx, 204, RULE_transform);
		int _la;
		try {
			setState(2575);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,339,_ctx) ) {
			case 1:
				_localctx = new IdentityTransformContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2562);
				qualifiedName();
				}
				break;
			case 2:
				_localctx = new ApplyTransformContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2563);
				((ApplyTransformContext)_localctx).transformName = identifier();
				setState(2564);
				match(LEFT_PAREN);
				setState(2565);
				((ApplyTransformContext)_localctx).transformArgument = transformArgument();
				((ApplyTransformContext)_localctx).argument.add(((ApplyTransformContext)_localctx).transformArgument);
				setState(2570);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(2566);
					match(COMMA);
					setState(2567);
					((ApplyTransformContext)_localctx).transformArgument = transformArgument();
					((ApplyTransformContext)_localctx).argument.add(((ApplyTransformContext)_localctx).transformArgument);
					}
					}
					setState(2572);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(2573);
				match(RIGHT_PAREN);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TransformArgumentContext extends ParserRuleContext {
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public ConstantContext constant() {
			return getRuleContext(ConstantContext.class,0);
		}
		public TransformArgumentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_transformArgument; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterTransformArgument(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitTransformArgument(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitTransformArgument(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TransformArgumentContext transformArgument() throws RecognitionException {
		TransformArgumentContext _localctx = new TransformArgumentContext(_ctx, getState());
		enterRule(_localctx, 206, RULE_transformArgument);
		try {
			setState(2579);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,340,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2577);
				qualifiedName();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2578);
				constant();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ExpressionContext extends ParserRuleContext {
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public ExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExpressionContext expression() throws RecognitionException {
		ExpressionContext _localctx = new ExpressionContext(_ctx, getState());
		enterRule(_localctx, 208, RULE_expression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2581);
			booleanExpression(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ExpressionSeqContext extends ParserRuleContext {
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public ExpressionSeqContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expressionSeq; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterExpressionSeq(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitExpressionSeq(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitExpressionSeq(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExpressionSeqContext expressionSeq() throws RecognitionException {
		ExpressionSeqContext _localctx = new ExpressionSeqContext(_ctx, getState());
		enterRule(_localctx, 210, RULE_expressionSeq);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2583);
			expression();
			setState(2588);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(2584);
				match(COMMA);
				setState(2585);
				expression();
				}
				}
				setState(2590);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class BooleanExpressionContext extends ParserRuleContext {
		public BooleanExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_booleanExpression; }
	 
		public BooleanExpressionContext() { }
		public void copyFrom(BooleanExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LogicalNotContext extends BooleanExpressionContext {
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public LogicalNotContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterLogicalNot(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitLogicalNot(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitLogicalNot(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class PredicatedContext extends BooleanExpressionContext {
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public PredicateContext predicate() {
			return getRuleContext(PredicateContext.class,0);
		}
		public PredicatedContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterPredicated(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitPredicated(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitPredicated(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ExistsContext extends BooleanExpressionContext {
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public ExistsContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterExists(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitExists(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitExists(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LogicalBinaryContext extends BooleanExpressionContext {
		public BooleanExpressionContext left;
		public Token operator;
		public BooleanExpressionContext right;
		public List<BooleanExpressionContext> booleanExpression() {
			return getRuleContexts(BooleanExpressionContext.class);
		}
		public BooleanExpressionContext booleanExpression(int i) {
			return getRuleContext(BooleanExpressionContext.class,i);
		}
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public LogicalBinaryContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterLogicalBinary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitLogicalBinary(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitLogicalBinary(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BooleanExpressionContext booleanExpression() throws RecognitionException {
		return booleanExpression(0);
	}

	private BooleanExpressionContext booleanExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		BooleanExpressionContext _localctx = new BooleanExpressionContext(_ctx, _parentState);
		BooleanExpressionContext _prevctx = _localctx;
		int _startState = 212;
		enterRecursionRule(_localctx, 212, RULE_booleanExpression, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2603);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,343,_ctx) ) {
			case 1:
				{
				_localctx = new LogicalNotContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(2592);
				match(NOT);
				setState(2593);
				booleanExpression(5);
				}
				break;
			case 2:
				{
				_localctx = new ExistsContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2594);
				match(EXISTS);
				setState(2595);
				match(LEFT_PAREN);
				setState(2596);
				query();
				setState(2597);
				match(RIGHT_PAREN);
				}
				break;
			case 3:
				{
				_localctx = new PredicatedContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2599);
				valueExpression(0);
				setState(2601);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,342,_ctx) ) {
				case 1:
					{
					setState(2600);
					predicate();
					}
					break;
				}
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(2613);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,345,_ctx);
			while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(2611);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,344,_ctx) ) {
					case 1:
						{
						_localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
						((LogicalBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
						setState(2605);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(2606);
						((LogicalBinaryContext)_localctx).operator = match(AND);
						setState(2607);
						((LogicalBinaryContext)_localctx).right = booleanExpression(3);
						}
						break;
					case 2:
						{
						_localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
						((LogicalBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
						setState(2608);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(2609);
						((LogicalBinaryContext)_localctx).operator = match(OR);
						setState(2610);
						((LogicalBinaryContext)_localctx).right = booleanExpression(2);
						}
						break;
					}
					} 
				}
				setState(2615);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,345,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PredicateContext extends ParserRuleContext {
		public Token kind;
		public ValueExpressionContext lower;
		public ValueExpressionContext upper;
		public ValueExpressionContext pattern;
		public Token quantifier;
		public Token escapeChar;
		public ValueExpressionContext right;
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public TerminalNode BETWEEN() { return getToken(SqlBaseParser.BETWEEN, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode RLIKE() { return getToken(SqlBaseParser.RLIKE, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public TerminalNode ILIKE() { return getToken(SqlBaseParser.ILIKE, 0); }
		public TerminalNode ANY() { return getToken(SqlBaseParser.ANY, 0); }
		public TerminalNode SOME() { return getToken(SqlBaseParser.SOME, 0); }
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public TerminalNode ESCAPE() { return getToken(SqlBaseParser.ESCAPE, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode IS() { return getToken(SqlBaseParser.IS, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public TerminalNode TRUE() { return getToken(SqlBaseParser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(SqlBaseParser.FALSE, 0); }
		public TerminalNode UNKNOWN() { return getToken(SqlBaseParser.UNKNOWN, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode DISTINCT() { return getToken(SqlBaseParser.DISTINCT, 0); }
		public PredicateContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_predicate; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterPredicate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitPredicate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitPredicate(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PredicateContext predicate() throws RecognitionException {
		PredicateContext _localctx = new PredicateContext(_ctx, getState());
		enterRule(_localctx, 214, RULE_predicate);
		int _la;
		try {
			setState(2698);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,359,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2617);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(2616);
					match(NOT);
					}
				}

				setState(2619);
				((PredicateContext)_localctx).kind = match(BETWEEN);
				setState(2620);
				((PredicateContext)_localctx).lower = valueExpression(0);
				setState(2621);
				match(AND);
				setState(2622);
				((PredicateContext)_localctx).upper = valueExpression(0);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2625);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(2624);
					match(NOT);
					}
				}

				setState(2627);
				((PredicateContext)_localctx).kind = match(IN);
				setState(2628);
				match(LEFT_PAREN);
				setState(2629);
				expression();
				setState(2634);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(2630);
					match(COMMA);
					setState(2631);
					expression();
					}
					}
					setState(2636);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(2637);
				match(RIGHT_PAREN);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(2640);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(2639);
					match(NOT);
					}
				}

				setState(2642);
				((PredicateContext)_localctx).kind = match(IN);
				setState(2643);
				match(LEFT_PAREN);
				setState(2644);
				query();
				setState(2645);
				match(RIGHT_PAREN);
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(2648);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(2647);
					match(NOT);
					}
				}

				setState(2650);
				((PredicateContext)_localctx).kind = match(RLIKE);
				setState(2651);
				((PredicateContext)_localctx).pattern = valueExpression(0);
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(2653);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(2652);
					match(NOT);
					}
				}

				setState(2655);
				((PredicateContext)_localctx).kind = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==LIKE || _la==ILIKE) ) {
					((PredicateContext)_localctx).kind = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(2656);
				((PredicateContext)_localctx).quantifier = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==ALL || _la==ANY || _la==SOME) ) {
					((PredicateContext)_localctx).quantifier = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(2670);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,353,_ctx) ) {
				case 1:
					{
					setState(2657);
					match(LEFT_PAREN);
					setState(2658);
					match(RIGHT_PAREN);
					}
					break;
				case 2:
					{
					setState(2659);
					match(LEFT_PAREN);
					setState(2660);
					expression();
					setState(2665);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==COMMA) {
						{
						{
						setState(2661);
						match(COMMA);
						setState(2662);
						expression();
						}
						}
						setState(2667);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(2668);
					match(RIGHT_PAREN);
					}
					break;
				}
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(2673);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(2672);
					match(NOT);
					}
				}

				setState(2675);
				((PredicateContext)_localctx).kind = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==LIKE || _la==ILIKE) ) {
					((PredicateContext)_localctx).kind = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(2676);
				((PredicateContext)_localctx).pattern = valueExpression(0);
				setState(2679);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,355,_ctx) ) {
				case 1:
					{
					setState(2677);
					match(ESCAPE);
					setState(2678);
					((PredicateContext)_localctx).escapeChar = match(STRING);
					}
					break;
				}
				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(2681);
				match(IS);
				setState(2683);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(2682);
					match(NOT);
					}
				}

				setState(2685);
				((PredicateContext)_localctx).kind = match(NULL);
				}
				break;
			case 8:
				enterOuterAlt(_localctx, 8);
				{
				setState(2686);
				match(IS);
				setState(2688);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(2687);
					match(NOT);
					}
				}

				setState(2690);
				((PredicateContext)_localctx).kind = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==FALSE || _la==TRUE || _la==UNKNOWN) ) {
					((PredicateContext)_localctx).kind = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			case 9:
				enterOuterAlt(_localctx, 9);
				{
				setState(2691);
				match(IS);
				setState(2693);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(2692);
					match(NOT);
					}
				}

				setState(2695);
				((PredicateContext)_localctx).kind = match(DISTINCT);
				setState(2696);
				match(FROM);
				setState(2697);
				((PredicateContext)_localctx).right = valueExpression(0);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ValueExpressionContext extends ParserRuleContext {
		public ValueExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_valueExpression; }
	 
		public ValueExpressionContext() { }
		public void copyFrom(ValueExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ValueExpressionDefaultContext extends ValueExpressionContext {
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public ValueExpressionDefaultContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterValueExpressionDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitValueExpressionDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitValueExpressionDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ComparisonContext extends ValueExpressionContext {
		public ValueExpressionContext left;
		public ValueExpressionContext right;
		public ComparisonOperatorContext comparisonOperator() {
			return getRuleContext(ComparisonOperatorContext.class,0);
		}
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public ComparisonContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterComparison(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitComparison(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitComparison(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ArithmeticBinaryContext extends ValueExpressionContext {
		public ValueExpressionContext left;
		public Token operator;
		public ValueExpressionContext right;
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
		public TerminalNode SLASH() { return getToken(SqlBaseParser.SLASH, 0); }
		public TerminalNode PERCENT() { return getToken(SqlBaseParser.PERCENT, 0); }
		public TerminalNode DIV() { return getToken(SqlBaseParser.DIV, 0); }
		public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public TerminalNode CONCAT_PIPE() { return getToken(SqlBaseParser.CONCAT_PIPE, 0); }
		public TerminalNode AMPERSAND() { return getToken(SqlBaseParser.AMPERSAND, 0); }
		public TerminalNode HAT() { return getToken(SqlBaseParser.HAT, 0); }
		public TerminalNode PIPE() { return getToken(SqlBaseParser.PIPE, 0); }
		public ArithmeticBinaryContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterArithmeticBinary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitArithmeticBinary(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitArithmeticBinary(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ArithmeticUnaryContext extends ValueExpressionContext {
		public Token operator;
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
		public TerminalNode TILDE() { return getToken(SqlBaseParser.TILDE, 0); }
		public ArithmeticUnaryContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterArithmeticUnary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitArithmeticUnary(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitArithmeticUnary(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ValueExpressionContext valueExpression() throws RecognitionException {
		return valueExpression(0);
	}

	private ValueExpressionContext valueExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		ValueExpressionContext _localctx = new ValueExpressionContext(_ctx, _parentState);
		ValueExpressionContext _prevctx = _localctx;
		int _startState = 216;
		enterRecursionRule(_localctx, 216, RULE_valueExpression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2704);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,360,_ctx) ) {
			case 1:
				{
				_localctx = new ValueExpressionDefaultContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(2701);
				primaryExpression(0);
				}
				break;
			case 2:
				{
				_localctx = new ArithmeticUnaryContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2702);
				((ArithmeticUnaryContext)_localctx).operator = _input.LT(1);
				_la = _input.LA(1);
				if ( !(((((_la - 295)) & ~0x3f) == 0 && ((1L << (_la - 295)) & 35L) != 0)) ) {
					((ArithmeticUnaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(2703);
				valueExpression(7);
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(2727);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,362,_ctx);
			while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(2725);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,361,_ctx) ) {
					case 1:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(2706);
						if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
						setState(2707);
						((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
						_la = _input.LA(1);
						if ( !(_la==DIV || ((((_la - 297)) & ~0x3f) == 0 && ((1L << (_la - 297)) & 7L) != 0)) ) {
							((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(2708);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(7);
						}
						break;
					case 2:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(2709);
						if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
						setState(2710);
						((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
						_la = _input.LA(1);
						if ( !(((((_la - 295)) & ~0x3f) == 0 && ((1L << (_la - 295)) & 259L) != 0)) ) {
							((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(2711);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(6);
						}
						break;
					case 3:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(2712);
						if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
						setState(2713);
						((ArithmeticBinaryContext)_localctx).operator = match(AMPERSAND);
						setState(2714);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(5);
						}
						break;
					case 4:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(2715);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(2716);
						((ArithmeticBinaryContext)_localctx).operator = match(HAT);
						setState(2717);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(4);
						}
						break;
					case 5:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(2718);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(2719);
						((ArithmeticBinaryContext)_localctx).operator = match(PIPE);
						setState(2720);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(3);
						}
						break;
					case 6:
						{
						_localctx = new ComparisonContext(new ValueExpressionContext(_parentctx, _parentState));
						((ComparisonContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(2721);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(2722);
						comparisonOperator();
						setState(2723);
						((ComparisonContext)_localctx).right = valueExpression(2);
						}
						break;
					}
					} 
				}
				setState(2729);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,362,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class DatetimeUnitContext extends ParserRuleContext {
		public TerminalNode YEAR() { return getToken(SqlBaseParser.YEAR, 0); }
		public TerminalNode QUARTER() { return getToken(SqlBaseParser.QUARTER, 0); }
		public TerminalNode MONTH() { return getToken(SqlBaseParser.MONTH, 0); }
		public TerminalNode WEEK() { return getToken(SqlBaseParser.WEEK, 0); }
		public TerminalNode DAY() { return getToken(SqlBaseParser.DAY, 0); }
		public TerminalNode DAYOFYEAR() { return getToken(SqlBaseParser.DAYOFYEAR, 0); }
		public TerminalNode HOUR() { return getToken(SqlBaseParser.HOUR, 0); }
		public TerminalNode MINUTE() { return getToken(SqlBaseParser.MINUTE, 0); }
		public TerminalNode SECOND() { return getToken(SqlBaseParser.SECOND, 0); }
		public TerminalNode MILLISECOND() { return getToken(SqlBaseParser.MILLISECOND, 0); }
		public TerminalNode MICROSECOND() { return getToken(SqlBaseParser.MICROSECOND, 0); }
		public DatetimeUnitContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_datetimeUnit; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterDatetimeUnit(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitDatetimeUnit(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitDatetimeUnit(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DatetimeUnitContext datetimeUnit() throws RecognitionException {
		DatetimeUnitContext _localctx = new DatetimeUnitContext(_ctx, getState());
		enterRule(_localctx, 218, RULE_datetimeUnit);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2730);
			_la = _input.LA(1);
			if ( !(_la==DAY || _la==DAYOFYEAR || ((((_la - 111)) & ~0x3f) == 0 && ((1L << (_la - 111)) & 4123168604161L) != 0) || _la==QUARTER || _la==SECOND || _la==WEEK || _la==YEAR) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PrimaryExpressionContext extends ParserRuleContext {
		public PrimaryExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_primaryExpression; }
	 
		public PrimaryExpressionContext() { }
		public void copyFrom(PrimaryExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class StructContext extends PrimaryExpressionContext {
		public NamedExpressionContext namedExpression;
		public List<NamedExpressionContext> argument = new ArrayList<NamedExpressionContext>();
		public TerminalNode STRUCT() { return getToken(SqlBaseParser.STRUCT, 0); }
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public List<NamedExpressionContext> namedExpression() {
			return getRuleContexts(NamedExpressionContext.class);
		}
		public NamedExpressionContext namedExpression(int i) {
			return getRuleContext(NamedExpressionContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public StructContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterStruct(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitStruct(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitStruct(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DereferenceContext extends PrimaryExpressionContext {
		public PrimaryExpressionContext base;
		public IdentifierContext fieldName;
		public TerminalNode DOT() { return getToken(SqlBaseParser.DOT, 0); }
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DereferenceContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterDereference(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitDereference(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitDereference(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TimestampaddContext extends PrimaryExpressionContext {
		public Token name;
		public DatetimeUnitContext unit;
		public ValueExpressionContext unitsAmount;
		public ValueExpressionContext timestamp;
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public DatetimeUnitContext datetimeUnit() {
			return getRuleContext(DatetimeUnitContext.class,0);
		}
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode TIMESTAMPADD() { return getToken(SqlBaseParser.TIMESTAMPADD, 0); }
		public TerminalNode DATEADD() { return getToken(SqlBaseParser.DATEADD, 0); }
		public TimestampaddContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterTimestampadd(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitTimestampadd(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitTimestampadd(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SubstringContext extends PrimaryExpressionContext {
		public ValueExpressionContext str;
		public ValueExpressionContext pos;
		public ValueExpressionContext len;
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public TerminalNode SUBSTR() { return getToken(SqlBaseParser.SUBSTR, 0); }
		public TerminalNode SUBSTRING() { return getToken(SqlBaseParser.SUBSTRING, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public TerminalNode FOR() { return getToken(SqlBaseParser.FOR, 0); }
		public SubstringContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterSubstring(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitSubstring(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitSubstring(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CastContext extends PrimaryExpressionContext {
		public Token name;
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public DataTypeContext dataType() {
			return getRuleContext(DataTypeContext.class,0);
		}
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public TerminalNode CAST() { return getToken(SqlBaseParser.CAST, 0); }
		public TerminalNode TRY_CAST() { return getToken(SqlBaseParser.TRY_CAST, 0); }
		public CastContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterCast(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitCast(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitCast(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LambdaContext extends PrimaryExpressionContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public TerminalNode ARROW() { return getToken(SqlBaseParser.ARROW, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public LambdaContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterLambda(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitLambda(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitLambda(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ParenthesizedExpressionContext extends PrimaryExpressionContext {
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public ParenthesizedExpressionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterParenthesizedExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitParenthesizedExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitParenthesizedExpression(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TrimContext extends PrimaryExpressionContext {
		public Token trimOption;
		public ValueExpressionContext trimStr;
		public ValueExpressionContext srcStr;
		public TerminalNode TRIM() { return getToken(SqlBaseParser.TRIM, 0); }
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode BOTH() { return getToken(SqlBaseParser.BOTH, 0); }
		public TerminalNode LEADING() { return getToken(SqlBaseParser.LEADING, 0); }
		public TerminalNode TRAILING() { return getToken(SqlBaseParser.TRAILING, 0); }
		public TrimContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterTrim(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitTrim(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitTrim(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SimpleCaseContext extends PrimaryExpressionContext {
		public ExpressionContext value;
		public ExpressionContext elseExpression;
		public TerminalNode CASE() { return getToken(SqlBaseParser.CASE, 0); }
		public TerminalNode END() { return getToken(SqlBaseParser.END, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public List<WhenClauseContext> whenClause() {
			return getRuleContexts(WhenClauseContext.class);
		}
		public WhenClauseContext whenClause(int i) {
			return getRuleContext(WhenClauseContext.class,i);
		}
		public TerminalNode ELSE() { return getToken(SqlBaseParser.ELSE, 0); }
		public SimpleCaseContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterSimpleCase(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitSimpleCase(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitSimpleCase(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class CurrentLikeContext extends PrimaryExpressionContext {
		public Token name;
		public TerminalNode CURRENT_DATE() { return getToken(SqlBaseParser.CURRENT_DATE, 0); }
		public TerminalNode CURRENT_TIMESTAMP() { return getToken(SqlBaseParser.CURRENT_TIMESTAMP, 0); }
		public TerminalNode CURRENT_USER() { return getToken(SqlBaseParser.CURRENT_USER, 0); }
		public CurrentLikeContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterCurrentLike(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitCurrentLike(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitCurrentLike(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ColumnReferenceContext extends PrimaryExpressionContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ColumnReferenceContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterColumnReference(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitColumnReference(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitColumnReference(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RowConstructorContext extends PrimaryExpressionContext {
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public List<NamedExpressionContext> namedExpression() {
			return getRuleContexts(NamedExpressionContext.class);
		}
		public NamedExpressionContext namedExpression(int i) {
			return getRuleContext(NamedExpressionContext.class,i);
		}
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public RowConstructorContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterRowConstructor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitRowConstructor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitRowConstructor(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LastContext extends PrimaryExpressionContext {
		public TerminalNode LAST() { return getToken(SqlBaseParser.LAST, 0); }
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public TerminalNode IGNORE() { return getToken(SqlBaseParser.IGNORE, 0); }
		public TerminalNode NULLS() { return getToken(SqlBaseParser.NULLS, 0); }
		public LastContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterLast(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitLast(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitLast(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class StarContext extends PrimaryExpressionContext {
		public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode DOT() { return getToken(SqlBaseParser.DOT, 0); }
		public StarContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterStar(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitStar(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitStar(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class OverlayContext extends PrimaryExpressionContext {
		public ValueExpressionContext input;
		public ValueExpressionContext replace;
		public ValueExpressionContext position;
		public ValueExpressionContext length;
		public TerminalNode OVERLAY() { return getToken(SqlBaseParser.OVERLAY, 0); }
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public TerminalNode PLACING() { return getToken(SqlBaseParser.PLACING, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode FOR() { return getToken(SqlBaseParser.FOR, 0); }
		public OverlayContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterOverlay(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitOverlay(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitOverlay(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SubscriptContext extends PrimaryExpressionContext {
		public PrimaryExpressionContext value;
		public ValueExpressionContext index;
		public TerminalNode LEFT_BRACKET() { return getToken(SqlBaseParser.LEFT_BRACKET, 0); }
		public TerminalNode RIGHT_BRACKET() { return getToken(SqlBaseParser.RIGHT_BRACKET, 0); }
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public SubscriptContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterSubscript(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitSubscript(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitSubscript(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TimestampdiffContext extends PrimaryExpressionContext {
		public Token name;
		public DatetimeUnitContext unit;
		public ValueExpressionContext startTimestamp;
		public ValueExpressionContext endTimestamp;
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public DatetimeUnitContext datetimeUnit() {
			return getRuleContext(DatetimeUnitContext.class,0);
		}
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode TIMESTAMPDIFF() { return getToken(SqlBaseParser.TIMESTAMPDIFF, 0); }
		public TerminalNode DATEDIFF() { return getToken(SqlBaseParser.DATEDIFF, 0); }
		public TimestampdiffContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterTimestampdiff(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitTimestampdiff(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitTimestampdiff(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SubqueryExpressionContext extends PrimaryExpressionContext {
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public SubqueryExpressionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterSubqueryExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitSubqueryExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitSubqueryExpression(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ConstantDefaultContext extends PrimaryExpressionContext {
		public ConstantContext constant() {
			return getRuleContext(ConstantContext.class,0);
		}
		public ConstantDefaultContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterConstantDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitConstantDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitConstantDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ExtractContext extends PrimaryExpressionContext {
		public IdentifierContext field;
		public ValueExpressionContext source;
		public TerminalNode EXTRACT() { return getToken(SqlBaseParser.EXTRACT, 0); }
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public ExtractContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterExtract(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitExtract(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitExtract(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class PercentileContext extends PrimaryExpressionContext {
		public Token name;
		public ValueExpressionContext percentage;
		public List<TerminalNode> LEFT_PAREN() { return getTokens(SqlBaseParser.LEFT_PAREN); }
		public TerminalNode LEFT_PAREN(int i) {
			return getToken(SqlBaseParser.LEFT_PAREN, i);
		}
		public List<TerminalNode> RIGHT_PAREN() { return getTokens(SqlBaseParser.RIGHT_PAREN); }
		public TerminalNode RIGHT_PAREN(int i) {
			return getToken(SqlBaseParser.RIGHT_PAREN, i);
		}
		public TerminalNode WITHIN() { return getToken(SqlBaseParser.WITHIN, 0); }
		public TerminalNode GROUP() { return getToken(SqlBaseParser.GROUP, 0); }
		public TerminalNode ORDER() { return getToken(SqlBaseParser.ORDER, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public SortItemContext sortItem() {
			return getRuleContext(SortItemContext.class,0);
		}
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public TerminalNode PERCENTILE_CONT() { return getToken(SqlBaseParser.PERCENTILE_CONT, 0); }
		public TerminalNode PERCENTILE_DISC() { return getToken(SqlBaseParser.PERCENTILE_DISC, 0); }
		public TerminalNode OVER() { return getToken(SqlBaseParser.OVER, 0); }
		public WindowSpecContext windowSpec() {
			return getRuleContext(WindowSpecContext.class,0);
		}
		public PercentileContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterPercentile(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitPercentile(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitPercentile(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class FunctionCallContext extends PrimaryExpressionContext {
		public ExpressionContext expression;
		public List<ExpressionContext> argument = new ArrayList<ExpressionContext>();
		public BooleanExpressionContext where;
		public Token nullsOption;
		public FunctionNameContext functionName() {
			return getRuleContext(FunctionNameContext.class,0);
		}
		public List<TerminalNode> LEFT_PAREN() { return getTokens(SqlBaseParser.LEFT_PAREN); }
		public TerminalNode LEFT_PAREN(int i) {
			return getToken(SqlBaseParser.LEFT_PAREN, i);
		}
		public List<TerminalNode> RIGHT_PAREN() { return getTokens(SqlBaseParser.RIGHT_PAREN); }
		public TerminalNode RIGHT_PAREN(int i) {
			return getToken(SqlBaseParser.RIGHT_PAREN, i);
		}
		public TerminalNode FILTER() { return getToken(SqlBaseParser.FILTER, 0); }
		public TerminalNode WHERE() { return getToken(SqlBaseParser.WHERE, 0); }
		public TerminalNode NULLS() { return getToken(SqlBaseParser.NULLS, 0); }
		public TerminalNode OVER() { return getToken(SqlBaseParser.OVER, 0); }
		public WindowSpecContext windowSpec() {
			return getRuleContext(WindowSpecContext.class,0);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public TerminalNode IGNORE() { return getToken(SqlBaseParser.IGNORE, 0); }
		public TerminalNode RESPECT() { return getToken(SqlBaseParser.RESPECT, 0); }
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public FunctionCallContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterFunctionCall(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitFunctionCall(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitFunctionCall(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SearchedCaseContext extends PrimaryExpressionContext {
		public ExpressionContext elseExpression;
		public TerminalNode CASE() { return getToken(SqlBaseParser.CASE, 0); }
		public TerminalNode END() { return getToken(SqlBaseParser.END, 0); }
		public List<WhenClauseContext> whenClause() {
			return getRuleContexts(WhenClauseContext.class);
		}
		public WhenClauseContext whenClause(int i) {
			return getRuleContext(WhenClauseContext.class,i);
		}
		public TerminalNode ELSE() { return getToken(SqlBaseParser.ELSE, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public SearchedCaseContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterSearchedCase(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitSearchedCase(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitSearchedCase(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class PositionContext extends PrimaryExpressionContext {
		public ValueExpressionContext substr;
		public ValueExpressionContext str;
		public TerminalNode POSITION() { return getToken(SqlBaseParser.POSITION, 0); }
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public PositionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterPosition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitPosition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitPosition(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class FirstContext extends PrimaryExpressionContext {
		public TerminalNode FIRST() { return getToken(SqlBaseParser.FIRST, 0); }
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public TerminalNode IGNORE() { return getToken(SqlBaseParser.IGNORE, 0); }
		public TerminalNode NULLS() { return getToken(SqlBaseParser.NULLS, 0); }
		public FirstContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterFirst(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitFirst(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitFirst(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PrimaryExpressionContext primaryExpression() throws RecognitionException {
		return primaryExpression(0);
	}

	private PrimaryExpressionContext primaryExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		PrimaryExpressionContext _localctx = new PrimaryExpressionContext(_ctx, _parentState);
		PrimaryExpressionContext _prevctx = _localctx;
		int _startState = 220;
		enterRecursionRule(_localctx, 220, RULE_primaryExpression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2953);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,384,_ctx) ) {
			case 1:
				{
				_localctx = new CurrentLikeContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(2733);
				((CurrentLikeContext)_localctx).name = _input.LT(1);
				_la = _input.LA(1);
				if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 468374361246531584L) != 0)) ) {
					((CurrentLikeContext)_localctx).name = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			case 2:
				{
				_localctx = new TimestampaddContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2734);
				((TimestampaddContext)_localctx).name = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==DATEADD || _la==TIMESTAMPADD) ) {
					((TimestampaddContext)_localctx).name = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(2735);
				match(LEFT_PAREN);
				setState(2736);
				((TimestampaddContext)_localctx).unit = datetimeUnit();
				setState(2737);
				match(COMMA);
				setState(2738);
				((TimestampaddContext)_localctx).unitsAmount = valueExpression(0);
				setState(2739);
				match(COMMA);
				setState(2740);
				((TimestampaddContext)_localctx).timestamp = valueExpression(0);
				setState(2741);
				match(RIGHT_PAREN);
				}
				break;
			case 3:
				{
				_localctx = new TimestampdiffContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2743);
				((TimestampdiffContext)_localctx).name = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==DATEDIFF || _la==TIMESTAMPDIFF) ) {
					((TimestampdiffContext)_localctx).name = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(2744);
				match(LEFT_PAREN);
				setState(2745);
				((TimestampdiffContext)_localctx).unit = datetimeUnit();
				setState(2746);
				match(COMMA);
				setState(2747);
				((TimestampdiffContext)_localctx).startTimestamp = valueExpression(0);
				setState(2748);
				match(COMMA);
				setState(2749);
				((TimestampdiffContext)_localctx).endTimestamp = valueExpression(0);
				setState(2750);
				match(RIGHT_PAREN);
				}
				break;
			case 4:
				{
				_localctx = new SearchedCaseContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2752);
				match(CASE);
				setState(2754); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(2753);
					whenClause();
					}
					}
					setState(2756); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==WHEN );
				setState(2760);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ELSE) {
					{
					setState(2758);
					match(ELSE);
					setState(2759);
					((SearchedCaseContext)_localctx).elseExpression = expression();
					}
				}

				setState(2762);
				match(END);
				}
				break;
			case 5:
				{
				_localctx = new SimpleCaseContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2764);
				match(CASE);
				setState(2765);
				((SimpleCaseContext)_localctx).value = expression();
				setState(2767); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(2766);
					whenClause();
					}
					}
					setState(2769); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==WHEN );
				setState(2773);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ELSE) {
					{
					setState(2771);
					match(ELSE);
					setState(2772);
					((SimpleCaseContext)_localctx).elseExpression = expression();
					}
				}

				setState(2775);
				match(END);
				}
				break;
			case 6:
				{
				_localctx = new CastContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2777);
				((CastContext)_localctx).name = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==CAST || _la==TRY_CAST) ) {
					((CastContext)_localctx).name = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(2778);
				match(LEFT_PAREN);
				setState(2779);
				expression();
				setState(2780);
				match(AS);
				setState(2781);
				dataType();
				setState(2782);
				match(RIGHT_PAREN);
				}
				break;
			case 7:
				{
				_localctx = new StructContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2784);
				match(STRUCT);
				setState(2785);
				match(LEFT_PAREN);
				setState(2794);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,368,_ctx) ) {
				case 1:
					{
					setState(2786);
					((StructContext)_localctx).namedExpression = namedExpression();
					((StructContext)_localctx).argument.add(((StructContext)_localctx).namedExpression);
					setState(2791);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==COMMA) {
						{
						{
						setState(2787);
						match(COMMA);
						setState(2788);
						((StructContext)_localctx).namedExpression = namedExpression();
						((StructContext)_localctx).argument.add(((StructContext)_localctx).namedExpression);
						}
						}
						setState(2793);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
					break;
				}
				setState(2796);
				match(RIGHT_PAREN);
				}
				break;
			case 8:
				{
				_localctx = new FirstContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2797);
				match(FIRST);
				setState(2798);
				match(LEFT_PAREN);
				setState(2799);
				expression();
				setState(2802);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IGNORE) {
					{
					setState(2800);
					match(IGNORE);
					setState(2801);
					match(NULLS);
					}
				}

				setState(2804);
				match(RIGHT_PAREN);
				}
				break;
			case 9:
				{
				_localctx = new LastContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2806);
				match(LAST);
				setState(2807);
				match(LEFT_PAREN);
				setState(2808);
				expression();
				setState(2811);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IGNORE) {
					{
					setState(2809);
					match(IGNORE);
					setState(2810);
					match(NULLS);
					}
				}

				setState(2813);
				match(RIGHT_PAREN);
				}
				break;
			case 10:
				{
				_localctx = new PositionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2815);
				match(POSITION);
				setState(2816);
				match(LEFT_PAREN);
				setState(2817);
				((PositionContext)_localctx).substr = valueExpression(0);
				setState(2818);
				match(IN);
				setState(2819);
				((PositionContext)_localctx).str = valueExpression(0);
				setState(2820);
				match(RIGHT_PAREN);
				}
				break;
			case 11:
				{
				_localctx = new ConstantDefaultContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2822);
				constant();
				}
				break;
			case 12:
				{
				_localctx = new StarContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2823);
				match(ASTERISK);
				}
				break;
			case 13:
				{
				_localctx = new StarContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2824);
				qualifiedName();
				setState(2825);
				match(DOT);
				setState(2826);
				match(ASTERISK);
				}
				break;
			case 14:
				{
				_localctx = new RowConstructorContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2828);
				match(LEFT_PAREN);
				setState(2829);
				namedExpression();
				setState(2832); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(2830);
					match(COMMA);
					setState(2831);
					namedExpression();
					}
					}
					setState(2834); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==COMMA );
				setState(2836);
				match(RIGHT_PAREN);
				}
				break;
			case 15:
				{
				_localctx = new SubqueryExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2838);
				match(LEFT_PAREN);
				setState(2839);
				query();
				setState(2840);
				match(RIGHT_PAREN);
				}
				break;
			case 16:
				{
				_localctx = new FunctionCallContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2842);
				functionName();
				setState(2843);
				match(LEFT_PAREN);
				setState(2855);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,374,_ctx) ) {
				case 1:
					{
					setState(2845);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,372,_ctx) ) {
					case 1:
						{
						setState(2844);
						setQuantifier();
						}
						break;
					}
					setState(2847);
					((FunctionCallContext)_localctx).expression = expression();
					((FunctionCallContext)_localctx).argument.add(((FunctionCallContext)_localctx).expression);
					setState(2852);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==COMMA) {
						{
						{
						setState(2848);
						match(COMMA);
						setState(2849);
						((FunctionCallContext)_localctx).expression = expression();
						((FunctionCallContext)_localctx).argument.add(((FunctionCallContext)_localctx).expression);
						}
						}
						setState(2854);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
					break;
				}
				setState(2857);
				match(RIGHT_PAREN);
				setState(2864);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,375,_ctx) ) {
				case 1:
					{
					setState(2858);
					match(FILTER);
					setState(2859);
					match(LEFT_PAREN);
					setState(2860);
					match(WHERE);
					setState(2861);
					((FunctionCallContext)_localctx).where = booleanExpression(0);
					setState(2862);
					match(RIGHT_PAREN);
					}
					break;
				}
				setState(2868);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,376,_ctx) ) {
				case 1:
					{
					setState(2866);
					((FunctionCallContext)_localctx).nullsOption = _input.LT(1);
					_la = _input.LA(1);
					if ( !(_la==IGNORE || _la==RESPECT) ) {
						((FunctionCallContext)_localctx).nullsOption = (Token)_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(2867);
					match(NULLS);
					}
					break;
				}
				setState(2872);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,377,_ctx) ) {
				case 1:
					{
					setState(2870);
					match(OVER);
					setState(2871);
					windowSpec();
					}
					break;
				}
				}
				break;
			case 17:
				{
				_localctx = new LambdaContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2874);
				identifier();
				setState(2875);
				match(ARROW);
				setState(2876);
				expression();
				}
				break;
			case 18:
				{
				_localctx = new LambdaContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2878);
				match(LEFT_PAREN);
				setState(2879);
				identifier();
				setState(2882); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(2880);
					match(COMMA);
					setState(2881);
					identifier();
					}
					}
					setState(2884); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==COMMA );
				setState(2886);
				match(RIGHT_PAREN);
				setState(2887);
				match(ARROW);
				setState(2888);
				expression();
				}
				break;
			case 19:
				{
				_localctx = new ColumnReferenceContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2890);
				identifier();
				}
				break;
			case 20:
				{
				_localctx = new ParenthesizedExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2891);
				match(LEFT_PAREN);
				setState(2892);
				expression();
				setState(2893);
				match(RIGHT_PAREN);
				}
				break;
			case 21:
				{
				_localctx = new ExtractContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2895);
				match(EXTRACT);
				setState(2896);
				match(LEFT_PAREN);
				setState(2897);
				((ExtractContext)_localctx).field = identifier();
				setState(2898);
				match(FROM);
				setState(2899);
				((ExtractContext)_localctx).source = valueExpression(0);
				setState(2900);
				match(RIGHT_PAREN);
				}
				break;
			case 22:
				{
				_localctx = new SubstringContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2902);
				_la = _input.LA(1);
				if ( !(_la==SUBSTR || _la==SUBSTRING) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(2903);
				match(LEFT_PAREN);
				setState(2904);
				((SubstringContext)_localctx).str = valueExpression(0);
				setState(2905);
				_la = _input.LA(1);
				if ( !(_la==COMMA || _la==FROM) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(2906);
				((SubstringContext)_localctx).pos = valueExpression(0);
				setState(2909);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMA || _la==FOR) {
					{
					setState(2907);
					_la = _input.LA(1);
					if ( !(_la==COMMA || _la==FOR) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(2908);
					((SubstringContext)_localctx).len = valueExpression(0);
					}
				}

				setState(2911);
				match(RIGHT_PAREN);
				}
				break;
			case 23:
				{
				_localctx = new TrimContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2913);
				match(TRIM);
				setState(2914);
				match(LEFT_PAREN);
				setState(2916);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,380,_ctx) ) {
				case 1:
					{
					setState(2915);
					((TrimContext)_localctx).trimOption = _input.LT(1);
					_la = _input.LA(1);
					if ( !(_la==BOTH || _la==LEADING || _la==TRAILING) ) {
						((TrimContext)_localctx).trimOption = (Token)_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
					break;
				}
				setState(2919);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,381,_ctx) ) {
				case 1:
					{
					setState(2918);
					((TrimContext)_localctx).trimStr = valueExpression(0);
					}
					break;
				}
				setState(2921);
				match(FROM);
				setState(2922);
				((TrimContext)_localctx).srcStr = valueExpression(0);
				setState(2923);
				match(RIGHT_PAREN);
				}
				break;
			case 24:
				{
				_localctx = new OverlayContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2925);
				match(OVERLAY);
				setState(2926);
				match(LEFT_PAREN);
				setState(2927);
				((OverlayContext)_localctx).input = valueExpression(0);
				setState(2928);
				match(PLACING);
				setState(2929);
				((OverlayContext)_localctx).replace = valueExpression(0);
				setState(2930);
				match(FROM);
				setState(2931);
				((OverlayContext)_localctx).position = valueExpression(0);
				setState(2934);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FOR) {
					{
					setState(2932);
					match(FOR);
					setState(2933);
					((OverlayContext)_localctx).length = valueExpression(0);
					}
				}

				setState(2936);
				match(RIGHT_PAREN);
				}
				break;
			case 25:
				{
				_localctx = new PercentileContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(2938);
				((PercentileContext)_localctx).name = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==PERCENTILE_CONT || _la==PERCENTILE_DISC) ) {
					((PercentileContext)_localctx).name = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(2939);
				match(LEFT_PAREN);
				setState(2940);
				((PercentileContext)_localctx).percentage = valueExpression(0);
				setState(2941);
				match(RIGHT_PAREN);
				setState(2942);
				match(WITHIN);
				setState(2943);
				match(GROUP);
				setState(2944);
				match(LEFT_PAREN);
				setState(2945);
				match(ORDER);
				setState(2946);
				match(BY);
				setState(2947);
				sortItem();
				setState(2948);
				match(RIGHT_PAREN);
				setState(2951);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,383,_ctx) ) {
				case 1:
					{
					setState(2949);
					match(OVER);
					setState(2950);
					windowSpec();
					}
					break;
				}
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(2965);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,386,_ctx);
			while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(2963);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,385,_ctx) ) {
					case 1:
						{
						_localctx = new SubscriptContext(new PrimaryExpressionContext(_parentctx, _parentState));
						((SubscriptContext)_localctx).value = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_primaryExpression);
						setState(2955);
						if (!(precpred(_ctx, 9))) throw new FailedPredicateException(this, "precpred(_ctx, 9)");
						setState(2956);
						match(LEFT_BRACKET);
						setState(2957);
						((SubscriptContext)_localctx).index = valueExpression(0);
						setState(2958);
						match(RIGHT_BRACKET);
						}
						break;
					case 2:
						{
						_localctx = new DereferenceContext(new PrimaryExpressionContext(_parentctx, _parentState));
						((DereferenceContext)_localctx).base = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_primaryExpression);
						setState(2960);
						if (!(precpred(_ctx, 7))) throw new FailedPredicateException(this, "precpred(_ctx, 7)");
						setState(2961);
						match(DOT);
						setState(2962);
						((DereferenceContext)_localctx).fieldName = identifier();
						}
						break;
					}
					} 
				}
				setState(2967);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,386,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ConstantContext extends ParserRuleContext {
		public ConstantContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_constant; }
	 
		public ConstantContext() { }
		public void copyFrom(ConstantContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class NullLiteralContext extends ConstantContext {
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public NullLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterNullLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitNullLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitNullLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class StringLiteralContext extends ConstantContext {
		public List<TerminalNode> STRING() { return getTokens(SqlBaseParser.STRING); }
		public TerminalNode STRING(int i) {
			return getToken(SqlBaseParser.STRING, i);
		}
		public StringLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterStringLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitStringLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitStringLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TypeConstructorContext extends ConstantContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TypeConstructorContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterTypeConstructor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitTypeConstructor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitTypeConstructor(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class IntervalLiteralContext extends ConstantContext {
		public IntervalContext interval() {
			return getRuleContext(IntervalContext.class,0);
		}
		public IntervalLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterIntervalLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitIntervalLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitIntervalLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class NumericLiteralContext extends ConstantContext {
		public NumberContext number() {
			return getRuleContext(NumberContext.class,0);
		}
		public NumericLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterNumericLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitNumericLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitNumericLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class BooleanLiteralContext extends ConstantContext {
		public BooleanValueContext booleanValue() {
			return getRuleContext(BooleanValueContext.class,0);
		}
		public BooleanLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterBooleanLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitBooleanLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitBooleanLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ConstantContext constant() throws RecognitionException {
		ConstantContext _localctx = new ConstantContext(_ctx, getState());
		enterRule(_localctx, 222, RULE_constant);
		try {
			int _alt;
			setState(2980);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,388,_ctx) ) {
			case 1:
				_localctx = new NullLiteralContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2968);
				match(NULL);
				}
				break;
			case 2:
				_localctx = new IntervalLiteralContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2969);
				interval();
				}
				break;
			case 3:
				_localctx = new TypeConstructorContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(2970);
				identifier();
				setState(2971);
				match(STRING);
				}
				break;
			case 4:
				_localctx = new NumericLiteralContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(2973);
				number();
				}
				break;
			case 5:
				_localctx = new BooleanLiteralContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(2974);
				booleanValue();
				}
				break;
			case 6:
				_localctx = new StringLiteralContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(2976); 
				_errHandler.sync(this);
				_alt = 1;
				do {
					switch (_alt) {
					case 1:
						{
						{
						setState(2975);
						match(STRING);
						}
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					setState(2978); 
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,387,_ctx);
				} while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ComparisonOperatorContext extends ParserRuleContext {
		public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
		public TerminalNode NEQ() { return getToken(SqlBaseParser.NEQ, 0); }
		public TerminalNode NEQJ() { return getToken(SqlBaseParser.NEQJ, 0); }
		public TerminalNode LT() { return getToken(SqlBaseParser.LT, 0); }
		public TerminalNode LTE() { return getToken(SqlBaseParser.LTE, 0); }
		public TerminalNode GT() { return getToken(SqlBaseParser.GT, 0); }
		public TerminalNode GTE() { return getToken(SqlBaseParser.GTE, 0); }
		public TerminalNode NSEQ() { return getToken(SqlBaseParser.NSEQ, 0); }
		public ComparisonOperatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_comparisonOperator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterComparisonOperator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitComparisonOperator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitComparisonOperator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ComparisonOperatorContext comparisonOperator() throws RecognitionException {
		ComparisonOperatorContext _localctx = new ComparisonOperatorContext(_ctx, getState());
		enterRule(_localctx, 224, RULE_comparisonOperator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2982);
			_la = _input.LA(1);
			if ( !(((((_la - 287)) & ~0x3f) == 0 && ((1L << (_la - 287)) & 255L) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ArithmeticOperatorContext extends ParserRuleContext {
		public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
		public TerminalNode SLASH() { return getToken(SqlBaseParser.SLASH, 0); }
		public TerminalNode PERCENT() { return getToken(SqlBaseParser.PERCENT, 0); }
		public TerminalNode DIV() { return getToken(SqlBaseParser.DIV, 0); }
		public TerminalNode TILDE() { return getToken(SqlBaseParser.TILDE, 0); }
		public TerminalNode AMPERSAND() { return getToken(SqlBaseParser.AMPERSAND, 0); }
		public TerminalNode PIPE() { return getToken(SqlBaseParser.PIPE, 0); }
		public TerminalNode CONCAT_PIPE() { return getToken(SqlBaseParser.CONCAT_PIPE, 0); }
		public TerminalNode HAT() { return getToken(SqlBaseParser.HAT, 0); }
		public ArithmeticOperatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_arithmeticOperator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterArithmeticOperator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitArithmeticOperator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitArithmeticOperator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ArithmeticOperatorContext arithmeticOperator() throws RecognitionException {
		ArithmeticOperatorContext _localctx = new ArithmeticOperatorContext(_ctx, getState());
		enterRule(_localctx, 226, RULE_arithmeticOperator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2984);
			_la = _input.LA(1);
			if ( !(_la==DIV || ((((_la - 295)) & ~0x3f) == 0 && ((1L << (_la - 295)) & 1023L) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PredicateOperatorContext extends ParserRuleContext {
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public PredicateOperatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_predicateOperator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterPredicateOperator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitPredicateOperator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitPredicateOperator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PredicateOperatorContext predicateOperator() throws RecognitionException {
		PredicateOperatorContext _localctx = new PredicateOperatorContext(_ctx, getState());
		enterRule(_localctx, 228, RULE_predicateOperator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2986);
			_la = _input.LA(1);
			if ( !(_la==AND || ((((_la - 115)) & ~0x3f) == 0 && ((1L << (_la - 115)) & 2260595906707457L) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class BooleanValueContext extends ParserRuleContext {
		public TerminalNode TRUE() { return getToken(SqlBaseParser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(SqlBaseParser.FALSE, 0); }
		public BooleanValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_booleanValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterBooleanValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitBooleanValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitBooleanValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BooleanValueContext booleanValue() throws RecognitionException {
		BooleanValueContext _localctx = new BooleanValueContext(_ctx, getState());
		enterRule(_localctx, 230, RULE_booleanValue);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2988);
			_la = _input.LA(1);
			if ( !(_la==FALSE || _la==TRUE) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class IntervalContext extends ParserRuleContext {
		public TerminalNode INTERVAL() { return getToken(SqlBaseParser.INTERVAL, 0); }
		public ErrorCapturingMultiUnitsIntervalContext errorCapturingMultiUnitsInterval() {
			return getRuleContext(ErrorCapturingMultiUnitsIntervalContext.class,0);
		}
		public ErrorCapturingUnitToUnitIntervalContext errorCapturingUnitToUnitInterval() {
			return getRuleContext(ErrorCapturingUnitToUnitIntervalContext.class,0);
		}
		public IntervalContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_interval; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterInterval(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitInterval(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitInterval(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IntervalContext interval() throws RecognitionException {
		IntervalContext _localctx = new IntervalContext(_ctx, getState());
		enterRule(_localctx, 232, RULE_interval);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2990);
			match(INTERVAL);
			setState(2993);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,389,_ctx) ) {
			case 1:
				{
				setState(2991);
				errorCapturingMultiUnitsInterval();
				}
				break;
			case 2:
				{
				setState(2992);
				errorCapturingUnitToUnitInterval();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ErrorCapturingMultiUnitsIntervalContext extends ParserRuleContext {
		public MultiUnitsIntervalContext body;
		public MultiUnitsIntervalContext multiUnitsInterval() {
			return getRuleContext(MultiUnitsIntervalContext.class,0);
		}
		public UnitToUnitIntervalContext unitToUnitInterval() {
			return getRuleContext(UnitToUnitIntervalContext.class,0);
		}
		public ErrorCapturingMultiUnitsIntervalContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_errorCapturingMultiUnitsInterval; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterErrorCapturingMultiUnitsInterval(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitErrorCapturingMultiUnitsInterval(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitErrorCapturingMultiUnitsInterval(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ErrorCapturingMultiUnitsIntervalContext errorCapturingMultiUnitsInterval() throws RecognitionException {
		ErrorCapturingMultiUnitsIntervalContext _localctx = new ErrorCapturingMultiUnitsIntervalContext(_ctx, getState());
		enterRule(_localctx, 234, RULE_errorCapturingMultiUnitsInterval);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2995);
			((ErrorCapturingMultiUnitsIntervalContext)_localctx).body = multiUnitsInterval();
			setState(2997);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,390,_ctx) ) {
			case 1:
				{
				setState(2996);
				unitToUnitInterval();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class MultiUnitsIntervalContext extends ParserRuleContext {
		public IdentifierContext identifier;
		public List<IdentifierContext> unit = new ArrayList<IdentifierContext>();
		public List<IntervalValueContext> intervalValue() {
			return getRuleContexts(IntervalValueContext.class);
		}
		public IntervalValueContext intervalValue(int i) {
			return getRuleContext(IntervalValueContext.class,i);
		}
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public MultiUnitsIntervalContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_multiUnitsInterval; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterMultiUnitsInterval(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitMultiUnitsInterval(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitMultiUnitsInterval(this);
			else return visitor.visitChildren(this);
		}
	}

	public final MultiUnitsIntervalContext multiUnitsInterval() throws RecognitionException {
		MultiUnitsIntervalContext _localctx = new MultiUnitsIntervalContext(_ctx, getState());
		enterRule(_localctx, 236, RULE_multiUnitsInterval);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(3002); 
			_errHandler.sync(this);
			_alt = 1;
			do {
				switch (_alt) {
				case 1:
					{
					{
					setState(2999);
					intervalValue();
					setState(3000);
					((MultiUnitsIntervalContext)_localctx).identifier = identifier();
					((MultiUnitsIntervalContext)_localctx).unit.add(((MultiUnitsIntervalContext)_localctx).identifier);
					}
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(3004); 
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,391,_ctx);
			} while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ErrorCapturingUnitToUnitIntervalContext extends ParserRuleContext {
		public UnitToUnitIntervalContext body;
		public MultiUnitsIntervalContext error1;
		public UnitToUnitIntervalContext error2;
		public List<UnitToUnitIntervalContext> unitToUnitInterval() {
			return getRuleContexts(UnitToUnitIntervalContext.class);
		}
		public UnitToUnitIntervalContext unitToUnitInterval(int i) {
			return getRuleContext(UnitToUnitIntervalContext.class,i);
		}
		public MultiUnitsIntervalContext multiUnitsInterval() {
			return getRuleContext(MultiUnitsIntervalContext.class,0);
		}
		public ErrorCapturingUnitToUnitIntervalContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_errorCapturingUnitToUnitInterval; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterErrorCapturingUnitToUnitInterval(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitErrorCapturingUnitToUnitInterval(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitErrorCapturingUnitToUnitInterval(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ErrorCapturingUnitToUnitIntervalContext errorCapturingUnitToUnitInterval() throws RecognitionException {
		ErrorCapturingUnitToUnitIntervalContext _localctx = new ErrorCapturingUnitToUnitIntervalContext(_ctx, getState());
		enterRule(_localctx, 238, RULE_errorCapturingUnitToUnitInterval);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(3006);
			((ErrorCapturingUnitToUnitIntervalContext)_localctx).body = unitToUnitInterval();
			setState(3009);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,392,_ctx) ) {
			case 1:
				{
				setState(3007);
				((ErrorCapturingUnitToUnitIntervalContext)_localctx).error1 = multiUnitsInterval();
				}
				break;
			case 2:
				{
				setState(3008);
				((ErrorCapturingUnitToUnitIntervalContext)_localctx).error2 = unitToUnitInterval();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class UnitToUnitIntervalContext extends ParserRuleContext {
		public IntervalValueContext value;
		public IdentifierContext from;
		public IdentifierContext to;
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public IntervalValueContext intervalValue() {
			return getRuleContext(IntervalValueContext.class,0);
		}
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public UnitToUnitIntervalContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_unitToUnitInterval; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterUnitToUnitInterval(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitUnitToUnitInterval(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitUnitToUnitInterval(this);
			else return visitor.visitChildren(this);
		}
	}

	public final UnitToUnitIntervalContext unitToUnitInterval() throws RecognitionException {
		UnitToUnitIntervalContext _localctx = new UnitToUnitIntervalContext(_ctx, getState());
		enterRule(_localctx, 240, RULE_unitToUnitInterval);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(3011);
			((UnitToUnitIntervalContext)_localctx).value = intervalValue();
			setState(3012);
			((UnitToUnitIntervalContext)_localctx).from = identifier();
			setState(3013);
			match(TO);
			setState(3014);
			((UnitToUnitIntervalContext)_localctx).to = identifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class IntervalValueContext extends ParserRuleContext {
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public TerminalNode DECIMAL_VALUE() { return getToken(SqlBaseParser.DECIMAL_VALUE, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public IntervalValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_intervalValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterIntervalValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitIntervalValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitIntervalValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IntervalValueContext intervalValue() throws RecognitionException {
		IntervalValueContext _localctx = new IntervalValueContext(_ctx, getState());
		enterRule(_localctx, 242, RULE_intervalValue);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(3017);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==PLUS || _la==MINUS) {
				{
				setState(3016);
				_la = _input.LA(1);
				if ( !(_la==PLUS || _la==MINUS) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			setState(3019);
			_la = _input.LA(1);
			if ( !(((((_la - 309)) & ~0x3f) == 0 && ((1L << (_la - 309)) & 81L) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ColPositionContext extends ParserRuleContext {
		public Token position;
		public ErrorCapturingIdentifierContext afterCol;
		public TerminalNode FIRST() { return getToken(SqlBaseParser.FIRST, 0); }
		public TerminalNode AFTER() { return getToken(SqlBaseParser.AFTER, 0); }
		public ErrorCapturingIdentifierContext errorCapturingIdentifier() {
			return getRuleContext(ErrorCapturingIdentifierContext.class,0);
		}
		public ColPositionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_colPosition; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterColPosition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitColPosition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitColPosition(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColPositionContext colPosition() throws RecognitionException {
		ColPositionContext _localctx = new ColPositionContext(_ctx, getState());
		enterRule(_localctx, 244, RULE_colPosition);
		try {
			setState(3024);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case FIRST:
				enterOuterAlt(_localctx, 1);
				{
				setState(3021);
				((ColPositionContext)_localctx).position = match(FIRST);
				}
				break;
			case AFTER:
				enterOuterAlt(_localctx, 2);
				{
				setState(3022);
				((ColPositionContext)_localctx).position = match(AFTER);
				setState(3023);
				((ColPositionContext)_localctx).afterCol = errorCapturingIdentifier();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class DataTypeContext extends ParserRuleContext {
		public DataTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dataType; }
	 
		public DataTypeContext() { }
		public void copyFrom(DataTypeContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ComplexDataTypeContext extends DataTypeContext {
		public Token complex;
		public TerminalNode LT() { return getToken(SqlBaseParser.LT, 0); }
		public List<DataTypeContext> dataType() {
			return getRuleContexts(DataTypeContext.class);
		}
		public DataTypeContext dataType(int i) {
			return getRuleContext(DataTypeContext.class,i);
		}
		public TerminalNode GT() { return getToken(SqlBaseParser.GT, 0); }
		public TerminalNode ARRAY() { return getToken(SqlBaseParser.ARRAY, 0); }
		public TerminalNode COMMA() { return getToken(SqlBaseParser.COMMA, 0); }
		public TerminalNode MAP() { return getToken(SqlBaseParser.MAP, 0); }
		public TerminalNode STRUCT() { return getToken(SqlBaseParser.STRUCT, 0); }
		public TerminalNode NEQ() { return getToken(SqlBaseParser.NEQ, 0); }
		public ComplexColTypeListContext complexColTypeList() {
			return getRuleContext(ComplexColTypeListContext.class,0);
		}
		public ComplexDataTypeContext(DataTypeContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterComplexDataType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitComplexDataType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitComplexDataType(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class YearMonthIntervalDataTypeContext extends DataTypeContext {
		public Token from;
		public Token to;
		public TerminalNode INTERVAL() { return getToken(SqlBaseParser.INTERVAL, 0); }
		public TerminalNode YEAR() { return getToken(SqlBaseParser.YEAR, 0); }
		public List<TerminalNode> MONTH() { return getTokens(SqlBaseParser.MONTH); }
		public TerminalNode MONTH(int i) {
			return getToken(SqlBaseParser.MONTH, i);
		}
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public YearMonthIntervalDataTypeContext(DataTypeContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterYearMonthIntervalDataType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitYearMonthIntervalDataType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitYearMonthIntervalDataType(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DayTimeIntervalDataTypeContext extends DataTypeContext {
		public Token from;
		public Token to;
		public TerminalNode INTERVAL() { return getToken(SqlBaseParser.INTERVAL, 0); }
		public TerminalNode DAY() { return getToken(SqlBaseParser.DAY, 0); }
		public List<TerminalNode> HOUR() { return getTokens(SqlBaseParser.HOUR); }
		public TerminalNode HOUR(int i) {
			return getToken(SqlBaseParser.HOUR, i);
		}
		public List<TerminalNode> MINUTE() { return getTokens(SqlBaseParser.MINUTE); }
		public TerminalNode MINUTE(int i) {
			return getToken(SqlBaseParser.MINUTE, i);
		}
		public List<TerminalNode> SECOND() { return getTokens(SqlBaseParser.SECOND); }
		public TerminalNode SECOND(int i) {
			return getToken(SqlBaseParser.SECOND, i);
		}
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public DayTimeIntervalDataTypeContext(DataTypeContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterDayTimeIntervalDataType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitDayTimeIntervalDataType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitDayTimeIntervalDataType(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class PrimitiveDataTypeContext extends DataTypeContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public List<TerminalNode> INTEGER_VALUE() { return getTokens(SqlBaseParser.INTEGER_VALUE); }
		public TerminalNode INTEGER_VALUE(int i) {
			return getToken(SqlBaseParser.INTEGER_VALUE, i);
		}
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public PrimitiveDataTypeContext(DataTypeContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterPrimitiveDataType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitPrimitiveDataType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitPrimitiveDataType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DataTypeContext dataType() throws RecognitionException {
		DataTypeContext _localctx = new DataTypeContext(_ctx, getState());
		enterRule(_localctx, 246, RULE_dataType);
		int _la;
		try {
			setState(3072);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,401,_ctx) ) {
			case 1:
				_localctx = new ComplexDataTypeContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(3026);
				((ComplexDataTypeContext)_localctx).complex = match(ARRAY);
				setState(3027);
				match(LT);
				setState(3028);
				dataType();
				setState(3029);
				match(GT);
				}
				break;
			case 2:
				_localctx = new ComplexDataTypeContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(3031);
				((ComplexDataTypeContext)_localctx).complex = match(MAP);
				setState(3032);
				match(LT);
				setState(3033);
				dataType();
				setState(3034);
				match(COMMA);
				setState(3035);
				dataType();
				setState(3036);
				match(GT);
				}
				break;
			case 3:
				_localctx = new ComplexDataTypeContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(3038);
				((ComplexDataTypeContext)_localctx).complex = match(STRUCT);
				setState(3045);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case LT:
					{
					setState(3039);
					match(LT);
					setState(3041);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,395,_ctx) ) {
					case 1:
						{
						setState(3040);
						complexColTypeList();
						}
						break;
					}
					setState(3043);
					match(GT);
					}
					break;
				case NEQ:
					{
					setState(3044);
					match(NEQ);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				break;
			case 4:
				_localctx = new YearMonthIntervalDataTypeContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(3047);
				match(INTERVAL);
				setState(3048);
				((YearMonthIntervalDataTypeContext)_localctx).from = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==MONTH || _la==YEAR) ) {
					((YearMonthIntervalDataTypeContext)_localctx).from = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(3051);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,397,_ctx) ) {
				case 1:
					{
					setState(3049);
					match(TO);
					setState(3050);
					((YearMonthIntervalDataTypeContext)_localctx).to = match(MONTH);
					}
					break;
				}
				}
				break;
			case 5:
				_localctx = new DayTimeIntervalDataTypeContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(3053);
				match(INTERVAL);
				setState(3054);
				((DayTimeIntervalDataTypeContext)_localctx).from = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==DAY || _la==HOUR || _la==MINUTE || _la==SECOND) ) {
					((DayTimeIntervalDataTypeContext)_localctx).from = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(3057);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,398,_ctx) ) {
				case 1:
					{
					setState(3055);
					match(TO);
					setState(3056);
					((DayTimeIntervalDataTypeContext)_localctx).to = _input.LT(1);
					_la = _input.LA(1);
					if ( !(_la==HOUR || _la==MINUTE || _la==SECOND) ) {
						((DayTimeIntervalDataTypeContext)_localctx).to = (Token)_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
					break;
				}
				}
				break;
			case 6:
				_localctx = new PrimitiveDataTypeContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(3059);
				identifier();
				setState(3070);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,400,_ctx) ) {
				case 1:
					{
					setState(3060);
					match(LEFT_PAREN);
					setState(3061);
					match(INTEGER_VALUE);
					setState(3066);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==COMMA) {
						{
						{
						setState(3062);
						match(COMMA);
						setState(3063);
						match(INTEGER_VALUE);
						}
						}
						setState(3068);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(3069);
					match(RIGHT_PAREN);
					}
					break;
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QualifiedColTypeWithPositionListContext extends ParserRuleContext {
		public List<QualifiedColTypeWithPositionContext> qualifiedColTypeWithPosition() {
			return getRuleContexts(QualifiedColTypeWithPositionContext.class);
		}
		public QualifiedColTypeWithPositionContext qualifiedColTypeWithPosition(int i) {
			return getRuleContext(QualifiedColTypeWithPositionContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public QualifiedColTypeWithPositionListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_qualifiedColTypeWithPositionList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterQualifiedColTypeWithPositionList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitQualifiedColTypeWithPositionList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitQualifiedColTypeWithPositionList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QualifiedColTypeWithPositionListContext qualifiedColTypeWithPositionList() throws RecognitionException {
		QualifiedColTypeWithPositionListContext _localctx = new QualifiedColTypeWithPositionListContext(_ctx, getState());
		enterRule(_localctx, 248, RULE_qualifiedColTypeWithPositionList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(3074);
			qualifiedColTypeWithPosition();
			setState(3079);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(3075);
				match(COMMA);
				setState(3076);
				qualifiedColTypeWithPosition();
				}
				}
				setState(3081);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QualifiedColTypeWithPositionContext extends ParserRuleContext {
		public MultipartIdentifierContext name;
		public DataTypeContext dataType() {
			return getRuleContext(DataTypeContext.class,0);
		}
		public MultipartIdentifierContext multipartIdentifier() {
			return getRuleContext(MultipartIdentifierContext.class,0);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public CommentSpecContext commentSpec() {
			return getRuleContext(CommentSpecContext.class,0);
		}
		public ColPositionContext colPosition() {
			return getRuleContext(ColPositionContext.class,0);
		}
		public QualifiedColTypeWithPositionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_qualifiedColTypeWithPosition; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterQualifiedColTypeWithPosition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitQualifiedColTypeWithPosition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitQualifiedColTypeWithPosition(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QualifiedColTypeWithPositionContext qualifiedColTypeWithPosition() throws RecognitionException {
		QualifiedColTypeWithPositionContext _localctx = new QualifiedColTypeWithPositionContext(_ctx, getState());
		enterRule(_localctx, 250, RULE_qualifiedColTypeWithPosition);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(3082);
			((QualifiedColTypeWithPositionContext)_localctx).name = multipartIdentifier();
			setState(3083);
			dataType();
			setState(3086);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==NOT) {
				{
				setState(3084);
				match(NOT);
				setState(3085);
				match(NULL);
				}
			}

			setState(3089);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COMMENT) {
				{
				setState(3088);
				commentSpec();
				}
			}

			setState(3092);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==AFTER || _la==FIRST) {
				{
				setState(3091);
				colPosition();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ColTypeListContext extends ParserRuleContext {
		public List<ColTypeContext> colType() {
			return getRuleContexts(ColTypeContext.class);
		}
		public ColTypeContext colType(int i) {
			return getRuleContext(ColTypeContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public ColTypeListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_colTypeList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterColTypeList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitColTypeList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitColTypeList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColTypeListContext colTypeList() throws RecognitionException {
		ColTypeListContext _localctx = new ColTypeListContext(_ctx, getState());
		enterRule(_localctx, 252, RULE_colTypeList);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(3094);
			colType();
			setState(3099);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,406,_ctx);
			while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(3095);
					match(COMMA);
					setState(3096);
					colType();
					}
					} 
				}
				setState(3101);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,406,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ColTypeContext extends ParserRuleContext {
		public ErrorCapturingIdentifierContext colName;
		public DataTypeContext dataType() {
			return getRuleContext(DataTypeContext.class,0);
		}
		public ErrorCapturingIdentifierContext errorCapturingIdentifier() {
			return getRuleContext(ErrorCapturingIdentifierContext.class,0);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public CommentSpecContext commentSpec() {
			return getRuleContext(CommentSpecContext.class,0);
		}
		public ColTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_colType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterColType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitColType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitColType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColTypeContext colType() throws RecognitionException {
		ColTypeContext _localctx = new ColTypeContext(_ctx, getState());
		enterRule(_localctx, 254, RULE_colType);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(3102);
			((ColTypeContext)_localctx).colName = errorCapturingIdentifier();
			setState(3103);
			dataType();
			setState(3106);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,407,_ctx) ) {
			case 1:
				{
				setState(3104);
				match(NOT);
				setState(3105);
				match(NULL);
				}
				break;
			}
			setState(3109);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,408,_ctx) ) {
			case 1:
				{
				setState(3108);
				commentSpec();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ComplexColTypeListContext extends ParserRuleContext {
		public List<ComplexColTypeContext> complexColType() {
			return getRuleContexts(ComplexColTypeContext.class);
		}
		public ComplexColTypeContext complexColType(int i) {
			return getRuleContext(ComplexColTypeContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public ComplexColTypeListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_complexColTypeList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterComplexColTypeList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitComplexColTypeList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitComplexColTypeList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ComplexColTypeListContext complexColTypeList() throws RecognitionException {
		ComplexColTypeListContext _localctx = new ComplexColTypeListContext(_ctx, getState());
		enterRule(_localctx, 256, RULE_complexColTypeList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(3111);
			complexColType();
			setState(3116);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(3112);
				match(COMMA);
				setState(3113);
				complexColType();
				}
				}
				setState(3118);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ComplexColTypeContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DataTypeContext dataType() {
			return getRuleContext(DataTypeContext.class,0);
		}
		public TerminalNode COLON() { return getToken(SqlBaseParser.COLON, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public CommentSpecContext commentSpec() {
			return getRuleContext(CommentSpecContext.class,0);
		}
		public ComplexColTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_complexColType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterComplexColType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitComplexColType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitComplexColType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ComplexColTypeContext complexColType() throws RecognitionException {
		ComplexColTypeContext _localctx = new ComplexColTypeContext(_ctx, getState());
		enterRule(_localctx, 258, RULE_complexColType);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(3119);
			identifier();
			setState(3121);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,410,_ctx) ) {
			case 1:
				{
				setState(3120);
				match(COLON);
				}
				break;
			}
			setState(3123);
			dataType();
			setState(3126);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==NOT) {
				{
				setState(3124);
				match(NOT);
				setState(3125);
				match(NULL);
				}
			}

			setState(3129);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COMMENT) {
				{
				setState(3128);
				commentSpec();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class WhenClauseContext extends ParserRuleContext {
		public ExpressionContext condition;
		public ExpressionContext result;
		public TerminalNode WHEN() { return getToken(SqlBaseParser.WHEN, 0); }
		public TerminalNode THEN() { return getToken(SqlBaseParser.THEN, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public WhenClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_whenClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterWhenClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitWhenClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitWhenClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WhenClauseContext whenClause() throws RecognitionException {
		WhenClauseContext _localctx = new WhenClauseContext(_ctx, getState());
		enterRule(_localctx, 260, RULE_whenClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(3131);
			match(WHEN);
			setState(3132);
			((WhenClauseContext)_localctx).condition = expression();
			setState(3133);
			match(THEN);
			setState(3134);
			((WhenClauseContext)_localctx).result = expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class WindowClauseContext extends ParserRuleContext {
		public TerminalNode WINDOW() { return getToken(SqlBaseParser.WINDOW, 0); }
		public List<NamedWindowContext> namedWindow() {
			return getRuleContexts(NamedWindowContext.class);
		}
		public NamedWindowContext namedWindow(int i) {
			return getRuleContext(NamedWindowContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public WindowClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_windowClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterWindowClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitWindowClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitWindowClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WindowClauseContext windowClause() throws RecognitionException {
		WindowClauseContext _localctx = new WindowClauseContext(_ctx, getState());
		enterRule(_localctx, 262, RULE_windowClause);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(3136);
			match(WINDOW);
			setState(3137);
			namedWindow();
			setState(3142);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,413,_ctx);
			while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(3138);
					match(COMMA);
					setState(3139);
					namedWindow();
					}
					} 
				}
				setState(3144);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,413,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NamedWindowContext extends ParserRuleContext {
		public ErrorCapturingIdentifierContext name;
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public WindowSpecContext windowSpec() {
			return getRuleContext(WindowSpecContext.class,0);
		}
		public ErrorCapturingIdentifierContext errorCapturingIdentifier() {
			return getRuleContext(ErrorCapturingIdentifierContext.class,0);
		}
		public NamedWindowContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_namedWindow; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterNamedWindow(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitNamedWindow(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitNamedWindow(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NamedWindowContext namedWindow() throws RecognitionException {
		NamedWindowContext _localctx = new NamedWindowContext(_ctx, getState());
		enterRule(_localctx, 264, RULE_namedWindow);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(3145);
			((NamedWindowContext)_localctx).name = errorCapturingIdentifier();
			setState(3146);
			match(AS);
			setState(3147);
			windowSpec();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class WindowSpecContext extends ParserRuleContext {
		public WindowSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_windowSpec; }
	 
		public WindowSpecContext() { }
		public void copyFrom(WindowSpecContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class WindowRefContext extends WindowSpecContext {
		public ErrorCapturingIdentifierContext name;
		public ErrorCapturingIdentifierContext errorCapturingIdentifier() {
			return getRuleContext(ErrorCapturingIdentifierContext.class,0);
		}
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public WindowRefContext(WindowSpecContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterWindowRef(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitWindowRef(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitWindowRef(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class WindowDefContext extends WindowSpecContext {
		public ExpressionContext expression;
		public List<ExpressionContext> partition = new ArrayList<ExpressionContext>();
		public TerminalNode LEFT_PAREN() { return getToken(SqlBaseParser.LEFT_PAREN, 0); }
		public TerminalNode RIGHT_PAREN() { return getToken(SqlBaseParser.RIGHT_PAREN, 0); }
		public TerminalNode CLUSTER() { return getToken(SqlBaseParser.CLUSTER, 0); }
		public List<TerminalNode> BY() { return getTokens(SqlBaseParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlBaseParser.BY, i);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public WindowFrameContext windowFrame() {
			return getRuleContext(WindowFrameContext.class,0);
		}
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public List<SortItemContext> sortItem() {
			return getRuleContexts(SortItemContext.class);
		}
		public SortItemContext sortItem(int i) {
			return getRuleContext(SortItemContext.class,i);
		}
		public TerminalNode PARTITION() { return getToken(SqlBaseParser.PARTITION, 0); }
		public TerminalNode DISTRIBUTE() { return getToken(SqlBaseParser.DISTRIBUTE, 0); }
		public TerminalNode ORDER() { return getToken(SqlBaseParser.ORDER, 0); }
		public TerminalNode SORT() { return getToken(SqlBaseParser.SORT, 0); }
		public WindowDefContext(WindowSpecContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterWindowDef(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitWindowDef(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitWindowDef(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WindowSpecContext windowSpec() throws RecognitionException {
		WindowSpecContext _localctx = new WindowSpecContext(_ctx, getState());
		enterRule(_localctx, 266, RULE_windowSpec);
		int _la;
		try {
			setState(3195);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,421,_ctx) ) {
			case 1:
				_localctx = new WindowRefContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(3149);
				((WindowRefContext)_localctx).name = errorCapturingIdentifier();
				}
				break;
			case 2:
				_localctx = new WindowRefContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(3150);
				match(LEFT_PAREN);
				setState(3151);
				((WindowRefContext)_localctx).name = errorCapturingIdentifier();
				setState(3152);
				match(RIGHT_PAREN);
				}
				break;
			case 3:
				_localctx = new WindowDefContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(3154);
				match(LEFT_PAREN);
				setState(3189);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case CLUSTER:
					{
					setState(3155);
					match(CLUSTER);
					setState(3156);
					match(BY);
					setState(3157);
					((WindowDefContext)_localctx).expression = expression();
					((WindowDefContext)_localctx).partition.add(((WindowDefContext)_localctx).expression);
					setState(3162);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==COMMA) {
						{
						{
						setState(3158);
						match(COMMA);
						setState(3159);
						((WindowDefContext)_localctx).expression = expression();
						((WindowDefContext)_localctx).partition.add(((WindowDefContext)_localctx).expression);
						}
						}
						setState(3164);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
					break;
				case RIGHT_PAREN:
				case DISTRIBUTE:
				case ORDER:
				case PARTITION:
				case RANGE:
				case ROWS:
				case SORT:
					{
					setState(3175);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==DISTRIBUTE || _la==PARTITION) {
						{
						setState(3165);
						_la = _input.LA(1);
						if ( !(_la==DISTRIBUTE || _la==PARTITION) ) {
						_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(3166);
						match(BY);
						setState(3167);
						((WindowDefContext)_localctx).expression = expression();
						((WindowDefContext)_localctx).partition.add(((WindowDefContext)_localctx).expression);
						setState(3172);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==COMMA) {
							{
							{
							setState(3168);
							match(COMMA);
							setState(3169);
							((WindowDefContext)_localctx).expression = expression();
							((WindowDefContext)_localctx).partition.add(((WindowDefContext)_localctx).expression);
							}
							}
							setState(3174);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
						}
					}

					setState(3187);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==ORDER || _la==SORT) {
						{
						setState(3177);
						_la = _input.LA(1);
						if ( !(_la==ORDER || _la==SORT) ) {
						_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(3178);
						match(BY);
						setState(3179);
						sortItem();
						setState(3184);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==COMMA) {
							{
							{
							setState(3180);
							match(COMMA);
							setState(3181);
							sortItem();
							}
							}
							setState(3186);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
						}
					}

					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(3192);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==RANGE || _la==ROWS) {
					{
					setState(3191);
					windowFrame();
					}
				}

				setState(3194);
				match(RIGHT_PAREN);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class WindowFrameContext extends ParserRuleContext {
		public Token frameType;
		public FrameBoundContext start;
		public FrameBoundContext end;
		public TerminalNode RANGE() { return getToken(SqlBaseParser.RANGE, 0); }
		public List<FrameBoundContext> frameBound() {
			return getRuleContexts(FrameBoundContext.class);
		}
		public FrameBoundContext frameBound(int i) {
			return getRuleContext(FrameBoundContext.class,i);
		}
		public TerminalNode ROWS() { return getToken(SqlBaseParser.ROWS, 0); }
		public TerminalNode BETWEEN() { return getToken(SqlBaseParser.BETWEEN, 0); }
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public WindowFrameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_windowFrame; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterWindowFrame(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitWindowFrame(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitWindowFrame(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WindowFrameContext windowFrame() throws RecognitionException {
		WindowFrameContext _localctx = new WindowFrameContext(_ctx, getState());
		enterRule(_localctx, 268, RULE_windowFrame);
		try {
			setState(3213);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,422,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(3197);
				((WindowFrameContext)_localctx).frameType = match(RANGE);
				setState(3198);
				((WindowFrameContext)_localctx).start = frameBound();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(3199);
				((WindowFrameContext)_localctx).frameType = match(ROWS);
				setState(3200);
				((WindowFrameContext)_localctx).start = frameBound();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(3201);
				((WindowFrameContext)_localctx).frameType = match(RANGE);
				setState(3202);
				match(BETWEEN);
				setState(3203);
				((WindowFrameContext)_localctx).start = frameBound();
				setState(3204);
				match(AND);
				setState(3205);
				((WindowFrameContext)_localctx).end = frameBound();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(3207);
				((WindowFrameContext)_localctx).frameType = match(ROWS);
				setState(3208);
				match(BETWEEN);
				setState(3209);
				((WindowFrameContext)_localctx).start = frameBound();
				setState(3210);
				match(AND);
				setState(3211);
				((WindowFrameContext)_localctx).end = frameBound();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class FrameBoundContext extends ParserRuleContext {
		public Token boundType;
		public TerminalNode UNBOUNDED() { return getToken(SqlBaseParser.UNBOUNDED, 0); }
		public TerminalNode PRECEDING() { return getToken(SqlBaseParser.PRECEDING, 0); }
		public TerminalNode FOLLOWING() { return getToken(SqlBaseParser.FOLLOWING, 0); }
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public TerminalNode CURRENT() { return getToken(SqlBaseParser.CURRENT, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public FrameBoundContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_frameBound; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterFrameBound(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitFrameBound(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitFrameBound(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FrameBoundContext frameBound() throws RecognitionException {
		FrameBoundContext _localctx = new FrameBoundContext(_ctx, getState());
		enterRule(_localctx, 270, RULE_frameBound);
		int _la;
		try {
			setState(3222);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,423,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(3215);
				match(UNBOUNDED);
				setState(3216);
				((FrameBoundContext)_localctx).boundType = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==FOLLOWING || _la==PRECEDING) ) {
					((FrameBoundContext)_localctx).boundType = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(3217);
				((FrameBoundContext)_localctx).boundType = match(CURRENT);
				setState(3218);
				match(ROW);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(3219);
				expression();
				setState(3220);
				((FrameBoundContext)_localctx).boundType = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==FOLLOWING || _la==PRECEDING) ) {
					((FrameBoundContext)_localctx).boundType = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QualifiedNameListContext extends ParserRuleContext {
		public List<QualifiedNameContext> qualifiedName() {
			return getRuleContexts(QualifiedNameContext.class);
		}
		public QualifiedNameContext qualifiedName(int i) {
			return getRuleContext(QualifiedNameContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(SqlBaseParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SqlBaseParser.COMMA, i);
		}
		public QualifiedNameListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_qualifiedNameList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterQualifiedNameList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitQualifiedNameList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitQualifiedNameList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QualifiedNameListContext qualifiedNameList() throws RecognitionException {
		QualifiedNameListContext _localctx = new QualifiedNameListContext(_ctx, getState());
		enterRule(_localctx, 272, RULE_qualifiedNameList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(3224);
			qualifiedName();
			setState(3229);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(3225);
				match(COMMA);
				setState(3226);
				qualifiedName();
				}
				}
				setState(3231);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class FunctionNameContext extends ParserRuleContext {
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode FILTER() { return getToken(SqlBaseParser.FILTER, 0); }
		public TerminalNode LEFT() { return getToken(SqlBaseParser.LEFT, 0); }
		public TerminalNode RIGHT() { return getToken(SqlBaseParser.RIGHT, 0); }
		public FunctionNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_functionName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterFunctionName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitFunctionName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitFunctionName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FunctionNameContext functionName() throws RecognitionException {
		FunctionNameContext _localctx = new FunctionNameContext(_ctx, getState());
		enterRule(_localctx, 274, RULE_functionName);
		try {
			setState(3236);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,425,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(3232);
				qualifiedName();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(3233);
				match(FILTER);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(3234);
				match(LEFT);
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(3235);
				match(RIGHT);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QualifiedNameContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public List<TerminalNode> DOT() { return getTokens(SqlBaseParser.DOT); }
		public TerminalNode DOT(int i) {
			return getToken(SqlBaseParser.DOT, i);
		}
		public QualifiedNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_qualifiedName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterQualifiedName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitQualifiedName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitQualifiedName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QualifiedNameContext qualifiedName() throws RecognitionException {
		QualifiedNameContext _localctx = new QualifiedNameContext(_ctx, getState());
		enterRule(_localctx, 276, RULE_qualifiedName);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(3238);
			identifier();
			setState(3243);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,426,_ctx);
			while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(3239);
					match(DOT);
					setState(3240);
					identifier();
					}
					} 
				}
				setState(3245);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,426,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ErrorCapturingIdentifierContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ErrorCapturingIdentifierExtraContext errorCapturingIdentifierExtra() {
			return getRuleContext(ErrorCapturingIdentifierExtraContext.class,0);
		}
		public ErrorCapturingIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_errorCapturingIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterErrorCapturingIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitErrorCapturingIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitErrorCapturingIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ErrorCapturingIdentifierContext errorCapturingIdentifier() throws RecognitionException {
		ErrorCapturingIdentifierContext _localctx = new ErrorCapturingIdentifierContext(_ctx, getState());
		enterRule(_localctx, 278, RULE_errorCapturingIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(3246);
			identifier();
			setState(3247);
			errorCapturingIdentifierExtra();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ErrorCapturingIdentifierExtraContext extends ParserRuleContext {
		public ErrorCapturingIdentifierExtraContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_errorCapturingIdentifierExtra; }
	 
		public ErrorCapturingIdentifierExtraContext() { }
		public void copyFrom(ErrorCapturingIdentifierExtraContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ErrorIdentContext extends ErrorCapturingIdentifierExtraContext {
		public List<TerminalNode> MINUS() { return getTokens(SqlBaseParser.MINUS); }
		public TerminalNode MINUS(int i) {
			return getToken(SqlBaseParser.MINUS, i);
		}
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public ErrorIdentContext(ErrorCapturingIdentifierExtraContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterErrorIdent(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitErrorIdent(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitErrorIdent(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RealIdentContext extends ErrorCapturingIdentifierExtraContext {
		public RealIdentContext(ErrorCapturingIdentifierExtraContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterRealIdent(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitRealIdent(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitRealIdent(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ErrorCapturingIdentifierExtraContext errorCapturingIdentifierExtra() throws RecognitionException {
		ErrorCapturingIdentifierExtraContext _localctx = new ErrorCapturingIdentifierExtraContext(_ctx, getState());
		enterRule(_localctx, 280, RULE_errorCapturingIdentifierExtra);
		try {
			int _alt;
			setState(3256);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,428,_ctx) ) {
			case 1:
				_localctx = new ErrorIdentContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(3251); 
				_errHandler.sync(this);
				_alt = 1;
				do {
					switch (_alt) {
					case 1:
						{
						{
						setState(3249);
						match(MINUS);
						setState(3250);
						identifier();
						}
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					setState(3253); 
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,427,_ctx);
				} while ( _alt!=2 && _alt!=com.github.ares.org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
				}
				break;
			case 2:
				_localctx = new RealIdentContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class IdentifierContext extends ParserRuleContext {
		public StrictIdentifierContext strictIdentifier() {
			return getRuleContext(StrictIdentifierContext.class,0);
		}
		public StrictNonReservedContext strictNonReserved() {
			return getRuleContext(StrictNonReservedContext.class,0);
		}
		public IdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierContext identifier() throws RecognitionException {
		IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
		enterRule(_localctx, 282, RULE_identifier);
		try {
			setState(3261);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,429,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(3258);
				strictIdentifier();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(3259);
				if (!(!SQL_standard_keyword_behavior)) throw new FailedPredicateException(this, "!SQL_standard_keyword_behavior");
				setState(3260);
				strictNonReserved();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class StrictIdentifierContext extends ParserRuleContext {
		public StrictIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_strictIdentifier; }
	 
		public StrictIdentifierContext() { }
		public void copyFrom(StrictIdentifierContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class QuotedIdentifierAlternativeContext extends StrictIdentifierContext {
		public QuotedIdentifierContext quotedIdentifier() {
			return getRuleContext(QuotedIdentifierContext.class,0);
		}
		public QuotedIdentifierAlternativeContext(StrictIdentifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterQuotedIdentifierAlternative(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitQuotedIdentifierAlternative(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitQuotedIdentifierAlternative(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class UnquotedIdentifierContext extends StrictIdentifierContext {
		public TerminalNode IDENTIFIER() { return getToken(SqlBaseParser.IDENTIFIER, 0); }
		public AnsiNonReservedContext ansiNonReserved() {
			return getRuleContext(AnsiNonReservedContext.class,0);
		}
		public NonReservedContext nonReserved() {
			return getRuleContext(NonReservedContext.class,0);
		}
		public UnquotedIdentifierContext(StrictIdentifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterUnquotedIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitUnquotedIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitUnquotedIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StrictIdentifierContext strictIdentifier() throws RecognitionException {
		StrictIdentifierContext _localctx = new StrictIdentifierContext(_ctx, getState());
		enterRule(_localctx, 284, RULE_strictIdentifier);
		try {
			setState(3269);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,430,_ctx) ) {
			case 1:
				_localctx = new UnquotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(3263);
				match(IDENTIFIER);
				}
				break;
			case 2:
				_localctx = new QuotedIdentifierAlternativeContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(3264);
				quotedIdentifier();
				}
				break;
			case 3:
				_localctx = new UnquotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(3265);
				if (!(SQL_standard_keyword_behavior)) throw new FailedPredicateException(this, "SQL_standard_keyword_behavior");
				setState(3266);
				ansiNonReserved();
				}
				break;
			case 4:
				_localctx = new UnquotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(3267);
				if (!(!SQL_standard_keyword_behavior)) throw new FailedPredicateException(this, "!SQL_standard_keyword_behavior");
				setState(3268);
				nonReserved();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QuotedIdentifierContext extends ParserRuleContext {
		public TerminalNode BACKQUOTED_IDENTIFIER() { return getToken(SqlBaseParser.BACKQUOTED_IDENTIFIER, 0); }
		public QuotedIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_quotedIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterQuotedIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitQuotedIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitQuotedIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QuotedIdentifierContext quotedIdentifier() throws RecognitionException {
		QuotedIdentifierContext _localctx = new QuotedIdentifierContext(_ctx, getState());
		enterRule(_localctx, 286, RULE_quotedIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(3271);
			match(BACKQUOTED_IDENTIFIER);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NumberContext extends ParserRuleContext {
		public NumberContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_number; }
	 
		public NumberContext() { }
		public void copyFrom(NumberContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DecimalLiteralContext extends NumberContext {
		public TerminalNode DECIMAL_VALUE() { return getToken(SqlBaseParser.DECIMAL_VALUE, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public DecimalLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterDecimalLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitDecimalLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitDecimalLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class BigIntLiteralContext extends NumberContext {
		public TerminalNode BIGINT_LITERAL() { return getToken(SqlBaseParser.BIGINT_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public BigIntLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterBigIntLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitBigIntLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitBigIntLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TinyIntLiteralContext extends NumberContext {
		public TerminalNode TINYINT_LITERAL() { return getToken(SqlBaseParser.TINYINT_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public TinyIntLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterTinyIntLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitTinyIntLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitTinyIntLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class LegacyDecimalLiteralContext extends NumberContext {
		public TerminalNode EXPONENT_VALUE() { return getToken(SqlBaseParser.EXPONENT_VALUE, 0); }
		public TerminalNode DECIMAL_VALUE() { return getToken(SqlBaseParser.DECIMAL_VALUE, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public LegacyDecimalLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterLegacyDecimalLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitLegacyDecimalLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitLegacyDecimalLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class BigDecimalLiteralContext extends NumberContext {
		public TerminalNode BIGDECIMAL_LITERAL() { return getToken(SqlBaseParser.BIGDECIMAL_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public BigDecimalLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterBigDecimalLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitBigDecimalLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitBigDecimalLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ExponentLiteralContext extends NumberContext {
		public TerminalNode EXPONENT_VALUE() { return getToken(SqlBaseParser.EXPONENT_VALUE, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public ExponentLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterExponentLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitExponentLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitExponentLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class DoubleLiteralContext extends NumberContext {
		public TerminalNode DOUBLE_LITERAL() { return getToken(SqlBaseParser.DOUBLE_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public DoubleLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterDoubleLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitDoubleLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitDoubleLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class IntegerLiteralContext extends NumberContext {
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public IntegerLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterIntegerLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitIntegerLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitIntegerLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class FloatLiteralContext extends NumberContext {
		public TerminalNode FLOAT_LITERAL() { return getToken(SqlBaseParser.FLOAT_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public FloatLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterFloatLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitFloatLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitFloatLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class SmallIntLiteralContext extends NumberContext {
		public TerminalNode SMALLINT_LITERAL() { return getToken(SqlBaseParser.SMALLINT_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public SmallIntLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterSmallIntLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitSmallIntLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitSmallIntLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NumberContext number() throws RecognitionException {
		NumberContext _localctx = new NumberContext(_ctx, getState());
		enterRule(_localctx, 288, RULE_number);
		int _la;
		try {
			setState(3316);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,441,_ctx) ) {
			case 1:
				_localctx = new ExponentLiteralContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(3273);
				if (!(!legacy_exponent_literal_as_decimal_enabled)) throw new FailedPredicateException(this, "!legacy_exponent_literal_as_decimal_enabled");
				setState(3275);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(3274);
					match(MINUS);
					}
				}

				setState(3277);
				match(EXPONENT_VALUE);
				}
				break;
			case 2:
				_localctx = new DecimalLiteralContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(3278);
				if (!(!legacy_exponent_literal_as_decimal_enabled)) throw new FailedPredicateException(this, "!legacy_exponent_literal_as_decimal_enabled");
				setState(3280);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(3279);
					match(MINUS);
					}
				}

				setState(3282);
				match(DECIMAL_VALUE);
				}
				break;
			case 3:
				_localctx = new LegacyDecimalLiteralContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(3283);
				if (!(legacy_exponent_literal_as_decimal_enabled)) throw new FailedPredicateException(this, "legacy_exponent_literal_as_decimal_enabled");
				setState(3285);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(3284);
					match(MINUS);
					}
				}

				setState(3287);
				_la = _input.LA(1);
				if ( !(_la==EXPONENT_VALUE || _la==DECIMAL_VALUE) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			case 4:
				_localctx = new IntegerLiteralContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(3289);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(3288);
					match(MINUS);
					}
				}

				setState(3291);
				match(INTEGER_VALUE);
				}
				break;
			case 5:
				_localctx = new BigIntLiteralContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(3293);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(3292);
					match(MINUS);
					}
				}

				setState(3295);
				match(BIGINT_LITERAL);
				}
				break;
			case 6:
				_localctx = new SmallIntLiteralContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(3297);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(3296);
					match(MINUS);
					}
				}

				setState(3299);
				match(SMALLINT_LITERAL);
				}
				break;
			case 7:
				_localctx = new TinyIntLiteralContext(_localctx);
				enterOuterAlt(_localctx, 7);
				{
				setState(3301);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(3300);
					match(MINUS);
					}
				}

				setState(3303);
				match(TINYINT_LITERAL);
				}
				break;
			case 8:
				_localctx = new DoubleLiteralContext(_localctx);
				enterOuterAlt(_localctx, 8);
				{
				setState(3305);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(3304);
					match(MINUS);
					}
				}

				setState(3307);
				match(DOUBLE_LITERAL);
				}
				break;
			case 9:
				_localctx = new FloatLiteralContext(_localctx);
				enterOuterAlt(_localctx, 9);
				{
				setState(3309);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(3308);
					match(MINUS);
					}
				}

				setState(3311);
				match(FLOAT_LITERAL);
				}
				break;
			case 10:
				_localctx = new BigDecimalLiteralContext(_localctx);
				enterOuterAlt(_localctx, 10);
				{
				setState(3313);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(3312);
					match(MINUS);
					}
				}

				setState(3315);
				match(BIGDECIMAL_LITERAL);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class AlterColumnActionContext extends ParserRuleContext {
		public Token setOrDrop;
		public TerminalNode TYPE() { return getToken(SqlBaseParser.TYPE, 0); }
		public DataTypeContext dataType() {
			return getRuleContext(DataTypeContext.class,0);
		}
		public CommentSpecContext commentSpec() {
			return getRuleContext(CommentSpecContext.class,0);
		}
		public ColPositionContext colPosition() {
			return getRuleContext(ColPositionContext.class,0);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public AlterColumnActionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_alterColumnAction; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterAlterColumnAction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitAlterColumnAction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitAlterColumnAction(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AlterColumnActionContext alterColumnAction() throws RecognitionException {
		AlterColumnActionContext _localctx = new AlterColumnActionContext(_ctx, getState());
		enterRule(_localctx, 290, RULE_alterColumnAction);
		int _la;
		try {
			setState(3325);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case TYPE:
				enterOuterAlt(_localctx, 1);
				{
				setState(3318);
				match(TYPE);
				setState(3319);
				dataType();
				}
				break;
			case COMMENT:
				enterOuterAlt(_localctx, 2);
				{
				setState(3320);
				commentSpec();
				}
				break;
			case AFTER:
			case FIRST:
				enterOuterAlt(_localctx, 3);
				{
				setState(3321);
				colPosition();
				}
				break;
			case DROP:
			case SET:
				enterOuterAlt(_localctx, 4);
				{
				setState(3322);
				((AlterColumnActionContext)_localctx).setOrDrop = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==DROP || _la==SET) ) {
					((AlterColumnActionContext)_localctx).setOrDrop = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(3323);
				match(NOT);
				setState(3324);
				match(NULL);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class AnsiNonReservedContext extends ParserRuleContext {
		public TerminalNode ADD() { return getToken(SqlBaseParser.ADD, 0); }
		public TerminalNode AFTER() { return getToken(SqlBaseParser.AFTER, 0); }
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode ANALYZE() { return getToken(SqlBaseParser.ANALYZE, 0); }
		public TerminalNode ANTI() { return getToken(SqlBaseParser.ANTI, 0); }
		public TerminalNode ARCHIVE() { return getToken(SqlBaseParser.ARCHIVE, 0); }
		public TerminalNode ARRAY() { return getToken(SqlBaseParser.ARRAY, 0); }
		public TerminalNode ASC() { return getToken(SqlBaseParser.ASC, 0); }
		public TerminalNode AT() { return getToken(SqlBaseParser.AT, 0); }
		public TerminalNode BETWEEN() { return getToken(SqlBaseParser.BETWEEN, 0); }
		public TerminalNode BUCKET() { return getToken(SqlBaseParser.BUCKET, 0); }
		public TerminalNode BUCKETS() { return getToken(SqlBaseParser.BUCKETS, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public TerminalNode CACHE() { return getToken(SqlBaseParser.CACHE, 0); }
		public TerminalNode CASCADE() { return getToken(SqlBaseParser.CASCADE, 0); }
		public TerminalNode CATALOG() { return getToken(SqlBaseParser.CATALOG, 0); }
		public TerminalNode CATALOGS() { return getToken(SqlBaseParser.CATALOGS, 0); }
		public TerminalNode CHANGE() { return getToken(SqlBaseParser.CHANGE, 0); }
		public TerminalNode CLEAR() { return getToken(SqlBaseParser.CLEAR, 0); }
		public TerminalNode CLUSTER() { return getToken(SqlBaseParser.CLUSTER, 0); }
		public TerminalNode CLUSTERED() { return getToken(SqlBaseParser.CLUSTERED, 0); }
		public TerminalNode CODEGEN() { return getToken(SqlBaseParser.CODEGEN, 0); }
		public TerminalNode COLLECTION() { return getToken(SqlBaseParser.COLLECTION, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public TerminalNode COMMIT() { return getToken(SqlBaseParser.COMMIT, 0); }
		public TerminalNode COMPACT() { return getToken(SqlBaseParser.COMPACT, 0); }
		public TerminalNode COMPACTIONS() { return getToken(SqlBaseParser.COMPACTIONS, 0); }
		public TerminalNode COMPUTE() { return getToken(SqlBaseParser.COMPUTE, 0); }
		public TerminalNode CONCATENATE() { return getToken(SqlBaseParser.CONCATENATE, 0); }
		public TerminalNode COST() { return getToken(SqlBaseParser.COST, 0); }
		public TerminalNode CUBE() { return getToken(SqlBaseParser.CUBE, 0); }
		public TerminalNode CURRENT() { return getToken(SqlBaseParser.CURRENT, 0); }
		public TerminalNode DATA() { return getToken(SqlBaseParser.DATA, 0); }
		public TerminalNode DATABASE() { return getToken(SqlBaseParser.DATABASE, 0); }
		public TerminalNode DATABASES() { return getToken(SqlBaseParser.DATABASES, 0); }
		public TerminalNode DATEADD() { return getToken(SqlBaseParser.DATEADD, 0); }
		public TerminalNode DATEDIFF() { return getToken(SqlBaseParser.DATEDIFF, 0); }
		public TerminalNode DAY() { return getToken(SqlBaseParser.DAY, 0); }
		public TerminalNode DAYOFYEAR() { return getToken(SqlBaseParser.DAYOFYEAR, 0); }
		public TerminalNode DBPROPERTIES() { return getToken(SqlBaseParser.DBPROPERTIES, 0); }
		public TerminalNode DEFINED() { return getToken(SqlBaseParser.DEFINED, 0); }
		public TerminalNode DELETE() { return getToken(SqlBaseParser.DELETE, 0); }
		public TerminalNode DELIMITED() { return getToken(SqlBaseParser.DELIMITED, 0); }
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode DESCRIBE() { return getToken(SqlBaseParser.DESCRIBE, 0); }
		public TerminalNode DFS() { return getToken(SqlBaseParser.DFS, 0); }
		public TerminalNode DIRECTORIES() { return getToken(SqlBaseParser.DIRECTORIES, 0); }
		public TerminalNode DIRECTORY() { return getToken(SqlBaseParser.DIRECTORY, 0); }
		public TerminalNode DISTRIBUTE() { return getToken(SqlBaseParser.DISTRIBUTE, 0); }
		public TerminalNode DIV() { return getToken(SqlBaseParser.DIV, 0); }
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode ESCAPED() { return getToken(SqlBaseParser.ESCAPED, 0); }
		public TerminalNode EXCHANGE() { return getToken(SqlBaseParser.EXCHANGE, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode EXPLAIN() { return getToken(SqlBaseParser.EXPLAIN, 0); }
		public TerminalNode EXPORT() { return getToken(SqlBaseParser.EXPORT, 0); }
		public TerminalNode EXTENDED() { return getToken(SqlBaseParser.EXTENDED, 0); }
		public TerminalNode EXTERNAL() { return getToken(SqlBaseParser.EXTERNAL, 0); }
		public TerminalNode EXTRACT() { return getToken(SqlBaseParser.EXTRACT, 0); }
		public TerminalNode FIELDS() { return getToken(SqlBaseParser.FIELDS, 0); }
		public TerminalNode FILEFORMAT() { return getToken(SqlBaseParser.FILEFORMAT, 0); }
		public TerminalNode FIRST() { return getToken(SqlBaseParser.FIRST, 0); }
		public TerminalNode FOLLOWING() { return getToken(SqlBaseParser.FOLLOWING, 0); }
		public TerminalNode FORMAT() { return getToken(SqlBaseParser.FORMAT, 0); }
		public TerminalNode FORMATTED() { return getToken(SqlBaseParser.FORMATTED, 0); }
		public TerminalNode FUNCTION() { return getToken(SqlBaseParser.FUNCTION, 0); }
		public TerminalNode FUNCTIONS() { return getToken(SqlBaseParser.FUNCTIONS, 0); }
		public TerminalNode GLOBAL() { return getToken(SqlBaseParser.GLOBAL, 0); }
		public TerminalNode GROUPING() { return getToken(SqlBaseParser.GROUPING, 0); }
		public TerminalNode HOUR() { return getToken(SqlBaseParser.HOUR, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode IGNORE() { return getToken(SqlBaseParser.IGNORE, 0); }
		public TerminalNode IMPORT() { return getToken(SqlBaseParser.IMPORT, 0); }
		public TerminalNode INDEX() { return getToken(SqlBaseParser.INDEX, 0); }
		public TerminalNode INDEXES() { return getToken(SqlBaseParser.INDEXES, 0); }
		public TerminalNode INPATH() { return getToken(SqlBaseParser.INPATH, 0); }
		public TerminalNode INPUTFORMAT() { return getToken(SqlBaseParser.INPUTFORMAT, 0); }
		public TerminalNode INSERT() { return getToken(SqlBaseParser.INSERT, 0); }
		public TerminalNode INTERVAL() { return getToken(SqlBaseParser.INTERVAL, 0); }
		public TerminalNode ITEMS() { return getToken(SqlBaseParser.ITEMS, 0); }
		public TerminalNode KEYS() { return getToken(SqlBaseParser.KEYS, 0); }
		public TerminalNode LAST() { return getToken(SqlBaseParser.LAST, 0); }
		public TerminalNode LAZY() { return getToken(SqlBaseParser.LAZY, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public TerminalNode ILIKE() { return getToken(SqlBaseParser.ILIKE, 0); }
		public TerminalNode LIMIT() { return getToken(SqlBaseParser.LIMIT, 0); }
		public TerminalNode LINES() { return getToken(SqlBaseParser.LINES, 0); }
		public TerminalNode LIST() { return getToken(SqlBaseParser.LIST, 0); }
		public TerminalNode LOAD() { return getToken(SqlBaseParser.LOAD, 0); }
		public TerminalNode LOCAL() { return getToken(SqlBaseParser.LOCAL, 0); }
		public TerminalNode LOCATION() { return getToken(SqlBaseParser.LOCATION, 0); }
		public TerminalNode LOCK() { return getToken(SqlBaseParser.LOCK, 0); }
		public TerminalNode LOCKS() { return getToken(SqlBaseParser.LOCKS, 0); }
		public TerminalNode LOGICAL() { return getToken(SqlBaseParser.LOGICAL, 0); }
		public TerminalNode MACRO() { return getToken(SqlBaseParser.MACRO, 0); }
		public TerminalNode MAP() { return getToken(SqlBaseParser.MAP, 0); }
		public TerminalNode MATCHED() { return getToken(SqlBaseParser.MATCHED, 0); }
		public TerminalNode MERGE() { return getToken(SqlBaseParser.MERGE, 0); }
		public TerminalNode MICROSECOND() { return getToken(SqlBaseParser.MICROSECOND, 0); }
		public TerminalNode MILLISECOND() { return getToken(SqlBaseParser.MILLISECOND, 0); }
		public TerminalNode MINUTE() { return getToken(SqlBaseParser.MINUTE, 0); }
		public TerminalNode MONTH() { return getToken(SqlBaseParser.MONTH, 0); }
		public TerminalNode MSCK() { return getToken(SqlBaseParser.MSCK, 0); }
		public TerminalNode NAMESPACE() { return getToken(SqlBaseParser.NAMESPACE, 0); }
		public TerminalNode NAMESPACES() { return getToken(SqlBaseParser.NAMESPACES, 0); }
		public TerminalNode NO() { return getToken(SqlBaseParser.NO, 0); }
		public TerminalNode NULLS() { return getToken(SqlBaseParser.NULLS, 0); }
		public TerminalNode OF() { return getToken(SqlBaseParser.OF, 0); }
		public TerminalNode OPTION() { return getToken(SqlBaseParser.OPTION, 0); }
		public TerminalNode OPTIONS() { return getToken(SqlBaseParser.OPTIONS, 0); }
		public TerminalNode OUT() { return getToken(SqlBaseParser.OUT, 0); }
		public TerminalNode OUTPUTFORMAT() { return getToken(SqlBaseParser.OUTPUTFORMAT, 0); }
		public TerminalNode OVER() { return getToken(SqlBaseParser.OVER, 0); }
		public TerminalNode OVERLAY() { return getToken(SqlBaseParser.OVERLAY, 0); }
		public TerminalNode OVERWRITE() { return getToken(SqlBaseParser.OVERWRITE, 0); }
		public TerminalNode PARTITION() { return getToken(SqlBaseParser.PARTITION, 0); }
		public TerminalNode PARTITIONED() { return getToken(SqlBaseParser.PARTITIONED, 0); }
		public TerminalNode PARTITIONS() { return getToken(SqlBaseParser.PARTITIONS, 0); }
		public TerminalNode PERCENTLIT() { return getToken(SqlBaseParser.PERCENTLIT, 0); }
		public TerminalNode PIVOT() { return getToken(SqlBaseParser.PIVOT, 0); }
		public TerminalNode PLACING() { return getToken(SqlBaseParser.PLACING, 0); }
		public TerminalNode POSITION() { return getToken(SqlBaseParser.POSITION, 0); }
		public TerminalNode PRECEDING() { return getToken(SqlBaseParser.PRECEDING, 0); }
		public TerminalNode PRINCIPALS() { return getToken(SqlBaseParser.PRINCIPALS, 0); }
		public TerminalNode PROPERTIES() { return getToken(SqlBaseParser.PROPERTIES, 0); }
		public TerminalNode PURGE() { return getToken(SqlBaseParser.PURGE, 0); }
		public TerminalNode QUARTER() { return getToken(SqlBaseParser.QUARTER, 0); }
		public TerminalNode QUERY() { return getToken(SqlBaseParser.QUERY, 0); }
		public TerminalNode RANGE() { return getToken(SqlBaseParser.RANGE, 0); }
		public TerminalNode RECORDREADER() { return getToken(SqlBaseParser.RECORDREADER, 0); }
		public TerminalNode RECORDWRITER() { return getToken(SqlBaseParser.RECORDWRITER, 0); }
		public TerminalNode RECOVER() { return getToken(SqlBaseParser.RECOVER, 0); }
		public TerminalNode REDUCE() { return getToken(SqlBaseParser.REDUCE, 0); }
		public TerminalNode REFRESH() { return getToken(SqlBaseParser.REFRESH, 0); }
		public TerminalNode RENAME() { return getToken(SqlBaseParser.RENAME, 0); }
		public TerminalNode REPAIR() { return getToken(SqlBaseParser.REPAIR, 0); }
		public TerminalNode REPEATABLE() { return getToken(SqlBaseParser.REPEATABLE, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode RESET() { return getToken(SqlBaseParser.RESET, 0); }
		public TerminalNode RESPECT() { return getToken(SqlBaseParser.RESPECT, 0); }
		public TerminalNode RESTRICT() { return getToken(SqlBaseParser.RESTRICT, 0); }
		public TerminalNode REVOKE() { return getToken(SqlBaseParser.REVOKE, 0); }
		public TerminalNode RLIKE() { return getToken(SqlBaseParser.RLIKE, 0); }
		public TerminalNode ROLE() { return getToken(SqlBaseParser.ROLE, 0); }
		public TerminalNode ROLES() { return getToken(SqlBaseParser.ROLES, 0); }
		public TerminalNode ROLLBACK() { return getToken(SqlBaseParser.ROLLBACK, 0); }
		public TerminalNode ROLLUP() { return getToken(SqlBaseParser.ROLLUP, 0); }
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public TerminalNode ROWS() { return getToken(SqlBaseParser.ROWS, 0); }
		public TerminalNode SCHEMA() { return getToken(SqlBaseParser.SCHEMA, 0); }
		public TerminalNode SCHEMAS() { return getToken(SqlBaseParser.SCHEMAS, 0); }
		public TerminalNode SECOND() { return getToken(SqlBaseParser.SECOND, 0); }
		public TerminalNode SEMI() { return getToken(SqlBaseParser.SEMI, 0); }
		public TerminalNode SEPARATED() { return getToken(SqlBaseParser.SEPARATED, 0); }
		public TerminalNode SERDE() { return getToken(SqlBaseParser.SERDE, 0); }
		public TerminalNode SERDEPROPERTIES() { return getToken(SqlBaseParser.SERDEPROPERTIES, 0); }
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode SETMINUS() { return getToken(SqlBaseParser.SETMINUS, 0); }
		public TerminalNode SETS() { return getToken(SqlBaseParser.SETS, 0); }
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode SKEWED() { return getToken(SqlBaseParser.SKEWED, 0); }
		public TerminalNode SORT() { return getToken(SqlBaseParser.SORT, 0); }
		public TerminalNode SORTED() { return getToken(SqlBaseParser.SORTED, 0); }
		public TerminalNode START() { return getToken(SqlBaseParser.START, 0); }
		public TerminalNode STATISTICS() { return getToken(SqlBaseParser.STATISTICS, 0); }
		public TerminalNode STORED() { return getToken(SqlBaseParser.STORED, 0); }
		public TerminalNode STRATIFY() { return getToken(SqlBaseParser.STRATIFY, 0); }
		public TerminalNode STRUCT() { return getToken(SqlBaseParser.STRUCT, 0); }
		public TerminalNode SUBSTR() { return getToken(SqlBaseParser.SUBSTR, 0); }
		public TerminalNode SUBSTRING() { return getToken(SqlBaseParser.SUBSTRING, 0); }
		public TerminalNode SYNC() { return getToken(SqlBaseParser.SYNC, 0); }
		public TerminalNode SYSTEM_TIME() { return getToken(SqlBaseParser.SYSTEM_TIME, 0); }
		public TerminalNode SYSTEM_VERSION() { return getToken(SqlBaseParser.SYSTEM_VERSION, 0); }
		public TerminalNode TABLES() { return getToken(SqlBaseParser.TABLES, 0); }
		public TerminalNode TABLESAMPLE() { return getToken(SqlBaseParser.TABLESAMPLE, 0); }
		public TerminalNode TBLPROPERTIES() { return getToken(SqlBaseParser.TBLPROPERTIES, 0); }
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode TERMINATED() { return getToken(SqlBaseParser.TERMINATED, 0); }
		public TerminalNode TIMESTAMP() { return getToken(SqlBaseParser.TIMESTAMP, 0); }
		public TerminalNode TIMESTAMPADD() { return getToken(SqlBaseParser.TIMESTAMPADD, 0); }
		public TerminalNode TIMESTAMPDIFF() { return getToken(SqlBaseParser.TIMESTAMPDIFF, 0); }
		public TerminalNode TOUCH() { return getToken(SqlBaseParser.TOUCH, 0); }
		public TerminalNode TRANSACTION() { return getToken(SqlBaseParser.TRANSACTION, 0); }
		public TerminalNode TRANSACTIONS() { return getToken(SqlBaseParser.TRANSACTIONS, 0); }
		public TerminalNode TRANSFORM() { return getToken(SqlBaseParser.TRANSFORM, 0); }
		public TerminalNode TRIM() { return getToken(SqlBaseParser.TRIM, 0); }
		public TerminalNode TRUE() { return getToken(SqlBaseParser.TRUE, 0); }
		public TerminalNode TRUNCATE() { return getToken(SqlBaseParser.TRUNCATE, 0); }
		public TerminalNode TRY_CAST() { return getToken(SqlBaseParser.TRY_CAST, 0); }
		public TerminalNode TYPE() { return getToken(SqlBaseParser.TYPE, 0); }
		public TerminalNode UNARCHIVE() { return getToken(SqlBaseParser.UNARCHIVE, 0); }
		public TerminalNode UNBOUNDED() { return getToken(SqlBaseParser.UNBOUNDED, 0); }
		public TerminalNode UNCACHE() { return getToken(SqlBaseParser.UNCACHE, 0); }
		public TerminalNode UNLOCK() { return getToken(SqlBaseParser.UNLOCK, 0); }
		public TerminalNode UNSET() { return getToken(SqlBaseParser.UNSET, 0); }
		public TerminalNode UPDATE() { return getToken(SqlBaseParser.UPDATE, 0); }
		public TerminalNode USE() { return getToken(SqlBaseParser.USE, 0); }
		public TerminalNode VALUES() { return getToken(SqlBaseParser.VALUES, 0); }
		public TerminalNode VERSION() { return getToken(SqlBaseParser.VERSION, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TerminalNode VIEWS() { return getToken(SqlBaseParser.VIEWS, 0); }
		public TerminalNode WEEK() { return getToken(SqlBaseParser.WEEK, 0); }
		public TerminalNode WINDOW() { return getToken(SqlBaseParser.WINDOW, 0); }
		public TerminalNode YEAR() { return getToken(SqlBaseParser.YEAR, 0); }
		public TerminalNode ZONE() { return getToken(SqlBaseParser.ZONE, 0); }
		public AnsiNonReservedContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ansiNonReserved; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterAnsiNonReserved(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitAnsiNonReserved(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitAnsiNonReserved(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AnsiNonReservedContext ansiNonReserved() throws RecognitionException {
		AnsiNonReservedContext _localctx = new AnsiNonReservedContext(_ctx, getState());
		enterRule(_localctx, 292, RULE_ansiNonReserved);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(3327);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & -547753072259278080L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 5456013313885632511L) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & -147513558668673077L) != 0) || ((((_la - 192)) & ~0x3f) == 0 && ((1L << (_la - 192)) & -5873256933871337489L) != 0) || ((((_la - 256)) & ~0x3f) == 0 && ((1L << (_la - 256)) & 1694098431L) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class StrictNonReservedContext extends ParserRuleContext {
		public TerminalNode ANTI() { return getToken(SqlBaseParser.ANTI, 0); }
		public TerminalNode CROSS() { return getToken(SqlBaseParser.CROSS, 0); }
		public TerminalNode EXCEPT() { return getToken(SqlBaseParser.EXCEPT, 0); }
		public TerminalNode FULL() { return getToken(SqlBaseParser.FULL, 0); }
		public TerminalNode INNER() { return getToken(SqlBaseParser.INNER, 0); }
		public TerminalNode INTERSECT() { return getToken(SqlBaseParser.INTERSECT, 0); }
		public TerminalNode JOIN() { return getToken(SqlBaseParser.JOIN, 0); }
		public TerminalNode LATERAL() { return getToken(SqlBaseParser.LATERAL, 0); }
		public TerminalNode LEFT() { return getToken(SqlBaseParser.LEFT, 0); }
		public TerminalNode NATURAL() { return getToken(SqlBaseParser.NATURAL, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public TerminalNode RIGHT() { return getToken(SqlBaseParser.RIGHT, 0); }
		public TerminalNode SEMI() { return getToken(SqlBaseParser.SEMI, 0); }
		public TerminalNode SETMINUS() { return getToken(SqlBaseParser.SETMINUS, 0); }
		public TerminalNode UNION() { return getToken(SqlBaseParser.UNION, 0); }
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public StrictNonReservedContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_strictNonReserved; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterStrictNonReserved(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitStrictNonReserved(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitStrictNonReserved(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StrictNonReservedContext strictNonReserved() throws RecognitionException {
		StrictNonReservedContext _localctx = new StrictNonReservedContext(_ctx, getState());
		enterRule(_localctx, 294, RULE_strictNonReserved);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(3329);
			_la = _input.LA(1);
			if ( !(_la==ANTI || _la==CROSS || ((((_la - 83)) & ~0x3f) == 0 && ((1L << (_la - 83)) & 1284813697843201L) != 0) || ((((_la - 156)) & ~0x3f) == 0 && ((1L << (_la - 156)) & 4612811918334230593L) != 0) || ((((_la - 224)) & ~0x3f) == 0 && ((1L << (_la - 224)) & 1130297953353729L) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NonReservedContext extends ParserRuleContext {
		public TerminalNode ADD() { return getToken(SqlBaseParser.ADD, 0); }
		public TerminalNode AFTER() { return getToken(SqlBaseParser.AFTER, 0); }
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode ANALYZE() { return getToken(SqlBaseParser.ANALYZE, 0); }
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public TerminalNode ANY() { return getToken(SqlBaseParser.ANY, 0); }
		public TerminalNode ARCHIVE() { return getToken(SqlBaseParser.ARCHIVE, 0); }
		public TerminalNode ARRAY() { return getToken(SqlBaseParser.ARRAY, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TerminalNode ASC() { return getToken(SqlBaseParser.ASC, 0); }
		public TerminalNode AT() { return getToken(SqlBaseParser.AT, 0); }
		public TerminalNode AUTHORIZATION() { return getToken(SqlBaseParser.AUTHORIZATION, 0); }
		public TerminalNode BETWEEN() { return getToken(SqlBaseParser.BETWEEN, 0); }
		public TerminalNode BOTH() { return getToken(SqlBaseParser.BOTH, 0); }
		public TerminalNode BUCKET() { return getToken(SqlBaseParser.BUCKET, 0); }
		public TerminalNode BUCKETS() { return getToken(SqlBaseParser.BUCKETS, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public TerminalNode CACHE() { return getToken(SqlBaseParser.CACHE, 0); }
		public TerminalNode CASCADE() { return getToken(SqlBaseParser.CASCADE, 0); }
		public TerminalNode CASE() { return getToken(SqlBaseParser.CASE, 0); }
		public TerminalNode CAST() { return getToken(SqlBaseParser.CAST, 0); }
		public TerminalNode CATALOG() { return getToken(SqlBaseParser.CATALOG, 0); }
		public TerminalNode CATALOGS() { return getToken(SqlBaseParser.CATALOGS, 0); }
		public TerminalNode CHANGE() { return getToken(SqlBaseParser.CHANGE, 0); }
		public TerminalNode CHECK() { return getToken(SqlBaseParser.CHECK, 0); }
		public TerminalNode CLEAR() { return getToken(SqlBaseParser.CLEAR, 0); }
		public TerminalNode CLUSTER() { return getToken(SqlBaseParser.CLUSTER, 0); }
		public TerminalNode CLUSTERED() { return getToken(SqlBaseParser.CLUSTERED, 0); }
		public TerminalNode CODEGEN() { return getToken(SqlBaseParser.CODEGEN, 0); }
		public TerminalNode COLLATE() { return getToken(SqlBaseParser.COLLATE, 0); }
		public TerminalNode COLLECTION() { return getToken(SqlBaseParser.COLLECTION, 0); }
		public TerminalNode COLUMN() { return getToken(SqlBaseParser.COLUMN, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public TerminalNode COMMIT() { return getToken(SqlBaseParser.COMMIT, 0); }
		public TerminalNode COMPACT() { return getToken(SqlBaseParser.COMPACT, 0); }
		public TerminalNode COMPACTIONS() { return getToken(SqlBaseParser.COMPACTIONS, 0); }
		public TerminalNode COMPUTE() { return getToken(SqlBaseParser.COMPUTE, 0); }
		public TerminalNode CONCATENATE() { return getToken(SqlBaseParser.CONCATENATE, 0); }
		public TerminalNode CONSTRAINT() { return getToken(SqlBaseParser.CONSTRAINT, 0); }
		public TerminalNode COST() { return getToken(SqlBaseParser.COST, 0); }
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode CUBE() { return getToken(SqlBaseParser.CUBE, 0); }
		public TerminalNode CURRENT() { return getToken(SqlBaseParser.CURRENT, 0); }
		public TerminalNode CURRENT_DATE() { return getToken(SqlBaseParser.CURRENT_DATE, 0); }
		public TerminalNode CURRENT_TIME() { return getToken(SqlBaseParser.CURRENT_TIME, 0); }
		public TerminalNode CURRENT_TIMESTAMP() { return getToken(SqlBaseParser.CURRENT_TIMESTAMP, 0); }
		public TerminalNode CURRENT_USER() { return getToken(SqlBaseParser.CURRENT_USER, 0); }
		public TerminalNode DATA() { return getToken(SqlBaseParser.DATA, 0); }
		public TerminalNode DATABASE() { return getToken(SqlBaseParser.DATABASE, 0); }
		public TerminalNode DATABASES() { return getToken(SqlBaseParser.DATABASES, 0); }
		public TerminalNode DATEADD() { return getToken(SqlBaseParser.DATEADD, 0); }
		public TerminalNode DATEDIFF() { return getToken(SqlBaseParser.DATEDIFF, 0); }
		public TerminalNode DAY() { return getToken(SqlBaseParser.DAY, 0); }
		public TerminalNode DAYOFYEAR() { return getToken(SqlBaseParser.DAYOFYEAR, 0); }
		public TerminalNode DBPROPERTIES() { return getToken(SqlBaseParser.DBPROPERTIES, 0); }
		public TerminalNode DEFINED() { return getToken(SqlBaseParser.DEFINED, 0); }
		public TerminalNode DELETE() { return getToken(SqlBaseParser.DELETE, 0); }
		public TerminalNode DELIMITED() { return getToken(SqlBaseParser.DELIMITED, 0); }
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode DESCRIBE() { return getToken(SqlBaseParser.DESCRIBE, 0); }
		public TerminalNode DFS() { return getToken(SqlBaseParser.DFS, 0); }
		public TerminalNode DIRECTORIES() { return getToken(SqlBaseParser.DIRECTORIES, 0); }
		public TerminalNode DIRECTORY() { return getToken(SqlBaseParser.DIRECTORY, 0); }
		public TerminalNode DISTINCT() { return getToken(SqlBaseParser.DISTINCT, 0); }
		public TerminalNode DISTRIBUTE() { return getToken(SqlBaseParser.DISTRIBUTE, 0); }
		public TerminalNode DIV() { return getToken(SqlBaseParser.DIV, 0); }
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode ELSE() { return getToken(SqlBaseParser.ELSE, 0); }
		public TerminalNode END() { return getToken(SqlBaseParser.END, 0); }
		public TerminalNode ESCAPE() { return getToken(SqlBaseParser.ESCAPE, 0); }
		public TerminalNode ESCAPED() { return getToken(SqlBaseParser.ESCAPED, 0); }
		public TerminalNode EXCHANGE() { return getToken(SqlBaseParser.EXCHANGE, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode EXPLAIN() { return getToken(SqlBaseParser.EXPLAIN, 0); }
		public TerminalNode EXPORT() { return getToken(SqlBaseParser.EXPORT, 0); }
		public TerminalNode EXTENDED() { return getToken(SqlBaseParser.EXTENDED, 0); }
		public TerminalNode EXTERNAL() { return getToken(SqlBaseParser.EXTERNAL, 0); }
		public TerminalNode EXTRACT() { return getToken(SqlBaseParser.EXTRACT, 0); }
		public TerminalNode FALSE() { return getToken(SqlBaseParser.FALSE, 0); }
		public TerminalNode FETCH() { return getToken(SqlBaseParser.FETCH, 0); }
		public TerminalNode FILTER() { return getToken(SqlBaseParser.FILTER, 0); }
		public TerminalNode FIELDS() { return getToken(SqlBaseParser.FIELDS, 0); }
		public TerminalNode FILEFORMAT() { return getToken(SqlBaseParser.FILEFORMAT, 0); }
		public TerminalNode FIRST() { return getToken(SqlBaseParser.FIRST, 0); }
		public TerminalNode FOLLOWING() { return getToken(SqlBaseParser.FOLLOWING, 0); }
		public TerminalNode FOR() { return getToken(SqlBaseParser.FOR, 0); }
		public TerminalNode FOREIGN() { return getToken(SqlBaseParser.FOREIGN, 0); }
		public TerminalNode FORMAT() { return getToken(SqlBaseParser.FORMAT, 0); }
		public TerminalNode FORMATTED() { return getToken(SqlBaseParser.FORMATTED, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode FUNCTION() { return getToken(SqlBaseParser.FUNCTION, 0); }
		public TerminalNode FUNCTIONS() { return getToken(SqlBaseParser.FUNCTIONS, 0); }
		public TerminalNode GLOBAL() { return getToken(SqlBaseParser.GLOBAL, 0); }
		public TerminalNode GRANT() { return getToken(SqlBaseParser.GRANT, 0); }
		public TerminalNode GROUP() { return getToken(SqlBaseParser.GROUP, 0); }
		public TerminalNode GROUPING() { return getToken(SqlBaseParser.GROUPING, 0); }
		public TerminalNode HAVING() { return getToken(SqlBaseParser.HAVING, 0); }
		public TerminalNode HOUR() { return getToken(SqlBaseParser.HOUR, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode IGNORE() { return getToken(SqlBaseParser.IGNORE, 0); }
		public TerminalNode IMPORT() { return getToken(SqlBaseParser.IMPORT, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public TerminalNode INDEX() { return getToken(SqlBaseParser.INDEX, 0); }
		public TerminalNode INDEXES() { return getToken(SqlBaseParser.INDEXES, 0); }
		public TerminalNode INPATH() { return getToken(SqlBaseParser.INPATH, 0); }
		public TerminalNode INPUTFORMAT() { return getToken(SqlBaseParser.INPUTFORMAT, 0); }
		public TerminalNode INSERT() { return getToken(SqlBaseParser.INSERT, 0); }
		public TerminalNode INTERVAL() { return getToken(SqlBaseParser.INTERVAL, 0); }
		public TerminalNode INTO() { return getToken(SqlBaseParser.INTO, 0); }
		public TerminalNode IS() { return getToken(SqlBaseParser.IS, 0); }
		public TerminalNode ITEMS() { return getToken(SqlBaseParser.ITEMS, 0); }
		public TerminalNode KEYS() { return getToken(SqlBaseParser.KEYS, 0); }
		public TerminalNode LAST() { return getToken(SqlBaseParser.LAST, 0); }
		public TerminalNode LAZY() { return getToken(SqlBaseParser.LAZY, 0); }
		public TerminalNode LEADING() { return getToken(SqlBaseParser.LEADING, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public TerminalNode ILIKE() { return getToken(SqlBaseParser.ILIKE, 0); }
		public TerminalNode LIMIT() { return getToken(SqlBaseParser.LIMIT, 0); }
		public TerminalNode LINES() { return getToken(SqlBaseParser.LINES, 0); }
		public TerminalNode LIST() { return getToken(SqlBaseParser.LIST, 0); }
		public TerminalNode LOAD() { return getToken(SqlBaseParser.LOAD, 0); }
		public TerminalNode LOCAL() { return getToken(SqlBaseParser.LOCAL, 0); }
		public TerminalNode LOCATION() { return getToken(SqlBaseParser.LOCATION, 0); }
		public TerminalNode LOCK() { return getToken(SqlBaseParser.LOCK, 0); }
		public TerminalNode LOCKS() { return getToken(SqlBaseParser.LOCKS, 0); }
		public TerminalNode LOGICAL() { return getToken(SqlBaseParser.LOGICAL, 0); }
		public TerminalNode MACRO() { return getToken(SqlBaseParser.MACRO, 0); }
		public TerminalNode MAP() { return getToken(SqlBaseParser.MAP, 0); }
		public TerminalNode MATCHED() { return getToken(SqlBaseParser.MATCHED, 0); }
		public TerminalNode MERGE() { return getToken(SqlBaseParser.MERGE, 0); }
		public TerminalNode MICROSECOND() { return getToken(SqlBaseParser.MICROSECOND, 0); }
		public TerminalNode MILLISECOND() { return getToken(SqlBaseParser.MILLISECOND, 0); }
		public TerminalNode MINUTE() { return getToken(SqlBaseParser.MINUTE, 0); }
		public TerminalNode MONTH() { return getToken(SqlBaseParser.MONTH, 0); }
		public TerminalNode MSCK() { return getToken(SqlBaseParser.MSCK, 0); }
		public TerminalNode NAMESPACE() { return getToken(SqlBaseParser.NAMESPACE, 0); }
		public TerminalNode NAMESPACES() { return getToken(SqlBaseParser.NAMESPACES, 0); }
		public TerminalNode NO() { return getToken(SqlBaseParser.NO, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public TerminalNode NULLS() { return getToken(SqlBaseParser.NULLS, 0); }
		public TerminalNode OF() { return getToken(SqlBaseParser.OF, 0); }
		public TerminalNode ONLY() { return getToken(SqlBaseParser.ONLY, 0); }
		public TerminalNode OPTION() { return getToken(SqlBaseParser.OPTION, 0); }
		public TerminalNode OPTIONS() { return getToken(SqlBaseParser.OPTIONS, 0); }
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public TerminalNode ORDER() { return getToken(SqlBaseParser.ORDER, 0); }
		public TerminalNode OUT() { return getToken(SqlBaseParser.OUT, 0); }
		public TerminalNode OUTER() { return getToken(SqlBaseParser.OUTER, 0); }
		public TerminalNode OUTPUTFORMAT() { return getToken(SqlBaseParser.OUTPUTFORMAT, 0); }
		public TerminalNode OVER() { return getToken(SqlBaseParser.OVER, 0); }
		public TerminalNode OVERLAPS() { return getToken(SqlBaseParser.OVERLAPS, 0); }
		public TerminalNode OVERLAY() { return getToken(SqlBaseParser.OVERLAY, 0); }
		public TerminalNode OVERWRITE() { return getToken(SqlBaseParser.OVERWRITE, 0); }
		public TerminalNode PARTITION() { return getToken(SqlBaseParser.PARTITION, 0); }
		public TerminalNode PARTITIONED() { return getToken(SqlBaseParser.PARTITIONED, 0); }
		public TerminalNode PARTITIONS() { return getToken(SqlBaseParser.PARTITIONS, 0); }
		public TerminalNode PERCENTILE_CONT() { return getToken(SqlBaseParser.PERCENTILE_CONT, 0); }
		public TerminalNode PERCENTILE_DISC() { return getToken(SqlBaseParser.PERCENTILE_DISC, 0); }
		public TerminalNode PERCENTLIT() { return getToken(SqlBaseParser.PERCENTLIT, 0); }
		public TerminalNode PIVOT() { return getToken(SqlBaseParser.PIVOT, 0); }
		public TerminalNode PLACING() { return getToken(SqlBaseParser.PLACING, 0); }
		public TerminalNode POSITION() { return getToken(SqlBaseParser.POSITION, 0); }
		public TerminalNode PRECEDING() { return getToken(SqlBaseParser.PRECEDING, 0); }
		public TerminalNode PRIMARY() { return getToken(SqlBaseParser.PRIMARY, 0); }
		public TerminalNode PRINCIPALS() { return getToken(SqlBaseParser.PRINCIPALS, 0); }
		public TerminalNode PROPERTIES() { return getToken(SqlBaseParser.PROPERTIES, 0); }
		public TerminalNode PURGE() { return getToken(SqlBaseParser.PURGE, 0); }
		public TerminalNode QUARTER() { return getToken(SqlBaseParser.QUARTER, 0); }
		public TerminalNode QUERY() { return getToken(SqlBaseParser.QUERY, 0); }
		public TerminalNode RANGE() { return getToken(SqlBaseParser.RANGE, 0); }
		public TerminalNode RECORDREADER() { return getToken(SqlBaseParser.RECORDREADER, 0); }
		public TerminalNode RECORDWRITER() { return getToken(SqlBaseParser.RECORDWRITER, 0); }
		public TerminalNode RECOVER() { return getToken(SqlBaseParser.RECOVER, 0); }
		public TerminalNode REDUCE() { return getToken(SqlBaseParser.REDUCE, 0); }
		public TerminalNode REFERENCES() { return getToken(SqlBaseParser.REFERENCES, 0); }
		public TerminalNode REFRESH() { return getToken(SqlBaseParser.REFRESH, 0); }
		public TerminalNode RENAME() { return getToken(SqlBaseParser.RENAME, 0); }
		public TerminalNode REPAIR() { return getToken(SqlBaseParser.REPAIR, 0); }
		public TerminalNode REPEATABLE() { return getToken(SqlBaseParser.REPEATABLE, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode RESET() { return getToken(SqlBaseParser.RESET, 0); }
		public TerminalNode RESPECT() { return getToken(SqlBaseParser.RESPECT, 0); }
		public TerminalNode RESTRICT() { return getToken(SqlBaseParser.RESTRICT, 0); }
		public TerminalNode REVOKE() { return getToken(SqlBaseParser.REVOKE, 0); }
		public TerminalNode RLIKE() { return getToken(SqlBaseParser.RLIKE, 0); }
		public TerminalNode ROLE() { return getToken(SqlBaseParser.ROLE, 0); }
		public TerminalNode ROLES() { return getToken(SqlBaseParser.ROLES, 0); }
		public TerminalNode ROLLBACK() { return getToken(SqlBaseParser.ROLLBACK, 0); }
		public TerminalNode ROLLUP() { return getToken(SqlBaseParser.ROLLUP, 0); }
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public TerminalNode ROWS() { return getToken(SqlBaseParser.ROWS, 0); }
		public TerminalNode SCHEMA() { return getToken(SqlBaseParser.SCHEMA, 0); }
		public TerminalNode SCHEMAS() { return getToken(SqlBaseParser.SCHEMAS, 0); }
		public TerminalNode SECOND() { return getToken(SqlBaseParser.SECOND, 0); }
		public TerminalNode SELECT() { return getToken(SqlBaseParser.SELECT, 0); }
		public TerminalNode SEPARATED() { return getToken(SqlBaseParser.SEPARATED, 0); }
		public TerminalNode SERDE() { return getToken(SqlBaseParser.SERDE, 0); }
		public TerminalNode SERDEPROPERTIES() { return getToken(SqlBaseParser.SERDEPROPERTIES, 0); }
		public TerminalNode SESSION_USER() { return getToken(SqlBaseParser.SESSION_USER, 0); }
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode SETS() { return getToken(SqlBaseParser.SETS, 0); }
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode SKEWED() { return getToken(SqlBaseParser.SKEWED, 0); }
		public TerminalNode SOME() { return getToken(SqlBaseParser.SOME, 0); }
		public TerminalNode SORT() { return getToken(SqlBaseParser.SORT, 0); }
		public TerminalNode SORTED() { return getToken(SqlBaseParser.SORTED, 0); }
		public TerminalNode START() { return getToken(SqlBaseParser.START, 0); }
		public TerminalNode STATISTICS() { return getToken(SqlBaseParser.STATISTICS, 0); }
		public TerminalNode STORED() { return getToken(SqlBaseParser.STORED, 0); }
		public TerminalNode STRATIFY() { return getToken(SqlBaseParser.STRATIFY, 0); }
		public TerminalNode STRUCT() { return getToken(SqlBaseParser.STRUCT, 0); }
		public TerminalNode SUBSTR() { return getToken(SqlBaseParser.SUBSTR, 0); }
		public TerminalNode SUBSTRING() { return getToken(SqlBaseParser.SUBSTRING, 0); }
		public TerminalNode SYNC() { return getToken(SqlBaseParser.SYNC, 0); }
		public TerminalNode SYSTEM_TIME() { return getToken(SqlBaseParser.SYSTEM_TIME, 0); }
		public TerminalNode SYSTEM_VERSION() { return getToken(SqlBaseParser.SYSTEM_VERSION, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode TABLES() { return getToken(SqlBaseParser.TABLES, 0); }
		public TerminalNode TABLESAMPLE() { return getToken(SqlBaseParser.TABLESAMPLE, 0); }
		public TerminalNode TBLPROPERTIES() { return getToken(SqlBaseParser.TBLPROPERTIES, 0); }
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode TERMINATED() { return getToken(SqlBaseParser.TERMINATED, 0); }
		public TerminalNode THEN() { return getToken(SqlBaseParser.THEN, 0); }
		public TerminalNode TIME() { return getToken(SqlBaseParser.TIME, 0); }
		public TerminalNode TIMESTAMP() { return getToken(SqlBaseParser.TIMESTAMP, 0); }
		public TerminalNode TIMESTAMPADD() { return getToken(SqlBaseParser.TIMESTAMPADD, 0); }
		public TerminalNode TIMESTAMPDIFF() { return getToken(SqlBaseParser.TIMESTAMPDIFF, 0); }
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public TerminalNode TOUCH() { return getToken(SqlBaseParser.TOUCH, 0); }
		public TerminalNode TRAILING() { return getToken(SqlBaseParser.TRAILING, 0); }
		public TerminalNode TRANSACTION() { return getToken(SqlBaseParser.TRANSACTION, 0); }
		public TerminalNode TRANSACTIONS() { return getToken(SqlBaseParser.TRANSACTIONS, 0); }
		public TerminalNode TRANSFORM() { return getToken(SqlBaseParser.TRANSFORM, 0); }
		public TerminalNode TRIM() { return getToken(SqlBaseParser.TRIM, 0); }
		public TerminalNode TRUE() { return getToken(SqlBaseParser.TRUE, 0); }
		public TerminalNode TRUNCATE() { return getToken(SqlBaseParser.TRUNCATE, 0); }
		public TerminalNode TRY_CAST() { return getToken(SqlBaseParser.TRY_CAST, 0); }
		public TerminalNode TYPE() { return getToken(SqlBaseParser.TYPE, 0); }
		public TerminalNode UNARCHIVE() { return getToken(SqlBaseParser.UNARCHIVE, 0); }
		public TerminalNode UNBOUNDED() { return getToken(SqlBaseParser.UNBOUNDED, 0); }
		public TerminalNode UNCACHE() { return getToken(SqlBaseParser.UNCACHE, 0); }
		public TerminalNode UNIQUE() { return getToken(SqlBaseParser.UNIQUE, 0); }
		public TerminalNode UNKNOWN() { return getToken(SqlBaseParser.UNKNOWN, 0); }
		public TerminalNode UNLOCK() { return getToken(SqlBaseParser.UNLOCK, 0); }
		public TerminalNode UNSET() { return getToken(SqlBaseParser.UNSET, 0); }
		public TerminalNode UPDATE() { return getToken(SqlBaseParser.UPDATE, 0); }
		public TerminalNode USE() { return getToken(SqlBaseParser.USE, 0); }
		public TerminalNode USER() { return getToken(SqlBaseParser.USER, 0); }
		public TerminalNode VALUES() { return getToken(SqlBaseParser.VALUES, 0); }
		public TerminalNode VERSION() { return getToken(SqlBaseParser.VERSION, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TerminalNode VIEWS() { return getToken(SqlBaseParser.VIEWS, 0); }
		public TerminalNode WEEK() { return getToken(SqlBaseParser.WEEK, 0); }
		public TerminalNode WHEN() { return getToken(SqlBaseParser.WHEN, 0); }
		public TerminalNode WHERE() { return getToken(SqlBaseParser.WHERE, 0); }
		public TerminalNode WINDOW() { return getToken(SqlBaseParser.WINDOW, 0); }
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode WITHIN() { return getToken(SqlBaseParser.WITHIN, 0); }
		public TerminalNode YEAR() { return getToken(SqlBaseParser.YEAR, 0); }
		public TerminalNode ZONE() { return getToken(SqlBaseParser.ZONE, 0); }
		public NonReservedContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nonReserved; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).enterNonReserved(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseParserListener ) ((SqlBaseParserListener)listener).exitNonReserved(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseParserVisitor ) return ((SqlBaseParserVisitor<? extends T>)visitor).visitNonReserved(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NonReservedContext nonReserved() throws RecognitionException {
		NonReservedContext _localctx = new NonReservedContext(_ctx, getState());
		enterRule(_localctx, 296, RULE_nonReserved);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(3331);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & -4503599627387136L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & 8917126712437243903L) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & -17448304677L) != 0) || ((((_la - 192)) & ~0x3f) == 0 && ((1L << (_la - 192)) & -4362092545L) != 0) || ((((_la - 256)) & ~0x3f) == 0 && ((1L << (_la - 256)) & 2147220479L) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
		switch (ruleIndex) {
		case 43:
			return queryTerm_sempred((QueryTermContext)_localctx, predIndex);
		case 106:
			return booleanExpression_sempred((BooleanExpressionContext)_localctx, predIndex);
		case 108:
			return valueExpression_sempred((ValueExpressionContext)_localctx, predIndex);
		case 110:
			return primaryExpression_sempred((PrimaryExpressionContext)_localctx, predIndex);
		case 141:
			return identifier_sempred((IdentifierContext)_localctx, predIndex);
		case 142:
			return strictIdentifier_sempred((StrictIdentifierContext)_localctx, predIndex);
		case 144:
			return number_sempred((NumberContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean queryTerm_sempred(QueryTermContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0:
			return precpred(_ctx, 3);
		case 1:
			return legacy_setops_precedence_enabled;
		case 2:
			return precpred(_ctx, 2);
		case 3:
			return !legacy_setops_precedence_enabled;
		case 4:
			return precpred(_ctx, 1);
		case 5:
			return !legacy_setops_precedence_enabled;
		}
		return true;
	}
	private boolean booleanExpression_sempred(BooleanExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 6:
			return precpred(_ctx, 2);
		case 7:
			return precpred(_ctx, 1);
		}
		return true;
	}
	private boolean valueExpression_sempred(ValueExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 8:
			return precpred(_ctx, 6);
		case 9:
			return precpred(_ctx, 5);
		case 10:
			return precpred(_ctx, 4);
		case 11:
			return precpred(_ctx, 3);
		case 12:
			return precpred(_ctx, 2);
		case 13:
			return precpred(_ctx, 1);
		}
		return true;
	}
	private boolean primaryExpression_sempred(PrimaryExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 14:
			return precpred(_ctx, 9);
		case 15:
			return precpred(_ctx, 7);
		}
		return true;
	}
	private boolean identifier_sempred(IdentifierContext _localctx, int predIndex) {
		switch (predIndex) {
		case 16:
			return !SQL_standard_keyword_behavior;
		}
		return true;
	}
	private boolean strictIdentifier_sempred(StrictIdentifierContext _localctx, int predIndex) {
		switch (predIndex) {
		case 17:
			return SQL_standard_keyword_behavior;
		case 18:
			return !SQL_standard_keyword_behavior;
		}
		return true;
	}
	private boolean number_sempred(NumberContext _localctx, int predIndex) {
		switch (predIndex) {
		case 19:
			return !legacy_exponent_literal_as_decimal_enabled;
		case 20:
			return !legacy_exponent_literal_as_decimal_enabled;
		case 21:
			return legacy_exponent_literal_as_decimal_enabled;
		}
		return true;
	}

	private static final String _serializedATNSegment0 =
		"\u0004\u0001\u0144\u0d06\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001"+
		"\u0002\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004\u0007\u0004"+
		"\u0002\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007\u0007\u0007"+
		"\u0002\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002\u000b\u0007\u000b"+
		"\u0002\f\u0007\f\u0002\r\u0007\r\u0002\u000e\u0007\u000e\u0002\u000f\u0007"+
		"\u000f\u0002\u0010\u0007\u0010\u0002\u0011\u0007\u0011\u0002\u0012\u0007"+
		"\u0012\u0002\u0013\u0007\u0013\u0002\u0014\u0007\u0014\u0002\u0015\u0007"+
		"\u0015\u0002\u0016\u0007\u0016\u0002\u0017\u0007\u0017\u0002\u0018\u0007"+
		"\u0018\u0002\u0019\u0007\u0019\u0002\u001a\u0007\u001a\u0002\u001b\u0007"+
		"\u001b\u0002\u001c\u0007\u001c\u0002\u001d\u0007\u001d\u0002\u001e\u0007"+
		"\u001e\u0002\u001f\u0007\u001f\u0002 \u0007 \u0002!\u0007!\u0002\"\u0007"+
		"\"\u0002#\u0007#\u0002$\u0007$\u0002%\u0007%\u0002&\u0007&\u0002\'\u0007"+
		"\'\u0002(\u0007(\u0002)\u0007)\u0002*\u0007*\u0002+\u0007+\u0002,\u0007"+
		",\u0002-\u0007-\u0002.\u0007.\u0002/\u0007/\u00020\u00070\u00021\u0007"+
		"1\u00022\u00072\u00023\u00073\u00024\u00074\u00025\u00075\u00026\u0007"+
		"6\u00027\u00077\u00028\u00078\u00029\u00079\u0002:\u0007:\u0002;\u0007"+
		";\u0002<\u0007<\u0002=\u0007=\u0002>\u0007>\u0002?\u0007?\u0002@\u0007"+
		"@\u0002A\u0007A\u0002B\u0007B\u0002C\u0007C\u0002D\u0007D\u0002E\u0007"+
		"E\u0002F\u0007F\u0002G\u0007G\u0002H\u0007H\u0002I\u0007I\u0002J\u0007"+
		"J\u0002K\u0007K\u0002L\u0007L\u0002M\u0007M\u0002N\u0007N\u0002O\u0007"+
		"O\u0002P\u0007P\u0002Q\u0007Q\u0002R\u0007R\u0002S\u0007S\u0002T\u0007"+
		"T\u0002U\u0007U\u0002V\u0007V\u0002W\u0007W\u0002X\u0007X\u0002Y\u0007"+
		"Y\u0002Z\u0007Z\u0002[\u0007[\u0002\\\u0007\\\u0002]\u0007]\u0002^\u0007"+
		"^\u0002_\u0007_\u0002`\u0007`\u0002a\u0007a\u0002b\u0007b\u0002c\u0007"+
		"c\u0002d\u0007d\u0002e\u0007e\u0002f\u0007f\u0002g\u0007g\u0002h\u0007"+
		"h\u0002i\u0007i\u0002j\u0007j\u0002k\u0007k\u0002l\u0007l\u0002m\u0007"+
		"m\u0002n\u0007n\u0002o\u0007o\u0002p\u0007p\u0002q\u0007q\u0002r\u0007"+
		"r\u0002s\u0007s\u0002t\u0007t\u0002u\u0007u\u0002v\u0007v\u0002w\u0007"+
		"w\u0002x\u0007x\u0002y\u0007y\u0002z\u0007z\u0002{\u0007{\u0002|\u0007"+
		"|\u0002}\u0007}\u0002~\u0007~\u0002\u007f\u0007\u007f\u0002\u0080\u0007"+
		"\u0080\u0002\u0081\u0007\u0081\u0002\u0082\u0007\u0082\u0002\u0083\u0007"+
		"\u0083\u0002\u0084\u0007\u0084\u0002\u0085\u0007\u0085\u0002\u0086\u0007"+
		"\u0086\u0002\u0087\u0007\u0087\u0002\u0088\u0007\u0088\u0002\u0089\u0007"+
		"\u0089\u0002\u008a\u0007\u008a\u0002\u008b\u0007\u008b\u0002\u008c\u0007"+
		"\u008c\u0002\u008d\u0007\u008d\u0002\u008e\u0007\u008e\u0002\u008f\u0007"+
		"\u008f\u0002\u0090\u0007\u0090\u0002\u0091\u0007\u0091\u0002\u0092\u0007"+
		"\u0092\u0002\u0093\u0007\u0093\u0002\u0094\u0007\u0094\u0001\u0000\u0001"+
		"\u0000\u0005\u0000\u012d\b\u0000\n\u0000\f\u0000\u0130\t\u0000\u0001\u0000"+
		"\u0001\u0000\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0002\u0001\u0002"+
		"\u0001\u0002\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0004\u0001\u0004"+
		"\u0001\u0004\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0006\u0001\u0006"+
		"\u0001\u0006\u0001\u0007\u0001\u0007\u0003\u0007\u0148\b\u0007\u0001\u0007"+
		"\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007"+
		"\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u0155\b\u0007"+
		"\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007"+
		"\u015c\b\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007"+
		"\u0001\u0007\u0005\u0007\u0164\b\u0007\n\u0007\f\u0007\u0167\t\u0007\u0001"+
		"\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001"+
		"\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001"+
		"\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u017a"+
		"\b\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u017e\b\u0007\u0001\u0007"+
		"\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u0184\b\u0007\u0001\u0007"+
		"\u0003\u0007\u0187\b\u0007\u0001\u0007\u0003\u0007\u018a\b\u0007\u0001"+
		"\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u0191"+
		"\b\u0007\u0001\u0007\u0003\u0007\u0194\b\u0007\u0001\u0007\u0001\u0007"+
		"\u0003\u0007\u0198\b\u0007\u0001\u0007\u0003\u0007\u019b\b\u0007\u0001"+
		"\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u01a2"+
		"\b\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001"+
		"\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0005\u0007\u01ad\b\u0007\n"+
		"\u0007\f\u0007\u01b0\t\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001"+
		"\u0007\u0001\u0007\u0003\u0007\u01b7\b\u0007\u0001\u0007\u0003\u0007\u01ba"+
		"\b\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u01be\b\u0007\u0001\u0007"+
		"\u0003\u0007\u01c1\b\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007"+
		"\u0003\u0007\u01c7\b\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007"+
		"\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007"+
		"\u01d2\b\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007"+
		"\u01d8\b\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u01dd\b"+
		"\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001"+
		"\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001"+
		"\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001"+
		"\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001"+
		"\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001"+
		"\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u01ff\b\u0007\u0001\u0007\u0001"+
		"\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001"+
		"\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u020c\b\u0007\u0001"+
		"\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001"+
		"\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001"+
		"\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001"+
		"\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u0225"+
		"\b\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001"+
		"\u0007\u0001\u0007\u0003\u0007\u022e\b\u0007\u0001\u0007\u0001\u0007\u0003"+
		"\u0007\u0232\b\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0003"+
		"\u0007\u0238\b\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u023c\b\u0007"+
		"\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u0241\b\u0007\u0001\u0007"+
		"\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u0247\b\u0007\u0001\u0007"+
		"\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007"+
		"\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u0253\b\u0007\u0001\u0007"+
		"\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007"+
		"\u025b\b\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007"+
		"\u0261\b\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007"+
		"\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007"+
		"\u0003\u0007\u026e\b\u0007\u0001\u0007\u0004\u0007\u0271\b\u0007\u000b"+
		"\u0007\f\u0007\u0272\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001"+
		"\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001"+
		"\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u0283\b\u0007\u0001"+
		"\u0007\u0001\u0007\u0001\u0007\u0005\u0007\u0288\b\u0007\n\u0007\f\u0007"+
		"\u028b\t\u0007\u0001\u0007\u0003\u0007\u028e\b\u0007\u0001\u0007\u0001"+
		"\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u0294\b\u0007\u0001\u0007\u0001"+
		"\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001"+
		"\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0003"+
		"\u0007\u02a3\b\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u02a7\b\u0007"+
		"\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u02ad\b\u0007"+
		"\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u02b3\b\u0007"+
		"\u0001\u0007\u0003\u0007\u02b6\b\u0007\u0001\u0007\u0003\u0007\u02b9\b"+
		"\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u02bf"+
		"\b\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u02c3\b\u0007\u0001\u0007"+
		"\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0005\u0007"+
		"\u02cb\b\u0007\n\u0007\f\u0007\u02ce\t\u0007\u0001\u0007\u0001\u0007\u0001"+
		"\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u02d6\b\u0007\u0001"+
		"\u0007\u0003\u0007\u02d9\b\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001"+
		"\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u02e2\b\u0007\u0001"+
		"\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u02e7\b\u0007\u0001\u0007\u0001"+
		"\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u02ed\b\u0007\u0001\u0007\u0001"+
		"\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u02f4\b\u0007\u0001"+
		"\u0007\u0003\u0007\u02f7\b\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001"+
		"\u0007\u0003\u0007\u02fd\b\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001"+
		"\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0005\u0007\u0306\b\u0007\n"+
		"\u0007\f\u0007\u0309\t\u0007\u0003\u0007\u030b\b\u0007\u0001\u0007\u0001"+
		"\u0007\u0003\u0007\u030f\b\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0003"+
		"\u0007\u0314\b\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u0319"+
		"\b\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0003"+
		"\u0007\u0320\b\u0007\u0001\u0007\u0003\u0007\u0323\b\u0007\u0001\u0007"+
		"\u0003\u0007\u0326\b\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007"+
		"\u0001\u0007\u0003\u0007\u032d\b\u0007\u0001\u0007\u0001\u0007\u0001\u0007"+
		"\u0003\u0007\u0332\b\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007"+
		"\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u033b\b\u0007\u0001\u0007"+
		"\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007"+
		"\u0343\b\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007"+
		"\u0349\b\u0007\u0001\u0007\u0003\u0007\u034c\b\u0007\u0001\u0007\u0003"+
		"\u0007\u034f\b\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0003"+
		"\u0007\u0355\b\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u0359\b\u0007"+
		"\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u035e\b\u0007\u0001\u0007"+
		"\u0003\u0007\u0361\b\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u0365\b"+
		"\u0007\u0003\u0007\u0367\b\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001"+
		"\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u036f\b\u0007\u0001\u0007\u0001"+
		"\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u0377"+
		"\b\u0007\u0001\u0007\u0003\u0007\u037a\b\u0007\u0001\u0007\u0001\u0007"+
		"\u0001\u0007\u0003\u0007\u037f\b\u0007\u0001\u0007\u0001\u0007\u0001\u0007"+
		"\u0001\u0007\u0003\u0007\u0385\b\u0007\u0001\u0007\u0001\u0007\u0001\u0007"+
		"\u0001\u0007\u0003\u0007\u038b\b\u0007\u0001\u0007\u0003\u0007\u038e\b"+
		"\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u0392\b\u0007\u0001\u0007\u0003"+
		"\u0007\u0395\b\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u0399\b\u0007"+
		"\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007"+
		"\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007"+
		"\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007"+
		"\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007"+
		"\u0005\u0007\u03b3\b\u0007\n\u0007\f\u0007\u03b6\t\u0007\u0003\u0007\u03b8"+
		"\b\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u03bc\b\u0007\u0001\u0007"+
		"\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u03c2\b\u0007\u0001\u0007"+
		"\u0003\u0007\u03c5\b\u0007\u0001\u0007\u0003\u0007\u03c8\b\u0007\u0001"+
		"\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u03ce\b\u0007\u0001"+
		"\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0003"+
		"\u0007\u03d6\b\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u03db"+
		"\b\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u03e1"+
		"\b\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u03e7"+
		"\b\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001"+
		"\u0007\u0003\u0007\u03ef\b\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0005"+
		"\u0007\u03f4\b\u0007\n\u0007\f\u0007\u03f7\t\u0007\u0001\u0007\u0001\u0007"+
		"\u0001\u0007\u0005\u0007\u03fc\b\u0007\n\u0007\f\u0007\u03ff\t\u0007\u0001"+
		"\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001"+
		"\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0005"+
		"\u0007\u040d\b\u0007\n\u0007\f\u0007\u0410\t\u0007\u0001\u0007\u0001\u0007"+
		"\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007"+
		"\u0001\u0007\u0005\u0007\u041b\b\u0007\n\u0007\f\u0007\u041e\t\u0007\u0003"+
		"\u0007\u0420\b\u0007\u0001\u0007\u0001\u0007\u0005\u0007\u0424\b\u0007"+
		"\n\u0007\f\u0007\u0427\t\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001"+
		"\u0007\u0005\u0007\u042d\b\u0007\n\u0007\f\u0007\u0430\t\u0007\u0001\u0007"+
		"\u0001\u0007\u0001\u0007\u0001\u0007\u0005\u0007\u0436\b\u0007\n\u0007"+
		"\f\u0007\u0439\t\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007"+
		"\u0001\u0007\u0003\u0007\u0440\b\u0007\u0001\u0007\u0001\u0007\u0001\u0007"+
		"\u0003\u0007\u0445\b\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007"+
		"\u044a\b\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007"+
		"\u0003\u0007\u0451\b\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007"+
		"\u0003\u0007\u0457\b\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007"+
		"\u045c\b\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0005\u0007"+
		"\u0462\b\u0007\n\u0007\f\u0007\u0465\t\u0007\u0003\u0007\u0467\b\u0007"+
		"\u0001\b\u0001\b\u0001\t\u0001\t\u0001\n\u0001\n\u0001\n\u0001\n\u0001"+
		"\n\u0001\n\u0003\n\u0473\b\n\u0001\n\u0001\n\u0003\n\u0477\b\n\u0001\n"+
		"\u0001\n\u0001\n\u0001\n\u0001\n\u0003\n\u047e\b\n\u0001\n\u0001\n\u0001"+
		"\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001"+
		"\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001"+
		"\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001"+
		"\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001"+
		"\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001"+
		"\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001"+
		"\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001"+
		"\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001"+
		"\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001"+
		"\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001"+
		"\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001"+
		"\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001"+
		"\n\u0001\n\u0001\n\u0001\n\u0003\n\u04f2\b\n\u0001\n\u0001\n\u0001\n\u0001"+
		"\n\u0001\n\u0001\n\u0003\n\u04fa\b\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001"+
		"\n\u0001\n\u0003\n\u0502\b\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001"+
		"\n\u0001\n\u0003\n\u050b\b\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001"+
		"\n\u0001\n\u0001\n\u0003\n\u0515\b\n\u0001\u000b\u0001\u000b\u0003\u000b"+
		"\u0519\b\u000b\u0001\u000b\u0003\u000b\u051c\b\u000b\u0001\u000b\u0001"+
		"\u000b\u0001\u000b\u0001\u000b\u0003\u000b\u0522\b\u000b\u0001\u000b\u0001"+
		"\u000b\u0001\f\u0001\f\u0003\f\u0528\b\f\u0001\f\u0001\f\u0001\f\u0001"+
		"\f\u0001\r\u0001\r\u0001\r\u0001\r\u0001\r\u0001\r\u0003\r\u0534\b\r\u0001"+
		"\r\u0001\r\u0001\r\u0001\r\u0001\u000e\u0001\u000e\u0001\u000e\u0001\u000e"+
		"\u0001\u000e\u0001\u000e\u0003\u000e\u0540\b\u000e\u0001\u000e\u0001\u000e"+
		"\u0001\u000e\u0003\u000e\u0545\b\u000e\u0001\u000f\u0001\u000f\u0001\u000f"+
		"\u0001\u0010\u0001\u0010\u0001\u0010\u0001\u0011\u0003\u0011\u054e\b\u0011"+
		"\u0001\u0011\u0001\u0011\u0001\u0011\u0001\u0012\u0001\u0012\u0001\u0012"+
		"\u0003\u0012\u0556\b\u0012\u0001\u0012\u0001\u0012\u0001\u0012\u0001\u0012"+
		"\u0001\u0012\u0003\u0012\u055d\b\u0012\u0003\u0012\u055f\b\u0012\u0001"+
		"\u0012\u0003\u0012\u0562\b\u0012\u0001\u0012\u0001\u0012\u0001\u0012\u0003"+
		"\u0012\u0567\b\u0012\u0001\u0012\u0001\u0012\u0003\u0012\u056b\b\u0012"+
		"\u0001\u0012\u0001\u0012\u0001\u0012\u0003\u0012\u0570\b\u0012\u0001\u0012"+
		"\u0003\u0012\u0573\b\u0012\u0001\u0012\u0001\u0012\u0001\u0012\u0003\u0012"+
		"\u0578\b\u0012\u0001\u0012\u0001\u0012\u0001\u0012\u0003\u0012\u057d\b"+
		"\u0012\u0001\u0012\u0003\u0012\u0580\b\u0012\u0001\u0012\u0001\u0012\u0001"+
		"\u0012\u0003\u0012\u0585\b\u0012\u0001\u0012\u0001\u0012\u0003\u0012\u0589"+
		"\b\u0012\u0001\u0012\u0001\u0012\u0001\u0012\u0003\u0012\u058e\b\u0012"+
		"\u0003\u0012\u0590\b\u0012\u0001\u0013\u0001\u0013\u0003\u0013\u0594\b"+
		"\u0013\u0001\u0014\u0001\u0014\u0001\u0014\u0001\u0014\u0001\u0014\u0005"+
		"\u0014\u059b\b\u0014\n\u0014\f\u0014\u059e\t\u0014\u0001\u0014\u0001\u0014"+
		"\u0001\u0015\u0001\u0015\u0001\u0015\u0003\u0015\u05a5\b\u0015\u0001\u0016"+
		"\u0001\u0016\u0001\u0017\u0001\u0017\u0001\u0018\u0001\u0018\u0001\u0018"+
		"\u0001\u0018\u0001\u0018\u0003\u0018\u05b0\b\u0018\u0001\u0019\u0001\u0019"+
		"\u0001\u0019\u0005\u0019\u05b5\b\u0019\n\u0019\f\u0019\u05b8\t\u0019\u0001"+
		"\u001a\u0001\u001a\u0001\u001a\u0001\u001a\u0005\u001a\u05be\b\u001a\n"+
		"\u001a\f\u001a\u05c1\t\u001a\u0001\u001b\u0001\u001b\u0003\u001b\u05c5"+
		"\b\u001b\u0001\u001b\u0003\u001b\u05c8\b\u001b\u0001\u001b\u0001\u001b"+
		"\u0001\u001b\u0001\u001b\u0001\u001c\u0001\u001c\u0001\u001c\u0001\u001d"+
		"\u0001\u001d\u0001\u001d\u0001\u001d\u0001\u001d\u0001\u001d\u0001\u001d"+
		"\u0001\u001d\u0001\u001d\u0001\u001d\u0001\u001d\u0001\u001d\u0001\u001d"+
		"\u0005\u001d\u05de\b\u001d\n\u001d\f\u001d\u05e1\t\u001d\u0001\u001e\u0001"+
		"\u001e\u0001\u001e\u0001\u001e\u0005\u001e\u05e7\b\u001e\n\u001e\f\u001e"+
		"\u05ea\t\u001e\u0001\u001e\u0001\u001e\u0001\u001f\u0001\u001f\u0003\u001f"+
		"\u05f0\b\u001f\u0001\u001f\u0003\u001f\u05f3\b\u001f\u0001 \u0001 \u0001"+
		" \u0005 \u05f8\b \n \f \u05fb\t \u0001 \u0003 \u05fe\b \u0001!\u0001!"+
		"\u0001!\u0001!\u0003!\u0604\b!\u0001\"\u0001\"\u0001\"\u0001\"\u0005\""+
		"\u060a\b\"\n\"\f\"\u060d\t\"\u0001\"\u0001\"\u0001#\u0001#\u0001#\u0001"+
		"#\u0005#\u0615\b#\n#\f#\u0618\t#\u0001#\u0001#\u0001$\u0001$\u0001$\u0001"+
		"$\u0001$\u0001$\u0003$\u0622\b$\u0001%\u0001%\u0001%\u0001%\u0001%\u0003"+
		"%\u0629\b%\u0001&\u0001&\u0001&\u0001&\u0003&\u062f\b&\u0001\'\u0001\'"+
		"\u0001\'\u0001(\u0001(\u0001(\u0001(\u0001(\u0004(\u0639\b(\u000b(\f("+
		"\u063a\u0001(\u0001(\u0001(\u0001(\u0001(\u0001(\u0001(\u0001(\u0003("+
		"\u0645\b(\u0001(\u0001(\u0001(\u0001(\u0001(\u0001(\u0003(\u064d\b(\u0001"+
		"(\u0003(\u0650\b(\u0001(\u0001(\u0001(\u0001(\u0001(\u0001(\u0001(\u0003"+
		"(\u0659\b(\u0001(\u0001(\u0001(\u0001(\u0001(\u0001(\u0003(\u0661\b(\u0001"+
		"(\u0001(\u0003(\u0665\b(\u0001(\u0001(\u0001(\u0001(\u0001(\u0001(\u0001"+
		"(\u0001(\u0001(\u0001(\u0003(\u0671\b(\u0001(\u0001(\u0001(\u0001(\u0003"+
		"(\u0677\b(\u0001(\u0003(\u067a\b(\u0001(\u0003(\u067d\b(\u0001(\u0003"+
		"(\u0680\b(\u0003(\u0682\b(\u0001)\u0001)\u0001)\u0001)\u0001)\u0005)\u0689"+
		"\b)\n)\f)\u068c\t)\u0003)\u068e\b)\u0001)\u0001)\u0001)\u0001)\u0001)"+
		"\u0005)\u0695\b)\n)\f)\u0698\t)\u0003)\u069a\b)\u0001)\u0001)\u0001)\u0001"+
		")\u0001)\u0005)\u06a1\b)\n)\f)\u06a4\t)\u0003)\u06a6\b)\u0001)\u0001)"+
		"\u0001)\u0001)\u0001)\u0005)\u06ad\b)\n)\f)\u06b0\t)\u0003)\u06b2\b)\u0001"+
		")\u0003)\u06b5\b)\u0001)\u0001)\u0001)\u0003)\u06ba\b)\u0003)\u06bc\b"+
		")\u0001*\u0001*\u0001*\u0001+\u0001+\u0001+\u0001+\u0001+\u0001+\u0001"+
		"+\u0003+\u06c8\b+\u0001+\u0001+\u0001+\u0001+\u0001+\u0003+\u06cf\b+\u0001"+
		"+\u0001+\u0001+\u0001+\u0001+\u0003+\u06d6\b+\u0001+\u0005+\u06d9\b+\n"+
		"+\f+\u06dc\t+\u0001,\u0001,\u0001,\u0001,\u0001,\u0001,\u0001,\u0001,"+
		"\u0001,\u0003,\u06e7\b,\u0001-\u0001-\u0003-\u06eb\b-\u0001-\u0001-\u0003"+
		"-\u06ef\b-\u0001.\u0001.\u0004.\u06f3\b.\u000b.\f.\u06f4\u0001/\u0001"+
		"/\u0003/\u06f9\b/\u0001/\u0001/\u0001/\u0001/\u0005/\u06ff\b/\n/\f/\u0702"+
		"\t/\u0001/\u0003/\u0705\b/\u0001/\u0003/\u0708\b/\u0001/\u0003/\u070b"+
		"\b/\u0001/\u0003/\u070e\b/\u0001/\u0001/\u0003/\u0712\b/\u00010\u0001"+
		"0\u00030\u0716\b0\u00010\u00050\u0719\b0\n0\f0\u071c\t0\u00010\u00030"+
		"\u071f\b0\u00010\u00030\u0722\b0\u00010\u00030\u0725\b0\u00010\u00030"+
		"\u0728\b0\u00010\u00010\u00030\u072c\b0\u00010\u00050\u072f\b0\n0\f0\u0732"+
		"\t0\u00010\u00030\u0735\b0\u00010\u00030\u0738\b0\u00010\u00030\u073b"+
		"\b0\u00010\u00030\u073e\b0\u00030\u0740\b0\u00011\u00011\u00011\u0001"+
		"1\u00031\u0746\b1\u00011\u00011\u00011\u00011\u00011\u00031\u074d\b1\u0001"+
		"1\u00011\u00011\u00031\u0752\b1\u00011\u00031\u0755\b1\u00011\u00031\u0758"+
		"\b1\u00011\u00011\u00031\u075c\b1\u00011\u00011\u00011\u00011\u00011\u0001"+
		"1\u00011\u00011\u00031\u0766\b1\u00011\u00011\u00031\u076a\b1\u00031\u076c"+
		"\b1\u00011\u00031\u076f\b1\u00011\u00011\u00031\u0773\b1\u00012\u0001"+
		"2\u00052\u0777\b2\n2\f2\u077a\t2\u00012\u00032\u077d\b2\u00012\u00012"+
		"\u00032\u0781\b2\u00013\u00013\u00013\u00013\u00053\u0787\b3\n3\f3\u078a"+
		"\t3\u00014\u00014\u00014\u00015\u00015\u00015\u00015\u00035\u0793\b5\u0001"+
		"5\u00015\u00015\u00016\u00016\u00016\u00016\u00016\u00036\u079d\b6\u0001"+
		"6\u00016\u00016\u00017\u00017\u00017\u00017\u00017\u00017\u00017\u0001"+
		"7\u00017\u00037\u07ab\b7\u00037\u07ad\b7\u00018\u00018\u00018\u00018\u0001"+
		"8\u00018\u00018\u00018\u00018\u00018\u00018\u00058\u07ba\b8\n8\f8\u07bd"+
		"\t8\u00018\u00018\u00038\u07c1\b8\u00019\u00019\u00019\u00059\u07c6\b"+
		"9\n9\f9\u07c9\t9\u0001:\u0001:\u0001:\u0001:\u0001;\u0001;\u0001;\u0001"+
		"<\u0001<\u0001<\u0001=\u0001=\u0001=\u0003=\u07d8\b=\u0001=\u0005=\u07db"+
		"\b=\n=\f=\u07de\t=\u0001=\u0001=\u0001>\u0001>\u0001>\u0001>\u0001>\u0001"+
		">\u0005>\u07e8\b>\n>\f>\u07eb\t>\u0001>\u0001>\u0003>\u07ef\b>\u0001?"+
		"\u0001?\u0001?\u0001?\u0005?\u07f5\b?\n?\f?\u07f8\t?\u0001?\u0005?\u07fb"+
		"\b?\n?\f?\u07fe\t?\u0001?\u0003?\u0801\b?\u0001@\u0003@\u0804\b@\u0001"+
		"@\u0001@\u0001@\u0001@\u0001@\u0003@\u080b\b@\u0001@\u0001@\u0001@\u0001"+
		"@\u0003@\u0811\b@\u0001A\u0001A\u0001A\u0001A\u0001A\u0005A\u0818\bA\n"+
		"A\fA\u081b\tA\u0001A\u0001A\u0001A\u0001A\u0001A\u0005A\u0822\bA\nA\f"+
		"A\u0825\tA\u0001A\u0001A\u0001A\u0001A\u0001A\u0001A\u0001A\u0001A\u0001"+
		"A\u0001A\u0005A\u0831\bA\nA\fA\u0834\tA\u0001A\u0001A\u0003A\u0838\bA"+
		"\u0003A\u083a\bA\u0001B\u0001B\u0003B\u083e\bB\u0001C\u0001C\u0001C\u0001"+
		"C\u0001C\u0005C\u0845\bC\nC\fC\u0848\tC\u0001C\u0001C\u0001C\u0001C\u0001"+
		"C\u0001C\u0001C\u0001C\u0005C\u0852\bC\nC\fC\u0855\tC\u0001C\u0001C\u0003"+
		"C\u0859\bC\u0001D\u0001D\u0003D\u085d\bD\u0001E\u0001E\u0001E\u0001E\u0005"+
		"E\u0863\bE\nE\fE\u0866\tE\u0003E\u0868\bE\u0001E\u0001E\u0003E\u086c\b"+
		"E\u0001F\u0001F\u0001F\u0001F\u0001F\u0001F\u0001F\u0001F\u0001F\u0001"+
		"F\u0005F\u0878\bF\nF\fF\u087b\tF\u0001F\u0001F\u0001F\u0001G\u0001G\u0001"+
		"G\u0001G\u0001G\u0005G\u0885\bG\nG\fG\u0888\tG\u0001G\u0001G\u0003G\u088c"+
		"\bG\u0001H\u0001H\u0003H\u0890\bH\u0001H\u0003H\u0893\bH\u0001I\u0001"+
		"I\u0001I\u0003I\u0898\bI\u0001I\u0001I\u0001I\u0001I\u0001I\u0005I\u089f"+
		"\bI\nI\fI\u08a2\tI\u0003I\u08a4\bI\u0001I\u0001I\u0001I\u0003I\u08a9\b"+
		"I\u0001I\u0001I\u0001I\u0005I\u08ae\bI\nI\fI\u08b1\tI\u0003I\u08b3\bI"+
		"\u0001J\u0001J\u0001K\u0003K\u08b8\bK\u0001K\u0001K\u0005K\u08bc\bK\n"+
		"K\fK\u08bf\tK\u0001L\u0001L\u0001L\u0003L\u08c4\bL\u0001L\u0001L\u0003"+
		"L\u08c8\bL\u0001L\u0001L\u0001L\u0001L\u0003L\u08ce\bL\u0001L\u0001L\u0003"+
		"L\u08d2\bL\u0001M\u0003M\u08d5\bM\u0001M\u0001M\u0001M\u0003M\u08da\b"+
		"M\u0001M\u0003M\u08dd\bM\u0001M\u0001M\u0001M\u0003M\u08e2\bM\u0001M\u0001"+
		"M\u0003M\u08e6\bM\u0001M\u0003M\u08e9\bM\u0001M\u0003M\u08ec\bM\u0001"+
		"N\u0001N\u0001N\u0001N\u0003N\u08f2\bN\u0001O\u0001O\u0001O\u0003O\u08f7"+
		"\bO\u0001O\u0001O\u0001O\u0001O\u0001O\u0003O\u08fe\bO\u0001P\u0003P\u0901"+
		"\bP\u0001P\u0001P\u0001P\u0001P\u0001P\u0001P\u0001P\u0001P\u0001P\u0001"+
		"P\u0001P\u0001P\u0001P\u0001P\u0001P\u0001P\u0003P\u0913\bP\u0003P\u0915"+
		"\bP\u0001P\u0003P\u0918\bP\u0001Q\u0001Q\u0001Q\u0001Q\u0001R\u0001R\u0001"+
		"R\u0005R\u0921\bR\nR\fR\u0924\tR\u0001S\u0001S\u0001S\u0001S\u0005S\u092a"+
		"\bS\nS\fS\u092d\tS\u0001S\u0001S\u0001T\u0001T\u0003T\u0933\bT\u0001U"+
		"\u0001U\u0001U\u0001U\u0005U\u0939\bU\nU\fU\u093c\tU\u0001U\u0001U\u0001"+
		"V\u0001V\u0003V\u0942\bV\u0001W\u0001W\u0003W\u0946\bW\u0001W\u0003W\u0949"+
		"\bW\u0001W\u0001W\u0001W\u0001W\u0001W\u0001W\u0003W\u0951\bW\u0001W\u0001"+
		"W\u0001W\u0001W\u0001W\u0001W\u0003W\u0959\bW\u0001W\u0001W\u0001W\u0001"+
		"W\u0003W\u095f\bW\u0001X\u0001X\u0001X\u0001X\u0005X\u0965\bX\nX\fX\u0968"+
		"\tX\u0001X\u0001X\u0001Y\u0001Y\u0001Y\u0001Y\u0001Y\u0005Y\u0971\bY\n"+
		"Y\fY\u0974\tY\u0003Y\u0976\bY\u0001Y\u0001Y\u0001Y\u0001Z\u0003Z\u097c"+
		"\bZ\u0001Z\u0001Z\u0003Z\u0980\bZ\u0003Z\u0982\bZ\u0001[\u0001[\u0001"+
		"[\u0001[\u0001[\u0001[\u0001[\u0003[\u098b\b[\u0001[\u0001[\u0001[\u0001"+
		"[\u0001[\u0001[\u0001[\u0001[\u0001[\u0001[\u0003[\u0997\b[\u0003[\u0999"+
		"\b[\u0001[\u0001[\u0001[\u0001[\u0001[\u0003[\u09a0\b[\u0001[\u0001[\u0001"+
		"[\u0001[\u0001[\u0003[\u09a7\b[\u0001[\u0001[\u0001[\u0001[\u0003[\u09ad"+
		"\b[\u0001[\u0001[\u0001[\u0001[\u0003[\u09b3\b[\u0003[\u09b5\b[\u0001"+
		"\\\u0001\\\u0001\\\u0005\\\u09ba\b\\\n\\\f\\\u09bd\t\\\u0001]\u0001]\u0001"+
		"]\u0005]\u09c2\b]\n]\f]\u09c5\t]\u0001^\u0001^\u0001^\u0005^\u09ca\b^"+
		"\n^\f^\u09cd\t^\u0001_\u0001_\u0001_\u0003_\u09d2\b_\u0001`\u0001`\u0001"+
		"`\u0003`\u09d7\b`\u0001`\u0001`\u0001a\u0001a\u0001a\u0003a\u09de\ba\u0001"+
		"a\u0001a\u0001b\u0001b\u0003b\u09e4\bb\u0001b\u0001b\u0003b\u09e8\bb\u0003"+
		"b\u09ea\bb\u0001c\u0001c\u0001c\u0005c\u09ef\bc\nc\fc\u09f2\tc\u0001d"+
		"\u0001d\u0001d\u0001d\u0005d\u09f8\bd\nd\fd\u09fb\td\u0001d\u0001d\u0001"+
		"e\u0001e\u0003e\u0a01\be\u0001f\u0001f\u0001f\u0001f\u0001f\u0001f\u0005"+
		"f\u0a09\bf\nf\ff\u0a0c\tf\u0001f\u0001f\u0003f\u0a10\bf\u0001g\u0001g"+
		"\u0003g\u0a14\bg\u0001h\u0001h\u0001i\u0001i\u0001i\u0005i\u0a1b\bi\n"+
		"i\fi\u0a1e\ti\u0001j\u0001j\u0001j\u0001j\u0001j\u0001j\u0001j\u0001j"+
		"\u0001j\u0001j\u0003j\u0a2a\bj\u0003j\u0a2c\bj\u0001j\u0001j\u0001j\u0001"+
		"j\u0001j\u0001j\u0005j\u0a34\bj\nj\fj\u0a37\tj\u0001k\u0003k\u0a3a\bk"+
		"\u0001k\u0001k\u0001k\u0001k\u0001k\u0001k\u0003k\u0a42\bk\u0001k\u0001"+
		"k\u0001k\u0001k\u0001k\u0005k\u0a49\bk\nk\fk\u0a4c\tk\u0001k\u0001k\u0001"+
		"k\u0003k\u0a51\bk\u0001k\u0001k\u0001k\u0001k\u0001k\u0001k\u0003k\u0a59"+
		"\bk\u0001k\u0001k\u0001k\u0003k\u0a5e\bk\u0001k\u0001k\u0001k\u0001k\u0001"+
		"k\u0001k\u0001k\u0001k\u0005k\u0a68\bk\nk\fk\u0a6b\tk\u0001k\u0001k\u0003"+
		"k\u0a6f\bk\u0001k\u0003k\u0a72\bk\u0001k\u0001k\u0001k\u0001k\u0003k\u0a78"+
		"\bk\u0001k\u0001k\u0003k\u0a7c\bk\u0001k\u0001k\u0001k\u0003k\u0a81\b"+
		"k\u0001k\u0001k\u0001k\u0003k\u0a86\bk\u0001k\u0001k\u0001k\u0003k\u0a8b"+
		"\bk\u0001l\u0001l\u0001l\u0001l\u0003l\u0a91\bl\u0001l\u0001l\u0001l\u0001"+
		"l\u0001l\u0001l\u0001l\u0001l\u0001l\u0001l\u0001l\u0001l\u0001l\u0001"+
		"l\u0001l\u0001l\u0001l\u0001l\u0001l\u0005l\u0aa6\bl\nl\fl\u0aa9\tl\u0001"+
		"m\u0001m\u0001n\u0001n\u0001n\u0001n\u0001n\u0001n\u0001n\u0001n\u0001"+
		"n\u0001n\u0001n\u0001n\u0001n\u0001n\u0001n\u0001n\u0001n\u0001n\u0001"+
		"n\u0001n\u0001n\u0001n\u0004n\u0ac3\bn\u000bn\fn\u0ac4\u0001n\u0001n\u0003"+
		"n\u0ac9\bn\u0001n\u0001n\u0001n\u0001n\u0001n\u0004n\u0ad0\bn\u000bn\f"+
		"n\u0ad1\u0001n\u0001n\u0003n\u0ad6\bn\u0001n\u0001n\u0001n\u0001n\u0001"+
		"n\u0001n\u0001n\u0001n\u0001n\u0001n\u0001n\u0001n\u0001n\u0001n\u0005"+
		"n\u0ae6\bn\nn\fn\u0ae9\tn\u0003n\u0aeb\bn\u0001n\u0001n\u0001n\u0001n"+
		"\u0001n\u0001n\u0003n\u0af3\bn\u0001n\u0001n\u0001n\u0001n\u0001n\u0001"+
		"n\u0001n\u0003n\u0afc\bn\u0001n\u0001n\u0001n\u0001n\u0001n\u0001n\u0001"+
		"n\u0001n\u0001n\u0001n\u0001n\u0001n\u0001n\u0001n\u0001n\u0001n\u0001"+
		"n\u0001n\u0001n\u0004n\u0b11\bn\u000bn\fn\u0b12\u0001n\u0001n\u0001n\u0001"+
		"n\u0001n\u0001n\u0001n\u0001n\u0001n\u0003n\u0b1e\bn\u0001n\u0001n\u0001"+
		"n\u0005n\u0b23\bn\nn\fn\u0b26\tn\u0003n\u0b28\bn\u0001n\u0001n\u0001n"+
		"\u0001n\u0001n\u0001n\u0001n\u0003n\u0b31\bn\u0001n\u0001n\u0003n\u0b35"+
		"\bn\u0001n\u0001n\u0003n\u0b39\bn\u0001n\u0001n\u0001n\u0001n\u0001n\u0001"+
		"n\u0001n\u0001n\u0004n\u0b43\bn\u000bn\fn\u0b44\u0001n\u0001n\u0001n\u0001"+
		"n\u0001n\u0001n\u0001n\u0001n\u0001n\u0001n\u0001n\u0001n\u0001n\u0001"+
		"n\u0001n\u0001n\u0001n\u0001n\u0001n\u0001n\u0001n\u0001n\u0001n\u0003"+
		"n\u0b5e\bn\u0001n\u0001n\u0001n\u0001n\u0001n\u0003n\u0b65\bn\u0001n\u0003"+
		"n\u0b68\bn\u0001n\u0001n\u0001n\u0001n\u0001n\u0001n\u0001n\u0001n\u0001"+
		"n\u0001n\u0001n\u0001n\u0001n\u0003n\u0b77\bn\u0001n\u0001n\u0001n\u0001"+
		"n\u0001n\u0001n\u0001n\u0001n\u0001n\u0001n\u0001n\u0001n\u0001n\u0001"+
		"n\u0001n\u0003n\u0b88\bn\u0003n\u0b8a\bn\u0001n\u0001n\u0001n\u0001n\u0001"+
		"n\u0001n\u0001n\u0001n\u0005n\u0b94\bn\nn\fn\u0b97\tn\u0001o\u0001o\u0001"+
		"o\u0001o\u0001o\u0001o\u0001o\u0001o\u0004o\u0ba1\bo\u000bo\fo\u0ba2\u0003"+
		"o\u0ba5\bo\u0001p\u0001p\u0001q\u0001q\u0001r\u0001r\u0001s\u0001s\u0001"+
		"t\u0001t\u0001t\u0003t\u0bb2\bt\u0001u\u0001u\u0003u\u0bb6\bu\u0001v\u0001"+
		"v\u0001v\u0004v\u0bbb\bv\u000bv\fv\u0bbc\u0001w\u0001w\u0001w\u0003w\u0bc2"+
		"\bw\u0001x\u0001x\u0001x\u0001x\u0001x\u0001y\u0003y\u0bca\by\u0001y\u0001"+
		"y\u0001z\u0001z\u0001z\u0003z\u0bd1\bz\u0001{\u0001{\u0001{\u0001{\u0001"+
		"{\u0001{\u0001{\u0001{\u0001{\u0001{\u0001{\u0001{\u0001{\u0001{\u0001"+
		"{\u0003{\u0be2\b{\u0001{\u0001{\u0003{\u0be6\b{\u0001{\u0001{\u0001{\u0001"+
		"{\u0003{\u0bec\b{\u0001{\u0001{\u0001{\u0001{\u0003{\u0bf2\b{\u0001{\u0001"+
		"{\u0001{\u0001{\u0001{\u0005{\u0bf9\b{\n{\f{\u0bfc\t{\u0001{\u0003{\u0bff"+
		"\b{\u0003{\u0c01\b{\u0001|\u0001|\u0001|\u0005|\u0c06\b|\n|\f|\u0c09\t"+
		"|\u0001}\u0001}\u0001}\u0001}\u0003}\u0c0f\b}\u0001}\u0003}\u0c12\b}\u0001"+
		"}\u0003}\u0c15\b}\u0001~\u0001~\u0001~\u0005~\u0c1a\b~\n~\f~\u0c1d\t~"+
		"\u0001\u007f\u0001\u007f\u0001\u007f\u0001\u007f\u0003\u007f\u0c23\b\u007f"+
		"\u0001\u007f\u0003\u007f\u0c26\b\u007f\u0001\u0080\u0001\u0080\u0001\u0080"+
		"\u0005\u0080\u0c2b\b\u0080\n\u0080\f\u0080\u0c2e\t\u0080\u0001\u0081\u0001"+
		"\u0081\u0003\u0081\u0c32\b\u0081\u0001\u0081\u0001\u0081\u0001\u0081\u0003"+
		"\u0081\u0c37\b\u0081\u0001\u0081\u0003\u0081\u0c3a\b\u0081\u0001\u0082"+
		"\u0001\u0082\u0001\u0082\u0001\u0082\u0001\u0082\u0001\u0083\u0001\u0083"+
		"\u0001\u0083\u0001\u0083\u0005\u0083\u0c45\b\u0083\n\u0083\f\u0083\u0c48"+
		"\t\u0083\u0001\u0084\u0001\u0084\u0001\u0084\u0001\u0084\u0001\u0085\u0001"+
		"\u0085\u0001\u0085\u0001\u0085\u0001\u0085\u0001\u0085\u0001\u0085\u0001"+
		"\u0085\u0001\u0085\u0001\u0085\u0001\u0085\u0005\u0085\u0c59\b\u0085\n"+
		"\u0085\f\u0085\u0c5c\t\u0085\u0001\u0085\u0001\u0085\u0001\u0085\u0001"+
		"\u0085\u0001\u0085\u0005\u0085\u0c63\b\u0085\n\u0085\f\u0085\u0c66\t\u0085"+
		"\u0003\u0085\u0c68\b\u0085\u0001\u0085\u0001\u0085\u0001\u0085\u0001\u0085"+
		"\u0001\u0085\u0005\u0085\u0c6f\b\u0085\n\u0085\f\u0085\u0c72\t\u0085\u0003"+
		"\u0085\u0c74\b\u0085\u0003\u0085\u0c76\b\u0085\u0001\u0085\u0003\u0085"+
		"\u0c79\b\u0085\u0001\u0085\u0003\u0085\u0c7c\b\u0085\u0001\u0086\u0001"+
		"\u0086\u0001\u0086\u0001\u0086\u0001\u0086\u0001\u0086\u0001\u0086\u0001"+
		"\u0086\u0001\u0086\u0001\u0086\u0001\u0086\u0001\u0086\u0001\u0086\u0001"+
		"\u0086\u0001\u0086\u0001\u0086\u0003\u0086\u0c8e\b\u0086\u0001\u0087\u0001"+
		"\u0087\u0001\u0087\u0001\u0087\u0001\u0087\u0001\u0087\u0001\u0087\u0003"+
		"\u0087\u0c97\b\u0087\u0001\u0088\u0001\u0088\u0001\u0088\u0005\u0088\u0c9c"+
		"\b\u0088\n\u0088\f\u0088\u0c9f\t\u0088\u0001\u0089\u0001\u0089\u0001\u0089"+
		"\u0001\u0089\u0003\u0089\u0ca5\b\u0089\u0001\u008a\u0001\u008a\u0001\u008a"+
		"\u0005\u008a\u0caa\b\u008a\n\u008a\f\u008a\u0cad\t\u008a\u0001\u008b\u0001"+
		"\u008b\u0001\u008b\u0001\u008c\u0001\u008c\u0004\u008c\u0cb4\b\u008c\u000b"+
		"\u008c\f\u008c\u0cb5\u0001\u008c\u0003\u008c\u0cb9\b\u008c\u0001\u008d"+
		"\u0001\u008d\u0001\u008d\u0003\u008d\u0cbe\b\u008d\u0001\u008e\u0001\u008e"+
		"\u0001\u008e\u0001\u008e\u0001\u008e\u0001\u008e\u0003\u008e\u0cc6\b\u008e"+
		"\u0001\u008f\u0001\u008f\u0001\u0090\u0001\u0090\u0003\u0090\u0ccc\b\u0090"+
		"\u0001\u0090\u0001\u0090\u0001\u0090\u0003\u0090\u0cd1\b\u0090\u0001\u0090"+
		"\u0001\u0090\u0001\u0090\u0003\u0090\u0cd6\b\u0090\u0001\u0090\u0001\u0090"+
		"\u0003\u0090\u0cda\b\u0090\u0001\u0090\u0001\u0090\u0003\u0090\u0cde\b"+
		"\u0090\u0001\u0090\u0001\u0090\u0003\u0090\u0ce2\b\u0090\u0001\u0090\u0001"+
		"\u0090\u0003\u0090\u0ce6\b\u0090\u0001\u0090\u0001\u0090\u0003\u0090\u0cea"+
		"\b\u0090\u0001\u0090\u0001\u0090\u0003\u0090\u0cee\b\u0090\u0001\u0090"+
		"\u0001\u0090\u0003\u0090\u0cf2\b\u0090\u0001\u0090\u0003\u0090\u0cf5\b"+
		"\u0090\u0001\u0091\u0001\u0091\u0001\u0091\u0001\u0091\u0001\u0091\u0001"+
		"\u0091\u0001\u0091\u0003\u0091\u0cfe\b\u0091\u0001\u0092\u0001\u0092\u0001"+
		"\u0093\u0001\u0093\u0001\u0094\u0001\u0094\u0001\u0094\t\u03b4\u03f5\u03fd"+
		"\u040e\u041c\u0425\u042e\u0437\u0463\u0004V\u00d4\u00d8\u00dc\u0095\u0000"+
		"\u0002\u0004\u0006\b\n\f\u000e\u0010\u0012\u0014\u0016\u0018\u001a\u001c"+
		"\u001e \"$&(*,.02468:<>@BDFHJLNPRTVXZ\\^`bdfhjlnprtvxz|~\u0080\u0082\u0084"+
		"\u0086\u0088\u008a\u008c\u008e\u0090\u0092\u0094\u0096\u0098\u009a\u009c"+
		"\u009e\u00a0\u00a2\u00a4\u00a6\u00a8\u00aa\u00ac\u00ae\u00b0\u00b2\u00b4"+
		"\u00b6\u00b8\u00ba\u00bc\u00be\u00c0\u00c2\u00c4\u00c6\u00c8\u00ca\u00cc"+
		"\u00ce\u00d0\u00d2\u00d4\u00d6\u00d8\u00da\u00dc\u00de\u00e0\u00e2\u00e4"+
		"\u00e6\u00e8\u00ea\u00ec\u00ee\u00f0\u00f2\u00f4\u00f6\u00f8\u00fa\u00fc"+
		"\u00fe\u0100\u0102\u0104\u0106\u0108\u010a\u010c\u010e\u0110\u0112\u0114"+
		"\u0116\u0118\u011a\u011c\u011e\u0120\u0122\u0124\u0126\u0128\u0000;\u0002"+
		"\u0000BB\u00bb\u00bb\u0002\u0000\u001c\u001c\u00cc\u00cc\u0002\u0000f"+
		"fss\u0001\u0000)*\u0002\u0000\u00f1\u00f1\u0115\u0115\u0002\u0000\u000b"+
		"\u000b!!\u0005\u0000&&22XXee\u0090\u0090\u0001\u0000FG\u0002\u0000XXe"+
		"e\u0002\u0000\u009f\u009f\u0135\u0135\u0003\u0000\b\bNN\u00ee\u00ee\u0002"+
		"\u0000\b\b\u008a\u008a\u0002\u0000\u008c\u008c\u0135\u0135\u0003\u0000"+
		">>\u009a\u009a\u00d7\u00d7\u0003\u0000??\u009b\u009b\u00d8\u00d8\u0004"+
		"\u0000SSzz\u00e0\u00e0\u010a\u010a\u0003\u0000SS\u00e0\u00e0\u010a\u010a"+
		"\u0002\u0000\u0013\u0013FF\u0002\u0000``\u0081\u0081\u0002\u0000\u00f0"+
		"\u00f0\u0114\u0114\u0002\u0000\u0135\u0135\u0139\u0139\u0002\u0000\u00ef"+
		"\u00ef\u00f9\u00f9\u0002\u000055\u00d3\u00d3\u0002\u0000\n\nKK\u0002\u0000"+
		"\u0139\u0139\u013b\u013b\u0001\u0000\u0086\u0087\u0003\u0000\n\n\u000f"+
		"\u000f\u00e4\u00e4\u0003\u0000[[\u0103\u0103\u010c\u010c\u0002\u0000\u0127"+
		"\u0128\u012c\u012c\u0002\u0000MM\u0129\u012b\u0002\u0000\u0127\u0128\u012f"+
		"\u012f\u0007\u0000;<oo\u0095\u0098\u00bd\u00bd\u00d6\u00d6\u0117\u0117"+
		"\u011d\u011d\u0002\u0000779:\u0002\u0000@@\u00fa\u00fa\u0002\u0000AA\u00fb"+
		"\u00fb\u0002\u0000\u001e\u001e\u0105\u0105\u0002\u0000qq\u00cb\u00cb\u0001"+
		"\u0000\u00ec\u00ed\u0002\u0000\u0004\u0004ff\u0002\u0000\u0004\u0004b"+
		"b\u0003\u0000\u0017\u0017\u0084\u0084\u00fe\u00fe\u0001\u0000\u00b2\u00b3"+
		"\u0001\u0000\u011f\u0126\u0002\u0000MM\u0127\u0130\u0004\u0000\r\rss\u009e"+
		"\u009e\u00a6\u00a6\u0002\u0000[[\u0103\u0103\u0001\u0000\u0127\u0128\u0003"+
		"\u0000\u0135\u0135\u0139\u0139\u013b\u013b\u0002\u0000\u0098\u0098\u011d"+
		"\u011d\u0004\u0000;;oo\u0097\u0097\u00d6\u00d6\u0003\u0000oo\u0097\u0097"+
		"\u00d6\u00d6\u0002\u0000LL\u00af\u00af\u0002\u0000\u00a7\u00a7\u00e5\u00e5"+
		"\u0002\u0000aa\u00b8\u00b8\u0001\u0000\u013a\u013b\u0002\u0000NN\u00df"+
		"\u00df3\u0000\b\t\u000b\f\u000e\u000e\u0010\u0011\u0013\u0014\u0016\u0016"+
		"\u0018\u001c\u001f!#&((*02256;JLNRRTZ]]_adehjmmortuwy{{~~\u0080\u0081"+
		"\u0083\u0083\u0086\u009b\u009d\u009d\u00a0\u00a1\u00a4\u00a5\u00a8\u00a8"+
		"\u00aa\u00ab\u00ad\u00b1\u00b4\u00b8\u00ba\u00c3\u00c5\u00cd\u00cf\u00d8"+
		"\u00da\u00dd\u00df\u00e3\u00e5\u00f0\u00f2\u00f6\u00f9\u00fb\u00fd\u00fd"+
		"\u00ff\u0109\u010d\u0110\u0113\u0117\u011a\u011a\u011d\u011e\u0010\u0000"+
		"\u000e\u000e44SSggvvzz\u007f\u007f\u0082\u0082\u0085\u0085\u009c\u009c"+
		"\u00a2\u00a2\u00ce\u00ce\u00da\u00da\u00e0\u00e0\u010a\u010a\u0112\u0112"+
		"\u0011\u0000\b\r\u000f35RTfhuwy{~\u0080\u0081\u0083\u0084\u0086\u009b"+
		"\u009d\u00a1\u00a3\u00cd\u00cf\u00d9\u00db\u00df\u00e1\u0109\u010b\u0111"+
		"\u0113\u011e\u0f0d\u0000\u012a\u0001\u0000\u0000\u0000\u0002\u0133\u0001"+
		"\u0000\u0000\u0000\u0004\u0136\u0001\u0000\u0000\u0000\u0006\u0139\u0001"+
		"\u0000\u0000\u0000\b\u013c\u0001\u0000\u0000\u0000\n\u013f\u0001\u0000"+
		"\u0000\u0000\f\u0142\u0001\u0000\u0000\u0000\u000e\u0466\u0001\u0000\u0000"+
		"\u0000\u0010\u0468\u0001\u0000\u0000\u0000\u0012\u046a\u0001\u0000\u0000"+
		"\u0000\u0014\u0514\u0001\u0000\u0000\u0000\u0016\u0516\u0001\u0000\u0000"+
		"\u0000\u0018\u0527\u0001\u0000\u0000\u0000\u001a\u052d\u0001\u0000\u0000"+
		"\u0000\u001c\u0539\u0001\u0000\u0000\u0000\u001e\u0546\u0001\u0000\u0000"+
		"\u0000 \u0549\u0001\u0000\u0000\u0000\"\u054d\u0001\u0000\u0000\u0000"+
		"$\u058f\u0001\u0000\u0000\u0000&\u0591\u0001\u0000\u0000\u0000(\u0595"+
		"\u0001\u0000\u0000\u0000*\u05a1\u0001\u0000\u0000\u0000,\u05a6\u0001\u0000"+
		"\u0000\u0000.\u05a8\u0001\u0000\u0000\u00000\u05af\u0001\u0000\u0000\u0000"+
		"2\u05b1\u0001\u0000\u0000\u00004\u05b9\u0001\u0000\u0000\u00006\u05c2"+
		"\u0001\u0000\u0000\u00008\u05cd\u0001\u0000\u0000\u0000:\u05df\u0001\u0000"+
		"\u0000\u0000<\u05e2\u0001\u0000\u0000\u0000>\u05ed\u0001\u0000\u0000\u0000"+
		"@\u05fd\u0001\u0000\u0000\u0000B\u0603\u0001\u0000\u0000\u0000D\u0605"+
		"\u0001\u0000\u0000\u0000F\u0610\u0001\u0000\u0000\u0000H\u0621\u0001\u0000"+
		"\u0000\u0000J\u0628\u0001\u0000\u0000\u0000L\u062a\u0001\u0000\u0000\u0000"+
		"N\u0630\u0001\u0000\u0000\u0000P\u0681\u0001\u0000\u0000\u0000R\u068d"+
		"\u0001\u0000\u0000\u0000T\u06bd\u0001\u0000\u0000\u0000V\u06c0\u0001\u0000"+
		"\u0000\u0000X\u06e6\u0001\u0000\u0000\u0000Z\u06e8\u0001\u0000\u0000\u0000"+
		"\\\u06f0\u0001\u0000\u0000\u0000^\u0711\u0001\u0000\u0000\u0000`\u073f"+
		"\u0001\u0000\u0000\u0000b\u0754\u0001\u0000\u0000\u0000d\u0774\u0001\u0000"+
		"\u0000\u0000f\u0782\u0001\u0000\u0000\u0000h\u078b\u0001\u0000\u0000\u0000"+
		"j\u078e\u0001\u0000\u0000\u0000l\u0797\u0001\u0000\u0000\u0000n\u07ac"+
		"\u0001\u0000\u0000\u0000p\u07c0\u0001\u0000\u0000\u0000r\u07c2\u0001\u0000"+
		"\u0000\u0000t\u07ca\u0001\u0000\u0000\u0000v\u07ce\u0001\u0000\u0000\u0000"+
		"x\u07d1\u0001\u0000\u0000\u0000z\u07d4\u0001\u0000\u0000\u0000|\u07ee"+
		"\u0001\u0000\u0000\u0000~\u07f0\u0001\u0000\u0000\u0000\u0080\u0810\u0001"+
		"\u0000\u0000\u0000\u0082\u0839\u0001\u0000\u0000\u0000\u0084\u083d\u0001"+
		"\u0000\u0000\u0000\u0086\u0858\u0001\u0000\u0000\u0000\u0088\u085c\u0001"+
		"\u0000\u0000\u0000\u008a\u086b\u0001\u0000\u0000\u0000\u008c\u086d\u0001"+
		"\u0000\u0000\u0000\u008e\u088b\u0001\u0000\u0000\u0000\u0090\u088d\u0001"+
		"\u0000\u0000\u0000\u0092\u0894\u0001\u0000\u0000\u0000\u0094\u08b4\u0001"+
		"\u0000\u0000\u0000\u0096\u08b7\u0001\u0000\u0000\u0000\u0098\u08d1\u0001"+
		"\u0000\u0000\u0000\u009a\u08eb\u0001\u0000\u0000\u0000\u009c\u08f1\u0001"+
		"\u0000\u0000\u0000\u009e\u08f3\u0001\u0000\u0000\u0000\u00a0\u0917\u0001"+
		"\u0000\u0000\u0000\u00a2\u0919\u0001\u0000\u0000\u0000\u00a4\u091d\u0001"+
		"\u0000\u0000\u0000\u00a6\u0925\u0001\u0000\u0000\u0000\u00a8\u0930\u0001"+
		"\u0000\u0000\u0000\u00aa\u0934\u0001\u0000\u0000\u0000\u00ac\u093f\u0001"+
		"\u0000\u0000\u0000\u00ae\u095e\u0001\u0000\u0000\u0000\u00b0\u0960\u0001"+
		"\u0000\u0000\u0000\u00b2\u096b\u0001\u0000\u0000\u0000\u00b4\u0981\u0001"+
		"\u0000\u0000\u0000\u00b6\u09b4\u0001\u0000\u0000\u0000\u00b8\u09b6\u0001"+
		"\u0000\u0000\u0000\u00ba\u09be\u0001\u0000\u0000\u0000\u00bc\u09c6\u0001"+
		"\u0000\u0000\u0000\u00be\u09ce\u0001\u0000\u0000\u0000\u00c0\u09d6\u0001"+
		"\u0000\u0000\u0000\u00c2\u09dd\u0001\u0000\u0000\u0000\u00c4\u09e1\u0001"+
		"\u0000\u0000\u0000\u00c6\u09eb\u0001\u0000\u0000\u0000\u00c8\u09f3\u0001"+
		"\u0000\u0000\u0000\u00ca\u0a00\u0001\u0000\u0000\u0000\u00cc\u0a0f\u0001"+
		"\u0000\u0000\u0000\u00ce\u0a13\u0001\u0000\u0000\u0000\u00d0\u0a15\u0001"+
		"\u0000\u0000\u0000\u00d2\u0a17\u0001\u0000\u0000\u0000\u00d4\u0a2b\u0001"+
		"\u0000\u0000\u0000\u00d6\u0a8a\u0001\u0000\u0000\u0000\u00d8\u0a90\u0001"+
		"\u0000\u0000\u0000\u00da\u0aaa\u0001\u0000\u0000\u0000\u00dc\u0b89\u0001"+
		"\u0000\u0000\u0000\u00de\u0ba4\u0001\u0000\u0000\u0000\u00e0\u0ba6\u0001"+
		"\u0000\u0000\u0000\u00e2\u0ba8\u0001\u0000\u0000\u0000\u00e4\u0baa\u0001"+
		"\u0000\u0000\u0000\u00e6\u0bac\u0001\u0000\u0000\u0000\u00e8\u0bae\u0001"+
		"\u0000\u0000\u0000\u00ea\u0bb3\u0001\u0000\u0000\u0000\u00ec\u0bba\u0001"+
		"\u0000\u0000\u0000\u00ee\u0bbe\u0001\u0000\u0000\u0000\u00f0\u0bc3\u0001"+
		"\u0000\u0000\u0000\u00f2\u0bc9\u0001\u0000\u0000\u0000\u00f4\u0bd0\u0001"+
		"\u0000\u0000\u0000\u00f6\u0c00\u0001\u0000\u0000\u0000\u00f8\u0c02\u0001"+
		"\u0000\u0000\u0000\u00fa\u0c0a\u0001\u0000\u0000\u0000\u00fc\u0c16\u0001"+
		"\u0000\u0000\u0000\u00fe\u0c1e\u0001\u0000\u0000\u0000\u0100\u0c27\u0001"+
		"\u0000\u0000\u0000\u0102\u0c2f\u0001\u0000\u0000\u0000\u0104\u0c3b\u0001"+
		"\u0000\u0000\u0000\u0106\u0c40\u0001\u0000\u0000\u0000\u0108\u0c49\u0001"+
		"\u0000\u0000\u0000\u010a\u0c7b\u0001\u0000\u0000\u0000\u010c\u0c8d\u0001"+
		"\u0000\u0000\u0000\u010e\u0c96\u0001\u0000\u0000\u0000\u0110\u0c98\u0001"+
		"\u0000\u0000\u0000\u0112\u0ca4\u0001\u0000\u0000\u0000\u0114\u0ca6\u0001"+
		"\u0000\u0000\u0000\u0116\u0cae\u0001\u0000\u0000\u0000\u0118\u0cb8\u0001"+
		"\u0000\u0000\u0000\u011a\u0cbd\u0001\u0000\u0000\u0000\u011c\u0cc5\u0001"+
		"\u0000\u0000\u0000\u011e\u0cc7\u0001\u0000\u0000\u0000\u0120\u0cf4\u0001"+
		"\u0000\u0000\u0000\u0122\u0cfd\u0001\u0000\u0000\u0000\u0124\u0cff\u0001"+
		"\u0000\u0000\u0000\u0126\u0d01\u0001\u0000\u0000\u0000\u0128\u0d03\u0001"+
		"\u0000\u0000\u0000\u012a\u012e\u0003\u000e\u0007\u0000\u012b\u012d\u0005"+
		"\u0001\u0000\u0000\u012c\u012b\u0001\u0000\u0000\u0000\u012d\u0130\u0001"+
		"\u0000\u0000\u0000\u012e\u012c\u0001\u0000\u0000\u0000\u012e\u012f\u0001"+
		"\u0000\u0000\u0000\u012f\u0131\u0001\u0000\u0000\u0000\u0130\u012e\u0001"+
		"\u0000\u0000\u0000\u0131\u0132\u0005\u0000\u0000\u0001\u0132\u0001\u0001"+
		"\u0000\u0000\u0000\u0133\u0134\u0003\u00c4b\u0000\u0134\u0135\u0005\u0000"+
		"\u0000\u0001\u0135\u0003\u0001\u0000\u0000\u0000\u0136\u0137\u0003\u00c0"+
		"`\u0000\u0137\u0138\u0005\u0000\u0000\u0001\u0138\u0005\u0001\u0000\u0000"+
		"\u0000\u0139\u013a\u0003\u00ba]\u0000\u013a\u013b\u0005\u0000\u0000\u0001"+
		"\u013b\u0007\u0001\u0000\u0000\u0000\u013c\u013d\u0003\u00c2a\u0000\u013d"+
		"\u013e\u0005\u0000\u0000\u0001\u013e\t\u0001\u0000\u0000\u0000\u013f\u0140"+
		"\u0003\u00f6{\u0000\u0140\u0141\u0005\u0000\u0000\u0001\u0141\u000b\u0001"+
		"\u0000\u0000\u0000\u0142\u0143\u0003\u00fc~\u0000\u0143\u0144\u0005\u0000"+
		"\u0000\u0001\u0144\r\u0001\u0000\u0000\u0000\u0145\u0467\u0003\"\u0011"+
		"\u0000\u0146\u0148\u00034\u001a\u0000\u0147\u0146\u0001\u0000\u0000\u0000"+
		"\u0147\u0148\u0001\u0000\u0000\u0000\u0148\u0149\u0001\u0000\u0000\u0000"+
		"\u0149\u0467\u0003P(\u0000\u014a\u014b\u0005\u0110\u0000\u0000\u014b\u0467"+
		"\u0003\u00ba]\u0000\u014c\u014d\u0005\u0110\u0000\u0000\u014d\u014e\u0003"+
		",\u0016\u0000\u014e\u014f\u0003\u00ba]\u0000\u014f\u0467\u0001\u0000\u0000"+
		"\u0000\u0150\u0151\u0005\u00df\u0000\u0000\u0151\u0154\u0005\u001f\u0000"+
		"\u0000\u0152\u0155\u0003\u011a\u008d\u0000\u0153\u0155\u0005\u0135\u0000"+
		"\u0000\u0154\u0152\u0001\u0000\u0000\u0000\u0154\u0153\u0001\u0000\u0000"+
		"\u0000\u0155\u0467\u0001\u0000\u0000\u0000\u0156\u0157\u00053\u0000\u0000"+
		"\u0157\u015b\u0003,\u0016\u0000\u0158\u0159\u0005p\u0000\u0000\u0159\u015a"+
		"\u0005\u009e\u0000\u0000\u015a\u015c\u0005U\u0000\u0000\u015b\u0158\u0001"+
		"\u0000\u0000\u0000\u015b\u015c\u0001\u0000\u0000\u0000\u015c\u015d\u0001"+
		"\u0000\u0000\u0000\u015d\u0165\u0003\u00ba]\u0000\u015e\u0164\u0003 \u0010"+
		"\u0000\u015f\u0164\u0003\u001e\u000f\u0000\u0160\u0161\u0005\u011b\u0000"+
		"\u0000\u0161\u0162\u0007\u0000\u0000\u0000\u0162\u0164\u0003<\u001e\u0000"+
		"\u0163\u015e\u0001\u0000\u0000\u0000\u0163\u015f\u0001\u0000\u0000\u0000"+
		"\u0163\u0160\u0001\u0000\u0000\u0000\u0164\u0167\u0001\u0000\u0000\u0000"+
		"\u0165\u0163\u0001\u0000\u0000\u0000\u0165\u0166\u0001\u0000\u0000\u0000"+
		"\u0166\u0467\u0001\u0000\u0000\u0000\u0167\u0165\u0001\u0000\u0000\u0000"+
		"\u0168\u0169\u0005\u000b\u0000\u0000\u0169\u016a\u0003,\u0016\u0000\u016a"+
		"\u016b\u0003\u00ba]\u0000\u016b\u016c\u0005\u00df\u0000\u0000\u016c\u016d"+
		"\u0007\u0000\u0000\u0000\u016d\u016e\u0003<\u001e\u0000\u016e\u0467\u0001"+
		"\u0000\u0000\u0000\u016f\u0170\u0005\u000b\u0000\u0000\u0170\u0171\u0003"+
		",\u0016\u0000\u0171\u0172\u0003\u00ba]\u0000\u0172\u0173\u0005\u00df\u0000"+
		"\u0000\u0173\u0174\u0003\u001e\u000f\u0000\u0174\u0467\u0001\u0000\u0000"+
		"\u0000\u0175\u0176\u0005N\u0000\u0000\u0176\u0179\u0003,\u0016\u0000\u0177"+
		"\u0178\u0005p\u0000\u0000\u0178\u017a\u0005U\u0000\u0000\u0179\u0177\u0001"+
		"\u0000\u0000\u0000\u0179\u017a\u0001\u0000\u0000\u0000\u017a\u017b\u0001"+
		"\u0000\u0000\u0000\u017b\u017d\u0003\u00ba]\u0000\u017c\u017e\u0007\u0001"+
		"\u0000\u0000\u017d\u017c\u0001\u0000\u0000\u0000\u017d\u017e\u0001\u0000"+
		"\u0000\u0000\u017e\u0467\u0001\u0000\u0000\u0000\u017f\u0180\u0005\u00e2"+
		"\u0000\u0000\u0180\u0183\u0003.\u0017\u0000\u0181\u0182\u0007\u0002\u0000"+
		"\u0000\u0182\u0184\u0003\u00ba]\u0000\u0183\u0181\u0001\u0000\u0000\u0000"+
		"\u0183\u0184\u0001\u0000\u0000\u0000\u0184\u0189\u0001\u0000\u0000\u0000"+
		"\u0185\u0187\u0005\u0086\u0000\u0000\u0186\u0185\u0001\u0000\u0000\u0000"+
		"\u0186\u0187\u0001\u0000\u0000\u0000\u0187\u0188\u0001\u0000\u0000\u0000"+
		"\u0188\u018a\u0005\u0135\u0000\u0000\u0189\u0186\u0001\u0000\u0000\u0000"+
		"\u0189\u018a\u0001\u0000\u0000\u0000\u018a\u0467\u0001\u0000\u0000\u0000"+
		"\u018b\u0190\u0003\u0016\u000b\u0000\u018c\u018d\u0005\u0002\u0000\u0000"+
		"\u018d\u018e\u0003\u00fc~\u0000\u018e\u018f\u0005\u0003\u0000\u0000\u018f"+
		"\u0191\u0001\u0000\u0000\u0000\u0190\u018c\u0001\u0000\u0000\u0000\u0190"+
		"\u0191\u0001\u0000\u0000\u0000\u0191\u0193\u0001\u0000\u0000\u0000\u0192"+
		"\u0194\u00038\u001c\u0000\u0193\u0192\u0001\u0000\u0000\u0000\u0193\u0194"+
		"\u0001\u0000\u0000\u0000\u0194\u0195\u0001\u0000\u0000\u0000\u0195\u019a"+
		"\u0003:\u001d\u0000\u0196\u0198\u0005\u0012\u0000\u0000\u0197\u0196\u0001"+
		"\u0000\u0000\u0000\u0197\u0198\u0001\u0000\u0000\u0000\u0198\u0199\u0001"+
		"\u0000\u0000\u0000\u0199\u019b\u0003\"\u0011\u0000\u019a\u0197\u0001\u0000"+
		"\u0000\u0000\u019a\u019b\u0001\u0000\u0000\u0000\u019b\u0467\u0001\u0000"+
		"\u0000\u0000\u019c\u019d\u00053\u0000\u0000\u019d\u01a1\u0005\u00f1\u0000"+
		"\u0000\u019e\u019f\u0005p\u0000\u0000\u019f\u01a0\u0005\u009e\u0000\u0000"+
		"\u01a0\u01a2\u0005U\u0000\u0000\u01a1\u019e\u0001\u0000\u0000\u0000\u01a1"+
		"\u01a2\u0001\u0000\u0000\u0000\u01a2\u01a3\u0001\u0000\u0000\u0000\u01a3"+
		"\u01a4\u0003\u00c0`\u0000\u01a4\u01a5\u0005\u0086\u0000\u0000\u01a5\u01ae"+
		"\u0003\u00c0`\u0000\u01a6\u01ad\u00038\u001c\u0000\u01a7\u01ad\u0003\u00b6"+
		"[\u0000\u01a8\u01ad\u0003H$\u0000\u01a9\u01ad\u0003\u001e\u000f\u0000"+
		"\u01aa\u01ab\u0005\u00f4\u0000\u0000\u01ab\u01ad\u0003<\u001e\u0000\u01ac"+
		"\u01a6\u0001\u0000\u0000\u0000\u01ac\u01a7\u0001\u0000\u0000\u0000\u01ac"+
		"\u01a8\u0001\u0000\u0000\u0000\u01ac\u01a9\u0001\u0000\u0000\u0000\u01ac"+
		"\u01aa\u0001\u0000\u0000\u0000\u01ad\u01b0\u0001\u0000\u0000\u0000\u01ae"+
		"\u01ac\u0001\u0000\u0000\u0000\u01ae\u01af\u0001\u0000\u0000\u0000\u01af"+
		"\u0467\u0001\u0000\u0000\u0000\u01b0\u01ae\u0001\u0000\u0000\u0000\u01b1"+
		"\u01b6\u0003\u0018\f\u0000\u01b2\u01b3\u0005\u0002\u0000\u0000\u01b3\u01b4"+
		"\u0003\u00fc~\u0000\u01b4\u01b5\u0005\u0003\u0000\u0000\u01b5\u01b7\u0001"+
		"\u0000\u0000\u0000\u01b6\u01b2\u0001\u0000\u0000\u0000\u01b6\u01b7\u0001"+
		"\u0000\u0000\u0000\u01b7\u01b9\u0001\u0000\u0000\u0000\u01b8\u01ba\u0003"+
		"8\u001c\u0000\u01b9\u01b8\u0001\u0000\u0000\u0000\u01b9\u01ba\u0001\u0000"+
		"\u0000\u0000\u01ba\u01bb\u0001\u0000\u0000\u0000\u01bb\u01c0\u0003:\u001d"+
		"\u0000\u01bc\u01be\u0005\u0012\u0000\u0000\u01bd\u01bc\u0001\u0000\u0000"+
		"\u0000\u01bd\u01be\u0001\u0000\u0000\u0000\u01be\u01bf\u0001\u0000\u0000"+
		"\u0000\u01bf\u01c1\u0003\"\u0011\u0000\u01c0\u01bd\u0001\u0000\u0000\u0000"+
		"\u01c0\u01c1\u0001\u0000\u0000\u0000\u01c1\u0467\u0001\u0000\u0000\u0000"+
		"\u01c2\u01c3\u0005\f\u0000\u0000\u01c3\u01c4\u0005\u00f1\u0000\u0000\u01c4"+
		"\u01c6\u0003\u00ba]\u0000\u01c5\u01c7\u0003(\u0014\u0000\u01c6\u01c5\u0001"+
		"\u0000\u0000\u0000\u01c6\u01c7\u0001\u0000\u0000\u0000\u01c7\u01c8\u0001"+
		"\u0000\u0000\u0000\u01c8\u01c9\u0005/\u0000\u0000\u01c9\u01d1\u0005\u00e8"+
		"\u0000\u0000\u01ca\u01d2\u0003\u011a\u008d\u0000\u01cb\u01cc\u0005b\u0000"+
		"\u0000\u01cc\u01cd\u0005*\u0000\u0000\u01cd\u01d2\u0003\u00a4R\u0000\u01ce"+
		"\u01cf\u0005b\u0000\u0000\u01cf\u01d0\u0005\n\u0000\u0000\u01d0\u01d2"+
		"\u0005*\u0000\u0000\u01d1\u01ca\u0001\u0000\u0000\u0000\u01d1\u01cb\u0001"+
		"\u0000\u0000\u0000\u01d1\u01ce\u0001\u0000\u0000\u0000\u01d1\u01d2\u0001"+
		"\u0000\u0000\u0000\u01d2\u0467\u0001\u0000\u0000\u0000\u01d3\u01d4\u0005"+
		"\f\u0000\u0000\u01d4\u01d7\u0005\u00f2\u0000\u0000\u01d5\u01d6\u0007\u0002"+
		"\u0000\u0000\u01d6\u01d8\u0003\u00ba]\u0000\u01d7\u01d5\u0001\u0000\u0000"+
		"\u0000\u01d7\u01d8\u0001\u0000\u0000\u0000\u01d8\u01d9\u0001\u0000\u0000"+
		"\u0000\u01d9\u01da\u0005/\u0000\u0000\u01da\u01dc\u0005\u00e8\u0000\u0000"+
		"\u01db\u01dd\u0003\u011a\u008d\u0000\u01dc\u01db\u0001\u0000\u0000\u0000"+
		"\u01dc\u01dd\u0001\u0000\u0000\u0000\u01dd\u0467\u0001\u0000\u0000\u0000"+
		"\u01de\u01df\u0005\u000b\u0000\u0000\u01df\u01e0\u0005\u00f1\u0000\u0000"+
		"\u01e0\u01e1\u0003\u00ba]\u0000\u01e1\u01e2\u0005\b\u0000\u0000\u01e2"+
		"\u01e3\u0007\u0003\u0000\u0000\u01e3\u01e4\u0003\u00f8|\u0000\u01e4\u0467"+
		"\u0001\u0000\u0000\u0000\u01e5\u01e6\u0005\u000b\u0000\u0000\u01e6\u01e7"+
		"\u0005\u00f1\u0000\u0000\u01e7\u01e8\u0003\u00ba]\u0000\u01e8\u01e9\u0005"+
		"\b\u0000\u0000\u01e9\u01ea\u0007\u0003\u0000\u0000\u01ea\u01eb\u0005\u0002"+
		"\u0000\u0000\u01eb\u01ec\u0003\u00f8|\u0000\u01ec\u01ed\u0005\u0003\u0000"+
		"\u0000\u01ed\u0467\u0001\u0000\u0000\u0000\u01ee\u01ef\u0005\u000b\u0000"+
		"\u0000\u01ef\u01f0\u0005\u00f1\u0000\u0000\u01f0\u01f1\u0003\u00ba]\u0000"+
		"\u01f1\u01f2\u0005\u00c6\u0000\u0000\u01f2\u01f3\u0005)\u0000\u0000\u01f3"+
		"\u01f4\u0003\u00ba]\u0000\u01f4\u01f5\u0005\u00fc\u0000\u0000\u01f5\u01f6"+
		"\u0003\u0116\u008b\u0000\u01f6\u0467\u0001\u0000\u0000\u0000\u01f7\u01f8"+
		"\u0005\u000b\u0000\u0000\u01f8\u01f9\u0005\u00f1\u0000\u0000\u01f9\u01fa"+
		"\u0003\u00ba]\u0000\u01fa\u01fb\u0005N\u0000\u0000\u01fb\u01fe\u0007\u0003"+
		"\u0000\u0000\u01fc\u01fd\u0005p\u0000\u0000\u01fd\u01ff\u0005U\u0000\u0000"+
		"\u01fe\u01fc\u0001\u0000\u0000\u0000\u01fe\u01ff\u0001\u0000\u0000\u0000"+
		"\u01ff\u0200\u0001\u0000\u0000\u0000\u0200\u0201\u0005\u0002\u0000\u0000"+
		"\u0201\u0202\u0003\u00b8\\\u0000\u0202\u0203\u0005\u0003\u0000\u0000\u0203"+
		"\u0467\u0001\u0000\u0000\u0000\u0204\u0205\u0005\u000b\u0000\u0000\u0205"+
		"\u0206\u0005\u00f1\u0000\u0000\u0206\u0207\u0003\u00ba]\u0000\u0207\u0208"+
		"\u0005N\u0000\u0000\u0208\u020b\u0007\u0003\u0000\u0000\u0209\u020a\u0005"+
		"p\u0000\u0000\u020a\u020c\u0005U\u0000\u0000\u020b\u0209\u0001\u0000\u0000"+
		"\u0000\u020b\u020c\u0001\u0000\u0000\u0000\u020c\u020d\u0001\u0000\u0000"+
		"\u0000\u020d\u020e\u0003\u00b8\\\u0000\u020e\u0467\u0001\u0000\u0000\u0000"+
		"\u020f\u0210\u0005\u000b\u0000\u0000\u0210\u0211\u0007\u0004\u0000\u0000"+
		"\u0211\u0212\u0003\u00ba]\u0000\u0212\u0213\u0005\u00c6\u0000\u0000\u0213"+
		"\u0214\u0005\u00fc\u0000\u0000\u0214\u0215\u0003\u00ba]\u0000\u0215\u0467"+
		"\u0001\u0000\u0000\u0000\u0216\u0217\u0005\u000b\u0000\u0000\u0217\u0218"+
		"\u0007\u0004\u0000\u0000\u0218\u0219\u0003\u00ba]\u0000\u0219\u021a\u0005"+
		"\u00df\u0000\u0000\u021a\u021b\u0005\u00f4\u0000\u0000\u021b\u021c\u0003"+
		"<\u001e\u0000\u021c\u0467\u0001\u0000\u0000\u0000\u021d\u021e\u0005\u000b"+
		"\u0000\u0000\u021e\u021f\u0007\u0004\u0000\u0000\u021f\u0220\u0003\u00ba"+
		"]\u0000\u0220\u0221\u0005\u010e\u0000\u0000\u0221\u0224\u0005\u00f4\u0000"+
		"\u0000\u0222\u0223\u0005p\u0000\u0000\u0223\u0225\u0005U\u0000\u0000\u0224"+
		"\u0222\u0001\u0000\u0000\u0000\u0224\u0225\u0001\u0000\u0000\u0000\u0225"+
		"\u0226\u0001\u0000\u0000\u0000\u0226\u0227\u0003<\u001e\u0000\u0227\u0467"+
		"\u0001\u0000\u0000\u0000\u0228\u0229\u0005\u000b\u0000\u0000\u0229\u022a"+
		"\u0005\u00f1\u0000\u0000\u022a\u022b\u0003\u00ba]\u0000\u022b\u022d\u0007"+
		"\u0005\u0000\u0000\u022c\u022e\u0005)\u0000\u0000\u022d\u022c\u0001\u0000"+
		"\u0000\u0000\u022d\u022e\u0001\u0000\u0000\u0000\u022e\u022f\u0001\u0000"+
		"\u0000\u0000\u022f\u0231\u0003\u00ba]\u0000\u0230\u0232\u0003\u0122\u0091"+
		"\u0000\u0231\u0230\u0001\u0000\u0000\u0000\u0231\u0232\u0001\u0000\u0000"+
		"\u0000\u0232\u0467\u0001\u0000\u0000\u0000\u0233\u0234\u0005\u000b\u0000"+
		"\u0000\u0234\u0235\u0005\u00f1\u0000\u0000\u0235\u0237\u0003\u00ba]\u0000"+
		"\u0236\u0238\u0003(\u0014\u0000\u0237\u0236\u0001\u0000\u0000\u0000\u0237"+
		"\u0238\u0001\u0000\u0000\u0000\u0238\u0239\u0001\u0000\u0000\u0000\u0239"+
		"\u023b\u0005!\u0000\u0000\u023a\u023c\u0005)\u0000\u0000\u023b\u023a\u0001"+
		"\u0000\u0000\u0000\u023b\u023c\u0001\u0000\u0000\u0000\u023c\u023d\u0001"+
		"\u0000\u0000\u0000\u023d\u023e\u0003\u00ba]\u0000\u023e\u0240\u0003\u00fe"+
		"\u007f\u0000\u023f\u0241\u0003\u00f4z\u0000\u0240\u023f\u0001\u0000\u0000"+
		"\u0000\u0240\u0241\u0001\u0000\u0000\u0000\u0241\u0467\u0001\u0000\u0000"+
		"\u0000\u0242\u0243\u0005\u000b\u0000\u0000\u0243\u0244\u0005\u00f1\u0000"+
		"\u0000\u0244\u0246\u0003\u00ba]\u0000\u0245\u0247\u0003(\u0014\u0000\u0246"+
		"\u0245\u0001\u0000\u0000\u0000\u0246\u0247\u0001\u0000\u0000\u0000\u0247"+
		"\u0248\u0001\u0000\u0000\u0000\u0248\u0249\u0005\u00c9\u0000\u0000\u0249"+
		"\u024a\u0005*\u0000\u0000\u024a\u024b\u0005\u0002\u0000\u0000\u024b\u024c"+
		"\u0003\u00f8|\u0000\u024c\u024d\u0005\u0003\u0000\u0000\u024d\u0467\u0001"+
		"\u0000\u0000\u0000\u024e\u024f\u0005\u000b\u0000\u0000\u024f\u0250\u0005"+
		"\u00f1\u0000\u0000\u0250\u0252\u0003\u00ba]\u0000\u0251\u0253\u0003(\u0014"+
		"\u0000\u0252\u0251\u0001\u0000\u0000\u0000\u0252\u0253\u0001\u0000\u0000"+
		"\u0000\u0253\u0254\u0001\u0000\u0000\u0000\u0254\u0255\u0005\u00df\u0000"+
		"\u0000\u0255\u0256\u0005\u00dc\u0000\u0000\u0256\u025a\u0005\u0135\u0000"+
		"\u0000\u0257\u0258\u0005\u011b\u0000\u0000\u0258\u0259\u0005\u00dd\u0000"+
		"\u0000\u0259\u025b\u0003<\u001e\u0000\u025a\u0257\u0001\u0000\u0000\u0000"+
		"\u025a\u025b\u0001\u0000\u0000\u0000\u025b\u0467\u0001\u0000\u0000\u0000"+
		"\u025c\u025d\u0005\u000b\u0000\u0000\u025d\u025e\u0005\u00f1\u0000\u0000"+
		"\u025e\u0260\u0003\u00ba]\u0000\u025f\u0261\u0003(\u0014\u0000\u0260\u025f"+
		"\u0001\u0000\u0000\u0000\u0260\u0261\u0001\u0000\u0000\u0000\u0261\u0262"+
		"\u0001\u0000\u0000\u0000\u0262\u0263\u0005\u00df\u0000\u0000\u0263\u0264"+
		"\u0005\u00dd\u0000\u0000\u0264\u0265\u0003<\u001e\u0000\u0265\u0467\u0001"+
		"\u0000\u0000\u0000\u0266\u0267\u0005\u000b\u0000\u0000\u0267\u0268\u0007"+
		"\u0004\u0000\u0000\u0268\u0269\u0003\u00ba]\u0000\u0269\u026d\u0005\b"+
		"\u0000\u0000\u026a\u026b\u0005p\u0000\u0000\u026b\u026c\u0005\u009e\u0000"+
		"\u0000\u026c\u026e\u0005U\u0000\u0000\u026d\u026a\u0001\u0000\u0000\u0000"+
		"\u026d\u026e\u0001\u0000\u0000\u0000\u026e\u0270\u0001\u0000\u0000\u0000"+
		"\u026f\u0271\u0003&\u0013\u0000\u0270\u026f\u0001\u0000\u0000\u0000\u0271"+
		"\u0272\u0001\u0000\u0000\u0000\u0272\u0270\u0001\u0000\u0000\u0000\u0272"+
		"\u0273\u0001\u0000\u0000\u0000\u0273\u0467\u0001\u0000\u0000\u0000\u0274"+
		"\u0275\u0005\u000b\u0000\u0000\u0275\u0276\u0005\u00f1\u0000\u0000\u0276"+
		"\u0277\u0003\u00ba]\u0000\u0277\u0278\u0003(\u0014\u0000\u0278\u0279\u0005"+
		"\u00c6\u0000\u0000\u0279\u027a\u0005\u00fc\u0000\u0000\u027a\u027b\u0003"+
		"(\u0014\u0000\u027b\u0467\u0001\u0000\u0000\u0000\u027c\u027d\u0005\u000b"+
		"\u0000\u0000\u027d\u027e\u0007\u0004\u0000\u0000\u027e\u027f\u0003\u00ba"+
		"]\u0000\u027f\u0282\u0005N\u0000\u0000\u0280\u0281\u0005p\u0000\u0000"+
		"\u0281\u0283\u0005U\u0000\u0000\u0282\u0280\u0001\u0000\u0000\u0000\u0282"+
		"\u0283\u0001\u0000\u0000\u0000\u0283\u0284\u0001\u0000\u0000\u0000\u0284"+
		"\u0289\u0003(\u0014\u0000\u0285\u0286\u0005\u0004\u0000\u0000\u0286\u0288"+
		"\u0003(\u0014\u0000\u0287\u0285\u0001\u0000\u0000\u0000\u0288\u028b\u0001"+
		"\u0000\u0000\u0000\u0289\u0287\u0001\u0000\u0000\u0000\u0289\u028a\u0001"+
		"\u0000\u0000\u0000\u028a\u028d\u0001\u0000\u0000\u0000\u028b\u0289\u0001"+
		"\u0000\u0000\u0000\u028c\u028e\u0005\u00bc\u0000\u0000\u028d\u028c\u0001"+
		"\u0000\u0000\u0000\u028d\u028e\u0001\u0000\u0000\u0000\u028e\u0467\u0001"+
		"\u0000\u0000\u0000\u028f\u0290\u0005\u000b\u0000\u0000\u0290\u0291\u0005"+
		"\u00f1\u0000\u0000\u0291\u0293\u0003\u00ba]\u0000\u0292\u0294\u0003(\u0014"+
		"\u0000\u0293\u0292\u0001\u0000\u0000\u0000\u0293\u0294\u0001\u0000\u0000"+
		"\u0000\u0294\u0295\u0001\u0000\u0000\u0000\u0295\u0296\u0005\u00df\u0000"+
		"\u0000\u0296\u0297\u0003\u001e\u000f\u0000\u0297\u0467\u0001\u0000\u0000"+
		"\u0000\u0298\u0299\u0005\u000b\u0000\u0000\u0299\u029a\u0005\u00f1\u0000"+
		"\u0000\u029a\u029b\u0003\u00ba]\u0000\u029b\u029c\u0005\u00c2\u0000\u0000"+
		"\u029c\u029d\u0005\u00b1\u0000\u0000\u029d\u0467\u0001\u0000\u0000\u0000"+
		"\u029e\u029f\u0005N\u0000\u0000\u029f\u02a2\u0005\u00f1\u0000\u0000\u02a0"+
		"\u02a1\u0005p\u0000\u0000\u02a1\u02a3\u0005U\u0000\u0000\u02a2\u02a0\u0001"+
		"\u0000\u0000\u0000\u02a2\u02a3\u0001\u0000\u0000\u0000\u02a3\u02a4\u0001"+
		"\u0000\u0000\u0000\u02a4\u02a6\u0003\u00ba]\u0000\u02a5\u02a7\u0005\u00bc"+
		"\u0000\u0000\u02a6\u02a5\u0001\u0000\u0000\u0000\u02a6\u02a7\u0001\u0000"+
		"\u0000\u0000\u02a7\u0467\u0001\u0000\u0000\u0000\u02a8\u02a9\u0005N\u0000"+
		"\u0000\u02a9\u02ac\u0005\u0115\u0000\u0000\u02aa\u02ab\u0005p\u0000\u0000"+
		"\u02ab\u02ad\u0005U\u0000\u0000\u02ac\u02aa\u0001\u0000\u0000\u0000\u02ac"+
		"\u02ad\u0001\u0000\u0000\u0000\u02ad\u02ae\u0001\u0000\u0000\u0000\u02ae"+
		"\u0467\u0003\u00ba]\u0000\u02af\u02b2\u00053\u0000\u0000\u02b0\u02b1\u0005"+
		"\u00a6\u0000\u0000\u02b1\u02b3\u0005\u00c9\u0000\u0000\u02b2\u02b0\u0001"+
		"\u0000\u0000\u0000\u02b2\u02b3\u0001\u0000\u0000\u0000\u02b3\u02b8\u0001"+
		"\u0000\u0000\u0000\u02b4\u02b6\u0005j\u0000\u0000\u02b5\u02b4\u0001\u0000"+
		"\u0000\u0000\u02b5\u02b6\u0001\u0000\u0000\u0000\u02b6\u02b7\u0001\u0000"+
		"\u0000\u0000\u02b7\u02b9\u0005\u00f5\u0000\u0000\u02b8\u02b5\u0001\u0000"+
		"\u0000\u0000\u02b8\u02b9\u0001\u0000\u0000\u0000\u02b9\u02ba\u0001\u0000"+
		"\u0000\u0000\u02ba\u02be\u0005\u0115\u0000\u0000\u02bb\u02bc\u0005p\u0000"+
		"\u0000\u02bc\u02bd\u0005\u009e\u0000\u0000\u02bd\u02bf\u0005U\u0000\u0000"+
		"\u02be\u02bb\u0001\u0000\u0000\u0000\u02be\u02bf\u0001\u0000\u0000\u0000"+
		"\u02bf\u02c0\u0001\u0000\u0000\u0000\u02c0\u02c2\u0003\u00ba]\u0000\u02c1"+
		"\u02c3\u0003\u00aaU\u0000\u02c2\u02c1\u0001\u0000\u0000\u0000\u02c2\u02c3"+
		"\u0001\u0000\u0000\u0000\u02c3\u02cc\u0001\u0000\u0000\u0000\u02c4\u02cb"+
		"\u0003 \u0010\u0000\u02c5\u02c6\u0005\u00b0\u0000\u0000\u02c6\u02c7\u0005"+
		"\u00a2\u0000\u0000\u02c7\u02cb\u0003\u00a2Q\u0000\u02c8\u02c9\u0005\u00f4"+
		"\u0000\u0000\u02c9\u02cb\u0003<\u001e\u0000\u02ca\u02c4\u0001\u0000\u0000"+
		"\u0000\u02ca\u02c5\u0001\u0000\u0000\u0000\u02ca\u02c8\u0001\u0000\u0000"+
		"\u0000\u02cb\u02ce\u0001\u0000\u0000\u0000\u02cc\u02ca\u0001\u0000\u0000"+
		"\u0000\u02cc\u02cd\u0001\u0000\u0000\u0000\u02cd\u02cf\u0001\u0000\u0000"+
		"\u0000\u02ce\u02cc\u0001\u0000\u0000\u0000\u02cf\u02d0\u0005\u0012\u0000"+
		"\u0000\u02d0\u02d1\u0003\"\u0011\u0000\u02d1\u0467\u0001\u0000\u0000\u0000"+
		"\u02d2\u02d5\u00053\u0000\u0000\u02d3\u02d4\u0005\u00a6\u0000\u0000\u02d4"+
		"\u02d6\u0005\u00c9\u0000\u0000\u02d5\u02d3\u0001\u0000\u0000\u0000\u02d5"+
		"\u02d6\u0001\u0000\u0000\u0000\u02d6\u02d8\u0001\u0000\u0000\u0000\u02d7"+
		"\u02d9\u0005j\u0000\u0000\u02d8\u02d7\u0001\u0000\u0000\u0000\u02d8\u02d9"+
		"\u0001\u0000\u0000\u0000\u02d9\u02da\u0001\u0000\u0000\u0000\u02da\u02db"+
		"\u0005\u00f5\u0000\u0000\u02db\u02dc\u0005\u0115\u0000\u0000\u02dc\u02e1"+
		"\u0003\u00c0`\u0000\u02dd\u02de\u0005\u0002\u0000\u0000\u02de\u02df\u0003"+
		"\u00fc~\u0000\u02df\u02e0\u0005\u0003\u0000\u0000\u02e0\u02e2\u0001\u0000"+
		"\u0000\u0000\u02e1\u02dd\u0001\u0000\u0000\u0000\u02e1\u02e2\u0001\u0000"+
		"\u0000\u0000\u02e2\u02e3\u0001\u0000\u0000\u0000\u02e3\u02e6\u00038\u001c"+
		"\u0000\u02e4\u02e5\u0005\u00a5\u0000\u0000\u02e5\u02e7\u0003<\u001e\u0000"+
		"\u02e6\u02e4\u0001\u0000\u0000\u0000\u02e6\u02e7\u0001\u0000\u0000\u0000"+
		"\u02e7\u0467\u0001\u0000\u0000\u0000\u02e8\u02e9\u0005\u000b\u0000\u0000"+
		"\u02e9\u02ea\u0005\u0115\u0000\u0000\u02ea\u02ec\u0003\u00ba]\u0000\u02eb"+
		"\u02ed\u0005\u0012\u0000\u0000\u02ec\u02eb\u0001\u0000\u0000\u0000\u02ec"+
		"\u02ed\u0001\u0000\u0000\u0000\u02ed\u02ee\u0001\u0000\u0000\u0000\u02ee"+
		"\u02ef\u0003\"\u0011\u0000\u02ef\u0467\u0001\u0000\u0000\u0000\u02f0\u02f3"+
		"\u00053\u0000\u0000\u02f1\u02f2\u0005\u00a6\u0000\u0000\u02f2\u02f4\u0005"+
		"\u00c9\u0000\u0000\u02f3\u02f1\u0001\u0000\u0000\u0000\u02f3\u02f4\u0001"+
		"\u0000\u0000\u0000\u02f4\u02f6\u0001\u0000\u0000\u0000\u02f5\u02f7\u0005"+
		"\u00f5\u0000\u0000\u02f6\u02f5\u0001\u0000\u0000\u0000\u02f6\u02f7\u0001"+
		"\u0000\u0000\u0000\u02f7\u02f8\u0001\u0000\u0000\u0000\u02f8\u02fc\u0005"+
		"h\u0000\u0000\u02f9\u02fa\u0005p\u0000\u0000\u02fa\u02fb\u0005\u009e\u0000"+
		"\u0000\u02fb\u02fd\u0005U\u0000\u0000\u02fc\u02f9\u0001\u0000\u0000\u0000"+
		"\u02fc\u02fd\u0001\u0000\u0000\u0000\u02fd\u02fe\u0001\u0000\u0000\u0000"+
		"\u02fe\u02ff\u0003\u00ba]\u0000\u02ff\u0300\u0005\u0012\u0000\u0000\u0300"+
		"\u030a\u0005\u0135\u0000\u0000\u0301\u0302\u0005\u0112\u0000\u0000\u0302"+
		"\u0307\u0003N\'\u0000\u0303\u0304\u0005\u0004\u0000\u0000\u0304\u0306"+
		"\u0003N\'\u0000\u0305\u0303\u0001\u0000\u0000\u0000\u0306\u0309\u0001"+
		"\u0000\u0000\u0000\u0307\u0305\u0001\u0000\u0000\u0000\u0307\u0308\u0001"+
		"\u0000\u0000\u0000\u0308\u030b\u0001\u0000\u0000\u0000\u0309\u0307\u0001"+
		"\u0000\u0000\u0000\u030a\u0301\u0001\u0000\u0000\u0000\u030a\u030b\u0001"+
		"\u0000\u0000\u0000\u030b\u0467\u0001\u0000\u0000\u0000\u030c\u030e\u0005"+
		"N\u0000\u0000\u030d\u030f\u0005\u00f5\u0000\u0000\u030e\u030d\u0001\u0000"+
		"\u0000\u0000\u030e\u030f\u0001\u0000\u0000\u0000\u030f\u0310\u0001\u0000"+
		"\u0000\u0000\u0310\u0313\u0005h\u0000\u0000\u0311\u0312\u0005p\u0000\u0000"+
		"\u0312\u0314\u0005U\u0000\u0000\u0313\u0311\u0001\u0000\u0000\u0000\u0313"+
		"\u0314\u0001\u0000\u0000\u0000\u0314\u0315\u0001\u0000\u0000\u0000\u0315"+
		"\u0467\u0003\u00ba]\u0000\u0316\u0318\u0005V\u0000\u0000\u0317\u0319\u0007"+
		"\u0006\u0000\u0000\u0318\u0317\u0001\u0000\u0000\u0000\u0318\u0319\u0001"+
		"\u0000\u0000\u0000\u0319\u031a\u0001\u0000\u0000\u0000\u031a\u0467\u0003"+
		"\u000e\u0007\u0000\u031b\u031c\u0005\u00e2\u0000\u0000\u031c\u031f\u0005"+
		"\u00f2\u0000\u0000\u031d\u031e\u0007\u0002\u0000\u0000\u031e\u0320\u0003"+
		"\u00ba]\u0000\u031f\u031d\u0001\u0000\u0000\u0000\u031f\u0320\u0001\u0000"+
		"\u0000\u0000\u0320\u0325\u0001\u0000\u0000\u0000\u0321\u0323\u0005\u0086"+
		"\u0000\u0000\u0322\u0321\u0001\u0000\u0000\u0000\u0322\u0323\u0001\u0000"+
		"\u0000\u0000\u0323\u0324\u0001\u0000\u0000\u0000\u0324\u0326\u0005\u0135"+
		"\u0000\u0000\u0325\u0322\u0001\u0000\u0000\u0000\u0325\u0326\u0001\u0000"+
		"\u0000\u0000\u0326\u0467\u0001\u0000\u0000\u0000\u0327\u0328\u0005\u00e2"+
		"\u0000\u0000\u0328\u0329\u0005\u00f1\u0000\u0000\u0329\u032c\u0005X\u0000"+
		"\u0000\u032a\u032b\u0007\u0002\u0000\u0000\u032b\u032d\u0003\u00ba]\u0000"+
		"\u032c\u032a\u0001\u0000\u0000\u0000\u032c\u032d\u0001\u0000\u0000\u0000"+
		"\u032d\u032e\u0001\u0000\u0000\u0000\u032e\u032f\u0005\u0086\u0000\u0000"+
		"\u032f\u0331\u0005\u0135\u0000\u0000\u0330\u0332\u0003(\u0014\u0000\u0331"+
		"\u0330\u0001\u0000\u0000\u0000\u0331\u0332\u0001\u0000\u0000\u0000\u0332"+
		"\u0467\u0001\u0000\u0000\u0000\u0333\u0334\u0005\u00e2\u0000\u0000\u0334"+
		"\u0335\u0005\u00f4\u0000\u0000\u0335\u033a\u0003\u00ba]\u0000\u0336\u0337"+
		"\u0005\u0002\u0000\u0000\u0337\u0338\u0003@ \u0000\u0338\u0339\u0005\u0003"+
		"\u0000\u0000\u0339\u033b\u0001\u0000\u0000\u0000\u033a\u0336\u0001\u0000"+
		"\u0000\u0000\u033a\u033b\u0001\u0000\u0000\u0000\u033b\u0467\u0001\u0000"+
		"\u0000\u0000\u033c\u033d\u0005\u00e2\u0000\u0000\u033d\u033e\u0005*\u0000"+
		"\u0000\u033e\u033f\u0007\u0002\u0000\u0000\u033f\u0342\u0003\u00ba]\u0000"+
		"\u0340\u0341\u0007\u0002\u0000\u0000\u0341\u0343\u0003\u00ba]\u0000\u0342"+
		"\u0340\u0001\u0000\u0000\u0000\u0342\u0343\u0001\u0000\u0000\u0000\u0343"+
		"\u0467\u0001\u0000\u0000\u0000\u0344\u0345\u0005\u00e2\u0000\u0000\u0345"+
		"\u0348\u0005\u0116\u0000\u0000\u0346\u0347\u0007\u0002\u0000\u0000\u0347"+
		"\u0349\u0003\u00ba]\u0000\u0348\u0346\u0001\u0000\u0000\u0000\u0348\u0349"+
		"\u0001\u0000\u0000\u0000\u0349\u034e\u0001\u0000\u0000\u0000\u034a\u034c"+
		"\u0005\u0086\u0000\u0000\u034b\u034a\u0001\u0000\u0000\u0000\u034b\u034c"+
		"\u0001\u0000\u0000\u0000\u034c\u034d\u0001\u0000\u0000\u0000\u034d\u034f"+
		"\u0005\u0135\u0000\u0000\u034e\u034b\u0001\u0000\u0000\u0000\u034e\u034f"+
		"\u0001\u0000\u0000\u0000\u034f\u0467\u0001\u0000\u0000\u0000\u0350\u0351"+
		"\u0005\u00e2\u0000\u0000\u0351\u0352\u0005\u00b1\u0000\u0000\u0352\u0354"+
		"\u0003\u00ba]\u0000\u0353\u0355\u0003(\u0014\u0000\u0354\u0353\u0001\u0000"+
		"\u0000\u0000\u0354\u0355\u0001\u0000\u0000\u0000\u0355\u0467\u0001\u0000"+
		"\u0000\u0000\u0356\u0358\u0005\u00e2\u0000\u0000\u0357\u0359\u0003\u011a"+
		"\u008d\u0000\u0358\u0357\u0001\u0000\u0000\u0000\u0358\u0359\u0001\u0000"+
		"\u0000\u0000\u0359\u035a\u0001\u0000\u0000\u0000\u035a\u035d\u0005i\u0000"+
		"\u0000\u035b\u035c\u0007\u0002\u0000\u0000\u035c\u035e\u0003\u00ba]\u0000"+
		"\u035d\u035b\u0001\u0000\u0000\u0000\u035d\u035e\u0001\u0000\u0000\u0000"+
		"\u035e\u0366\u0001\u0000\u0000\u0000\u035f\u0361\u0005\u0086\u0000\u0000"+
		"\u0360\u035f\u0001\u0000\u0000\u0000\u0360\u0361\u0001\u0000\u0000\u0000"+
		"\u0361\u0364\u0001\u0000\u0000\u0000\u0362\u0365\u0003\u00ba]\u0000\u0363"+
		"\u0365\u0005\u0135\u0000\u0000\u0364\u0362\u0001\u0000\u0000\u0000\u0364"+
		"\u0363\u0001\u0000\u0000\u0000\u0365\u0367\u0001\u0000\u0000\u0000\u0366"+
		"\u0360\u0001\u0000\u0000\u0000\u0366\u0367\u0001\u0000\u0000\u0000\u0367"+
		"\u0467\u0001\u0000\u0000\u0000\u0368\u0369\u0005\u00e2\u0000\u0000\u0369"+
		"\u036a\u00053\u0000\u0000\u036a\u036b\u0005\u00f1\u0000\u0000\u036b\u036e"+
		"\u0003\u00ba]\u0000\u036c\u036d\u0005\u0012\u0000\u0000\u036d\u036f\u0005"+
		"\u00dc\u0000\u0000\u036e\u036c\u0001\u0000\u0000\u0000\u036e\u036f\u0001"+
		"\u0000\u0000\u0000\u036f\u0467\u0001\u0000\u0000\u0000\u0370\u0371\u0005"+
		"\u00e2\u0000\u0000\u0371\u0372\u00056\u0000\u0000\u0372\u0467\u0003,\u0016"+
		"\u0000\u0373\u0374\u0005\u00e2\u0000\u0000\u0374\u0379\u0005 \u0000\u0000"+
		"\u0375\u0377\u0005\u0086\u0000\u0000\u0376\u0375\u0001\u0000\u0000\u0000"+
		"\u0376\u0377\u0001\u0000\u0000\u0000\u0377\u0378\u0001\u0000\u0000\u0000"+
		"\u0378\u037a\u0005\u0135\u0000\u0000\u0379\u0376\u0001\u0000\u0000\u0000"+
		"\u0379\u037a\u0001\u0000\u0000\u0000\u037a\u0467\u0001\u0000\u0000\u0000"+
		"\u037b\u037c\u0007\u0007\u0000\u0000\u037c\u037e\u0005h\u0000\u0000\u037d"+
		"\u037f\u0005X\u0000\u0000\u037e\u037d\u0001\u0000\u0000\u0000\u037e\u037f"+
		"\u0001\u0000\u0000\u0000\u037f\u0380\u0001\u0000\u0000\u0000\u0380\u0467"+
		"\u00030\u0018\u0000\u0381\u0382\u0007\u0007\u0000\u0000\u0382\u0384\u0003"+
		",\u0016\u0000\u0383\u0385\u0005X\u0000\u0000\u0384\u0383\u0001\u0000\u0000"+
		"\u0000\u0384\u0385\u0001\u0000\u0000\u0000\u0385\u0386\u0001\u0000\u0000"+
		"\u0000\u0386\u0387\u0003\u00ba]\u0000\u0387\u0467\u0001\u0000\u0000\u0000"+
		"\u0388\u038a\u0007\u0007\u0000\u0000\u0389\u038b\u0005\u00f1\u0000\u0000"+
		"\u038a\u0389\u0001\u0000\u0000\u0000\u038a\u038b\u0001\u0000\u0000\u0000"+
		"\u038b\u038d\u0001\u0000\u0000\u0000\u038c\u038e\u0007\b\u0000\u0000\u038d"+
		"\u038c\u0001\u0000\u0000\u0000\u038d\u038e\u0001\u0000\u0000\u0000\u038e"+
		"\u038f\u0001\u0000\u0000\u0000\u038f\u0391\u0003\u00ba]\u0000\u0390\u0392"+
		"\u0003(\u0014\u0000\u0391\u0390\u0001\u0000\u0000\u0000\u0391\u0392\u0001"+
		"\u0000\u0000\u0000\u0392\u0394\u0001\u0000\u0000\u0000\u0393\u0395\u0003"+
		"2\u0019\u0000\u0394\u0393\u0001\u0000\u0000\u0000\u0394\u0395\u0001\u0000"+
		"\u0000\u0000\u0395\u0467\u0001\u0000\u0000\u0000\u0396\u0398\u0007\u0007"+
		"\u0000\u0000\u0397\u0399\u0005\u00be\u0000\u0000\u0398\u0397\u0001\u0000"+
		"\u0000\u0000\u0398\u0399\u0001\u0000\u0000\u0000\u0399\u039a\u0001\u0000"+
		"\u0000\u0000\u039a\u0467\u0003\"\u0011\u0000\u039b\u039c\u0005+\u0000"+
		"\u0000\u039c\u039d\u0005\u00a2\u0000\u0000\u039d\u039e\u0003,\u0016\u0000"+
		"\u039e\u039f\u0003\u00ba]\u0000\u039f\u03a0\u0005}\u0000\u0000\u03a0\u03a1"+
		"\u0007\t\u0000\u0000\u03a1\u0467\u0001\u0000\u0000\u0000\u03a2\u03a3\u0005"+
		"+\u0000\u0000\u03a3\u03a4\u0005\u00a2\u0000\u0000\u03a4\u03a5\u0005\u00f1"+
		"\u0000\u0000\u03a5\u03a6\u0003\u00ba]\u0000\u03a6\u03a7\u0005}\u0000\u0000"+
		"\u03a7\u03a8\u0007\t\u0000\u0000\u03a8\u0467\u0001\u0000\u0000\u0000\u03a9"+
		"\u03aa\u0005\u00c5\u0000\u0000\u03aa\u03ab\u0005\u00f1\u0000\u0000\u03ab"+
		"\u0467\u0003\u00ba]\u0000\u03ac\u03ad\u0005\u00c5\u0000\u0000\u03ad\u03ae"+
		"\u0005h\u0000\u0000\u03ae\u0467\u0003\u00ba]\u0000\u03af\u03b7\u0005\u00c5"+
		"\u0000\u0000\u03b0\u03b8\u0005\u0135\u0000\u0000\u03b1\u03b3\t\u0000\u0000"+
		"\u0000\u03b2\u03b1\u0001\u0000\u0000\u0000\u03b3\u03b6\u0001\u0000\u0000"+
		"\u0000\u03b4\u03b5\u0001\u0000\u0000\u0000\u03b4\u03b2\u0001\u0000\u0000"+
		"\u0000\u03b5\u03b8\u0001\u0000\u0000\u0000\u03b6\u03b4\u0001\u0000\u0000"+
		"\u0000\u03b7\u03b0\u0001\u0000\u0000\u0000\u03b7\u03b4\u0001\u0000\u0000"+
		"\u0000\u03b8\u0467\u0001\u0000\u0000\u0000\u03b9\u03bb\u0005\u001b\u0000"+
		"\u0000\u03ba\u03bc\u0005\u0083\u0000\u0000\u03bb\u03ba\u0001\u0000\u0000"+
		"\u0000\u03bb\u03bc\u0001\u0000\u0000\u0000\u03bc\u03bd\u0001\u0000\u0000"+
		"\u0000\u03bd\u03be\u0005\u00f1\u0000\u0000\u03be\u03c1\u0003\u00ba]\u0000"+
		"\u03bf\u03c0\u0005\u00a5\u0000\u0000\u03c0\u03c2\u0003<\u001e\u0000\u03c1"+
		"\u03bf\u0001\u0000\u0000\u0000\u03c1\u03c2\u0001\u0000\u0000\u0000\u03c2"+
		"\u03c7\u0001\u0000\u0000\u0000\u03c3\u03c5\u0005\u0012\u0000\u0000\u03c4"+
		"\u03c3\u0001\u0000\u0000\u0000\u03c4\u03c5\u0001\u0000\u0000\u0000\u03c5"+
		"\u03c6\u0001\u0000\u0000\u0000\u03c6\u03c8\u0003\"\u0011\u0000\u03c7\u03c4"+
		"\u0001\u0000\u0000\u0000\u03c7\u03c8\u0001\u0000\u0000\u0000\u03c8\u0467"+
		"\u0001\u0000\u0000\u0000\u03c9\u03ca\u0005\u0109\u0000\u0000\u03ca\u03cd"+
		"\u0005\u00f1\u0000\u0000\u03cb\u03cc\u0005p\u0000\u0000\u03cc\u03ce\u0005"+
		"U\u0000\u0000\u03cd\u03cb\u0001\u0000\u0000\u0000\u03cd\u03ce\u0001\u0000"+
		"\u0000\u0000\u03ce\u03cf\u0001\u0000\u0000\u0000\u03cf\u0467\u0003\u00ba"+
		"]\u0000\u03d0\u03d1\u0005#\u0000\u0000\u03d1\u0467\u0005\u001b\u0000\u0000"+
		"\u03d2\u03d3\u0005\u008b\u0000\u0000\u03d3\u03d5\u0005=\u0000\u0000\u03d4"+
		"\u03d6\u0005\u008c\u0000\u0000\u03d5\u03d4\u0001\u0000\u0000\u0000\u03d5"+
		"\u03d6\u0001\u0000\u0000\u0000\u03d6\u03d7\u0001\u0000\u0000\u0000\u03d7"+
		"\u03d8\u0005w\u0000\u0000\u03d8\u03da\u0005\u0135\u0000\u0000\u03d9\u03db"+
		"\u0005\u00ae\u0000\u0000\u03da\u03d9\u0001\u0000\u0000\u0000\u03da\u03db"+
		"\u0001\u0000\u0000\u0000\u03db\u03dc\u0001\u0000\u0000\u0000\u03dc\u03dd"+
		"\u0005|\u0000\u0000\u03dd\u03de\u0005\u00f1\u0000\u0000\u03de\u03e0\u0003"+
		"\u00ba]\u0000\u03df\u03e1\u0003(\u0014\u0000\u03e0\u03df\u0001\u0000\u0000"+
		"\u0000\u03e0\u03e1\u0001\u0000\u0000\u0000\u03e1\u0467\u0001\u0000\u0000"+
		"\u0000\u03e2\u03e3\u0005\u0104\u0000\u0000\u03e3\u03e4\u0005\u00f1\u0000"+
		"\u0000\u03e4\u03e6\u0003\u00ba]\u0000\u03e5\u03e7\u0003(\u0014\u0000\u03e6"+
		"\u03e5\u0001\u0000\u0000\u0000\u03e6\u03e7\u0001\u0000\u0000\u0000\u03e7"+
		"\u0467\u0001\u0000\u0000\u0000\u03e8\u03e9\u0005\u0099\u0000\u0000\u03e9"+
		"\u03ea\u0005\u00c7\u0000\u0000\u03ea\u03eb\u0005\u00f1\u0000\u0000\u03eb"+
		"\u03ee\u0003\u00ba]\u0000\u03ec\u03ed\u0007\n\u0000\u0000\u03ed\u03ef"+
		"\u0005\u00b1\u0000\u0000\u03ee\u03ec\u0001\u0000\u0000\u0000\u03ee\u03ef"+
		"\u0001\u0000\u0000\u0000\u03ef\u0467\u0001\u0000\u0000\u0000\u03f0\u03f1"+
		"\u0007\u000b\u0000\u0000\u03f1\u03f5\u0003\u011a\u008d\u0000\u03f2\u03f4"+
		"\t\u0000\u0000\u0000\u03f3\u03f2\u0001\u0000\u0000\u0000\u03f4\u03f7\u0001"+
		"\u0000\u0000\u0000\u03f5\u03f6\u0001\u0000\u0000\u0000\u03f5\u03f3\u0001"+
		"\u0000\u0000\u0000\u03f6\u0467\u0001\u0000\u0000\u0000\u03f7\u03f5\u0001"+
		"\u0000\u0000\u0000\u03f8\u03f9\u0005\u00df\u0000\u0000\u03f9\u03fd\u0005"+
		"\u00d0\u0000\u0000\u03fa\u03fc\t\u0000\u0000\u0000\u03fb\u03fa\u0001\u0000"+
		"\u0000\u0000\u03fc\u03ff\u0001\u0000\u0000\u0000\u03fd\u03fe\u0001\u0000"+
		"\u0000\u0000\u03fd\u03fb\u0001\u0000\u0000\u0000\u03fe\u0467\u0001\u0000"+
		"\u0000\u0000\u03ff\u03fd\u0001\u0000\u0000\u0000\u0400\u0401\u0005\u00df"+
		"\u0000\u0000\u0401\u0402\u0005\u00f8\u0000\u0000\u0402\u0403\u0005\u011e"+
		"\u0000\u0000\u0403\u0467\u0003\u00e8t\u0000\u0404\u0405\u0005\u00df\u0000"+
		"\u0000\u0405\u0406\u0005\u00f8\u0000\u0000\u0406\u0407\u0005\u011e\u0000"+
		"\u0000\u0407\u0467\u0007\f\u0000\u0000\u0408\u0409\u0005\u00df\u0000\u0000"+
		"\u0409\u040a\u0005\u00f8\u0000\u0000\u040a\u040e\u0005\u011e\u0000\u0000"+
		"\u040b\u040d\t\u0000\u0000\u0000\u040c\u040b\u0001\u0000\u0000\u0000\u040d"+
		"\u0410\u0001\u0000\u0000\u0000\u040e\u040f\u0001\u0000\u0000\u0000\u040e"+
		"\u040c\u0001\u0000\u0000\u0000\u040f\u0467\u0001\u0000\u0000\u0000\u0410"+
		"\u040e\u0001\u0000\u0000\u0000\u0411\u0412\u0005\u00df\u0000\u0000\u0412"+
		"\u0413\u0003\u0010\b\u0000\u0413\u0414\u0005\u011f\u0000\u0000\u0414\u0415"+
		"\u0003\u0012\t\u0000\u0415\u0467\u0001\u0000\u0000\u0000\u0416\u0417\u0005"+
		"\u00df\u0000\u0000\u0417\u041f\u0003\u0010\b\u0000\u0418\u041c\u0005\u011f"+
		"\u0000\u0000\u0419\u041b\t\u0000\u0000\u0000\u041a\u0419\u0001\u0000\u0000"+
		"\u0000\u041b\u041e\u0001\u0000\u0000\u0000\u041c\u041d\u0001\u0000\u0000"+
		"\u0000\u041c\u041a\u0001\u0000\u0000\u0000\u041d\u0420\u0001\u0000\u0000"+
		"\u0000\u041e\u041c\u0001\u0000\u0000\u0000\u041f\u0418\u0001\u0000\u0000"+
		"\u0000\u041f\u0420\u0001\u0000\u0000\u0000\u0420\u0467\u0001\u0000\u0000"+
		"\u0000\u0421\u0425\u0005\u00df\u0000\u0000\u0422\u0424\t\u0000\u0000\u0000"+
		"\u0423\u0422\u0001\u0000\u0000\u0000\u0424\u0427\u0001\u0000\u0000\u0000"+
		"\u0425\u0426\u0001\u0000\u0000\u0000\u0425\u0423\u0001\u0000\u0000\u0000"+
		"\u0426\u0428\u0001\u0000\u0000\u0000\u0427\u0425\u0001\u0000\u0000\u0000"+
		"\u0428\u0429\u0005\u011f\u0000\u0000\u0429\u0467\u0003\u0012\t\u0000\u042a"+
		"\u042e\u0005\u00df\u0000\u0000\u042b\u042d\t\u0000\u0000\u0000\u042c\u042b"+
		"\u0001\u0000\u0000\u0000\u042d\u0430\u0001\u0000\u0000\u0000\u042e\u042f"+
		"\u0001\u0000\u0000\u0000\u042e\u042c\u0001\u0000\u0000\u0000\u042f\u0467"+
		"\u0001\u0000\u0000\u0000\u0430\u042e\u0001\u0000\u0000\u0000\u0431\u0432"+
		"\u0005\u00ca\u0000\u0000\u0432\u0467\u0003\u0010\b\u0000\u0433\u0437\u0005"+
		"\u00ca\u0000\u0000\u0434\u0436\t\u0000\u0000\u0000\u0435\u0434\u0001\u0000"+
		"\u0000\u0000\u0436\u0439\u0001\u0000\u0000\u0000\u0437\u0438\u0001\u0000"+
		"\u0000\u0000\u0437\u0435\u0001\u0000\u0000\u0000\u0438\u0467\u0001\u0000"+
		"\u0000\u0000\u0439\u0437\u0001\u0000\u0000\u0000\u043a\u043b\u00053\u0000"+
		"\u0000\u043b\u043f\u0005t\u0000\u0000\u043c\u043d\u0005p\u0000\u0000\u043d"+
		"\u043e\u0005\u009e\u0000\u0000\u043e\u0440\u0005U\u0000\u0000\u043f\u043c"+
		"\u0001\u0000\u0000\u0000\u043f\u0440\u0001\u0000\u0000\u0000\u0440\u0441"+
		"\u0001\u0000\u0000\u0000\u0441\u0442\u0003\u011a\u008d\u0000\u0442\u0444"+
		"\u0005\u00a2\u0000\u0000\u0443\u0445\u0005\u00f1\u0000\u0000\u0444\u0443"+
		"\u0001\u0000\u0000\u0000\u0444\u0445\u0001\u0000\u0000\u0000\u0445\u0446"+
		"\u0001\u0000\u0000\u0000\u0446\u0449\u0003\u00ba]\u0000\u0447\u0448\u0005"+
		"\u0112\u0000\u0000\u0448\u044a\u0003\u011a\u008d\u0000\u0449\u0447\u0001"+
		"\u0000\u0000\u0000\u0449\u044a\u0001\u0000\u0000\u0000\u044a\u044b\u0001"+
		"\u0000\u0000\u0000\u044b\u044c\u0005\u0002\u0000\u0000\u044c\u044d\u0003"+
		"\u00bc^\u0000\u044d\u0450\u0005\u0003\u0000\u0000\u044e\u044f\u0005\u00a5"+
		"\u0000\u0000\u044f\u0451\u0003<\u001e\u0000\u0450\u044e\u0001\u0000\u0000"+
		"\u0000\u0450\u0451\u0001\u0000\u0000\u0000\u0451\u0467\u0001\u0000\u0000"+
		"\u0000\u0452\u0453\u0005N\u0000\u0000\u0453\u0456\u0005t\u0000\u0000\u0454"+
		"\u0455\u0005p\u0000\u0000\u0455\u0457\u0005U\u0000\u0000\u0456\u0454\u0001"+
		"\u0000\u0000\u0000\u0456\u0457\u0001\u0000\u0000\u0000\u0457\u0458\u0001"+
		"\u0000\u0000\u0000\u0458\u0459\u0003\u011a\u008d\u0000\u0459\u045b\u0005"+
		"\u00a2\u0000\u0000\u045a\u045c\u0005\u00f1\u0000\u0000\u045b\u045a\u0001"+
		"\u0000\u0000\u0000\u045b\u045c\u0001\u0000\u0000\u0000\u045c\u045d\u0001"+
		"\u0000\u0000\u0000\u045d\u045e\u0003\u00ba]\u0000\u045e\u0467\u0001\u0000"+
		"\u0000\u0000\u045f\u0463\u0003\u0014\n\u0000\u0460\u0462\t\u0000\u0000"+
		"\u0000\u0461\u0460\u0001\u0000\u0000\u0000\u0462\u0465\u0001\u0000\u0000"+
		"\u0000\u0463\u0464\u0001\u0000\u0000\u0000\u0463\u0461\u0001\u0000\u0000"+
		"\u0000\u0464\u0467\u0001\u0000\u0000\u0000\u0465\u0463\u0001\u0000\u0000"+
		"\u0000\u0466\u0145\u0001\u0000\u0000\u0000\u0466\u0147\u0001\u0000\u0000"+
		"\u0000\u0466\u014a\u0001\u0000\u0000\u0000\u0466\u014c\u0001\u0000\u0000"+
		"\u0000\u0466\u0150\u0001\u0000\u0000\u0000\u0466\u0156\u0001\u0000\u0000"+
		"\u0000\u0466\u0168\u0001\u0000\u0000\u0000\u0466\u016f\u0001\u0000\u0000"+
		"\u0000\u0466\u0175\u0001\u0000\u0000\u0000\u0466\u017f\u0001\u0000\u0000"+
		"\u0000\u0466\u018b\u0001\u0000\u0000\u0000\u0466\u019c\u0001\u0000\u0000"+
		"\u0000\u0466\u01b1\u0001\u0000\u0000\u0000\u0466\u01c2\u0001\u0000\u0000"+
		"\u0000\u0466\u01d3\u0001\u0000\u0000\u0000\u0466\u01de\u0001\u0000\u0000"+
		"\u0000\u0466\u01e5\u0001\u0000\u0000\u0000\u0466\u01ee\u0001\u0000\u0000"+
		"\u0000\u0466\u01f7\u0001\u0000\u0000\u0000\u0466\u0204\u0001\u0000\u0000"+
		"\u0000\u0466\u020f\u0001\u0000\u0000\u0000\u0466\u0216\u0001\u0000\u0000"+
		"\u0000\u0466\u021d\u0001\u0000\u0000\u0000\u0466\u0228\u0001\u0000\u0000"+
		"\u0000\u0466\u0233\u0001\u0000\u0000\u0000\u0466\u0242\u0001\u0000\u0000"+
		"\u0000\u0466\u024e\u0001\u0000\u0000\u0000\u0466\u025c\u0001\u0000\u0000"+
		"\u0000\u0466\u0266\u0001\u0000\u0000\u0000\u0466\u0274\u0001\u0000\u0000"+
		"\u0000\u0466\u027c\u0001\u0000\u0000\u0000\u0466\u028f\u0001\u0000\u0000"+
		"\u0000\u0466\u0298\u0001\u0000\u0000\u0000\u0466\u029e\u0001\u0000\u0000"+
		"\u0000\u0466\u02a8\u0001\u0000\u0000\u0000\u0466\u02af\u0001\u0000\u0000"+
		"\u0000\u0466\u02d2\u0001\u0000\u0000\u0000\u0466\u02e8\u0001\u0000\u0000"+
		"\u0000\u0466\u02f0\u0001\u0000\u0000\u0000\u0466\u030c\u0001\u0000\u0000"+
		"\u0000\u0466\u0316\u0001\u0000\u0000\u0000\u0466\u031b\u0001\u0000\u0000"+
		"\u0000\u0466\u0327\u0001\u0000\u0000\u0000\u0466\u0333\u0001\u0000\u0000"+
		"\u0000\u0466\u033c\u0001\u0000\u0000\u0000\u0466\u0344\u0001\u0000\u0000"+
		"\u0000\u0466\u0350\u0001\u0000\u0000\u0000\u0466\u0356\u0001\u0000\u0000"+
		"\u0000\u0466\u0368\u0001\u0000\u0000\u0000\u0466\u0370\u0001\u0000\u0000"+
		"\u0000\u0466\u0373\u0001\u0000\u0000\u0000\u0466\u037b\u0001\u0000\u0000"+
		"\u0000\u0466\u0381\u0001\u0000\u0000\u0000\u0466\u0388\u0001\u0000\u0000"+
		"\u0000\u0466\u0396\u0001\u0000\u0000\u0000\u0466\u039b\u0001\u0000\u0000"+
		"\u0000\u0466\u03a2\u0001\u0000\u0000\u0000\u0466\u03a9\u0001\u0000\u0000"+
		"\u0000\u0466\u03ac\u0001\u0000\u0000\u0000\u0466\u03af\u0001\u0000\u0000"+
		"\u0000\u0466\u03b9\u0001\u0000\u0000\u0000\u0466\u03c9\u0001\u0000\u0000"+
		"\u0000\u0466\u03d0\u0001\u0000\u0000\u0000\u0466\u03d2\u0001\u0000\u0000"+
		"\u0000\u0466\u03e2\u0001\u0000\u0000\u0000\u0466\u03e8\u0001\u0000\u0000"+
		"\u0000\u0466\u03f0\u0001\u0000\u0000\u0000\u0466\u03f8\u0001\u0000\u0000"+
		"\u0000\u0466\u0400\u0001\u0000\u0000\u0000\u0466\u0404\u0001\u0000\u0000"+
		"\u0000\u0466\u0408\u0001\u0000\u0000\u0000\u0466\u0411\u0001\u0000\u0000"+
		"\u0000\u0466\u0416\u0001\u0000\u0000\u0000\u0466\u0421\u0001\u0000\u0000"+
		"\u0000\u0466\u042a\u0001\u0000\u0000\u0000\u0466\u0431\u0001\u0000\u0000"+
		"\u0000\u0466\u0433\u0001\u0000\u0000\u0000\u0466\u043a\u0001\u0000\u0000"+
		"\u0000\u0466\u0452\u0001\u0000\u0000\u0000\u0466\u045f\u0001\u0000\u0000"+
		"\u0000\u0467\u000f\u0001\u0000\u0000\u0000\u0468\u0469\u0003\u011e\u008f"+
		"\u0000\u0469\u0011\u0001\u0000\u0000\u0000\u046a\u046b\u0003\u011e\u008f"+
		"\u0000\u046b\u0013\u0001\u0000\u0000\u0000\u046c\u046d\u00053\u0000\u0000"+
		"\u046d\u0515\u0005\u00d0\u0000\u0000\u046e\u046f\u0005N\u0000\u0000\u046f"+
		"\u0515\u0005\u00d0\u0000\u0000\u0470\u0472\u0005k\u0000\u0000\u0471\u0473"+
		"\u0005\u00d0\u0000\u0000\u0472\u0471\u0001\u0000\u0000\u0000\u0472\u0473"+
		"\u0001\u0000\u0000\u0000\u0473\u0515\u0001\u0000\u0000\u0000\u0474\u0476"+
		"\u0005\u00cd\u0000\u0000\u0475\u0477\u0005\u00d0\u0000\u0000\u0476\u0475"+
		"\u0001\u0000\u0000\u0000\u0476\u0477\u0001\u0000\u0000\u0000\u0477\u0515"+
		"\u0001\u0000\u0000\u0000\u0478\u0479\u0005\u00e2\u0000\u0000\u0479\u0515"+
		"\u0005k\u0000\u0000\u047a\u047b\u0005\u00e2\u0000\u0000\u047b\u047d\u0005"+
		"\u00d0\u0000\u0000\u047c\u047e\u0005k\u0000\u0000\u047d\u047c\u0001\u0000"+
		"\u0000\u0000\u047d\u047e\u0001\u0000\u0000\u0000\u047e\u0515\u0001\u0000"+
		"\u0000\u0000\u047f\u0480\u0005\u00e2\u0000\u0000\u0480\u0515\u0005\u00ba"+
		"\u0000\u0000\u0481\u0482\u0005\u00e2\u0000\u0000\u0482\u0515\u0005\u00d1"+
		"\u0000\u0000\u0483\u0484\u0005\u00e2\u0000\u0000\u0484\u0485\u00056\u0000"+
		"\u0000\u0485\u0515\u0005\u00d1\u0000\u0000\u0486\u0487\u0005W\u0000\u0000"+
		"\u0487\u0515\u0005\u00f1\u0000\u0000\u0488\u0489\u0005r\u0000\u0000\u0489"+
		"\u0515\u0005\u00f1\u0000\u0000\u048a\u048b\u0005\u00e2\u0000\u0000\u048b"+
		"\u0515\u0005.\u0000\u0000\u048c\u048d\u0005\u00e2\u0000\u0000\u048d\u048e"+
		"\u00053\u0000\u0000\u048e\u0515\u0005\u00f1\u0000\u0000\u048f\u0490\u0005"+
		"\u00e2\u0000\u0000\u0490\u0515\u0005\u0100\u0000\u0000\u0491\u0492\u0005"+
		"\u00e2\u0000\u0000\u0492\u0515\u0005u\u0000\u0000\u0493\u0494\u0005\u00e2"+
		"\u0000\u0000\u0494\u0515\u0005\u008f\u0000\u0000\u0495\u0496\u00053\u0000"+
		"\u0000\u0496\u0515\u0005t\u0000\u0000\u0497\u0498\u0005N\u0000\u0000\u0498"+
		"\u0515\u0005t\u0000\u0000\u0499\u049a\u0005\u000b\u0000\u0000\u049a\u0515"+
		"\u0005t\u0000\u0000\u049b\u049c\u0005\u008e\u0000\u0000\u049c\u0515\u0005"+
		"\u00f1\u0000\u0000\u049d\u049e\u0005\u008e\u0000\u0000\u049e\u0515\u0005"+
		">\u0000\u0000\u049f\u04a0\u0005\u010d\u0000\u0000\u04a0\u0515\u0005\u00f1"+
		"\u0000\u0000\u04a1\u04a2\u0005\u010d\u0000\u0000\u04a2\u0515\u0005>\u0000"+
		"\u0000\u04a3\u04a4\u00053\u0000\u0000\u04a4\u04a5\u0005\u00f5\u0000\u0000"+
		"\u04a5\u0515\u0005\u0091\u0000\u0000\u04a6\u04a7\u0005N\u0000\u0000\u04a7"+
		"\u04a8\u0005\u00f5\u0000\u0000\u04a8\u0515\u0005\u0091\u0000\u0000\u04a9"+
		"\u04aa\u0005\u000b\u0000\u0000\u04aa\u04ab\u0005\u00f1\u0000\u0000\u04ab"+
		"\u04ac\u0003\u00c0`\u0000\u04ac\u04ad\u0005\u009e\u0000\u0000\u04ad\u04ae"+
		"\u0005%\u0000\u0000\u04ae\u0515\u0001\u0000\u0000\u0000\u04af\u04b0\u0005"+
		"\u000b\u0000\u0000\u04b0\u04b1\u0005\u00f1\u0000\u0000\u04b1\u04b2\u0003"+
		"\u00c0`\u0000\u04b2\u04b3\u0005%\u0000\u0000\u04b3\u04b4\u0005\u001a\u0000"+
		"\u0000\u04b4\u0515\u0001\u0000\u0000\u0000\u04b5\u04b6\u0005\u000b\u0000"+
		"\u0000\u04b6\u04b7\u0005\u00f1\u0000\u0000\u04b7\u04b8\u0003\u00c0`\u0000"+
		"\u04b8\u04b9\u0005\u009e\u0000\u0000\u04b9\u04ba\u0005\u00e6\u0000\u0000"+
		"\u04ba\u0515\u0001\u0000\u0000\u0000\u04bb\u04bc\u0005\u000b\u0000\u0000"+
		"\u04bc\u04bd\u0005\u00f1\u0000\u0000\u04bd\u04be\u0003\u00c0`\u0000\u04be"+
		"\u04bf\u0005\u00e3\u0000\u0000\u04bf\u04c0\u0005\u001a\u0000\u0000\u04c0"+
		"\u0515\u0001\u0000\u0000\u0000\u04c1\u04c2\u0005\u000b\u0000\u0000\u04c2"+
		"\u04c3\u0005\u00f1\u0000\u0000\u04c3\u04c4\u0003\u00c0`\u0000\u04c4\u04c5"+
		"\u0005\u009e\u0000\u0000\u04c5\u04c6\u0005\u00e3\u0000\u0000\u04c6\u0515"+
		"\u0001\u0000\u0000\u0000\u04c7\u04c8\u0005\u000b\u0000\u0000\u04c8\u04c9"+
		"\u0005\u00f1\u0000\u0000\u04c9\u04ca\u0003\u00c0`\u0000\u04ca\u04cb\u0005"+
		"\u009e\u0000\u0000\u04cb\u04cc\u0005\u00e9\u0000\u0000\u04cc\u04cd\u0005"+
		"\u0012\u0000\u0000\u04cd\u04ce\u0005I\u0000\u0000\u04ce\u0515\u0001\u0000"+
		"\u0000\u0000\u04cf\u04d0\u0005\u000b\u0000\u0000\u04d0\u04d1\u0005\u00f1"+
		"\u0000\u0000\u04d1\u04d2\u0003\u00c0`\u0000\u04d2\u04d3\u0005\u00df\u0000"+
		"\u0000\u04d3\u04d4\u0005\u00e3\u0000\u0000\u04d4\u04d5\u0005\u008d\u0000"+
		"\u0000\u04d5\u0515\u0001\u0000\u0000\u0000\u04d6\u04d7\u0005\u000b\u0000"+
		"\u0000\u04d7\u04d8\u0005\u00f1\u0000\u0000\u04d8\u04d9\u0003\u00c0`\u0000"+
		"\u04d9\u04da\u0005T\u0000\u0000\u04da\u04db\u0005\u00af\u0000\u0000\u04db"+
		"\u0515\u0001\u0000\u0000\u0000\u04dc\u04dd\u0005\u000b\u0000\u0000\u04dd"+
		"\u04de\u0005\u00f1\u0000\u0000\u04de\u04df\u0003\u00c0`\u0000\u04df\u04e0"+
		"\u0005\u0010\u0000\u0000\u04e0\u04e1\u0005\u00af\u0000\u0000\u04e1\u0515"+
		"\u0001\u0000\u0000\u0000\u04e2\u04e3\u0005\u000b\u0000\u0000\u04e3\u04e4"+
		"\u0005\u00f1\u0000\u0000\u04e4\u04e5\u0003\u00c0`\u0000\u04e5\u04e6\u0005"+
		"\u0107\u0000\u0000\u04e6\u04e7\u0005\u00af\u0000\u0000\u04e7\u0515\u0001"+
		"\u0000\u0000\u0000\u04e8\u04e9\u0005\u000b\u0000\u0000\u04e9\u04ea\u0005"+
		"\u00f1\u0000\u0000\u04ea\u04eb\u0003\u00c0`\u0000\u04eb\u04ec\u0005\u00fd"+
		"\u0000\u0000\u04ec\u0515\u0001\u0000\u0000\u0000\u04ed\u04ee\u0005\u000b"+
		"\u0000\u0000\u04ee\u04ef\u0005\u00f1\u0000\u0000\u04ef\u04f1\u0003\u00c0"+
		"`\u0000\u04f0\u04f2\u0003(\u0014\u0000\u04f1\u04f0\u0001\u0000\u0000\u0000"+
		"\u04f1\u04f2\u0001\u0000\u0000\u0000\u04f2\u04f3\u0001\u0000\u0000\u0000"+
		"\u04f3\u04f4\u0005-\u0000\u0000\u04f4\u0515\u0001\u0000\u0000\u0000\u04f5"+
		"\u04f6\u0005\u000b\u0000\u0000\u04f6\u04f7\u0005\u00f1\u0000\u0000\u04f7"+
		"\u04f9\u0003\u00c0`\u0000\u04f8\u04fa\u0003(\u0014\u0000\u04f9\u04f8\u0001"+
		"\u0000\u0000\u0000\u04f9\u04fa\u0001\u0000\u0000\u0000\u04fa\u04fb\u0001"+
		"\u0000\u0000\u0000\u04fb\u04fc\u00050\u0000\u0000\u04fc\u0515\u0001\u0000"+
		"\u0000\u0000\u04fd\u04fe\u0005\u000b\u0000\u0000\u04fe\u04ff\u0005\u00f1"+
		"\u0000\u0000\u04ff\u0501\u0003\u00c0`\u0000\u0500\u0502\u0003(\u0014\u0000"+
		"\u0501\u0500\u0001\u0000\u0000\u0000\u0501\u0502\u0001\u0000\u0000\u0000"+
		"\u0502\u0503\u0001\u0000\u0000\u0000\u0503\u0504\u0005\u00df\u0000\u0000"+
		"\u0504\u0505\u0005_\u0000\u0000\u0505\u0515\u0001\u0000\u0000\u0000\u0506"+
		"\u0507\u0005\u000b\u0000\u0000\u0507\u0508\u0005\u00f1\u0000\u0000\u0508"+
		"\u050a\u0003\u00c0`\u0000\u0509\u050b\u0003(\u0014\u0000\u050a\u0509\u0001"+
		"\u0000\u0000\u0000\u050a\u050b\u0001\u0000\u0000\u0000\u050b\u050c\u0001"+
		"\u0000\u0000\u0000\u050c\u050d\u0005\u00c9\u0000\u0000\u050d\u050e\u0005"+
		"*\u0000\u0000\u050e\u0515\u0001\u0000\u0000\u0000\u050f\u0510\u0005\u00e7"+
		"\u0000\u0000\u0510\u0515\u0005\u00ff\u0000\u0000\u0511\u0515\u0005,\u0000"+
		"\u0000\u0512\u0515\u0005\u00d2\u0000\u0000\u0513\u0515\u0005H\u0000\u0000"+
		"\u0514\u046c\u0001\u0000\u0000\u0000\u0514\u046e\u0001\u0000\u0000\u0000"+
		"\u0514\u0470\u0001\u0000\u0000\u0000\u0514\u0474\u0001\u0000\u0000\u0000"+
		"\u0514\u0478\u0001\u0000\u0000\u0000\u0514\u047a\u0001\u0000\u0000\u0000"+
		"\u0514\u047f\u0001\u0000\u0000\u0000\u0514\u0481\u0001\u0000\u0000\u0000"+
		"\u0514\u0483\u0001\u0000\u0000\u0000\u0514\u0486\u0001\u0000\u0000\u0000"+
		"\u0514\u0488\u0001\u0000\u0000\u0000\u0514\u048a\u0001\u0000\u0000\u0000"+
		"\u0514\u048c\u0001\u0000\u0000\u0000\u0514\u048f\u0001\u0000\u0000\u0000"+
		"\u0514\u0491\u0001\u0000\u0000\u0000\u0514\u0493\u0001\u0000\u0000\u0000"+
		"\u0514\u0495\u0001\u0000\u0000\u0000\u0514\u0497\u0001\u0000\u0000\u0000"+
		"\u0514\u0499\u0001\u0000\u0000\u0000\u0514\u049b\u0001\u0000\u0000\u0000"+
		"\u0514\u049d\u0001\u0000\u0000\u0000\u0514\u049f\u0001\u0000\u0000\u0000"+
		"\u0514\u04a1\u0001\u0000\u0000\u0000\u0514\u04a3\u0001\u0000\u0000\u0000"+
		"\u0514\u04a6\u0001\u0000\u0000\u0000\u0514\u04a9\u0001\u0000\u0000\u0000"+
		"\u0514\u04af\u0001\u0000\u0000\u0000\u0514\u04b5\u0001\u0000\u0000\u0000"+
		"\u0514\u04bb\u0001\u0000\u0000\u0000\u0514\u04c1\u0001\u0000\u0000\u0000"+
		"\u0514\u04c7\u0001\u0000\u0000\u0000\u0514\u04cf\u0001\u0000\u0000\u0000"+
		"\u0514\u04d6\u0001\u0000\u0000\u0000\u0514\u04dc\u0001\u0000\u0000\u0000"+
		"\u0514\u04e2\u0001\u0000\u0000\u0000\u0514\u04e8\u0001\u0000\u0000\u0000"+
		"\u0514\u04ed\u0001\u0000\u0000\u0000\u0514\u04f5\u0001\u0000\u0000\u0000"+
		"\u0514\u04fd\u0001\u0000\u0000\u0000\u0514\u0506\u0001\u0000\u0000\u0000"+
		"\u0514\u050f\u0001\u0000\u0000\u0000\u0514\u0511\u0001\u0000\u0000\u0000"+
		"\u0514\u0512\u0001\u0000\u0000\u0000\u0514\u0513\u0001\u0000\u0000\u0000"+
		"\u0515\u0015\u0001\u0000\u0000\u0000\u0516\u0518\u00053\u0000\u0000\u0517"+
		"\u0519\u0005\u00f5\u0000\u0000\u0518\u0517\u0001\u0000\u0000\u0000\u0518"+
		"\u0519\u0001\u0000\u0000\u0000\u0519\u051b\u0001\u0000\u0000\u0000\u051a"+
		"\u051c\u0005Y\u0000\u0000\u051b\u051a\u0001\u0000\u0000\u0000\u051b\u051c"+
		"\u0001\u0000\u0000\u0000\u051c\u051d\u0001\u0000\u0000\u0000\u051d\u0521"+
		"\u0005\u00f1\u0000\u0000\u051e\u051f\u0005p\u0000\u0000\u051f\u0520\u0005"+
		"\u009e\u0000\u0000\u0520\u0522\u0005U\u0000\u0000\u0521\u051e\u0001\u0000"+
		"\u0000\u0000\u0521\u0522\u0001\u0000\u0000\u0000\u0522\u0523\u0001\u0000"+
		"\u0000\u0000\u0523\u0524\u0003\u00ba]\u0000\u0524\u0017\u0001\u0000\u0000"+
		"\u0000\u0525\u0526\u00053\u0000\u0000\u0526\u0528\u0005\u00a6\u0000\u0000"+
		"\u0527\u0525\u0001\u0000\u0000\u0000\u0527\u0528\u0001\u0000\u0000\u0000"+
		"\u0528\u0529\u0001\u0000\u0000\u0000\u0529\u052a\u0005\u00c9\u0000\u0000"+
		"\u052a\u052b\u0005\u00f1\u0000\u0000\u052b\u052c\u0003\u00ba]\u0000\u052c"+
		"\u0019\u0001\u0000\u0000\u0000\u052d\u052e\u0005%\u0000\u0000\u052e\u052f"+
		"\u0005\u001a\u0000\u0000\u052f\u0533\u0003\u00a2Q\u0000\u0530\u0531\u0005"+
		"\u00e6\u0000\u0000\u0531\u0532\u0005\u001a\u0000\u0000\u0532\u0534\u0003"+
		"\u00a6S\u0000\u0533\u0530\u0001\u0000\u0000\u0000\u0533\u0534\u0001\u0000"+
		"\u0000\u0000\u0534\u0535\u0001\u0000\u0000\u0000\u0535\u0536\u0005|\u0000"+
		"\u0000\u0536\u0537\u0005\u0139\u0000\u0000\u0537\u0538\u0005\u0019\u0000"+
		"\u0000\u0538\u001b\u0001\u0000\u0000\u0000\u0539\u053a\u0005\u00e3\u0000"+
		"\u0000\u053a\u053b\u0005\u001a\u0000\u0000\u053b\u053c\u0003\u00a2Q\u0000"+
		"\u053c\u053f\u0005\u00a2\u0000\u0000\u053d\u0540\u0003D\"\u0000\u053e"+
		"\u0540\u0003F#\u0000\u053f\u053d\u0001\u0000\u0000\u0000\u053f\u053e\u0001"+
		"\u0000\u0000\u0000\u0540\u0544\u0001\u0000\u0000\u0000\u0541\u0542\u0005"+
		"\u00e9\u0000\u0000\u0542\u0543\u0005\u0012\u0000\u0000\u0543\u0545\u0005"+
		"I\u0000\u0000\u0544\u0541\u0001\u0000\u0000\u0000\u0544\u0545\u0001\u0000"+
		"\u0000\u0000\u0545\u001d\u0001\u0000\u0000\u0000\u0546\u0547\u0005\u008d"+
		"\u0000\u0000\u0547\u0548\u0005\u0135\u0000\u0000\u0548\u001f\u0001\u0000"+
		"\u0000\u0000\u0549\u054a\u0005+\u0000\u0000\u054a\u054b\u0005\u0135\u0000"+
		"\u0000\u054b!\u0001\u0000\u0000\u0000\u054c\u054e\u00034\u001a\u0000\u054d"+
		"\u054c\u0001\u0000\u0000\u0000\u054d\u054e\u0001\u0000\u0000\u0000\u054e"+
		"\u054f\u0001\u0000\u0000\u0000\u054f\u0550\u0003V+\u0000\u0550\u0551\u0003"+
		"R)\u0000\u0551#\u0001\u0000\u0000\u0000\u0552\u0553\u0005y\u0000\u0000"+
		"\u0553\u0555\u0005\u00ae\u0000\u0000\u0554\u0556\u0005\u00f1\u0000\u0000"+
		"\u0555\u0554\u0001\u0000\u0000\u0000\u0555\u0556\u0001\u0000\u0000\u0000"+
		"\u0556\u0557\u0001\u0000\u0000\u0000\u0557\u055e\u0003\u00ba]\u0000\u0558"+
		"\u055c\u0003(\u0014\u0000\u0559\u055a\u0005p\u0000\u0000\u055a\u055b\u0005"+
		"\u009e\u0000\u0000\u055b\u055d\u0005U\u0000\u0000\u055c\u0559\u0001\u0000"+
		"\u0000\u0000\u055c\u055d\u0001\u0000\u0000\u0000\u055d\u055f\u0001\u0000"+
		"\u0000\u0000\u055e\u0558\u0001\u0000\u0000\u0000\u055e\u055f\u0001\u0000"+
		"\u0000\u0000\u055f\u0561\u0001\u0000\u0000\u0000\u0560\u0562\u0003\u00a2"+
		"Q\u0000\u0561\u0560\u0001\u0000\u0000\u0000\u0561\u0562\u0001\u0000\u0000"+
		"\u0000\u0562\u0590\u0001\u0000\u0000\u0000\u0563\u0564\u0005y\u0000\u0000"+
		"\u0564\u0566\u0005|\u0000\u0000\u0565\u0567\u0005\u00f1\u0000\u0000\u0566"+
		"\u0565\u0001\u0000\u0000\u0000\u0566\u0567\u0001\u0000\u0000\u0000\u0567"+
		"\u0568\u0001\u0000\u0000\u0000\u0568\u056a\u0003\u00ba]\u0000\u0569\u056b"+
		"\u0003(\u0014\u0000\u056a\u0569\u0001\u0000\u0000\u0000\u056a\u056b\u0001"+
		"\u0000\u0000\u0000\u056b\u056f\u0001\u0000\u0000\u0000\u056c\u056d\u0005"+
		"p\u0000\u0000\u056d\u056e\u0005\u009e\u0000\u0000\u056e\u0570\u0005U\u0000"+
		"\u0000\u056f\u056c\u0001\u0000\u0000\u0000\u056f\u0570\u0001\u0000\u0000"+
		"\u0000\u0570\u0572\u0001\u0000\u0000\u0000\u0571\u0573\u0003\u00a2Q\u0000"+
		"\u0572\u0571\u0001\u0000\u0000\u0000\u0572\u0573\u0001\u0000\u0000\u0000"+
		"\u0573\u0590\u0001\u0000\u0000\u0000\u0574\u0575\u0005y\u0000\u0000\u0575"+
		"\u0577\u0005\u00ae\u0000\u0000\u0576\u0578\u0005\u008c\u0000\u0000\u0577"+
		"\u0576\u0001\u0000\u0000\u0000\u0577\u0578\u0001\u0000\u0000\u0000\u0578"+
		"\u0579\u0001\u0000\u0000\u0000\u0579\u057a\u0005J\u0000\u0000\u057a\u057c"+
		"\u0005\u0135\u0000\u0000\u057b\u057d\u0003\u00b6[\u0000\u057c\u057b\u0001"+
		"\u0000\u0000\u0000\u057c\u057d\u0001\u0000\u0000\u0000\u057d\u057f\u0001"+
		"\u0000\u0000\u0000\u057e\u0580\u0003H$\u0000\u057f\u057e\u0001\u0000\u0000"+
		"\u0000\u057f\u0580\u0001\u0000\u0000\u0000\u0580\u0590\u0001\u0000\u0000"+
		"\u0000\u0581\u0582\u0005y\u0000\u0000\u0582\u0584\u0005\u00ae\u0000\u0000"+
		"\u0583\u0585\u0005\u008c\u0000\u0000\u0584\u0583\u0001\u0000\u0000\u0000"+
		"\u0584\u0585\u0001\u0000\u0000\u0000\u0585\u0586\u0001\u0000\u0000\u0000"+
		"\u0586\u0588\u0005J\u0000\u0000\u0587\u0589\u0005\u0135\u0000\u0000\u0588"+
		"\u0587\u0001\u0000\u0000\u0000\u0588\u0589\u0001\u0000\u0000\u0000\u0589"+
		"\u058a\u0001\u0000\u0000\u0000\u058a\u058d\u00038\u001c\u0000\u058b\u058c"+
		"\u0005\u00a5\u0000\u0000\u058c\u058e\u0003<\u001e\u0000\u058d\u058b\u0001"+
		"\u0000\u0000\u0000\u058d\u058e\u0001\u0000\u0000\u0000\u058e\u0590\u0001"+
		"\u0000\u0000\u0000\u058f\u0552\u0001\u0000\u0000\u0000\u058f\u0563\u0001"+
		"\u0000\u0000\u0000\u058f\u0574\u0001\u0000\u0000\u0000\u058f\u0581\u0001"+
		"\u0000\u0000\u0000\u0590%\u0001\u0000\u0000\u0000\u0591\u0593\u0003(\u0014"+
		"\u0000\u0592\u0594\u0003\u001e\u000f\u0000\u0593\u0592\u0001\u0000\u0000"+
		"\u0000\u0593\u0594\u0001\u0000\u0000\u0000\u0594\'\u0001\u0000\u0000\u0000"+
		"\u0595\u0596\u0005\u00af\u0000\u0000\u0596\u0597\u0005\u0002\u0000\u0000"+
		"\u0597\u059c\u0003*\u0015\u0000\u0598\u0599\u0005\u0004\u0000\u0000\u0599"+
		"\u059b\u0003*\u0015\u0000\u059a\u0598\u0001\u0000\u0000\u0000\u059b\u059e"+
		"\u0001\u0000\u0000\u0000\u059c\u059a\u0001\u0000\u0000\u0000\u059c\u059d"+
		"\u0001\u0000\u0000\u0000\u059d\u059f\u0001\u0000\u0000\u0000\u059e\u059c"+
		"\u0001\u0000\u0000\u0000\u059f\u05a0\u0005\u0003\u0000\u0000\u05a0)\u0001"+
		"\u0000\u0000\u0000\u05a1\u05a4\u0003\u011a\u008d\u0000\u05a2\u05a3\u0005"+
		"\u011f\u0000\u0000\u05a3\u05a5\u0003\u00deo\u0000\u05a4\u05a2\u0001\u0000"+
		"\u0000\u0000\u05a4\u05a5\u0001\u0000\u0000\u0000\u05a5+\u0001\u0000\u0000"+
		"\u0000\u05a6\u05a7\u0007\r\u0000\u0000\u05a7-\u0001\u0000\u0000\u0000"+
		"\u05a8\u05a9\u0007\u000e\u0000\u0000\u05a9/\u0001\u0000\u0000\u0000\u05aa"+
		"\u05b0\u0003\u0114\u008a\u0000\u05ab\u05b0\u0005\u0135\u0000\u0000\u05ac"+
		"\u05b0\u0003\u00e0p\u0000\u05ad\u05b0\u0003\u00e2q\u0000\u05ae\u05b0\u0003"+
		"\u00e4r\u0000\u05af\u05aa\u0001\u0000\u0000\u0000\u05af\u05ab\u0001\u0000"+
		"\u0000\u0000\u05af\u05ac\u0001\u0000\u0000\u0000\u05af\u05ad\u0001\u0000"+
		"\u0000\u0000\u05af\u05ae\u0001\u0000\u0000\u0000\u05b01\u0001\u0000\u0000"+
		"\u0000\u05b1\u05b6\u0003\u011a\u008d\u0000\u05b2\u05b3\u0005\u0005\u0000"+
		"\u0000\u05b3\u05b5\u0003\u011a\u008d\u0000\u05b4\u05b2\u0001\u0000\u0000"+
		"\u0000\u05b5\u05b8\u0001\u0000\u0000\u0000\u05b6\u05b4\u0001\u0000\u0000"+
		"\u0000\u05b6\u05b7\u0001\u0000\u0000\u0000\u05b73\u0001\u0000\u0000\u0000"+
		"\u05b8\u05b6\u0001\u0000\u0000\u0000\u05b9\u05ba\u0005\u011b\u0000\u0000"+
		"\u05ba\u05bf\u00036\u001b\u0000\u05bb\u05bc\u0005\u0004\u0000\u0000\u05bc"+
		"\u05be\u00036\u001b\u0000\u05bd\u05bb\u0001\u0000\u0000\u0000\u05be\u05c1"+
		"\u0001\u0000\u0000\u0000\u05bf\u05bd\u0001\u0000\u0000\u0000\u05bf\u05c0"+
		"\u0001\u0000\u0000\u0000\u05c05\u0001\u0000\u0000\u0000\u05c1\u05bf\u0001"+
		"\u0000\u0000\u0000\u05c2\u05c4\u0003\u0116\u008b\u0000\u05c3\u05c5\u0003"+
		"\u00a2Q\u0000\u05c4\u05c3\u0001\u0000\u0000\u0000\u05c4\u05c5\u0001\u0000"+
		"\u0000\u0000\u05c5\u05c7\u0001\u0000\u0000\u0000\u05c6\u05c8\u0005\u0012"+
		"\u0000\u0000\u05c7\u05c6\u0001\u0000\u0000\u0000\u05c7\u05c8\u0001\u0000"+
		"\u0000\u0000\u05c8\u05c9\u0001\u0000\u0000\u0000\u05c9\u05ca\u0005\u0002"+
		"\u0000\u0000\u05ca\u05cb\u0003\"\u0011\u0000\u05cb\u05cc\u0005\u0003\u0000"+
		"\u0000\u05cc7\u0001\u0000\u0000\u0000\u05cd\u05ce\u0005\u0112\u0000\u0000"+
		"\u05ce\u05cf\u0003\u00ba]\u0000\u05cf9\u0001\u0000\u0000\u0000\u05d0\u05d1"+
		"\u0005\u00a5\u0000\u0000\u05d1\u05de\u0003<\u001e\u0000\u05d2\u05d3\u0005"+
		"\u00b0\u0000\u0000\u05d3\u05d4\u0005\u001a\u0000\u0000\u05d4\u05de\u0003"+
		"\u00c8d\u0000\u05d5\u05de\u0003\u001c\u000e\u0000\u05d6\u05de\u0003\u001a"+
		"\r\u0000\u05d7\u05de\u0003\u00b6[\u0000\u05d8\u05de\u0003H$\u0000\u05d9"+
		"\u05de\u0003\u001e\u000f\u0000\u05da\u05de\u0003 \u0010\u0000\u05db\u05dc"+
		"\u0005\u00f4\u0000\u0000\u05dc\u05de\u0003<\u001e\u0000\u05dd\u05d0\u0001"+
		"\u0000\u0000\u0000\u05dd\u05d2\u0001\u0000\u0000\u0000\u05dd\u05d5\u0001"+
		"\u0000\u0000\u0000\u05dd\u05d6\u0001\u0000\u0000\u0000\u05dd\u05d7\u0001"+
		"\u0000\u0000\u0000\u05dd\u05d8\u0001\u0000\u0000\u0000\u05dd\u05d9\u0001"+
		"\u0000\u0000\u0000\u05dd\u05da\u0001\u0000\u0000\u0000\u05dd\u05db\u0001"+
		"\u0000\u0000\u0000\u05de\u05e1\u0001\u0000\u0000\u0000\u05df\u05dd\u0001"+
		"\u0000\u0000\u0000\u05df\u05e0\u0001\u0000\u0000\u0000\u05e0;\u0001\u0000"+
		"\u0000\u0000\u05e1\u05df\u0001\u0000\u0000\u0000\u05e2\u05e3\u0005\u0002"+
		"\u0000\u0000\u05e3\u05e8\u0003>\u001f\u0000\u05e4\u05e5\u0005\u0004\u0000"+
		"\u0000\u05e5\u05e7\u0003>\u001f\u0000\u05e6\u05e4\u0001\u0000\u0000\u0000"+
		"\u05e7\u05ea\u0001\u0000\u0000\u0000\u05e8\u05e6\u0001\u0000\u0000\u0000"+
		"\u05e8\u05e9\u0001\u0000\u0000\u0000\u05e9\u05eb\u0001\u0000\u0000\u0000"+
		"\u05ea\u05e8\u0001\u0000\u0000\u0000\u05eb\u05ec\u0005\u0003\u0000\u0000"+
		"\u05ec=\u0001\u0000\u0000\u0000\u05ed\u05f2\u0003@ \u0000\u05ee\u05f0"+
		"\u0005\u011f\u0000\u0000\u05ef\u05ee\u0001\u0000\u0000\u0000\u05ef\u05f0"+
		"\u0001\u0000\u0000\u0000\u05f0\u05f1\u0001\u0000\u0000\u0000\u05f1\u05f3"+
		"\u0003B!\u0000\u05f2\u05ef\u0001\u0000\u0000\u0000\u05f2\u05f3\u0001\u0000"+
		"\u0000\u0000\u05f3?\u0001\u0000\u0000\u0000\u05f4\u05f9\u0003\u011a\u008d"+
		"\u0000\u05f5\u05f6\u0005\u0005\u0000\u0000\u05f6\u05f8\u0003\u011a\u008d"+
		"\u0000\u05f7\u05f5\u0001\u0000\u0000\u0000\u05f8\u05fb\u0001\u0000\u0000"+
		"\u0000\u05f9\u05f7\u0001\u0000\u0000\u0000\u05f9\u05fa\u0001\u0000\u0000"+
		"\u0000\u05fa\u05fe\u0001\u0000\u0000\u0000\u05fb\u05f9\u0001\u0000\u0000"+
		"\u0000\u05fc\u05fe\u0005\u0135\u0000\u0000\u05fd\u05f4\u0001\u0000\u0000"+
		"\u0000\u05fd\u05fc\u0001\u0000\u0000\u0000\u05feA\u0001\u0000\u0000\u0000"+
		"\u05ff\u0604\u0005\u0139\u0000\u0000\u0600\u0604\u0005\u013b\u0000\u0000"+
		"\u0601\u0604\u0003\u00e6s\u0000\u0602\u0604\u0005\u0135\u0000\u0000\u0603"+
		"\u05ff\u0001\u0000\u0000\u0000\u0603\u0600\u0001\u0000\u0000\u0000\u0603"+
		"\u0601\u0001\u0000\u0000\u0000\u0603\u0602\u0001\u0000\u0000\u0000\u0604"+
		"C\u0001\u0000\u0000\u0000\u0605\u0606\u0005\u0002\u0000\u0000\u0606\u060b"+
		"\u0003\u00deo\u0000\u0607\u0608\u0005\u0004\u0000\u0000\u0608\u060a\u0003"+
		"\u00deo\u0000\u0609\u0607\u0001\u0000\u0000\u0000\u060a\u060d\u0001\u0000"+
		"\u0000\u0000\u060b\u0609\u0001\u0000\u0000\u0000\u060b\u060c\u0001\u0000"+
		"\u0000\u0000\u060c\u060e\u0001\u0000\u0000\u0000\u060d\u060b\u0001\u0000"+
		"\u0000\u0000\u060e\u060f\u0005\u0003\u0000\u0000\u060fE\u0001\u0000\u0000"+
		"\u0000\u0610\u0611\u0005\u0002\u0000\u0000\u0611\u0616\u0003D\"\u0000"+
		"\u0612\u0613\u0005\u0004\u0000\u0000\u0613\u0615\u0003D\"\u0000\u0614"+
		"\u0612\u0001\u0000\u0000\u0000\u0615\u0618\u0001\u0000\u0000\u0000\u0616"+
		"\u0614\u0001\u0000\u0000\u0000\u0616\u0617\u0001\u0000\u0000\u0000\u0617"+
		"\u0619\u0001\u0000\u0000\u0000\u0618\u0616\u0001\u0000\u0000\u0000\u0619"+
		"\u061a\u0005\u0003\u0000\u0000\u061aG\u0001\u0000\u0000\u0000\u061b\u061c"+
		"\u0005\u00e9\u0000\u0000\u061c\u061d\u0005\u0012\u0000\u0000\u061d\u0622"+
		"\u0003J%\u0000\u061e\u061f\u0005\u00e9\u0000\u0000\u061f\u0620\u0005\u001a"+
		"\u0000\u0000\u0620\u0622\u0003L&\u0000\u0621\u061b\u0001\u0000\u0000\u0000"+
		"\u0621\u061e\u0001\u0000\u0000\u0000\u0622I\u0001\u0000\u0000\u0000\u0623"+
		"\u0624\u0005x\u0000\u0000\u0624\u0625\u0005\u0135\u0000\u0000\u0625\u0626"+
		"\u0005\u00aa\u0000\u0000\u0626\u0629\u0005\u0135\u0000\u0000\u0627\u0629"+
		"\u0003\u011a\u008d\u0000\u0628\u0623\u0001\u0000\u0000\u0000\u0628\u0627"+
		"\u0001\u0000\u0000\u0000\u0629K\u0001\u0000\u0000\u0000\u062a\u062e\u0005"+
		"\u0135\u0000\u0000\u062b\u062c\u0005\u011b\u0000\u0000\u062c\u062d\u0005"+
		"\u00dd\u0000\u0000\u062d\u062f\u0003<\u001e\u0000\u062e\u062b\u0001\u0000"+
		"\u0000\u0000\u062e\u062f\u0001\u0000\u0000\u0000\u062fM\u0001\u0000\u0000"+
		"\u0000\u0630\u0631\u0003\u011a\u008d\u0000\u0631\u0632\u0005\u0135\u0000"+
		"\u0000\u0632O\u0001\u0000\u0000\u0000\u0633\u0634\u0003$\u0012\u0000\u0634"+
		"\u0635\u0003\"\u0011\u0000\u0635\u0682\u0001\u0000\u0000\u0000\u0636\u0638"+
		"\u0003~?\u0000\u0637\u0639\u0003T*\u0000\u0638\u0637\u0001\u0000\u0000"+
		"\u0000\u0639\u063a\u0001\u0000\u0000\u0000\u063a\u0638\u0001\u0000\u0000"+
		"\u0000\u063a\u063b\u0001\u0000\u0000\u0000\u063b\u0682\u0001\u0000\u0000"+
		"\u0000\u063c\u063d\u0005D\u0000\u0000\u063d\u063e\u0005f\u0000\u0000\u063e"+
		"\u063f\u0003\u00ba]\u0000\u063f\u0644\u0003\u00b4Z\u0000\u0640\u0641\u0005"+
		"\u0004\u0000\u0000\u0641\u0642\u0003\u00ba]\u0000\u0642\u0643\u0003\u00b4"+
		"Z\u0000\u0643\u0645\u0001\u0000\u0000\u0000\u0644\u0640\u0001\u0000\u0000"+
		"\u0000\u0644\u0645\u0001\u0000\u0000\u0000\u0645\u064c\u0001\u0000\u0000"+
		"\u0000\u0646\u0647\u0005\u0004\u0000\u0000\u0647\u0648\u0005\u0002\u0000"+
		"\u0000\u0648\u0649\u0003\"\u0011\u0000\u0649\u064a\u0005\u0003\u0000\u0000"+
		"\u064a\u064b\u0003\u00b4Z\u0000\u064b\u064d\u0001\u0000\u0000\u0000\u064c"+
		"\u0646\u0001\u0000\u0000\u0000\u064c\u064d\u0001\u0000\u0000\u0000\u064d"+
		"\u064f\u0001\u0000\u0000\u0000\u064e\u0650\u0003v;\u0000\u064f\u064e\u0001"+
		"\u0000\u0000\u0000\u064f\u0650\u0001\u0000\u0000\u0000\u0650\u0682\u0001"+
		"\u0000\u0000\u0000\u0651\u0652\u0005\u010f\u0000\u0000\u0652\u0653\u0003"+
		"\u00ba]\u0000\u0653\u0658\u0003\u00b4Z\u0000\u0654\u0655\u0005\u0004\u0000"+
		"\u0000\u0655\u0656\u0003\u00ba]\u0000\u0656\u0657\u0003\u00b4Z\u0000\u0657"+
		"\u0659\u0001\u0000\u0000\u0000\u0658\u0654\u0001\u0000\u0000\u0000\u0658"+
		"\u0659\u0001\u0000\u0000\u0000\u0659\u0660\u0001\u0000\u0000\u0000\u065a"+
		"\u065b\u0005\u0004\u0000\u0000\u065b\u065c\u0005\u0002\u0000\u0000\u065c"+
		"\u065d\u0003\"\u0011\u0000\u065d\u065e\u0005\u0003\u0000\u0000\u065e\u065f"+
		"\u0003\u00b4Z\u0000\u065f\u0661\u0001\u0000\u0000\u0000\u0660\u065a\u0001"+
		"\u0000\u0000\u0000\u0660\u0661\u0001\u0000\u0000\u0000\u0661\u0662\u0001"+
		"\u0000\u0000\u0000\u0662\u0664\u0003h4\u0000\u0663\u0665\u0003v;\u0000"+
		"\u0664\u0663\u0001\u0000\u0000\u0000\u0664\u0665\u0001\u0000\u0000\u0000"+
		"\u0665\u0682\u0001\u0000\u0000\u0000\u0666\u0667\u0005\u0094\u0000\u0000"+
		"\u0667\u0668\u0005|\u0000\u0000\u0668\u0669\u0003\u00ba]\u0000\u0669\u066a"+
		"\u0003\u00b4Z\u0000\u066a\u0670\u0005\u0112\u0000\u0000\u066b\u0671\u0003"+
		"\u00ba]\u0000\u066c\u066d\u0005\u0002\u0000\u0000\u066d\u066e\u0003\""+
		"\u0011\u0000\u066e\u066f\u0005\u0003\u0000\u0000\u066f\u0671\u0001\u0000"+
		"\u0000\u0000\u0670\u066b\u0001\u0000\u0000\u0000\u0670\u066c\u0001\u0000"+
		"\u0000\u0000\u0671\u0672\u0001\u0000\u0000\u0000\u0672\u0673\u0003\u00b4"+
		"Z\u0000\u0673\u0674\u0005\u00a2\u0000\u0000\u0674\u0676\u0003\u00d4j\u0000"+
		"\u0675\u0677\u0003j5\u0000\u0676\u0675\u0001\u0000\u0000\u0000\u0676\u0677"+
		"\u0001\u0000\u0000\u0000\u0677\u0679\u0001\u0000\u0000\u0000\u0678\u067a"+
		"\u0003l6\u0000\u0679\u0678\u0001\u0000\u0000\u0000\u0679\u067a\u0001\u0000"+
		"\u0000\u0000\u067a\u067c\u0001\u0000\u0000\u0000\u067b\u067d\u0003j5\u0000"+
		"\u067c\u067b\u0001\u0000\u0000\u0000\u067c\u067d\u0001\u0000\u0000\u0000"+
		"\u067d\u067f\u0001\u0000\u0000\u0000\u067e\u0680\u0003l6\u0000\u067f\u067e"+
		"\u0001\u0000\u0000\u0000\u067f\u0680\u0001\u0000\u0000\u0000\u0680\u0682"+
		"\u0001\u0000\u0000\u0000\u0681\u0633\u0001\u0000\u0000\u0000\u0681\u0636"+
		"\u0001\u0000\u0000\u0000\u0681\u063c\u0001\u0000\u0000\u0000\u0681\u0651"+
		"\u0001\u0000\u0000\u0000\u0681\u0666\u0001\u0000\u0000\u0000\u0682Q\u0001"+
		"\u0000\u0000\u0000\u0683\u0684\u0005\u00a7\u0000\u0000\u0684\u0685\u0005"+
		"\u001a\u0000\u0000\u0685\u068a\u0003Z-\u0000\u0686\u0687\u0005\u0004\u0000"+
		"\u0000\u0687\u0689\u0003Z-\u0000\u0688\u0686\u0001\u0000\u0000\u0000\u0689"+
		"\u068c\u0001\u0000\u0000\u0000\u068a\u0688\u0001\u0000\u0000\u0000\u068a"+
		"\u068b\u0001\u0000\u0000\u0000\u068b\u068e\u0001\u0000\u0000\u0000\u068c"+
		"\u068a\u0001\u0000\u0000\u0000\u068d\u0683\u0001\u0000\u0000\u0000\u068d"+
		"\u068e\u0001\u0000\u0000\u0000\u068e\u0699\u0001\u0000\u0000\u0000\u068f"+
		"\u0690\u0005$\u0000\u0000\u0690\u0691\u0005\u001a\u0000\u0000\u0691\u0696"+
		"\u0003\u00d0h\u0000\u0692\u0693\u0005\u0004\u0000\u0000\u0693\u0695\u0003"+
		"\u00d0h\u0000\u0694\u0692\u0001\u0000\u0000\u0000\u0695\u0698\u0001\u0000"+
		"\u0000\u0000\u0696\u0694\u0001\u0000\u0000\u0000\u0696\u0697\u0001\u0000"+
		"\u0000\u0000\u0697\u069a\u0001\u0000\u0000\u0000\u0698\u0696\u0001\u0000"+
		"\u0000\u0000\u0699\u068f\u0001\u0000\u0000\u0000\u0699\u069a\u0001\u0000"+
		"\u0000\u0000\u069a\u06a5\u0001\u0000\u0000\u0000\u069b\u069c\u0005L\u0000"+
		"\u0000\u069c\u069d\u0005\u001a\u0000\u0000\u069d\u06a2\u0003\u00d0h\u0000"+
		"\u069e\u069f\u0005\u0004\u0000\u0000\u069f\u06a1\u0003\u00d0h\u0000\u06a0"+
		"\u069e\u0001\u0000\u0000\u0000\u06a1\u06a4\u0001\u0000\u0000\u0000\u06a2"+
		"\u06a0\u0001\u0000\u0000\u0000\u06a2\u06a3\u0001\u0000\u0000\u0000\u06a3"+
		"\u06a6\u0001\u0000\u0000\u0000\u06a4\u06a2\u0001\u0000\u0000\u0000\u06a5"+
		"\u069b\u0001\u0000\u0000\u0000\u06a5\u06a6\u0001\u0000\u0000\u0000\u06a6"+
		"\u06b1\u0001\u0000\u0000\u0000\u06a7\u06a8\u0005\u00e5\u0000\u0000\u06a8"+
		"\u06a9\u0005\u001a\u0000\u0000\u06a9\u06ae\u0003Z-\u0000\u06aa\u06ab\u0005"+
		"\u0004\u0000\u0000\u06ab\u06ad\u0003Z-\u0000\u06ac\u06aa\u0001\u0000\u0000"+
		"\u0000\u06ad\u06b0\u0001\u0000\u0000\u0000\u06ae\u06ac\u0001\u0000\u0000"+
		"\u0000\u06ae\u06af\u0001\u0000\u0000\u0000\u06af\u06b2\u0001\u0000\u0000"+
		"\u0000\u06b0\u06ae\u0001\u0000\u0000\u0000\u06b1\u06a7\u0001\u0000\u0000"+
		"\u0000\u06b1\u06b2\u0001\u0000\u0000\u0000\u06b2\u06b4\u0001\u0000\u0000"+
		"\u0000\u06b3\u06b5\u0003\u0106\u0083\u0000\u06b4\u06b3\u0001\u0000\u0000"+
		"\u0000\u06b4\u06b5\u0001\u0000\u0000\u0000\u06b5\u06bb\u0001\u0000\u0000"+
		"\u0000\u06b6\u06b9\u0005\u0088\u0000\u0000\u06b7\u06ba\u0005\n\u0000\u0000"+
		"\u06b8\u06ba\u0003\u00d0h\u0000\u06b9\u06b7\u0001\u0000\u0000\u0000\u06b9"+
		"\u06b8\u0001\u0000\u0000\u0000\u06ba\u06bc\u0001\u0000\u0000\u0000\u06bb"+
		"\u06b6\u0001\u0000\u0000\u0000\u06bb\u06bc\u0001\u0000\u0000\u0000\u06bc"+
		"S\u0001\u0000\u0000\u0000\u06bd\u06be\u0003$\u0012\u0000\u06be\u06bf\u0003"+
		"^/\u0000\u06bfU\u0001\u0000\u0000\u0000\u06c0\u06c1\u0006+\uffff\uffff"+
		"\u0000\u06c1\u06c2\u0003X,\u0000\u06c2\u06da\u0001\u0000\u0000\u0000\u06c3"+
		"\u06c4\n\u0003\u0000\u0000\u06c4\u06c5\u0004+\u0001\u0000\u06c5\u06c7"+
		"\u0007\u000f\u0000\u0000\u06c6\u06c8\u0003\u0094J\u0000\u06c7\u06c6\u0001"+
		"\u0000\u0000\u0000\u06c7\u06c8\u0001\u0000\u0000\u0000\u06c8\u06c9\u0001"+
		"\u0000\u0000\u0000\u06c9\u06d9\u0003V+\u0004\u06ca\u06cb\n\u0002\u0000"+
		"\u0000\u06cb\u06cc\u0004+\u0003\u0000\u06cc\u06ce\u0005z\u0000\u0000\u06cd"+
		"\u06cf\u0003\u0094J\u0000\u06ce\u06cd\u0001\u0000\u0000\u0000\u06ce\u06cf"+
		"\u0001\u0000\u0000\u0000\u06cf\u06d0\u0001\u0000\u0000\u0000\u06d0\u06d9"+
		"\u0003V+\u0003\u06d1\u06d2\n\u0001\u0000\u0000\u06d2\u06d3\u0004+\u0005"+
		"\u0000\u06d3\u06d5\u0007\u0010\u0000\u0000\u06d4\u06d6\u0003\u0094J\u0000"+
		"\u06d5\u06d4\u0001\u0000\u0000\u0000\u06d5\u06d6\u0001\u0000\u0000\u0000"+
		"\u06d6\u06d7\u0001\u0000\u0000\u0000\u06d7\u06d9\u0003V+\u0002\u06d8\u06c3"+
		"\u0001\u0000\u0000\u0000\u06d8\u06ca\u0001\u0000\u0000\u0000\u06d8\u06d1"+
		"\u0001\u0000\u0000\u0000\u06d9\u06dc\u0001\u0000\u0000\u0000\u06da\u06d8"+
		"\u0001\u0000\u0000\u0000\u06da\u06db\u0001\u0000\u0000\u0000\u06dbW\u0001"+
		"\u0000\u0000\u0000\u06dc\u06da\u0001\u0000\u0000\u0000\u06dd\u06e7\u0003"+
		"`0\u0000\u06de\u06e7\u0003\\.\u0000\u06df\u06e0\u0005\u00f1\u0000\u0000"+
		"\u06e0\u06e7\u0003\u00ba]\u0000\u06e1\u06e7\u0003\u00b0X\u0000\u06e2\u06e3"+
		"\u0005\u0002\u0000\u0000\u06e3\u06e4\u0003\"\u0011\u0000\u06e4\u06e5\u0005"+
		"\u0003\u0000\u0000\u06e5\u06e7\u0001\u0000\u0000\u0000\u06e6\u06dd\u0001"+
		"\u0000\u0000\u0000\u06e6\u06de\u0001\u0000\u0000\u0000\u06e6\u06df\u0001"+
		"\u0000\u0000\u0000\u06e6\u06e1\u0001\u0000\u0000\u0000\u06e6\u06e2\u0001"+
		"\u0000\u0000\u0000\u06e7Y\u0001\u0000\u0000\u0000\u06e8\u06ea\u0003\u00d0"+
		"h\u0000\u06e9\u06eb\u0007\u0011\u0000\u0000\u06ea\u06e9\u0001\u0000\u0000"+
		"\u0000\u06ea\u06eb\u0001\u0000\u0000\u0000\u06eb\u06ee\u0001\u0000\u0000"+
		"\u0000\u06ec\u06ed\u0005\u00a0\u0000\u0000\u06ed\u06ef\u0007\u0012\u0000"+
		"\u0000\u06ee\u06ec\u0001\u0000\u0000\u0000\u06ee\u06ef\u0001\u0000\u0000"+
		"\u0000\u06ef[\u0001\u0000\u0000\u0000\u06f0\u06f2\u0003~?\u0000\u06f1"+
		"\u06f3\u0003^/\u0000\u06f2\u06f1\u0001\u0000\u0000\u0000\u06f3\u06f4\u0001"+
		"\u0000\u0000\u0000\u06f4\u06f2\u0001\u0000\u0000\u0000\u06f4\u06f5\u0001"+
		"\u0000\u0000\u0000\u06f5]\u0001\u0000\u0000\u0000\u06f6\u06f8\u0003b1"+
		"\u0000\u06f7\u06f9\u0003v;\u0000\u06f8\u06f7\u0001\u0000\u0000\u0000\u06f8"+
		"\u06f9\u0001\u0000\u0000\u0000\u06f9\u06fa\u0001\u0000\u0000\u0000\u06fa"+
		"\u06fb\u0003R)\u0000\u06fb\u0712\u0001\u0000\u0000\u0000\u06fc\u0700\u0003"+
		"d2\u0000\u06fd\u06ff\u0003\u0092I\u0000\u06fe\u06fd\u0001\u0000\u0000"+
		"\u0000\u06ff\u0702\u0001\u0000\u0000\u0000\u0700\u06fe\u0001\u0000\u0000"+
		"\u0000\u0700\u0701\u0001\u0000\u0000\u0000\u0701\u0704\u0001\u0000\u0000"+
		"\u0000\u0702\u0700\u0001\u0000\u0000\u0000\u0703\u0705\u0003v;\u0000\u0704"+
		"\u0703\u0001\u0000\u0000\u0000\u0704\u0705\u0001\u0000\u0000\u0000\u0705"+
		"\u0707\u0001\u0000\u0000\u0000\u0706\u0708\u0003\u0082A\u0000\u0707\u0706"+
		"\u0001\u0000\u0000\u0000\u0707\u0708\u0001\u0000\u0000\u0000\u0708\u070a"+
		"\u0001\u0000\u0000\u0000\u0709\u070b\u0003x<\u0000\u070a\u0709\u0001\u0000"+
		"\u0000\u0000\u070a\u070b\u0001\u0000\u0000\u0000\u070b\u070d\u0001\u0000"+
		"\u0000\u0000\u070c\u070e\u0003\u0106\u0083\u0000\u070d\u070c\u0001\u0000"+
		"\u0000\u0000\u070d\u070e\u0001\u0000\u0000\u0000\u070e\u070f\u0001\u0000"+
		"\u0000\u0000\u070f\u0710\u0003R)\u0000\u0710\u0712\u0001\u0000\u0000\u0000"+
		"\u0711\u06f6\u0001\u0000\u0000\u0000\u0711\u06fc\u0001\u0000\u0000\u0000"+
		"\u0712_\u0001\u0000\u0000\u0000\u0713\u0715\u0003b1\u0000\u0714\u0716"+
		"\u0003~?\u0000\u0715\u0714\u0001\u0000\u0000\u0000\u0715\u0716\u0001\u0000"+
		"\u0000\u0000\u0716\u071a\u0001\u0000\u0000\u0000\u0717\u0719\u0003\u0092"+
		"I\u0000\u0718\u0717\u0001\u0000\u0000\u0000\u0719\u071c\u0001\u0000\u0000"+
		"\u0000\u071a\u0718\u0001\u0000\u0000\u0000\u071a\u071b\u0001\u0000\u0000"+
		"\u0000\u071b\u071e\u0001\u0000\u0000\u0000\u071c\u071a\u0001\u0000\u0000"+
		"\u0000\u071d\u071f\u0003v;\u0000\u071e\u071d\u0001\u0000\u0000\u0000\u071e"+
		"\u071f\u0001\u0000\u0000\u0000\u071f\u0721\u0001\u0000\u0000\u0000\u0720"+
		"\u0722\u0003\u0082A\u0000\u0721\u0720\u0001\u0000\u0000\u0000\u0721\u0722"+
		"\u0001\u0000\u0000\u0000\u0722\u0724\u0001\u0000\u0000\u0000\u0723\u0725"+
		"\u0003x<\u0000\u0724\u0723\u0001\u0000\u0000\u0000\u0724\u0725\u0001\u0000"+
		"\u0000\u0000\u0725\u0727\u0001\u0000\u0000\u0000\u0726\u0728\u0003\u0106"+
		"\u0083\u0000\u0727\u0726\u0001\u0000\u0000\u0000\u0727\u0728\u0001\u0000"+
		"\u0000\u0000\u0728\u0740\u0001\u0000\u0000\u0000\u0729\u072b\u0003d2\u0000"+
		"\u072a\u072c\u0003~?\u0000\u072b\u072a\u0001\u0000\u0000\u0000\u072b\u072c"+
		"\u0001\u0000\u0000\u0000\u072c\u0730\u0001\u0000\u0000\u0000\u072d\u072f"+
		"\u0003\u0092I\u0000\u072e\u072d\u0001\u0000\u0000\u0000\u072f\u0732\u0001"+
		"\u0000\u0000\u0000\u0730\u072e\u0001\u0000\u0000\u0000\u0730\u0731\u0001"+
		"\u0000\u0000\u0000\u0731\u0734\u0001\u0000\u0000\u0000\u0732\u0730\u0001"+
		"\u0000\u0000\u0000\u0733\u0735\u0003v;\u0000\u0734\u0733\u0001\u0000\u0000"+
		"\u0000\u0734\u0735\u0001\u0000\u0000\u0000\u0735\u0737\u0001\u0000\u0000"+
		"\u0000\u0736\u0738\u0003\u0082A\u0000\u0737\u0736\u0001\u0000\u0000\u0000"+
		"\u0737\u0738\u0001\u0000\u0000\u0000\u0738\u073a\u0001\u0000\u0000\u0000"+
		"\u0739\u073b\u0003x<\u0000\u073a\u0739\u0001\u0000\u0000\u0000\u073a\u073b"+
		"\u0001\u0000\u0000\u0000\u073b\u073d\u0001\u0000\u0000\u0000\u073c\u073e"+
		"\u0003\u0106\u0083\u0000\u073d\u073c\u0001\u0000\u0000\u0000\u073d\u073e"+
		"\u0001\u0000\u0000\u0000\u073e\u0740\u0001\u0000\u0000\u0000\u073f\u0713"+
		"\u0001\u0000\u0000\u0000\u073f\u0729\u0001\u0000\u0000\u0000\u0740a\u0001"+
		"\u0000\u0000\u0000\u0741\u0742\u0005\u00d9\u0000\u0000\u0742\u0743\u0005"+
		"\u0101\u0000\u0000\u0743\u0745\u0005\u0002\u0000\u0000\u0744\u0746\u0003"+
		"\u0094J\u0000\u0745\u0744\u0001\u0000\u0000\u0000\u0745\u0746\u0001\u0000"+
		"\u0000\u0000\u0746\u0747\u0001\u0000\u0000\u0000\u0747\u0748\u0003\u00d2"+
		"i\u0000\u0748\u0749\u0005\u0003\u0000\u0000\u0749\u0755\u0001\u0000\u0000"+
		"\u0000\u074a\u074c\u0005\u0092\u0000\u0000\u074b\u074d\u0003\u0094J\u0000"+
		"\u074c\u074b\u0001\u0000\u0000\u0000\u074c\u074d\u0001\u0000\u0000\u0000"+
		"\u074d\u074e\u0001\u0000\u0000\u0000\u074e\u0755\u0003\u00d2i\u0000\u074f"+
		"\u0751\u0005\u00c3\u0000\u0000\u0750\u0752\u0003\u0094J\u0000\u0751\u0750"+
		"\u0001\u0000\u0000\u0000\u0751\u0752\u0001\u0000\u0000\u0000\u0752\u0753"+
		"\u0001\u0000\u0000\u0000\u0753\u0755\u0003\u00d2i\u0000\u0754\u0741\u0001"+
		"\u0000\u0000\u0000\u0754\u074a\u0001\u0000\u0000\u0000\u0754\u074f\u0001"+
		"\u0000\u0000\u0000\u0755\u0757\u0001\u0000\u0000\u0000\u0756\u0758\u0003"+
		"\u00b6[\u0000\u0757\u0756\u0001\u0000\u0000\u0000\u0757\u0758\u0001\u0000"+
		"\u0000\u0000\u0758\u075b\u0001\u0000\u0000\u0000\u0759\u075a\u0005\u00c1"+
		"\u0000\u0000\u075a\u075c\u0005\u0135\u0000\u0000\u075b\u0759\u0001\u0000"+
		"\u0000\u0000\u075b\u075c\u0001\u0000\u0000\u0000\u075c\u075d\u0001\u0000"+
		"\u0000\u0000\u075d\u075e\u0005\u0112\u0000\u0000\u075e\u076b\u0005\u0135"+
		"\u0000\u0000\u075f\u0769\u0005\u0012\u0000\u0000\u0760\u076a\u0003\u00a4"+
		"R\u0000\u0761\u076a\u0003\u00fc~\u0000\u0762\u0765\u0005\u0002\u0000\u0000"+
		"\u0763\u0766\u0003\u00a4R\u0000\u0764\u0766\u0003\u00fc~\u0000\u0765\u0763"+
		"\u0001\u0000\u0000\u0000\u0765\u0764\u0001\u0000\u0000\u0000\u0766\u0767"+
		"\u0001\u0000\u0000\u0000\u0767\u0768\u0005\u0003\u0000\u0000\u0768\u076a"+
		"\u0001\u0000\u0000\u0000\u0769\u0760\u0001\u0000\u0000\u0000\u0769\u0761"+
		"\u0001\u0000\u0000\u0000\u0769\u0762\u0001\u0000\u0000\u0000\u076a\u076c"+
		"\u0001\u0000\u0000\u0000\u076b\u075f\u0001\u0000\u0000\u0000\u076b\u076c"+
		"\u0001\u0000\u0000\u0000\u076c\u076e\u0001\u0000\u0000\u0000\u076d\u076f"+
		"\u0003\u00b6[\u0000\u076e\u076d\u0001\u0000\u0000\u0000\u076e\u076f\u0001"+
		"\u0000\u0000\u0000\u076f\u0772\u0001\u0000\u0000\u0000\u0770\u0771\u0005"+
		"\u00c0\u0000\u0000\u0771\u0773\u0005\u0135\u0000\u0000\u0772\u0770\u0001"+
		"\u0000\u0000\u0000\u0772\u0773\u0001\u0000\u0000\u0000\u0773c\u0001\u0000"+
		"\u0000\u0000\u0774\u0778\u0005\u00d9\u0000\u0000\u0775\u0777\u0003z=\u0000"+
		"\u0776\u0775\u0001\u0000\u0000\u0000\u0777\u077a\u0001\u0000\u0000\u0000"+
		"\u0778\u0776\u0001\u0000\u0000\u0000\u0778\u0779\u0001\u0000\u0000\u0000"+
		"\u0779\u077c\u0001\u0000\u0000\u0000\u077a\u0778\u0001\u0000\u0000\u0000"+
		"\u077b\u077d\u0003\u0094J\u0000\u077c\u077b\u0001\u0000\u0000\u0000\u077c"+
		"\u077d\u0001\u0000\u0000\u0000\u077d\u077e\u0001\u0000\u0000\u0000\u077e"+
		"\u0780\u0003\u00c6c\u0000\u077f\u0781\u0003f3\u0000\u0780\u077f\u0001"+
		"\u0000\u0000\u0000\u0780\u0781\u0001\u0000\u0000\u0000\u0781e\u0001\u0000"+
		"\u0000\u0000\u0782\u0783\u0005|\u0000\u0000\u0783\u0788\u0003\u00d0h\u0000"+
		"\u0784\u0785\u0005\u0004\u0000\u0000\u0785\u0787\u0003\u00d0h\u0000\u0786"+
		"\u0784\u0001\u0000\u0000\u0000\u0787\u078a\u0001\u0000\u0000\u0000\u0788"+
		"\u0786\u0001\u0000\u0000\u0000\u0788\u0789\u0001\u0000\u0000\u0000\u0789"+
		"g\u0001\u0000\u0000\u0000\u078a\u0788\u0001\u0000\u0000\u0000\u078b\u078c"+
		"\u0005\u00df\u0000\u0000\u078c\u078d\u0003r9\u0000\u078di\u0001\u0000"+
		"\u0000\u0000\u078e\u078f\u0005\u0118\u0000\u0000\u078f\u0792\u0005\u0093"+
		"\u0000\u0000\u0790\u0791\u0005\r\u0000\u0000\u0791\u0793\u0003\u00d4j"+
		"\u0000\u0792\u0790\u0001\u0000\u0000\u0000\u0792\u0793\u0001\u0000\u0000"+
		"\u0000\u0793\u0794\u0001\u0000\u0000\u0000\u0794\u0795\u0005\u00f7\u0000"+
		"\u0000\u0795\u0796\u0003n7\u0000\u0796k\u0001\u0000\u0000\u0000\u0797"+
		"\u0798\u0005\u0118\u0000\u0000\u0798\u0799\u0005\u009e\u0000\u0000\u0799"+
		"\u079c\u0005\u0093\u0000\u0000\u079a\u079b\u0005\r\u0000\u0000\u079b\u079d"+
		"\u0003\u00d4j\u0000\u079c\u079a\u0001\u0000\u0000\u0000\u079c\u079d\u0001"+
		"\u0000\u0000\u0000\u079d\u079e\u0001\u0000\u0000\u0000\u079e\u079f\u0005"+
		"\u00f7\u0000\u0000\u079f\u07a0\u0003p8\u0000\u07a0m\u0001\u0000\u0000"+
		"\u0000\u07a1\u07ad\u0005D\u0000\u0000\u07a2\u07a3\u0005\u010f\u0000\u0000"+
		"\u07a3\u07a4\u0005\u00df\u0000\u0000\u07a4\u07ad\u0005\u0129\u0000\u0000"+
		"\u07a5\u07a6\u0005\u010f\u0000\u0000\u07a6\u07a7\u0005\u00df\u0000\u0000"+
		"\u07a7\u07aa\u0003r9\u0000\u07a8\u07a9\u0005\u0119\u0000\u0000\u07a9\u07ab"+
		"\u0003\u00d4j\u0000\u07aa\u07a8\u0001\u0000\u0000\u0000\u07aa\u07ab\u0001"+
		"\u0000\u0000\u0000\u07ab\u07ad\u0001\u0000\u0000\u0000\u07ac\u07a1\u0001"+
		"\u0000\u0000\u0000\u07ac\u07a2\u0001\u0000\u0000\u0000\u07ac\u07a5\u0001"+
		"\u0000\u0000\u0000\u07ado\u0001\u0000\u0000\u0000\u07ae\u07af\u0005y\u0000"+
		"\u0000\u07af\u07c1\u0005\u0129\u0000\u0000\u07b0\u07b1\u0005y\u0000\u0000"+
		"\u07b1\u07b2\u0005\u0002\u0000\u0000\u07b2\u07b3\u0003\u00b8\\\u0000\u07b3"+
		"\u07b4\u0005\u0003\u0000\u0000\u07b4\u07b5\u0005\u0113\u0000\u0000\u07b5"+
		"\u07b6\u0005\u0002\u0000\u0000\u07b6\u07bb\u0003\u00d0h\u0000\u07b7\u07b8"+
		"\u0005\u0004\u0000\u0000\u07b8\u07ba\u0003\u00d0h\u0000\u07b9\u07b7\u0001"+
		"\u0000\u0000\u0000\u07ba\u07bd\u0001\u0000\u0000\u0000\u07bb\u07b9\u0001"+
		"\u0000\u0000\u0000\u07bb\u07bc\u0001\u0000\u0000\u0000\u07bc\u07be\u0001"+
		"\u0000\u0000\u0000\u07bd\u07bb\u0001\u0000\u0000\u0000\u07be\u07bf\u0005"+
		"\u0003\u0000\u0000\u07bf\u07c1\u0001\u0000\u0000\u0000\u07c0\u07ae\u0001"+
		"\u0000\u0000\u0000\u07c0\u07b0\u0001\u0000\u0000\u0000\u07c1q\u0001\u0000"+
		"\u0000\u0000\u07c2\u07c7\u0003t:\u0000\u07c3\u07c4\u0005\u0004\u0000\u0000"+
		"\u07c4\u07c6\u0003t:\u0000\u07c5\u07c3\u0001\u0000\u0000\u0000\u07c6\u07c9"+
		"\u0001\u0000\u0000\u0000\u07c7\u07c5\u0001\u0000\u0000\u0000\u07c7\u07c8"+
		"\u0001\u0000\u0000\u0000\u07c8s\u0001\u0000\u0000\u0000\u07c9\u07c7\u0001"+
		"\u0000\u0000\u0000\u07ca\u07cb\u0003\u00ba]\u0000\u07cb\u07cc\u0005\u011f"+
		"\u0000\u0000\u07cc\u07cd\u0003\u00d0h\u0000\u07cdu\u0001\u0000\u0000\u0000"+
		"\u07ce\u07cf\u0005\u0119\u0000\u0000\u07cf\u07d0\u0003\u00d4j\u0000\u07d0"+
		"w\u0001\u0000\u0000\u0000\u07d1\u07d2\u0005n\u0000\u0000\u07d2\u07d3\u0003"+
		"\u00d4j\u0000\u07d3y\u0001\u0000\u0000\u0000\u07d4\u07d5\u0005\u0133\u0000"+
		"\u0000\u07d5\u07dc\u0003|>\u0000\u07d6\u07d8\u0005\u0004\u0000\u0000\u07d7"+
		"\u07d6\u0001\u0000\u0000\u0000\u07d7\u07d8\u0001\u0000\u0000\u0000\u07d8"+
		"\u07d9\u0001\u0000\u0000\u0000\u07d9\u07db\u0003|>\u0000\u07da\u07d7\u0001"+
		"\u0000\u0000\u0000\u07db\u07de\u0001\u0000\u0000\u0000\u07dc\u07da\u0001"+
		"\u0000\u0000\u0000\u07dc\u07dd\u0001\u0000\u0000\u0000\u07dd\u07df\u0001"+
		"\u0000\u0000\u0000\u07de\u07dc\u0001\u0000\u0000\u0000\u07df\u07e0\u0005"+
		"\u0134\u0000\u0000\u07e0{\u0001\u0000\u0000\u0000\u07e1\u07ef\u0003\u011a"+
		"\u008d\u0000\u07e2\u07e3\u0003\u011a\u008d\u0000\u07e3\u07e4\u0005\u0002"+
		"\u0000\u0000\u07e4\u07e9\u0003\u00dcn\u0000\u07e5\u07e6\u0005\u0004\u0000"+
		"\u0000\u07e6\u07e8\u0003\u00dcn\u0000\u07e7\u07e5\u0001\u0000\u0000\u0000"+
		"\u07e8\u07eb\u0001\u0000\u0000\u0000\u07e9\u07e7\u0001\u0000\u0000\u0000"+
		"\u07e9\u07ea\u0001\u0000\u0000\u0000\u07ea\u07ec\u0001\u0000\u0000\u0000"+
		"\u07eb\u07e9\u0001\u0000\u0000\u0000\u07ec\u07ed\u0005\u0003\u0000\u0000"+
		"\u07ed\u07ef\u0001\u0000\u0000\u0000\u07ee\u07e1\u0001\u0000\u0000\u0000"+
		"\u07ee\u07e2\u0001\u0000\u0000\u0000\u07ef}\u0001\u0000\u0000\u0000\u07f0"+
		"\u07f1\u0005f\u0000\u0000\u07f1\u07f6\u0003\u0096K\u0000\u07f2\u07f3\u0005"+
		"\u0004\u0000\u0000\u07f3\u07f5\u0003\u0096K\u0000\u07f4\u07f2\u0001\u0000"+
		"\u0000\u0000\u07f5\u07f8\u0001\u0000\u0000\u0000\u07f6\u07f4\u0001\u0000"+
		"\u0000\u0000\u07f6\u07f7\u0001\u0000\u0000\u0000\u07f7\u07fc\u0001\u0000"+
		"\u0000\u0000\u07f8\u07f6\u0001\u0000\u0000\u0000\u07f9\u07fb\u0003\u0092"+
		"I\u0000\u07fa\u07f9\u0001\u0000\u0000\u0000\u07fb\u07fe\u0001\u0000\u0000"+
		"\u0000\u07fc\u07fa\u0001\u0000\u0000\u0000\u07fc\u07fd\u0001\u0000\u0000"+
		"\u0000\u07fd\u0800\u0001\u0000\u0000\u0000\u07fe\u07fc\u0001\u0000\u0000"+
		"\u0000\u07ff\u0801\u0003\u008cF\u0000\u0800\u07ff\u0001\u0000\u0000\u0000"+
		"\u0800\u0801\u0001\u0000\u0000\u0000\u0801\u007f\u0001\u0000\u0000\u0000"+
		"\u0802\u0804\u0005b\u0000\u0000\u0803\u0802\u0001\u0000\u0000\u0000\u0803"+
		"\u0804\u0001\u0000\u0000\u0000\u0804\u0805\u0001\u0000\u0000\u0000\u0805"+
		"\u0806\u0007\u0013\u0000\u0000\u0806\u0807\u0005\u0012\u0000\u0000\u0807"+
		"\u0808\u0005\u00a1\u0000\u0000\u0808\u0811\u0007\u0014\u0000\u0000\u0809"+
		"\u080b\u0005b\u0000\u0000\u080a\u0809\u0001\u0000\u0000\u0000\u080a\u080b"+
		"\u0001\u0000\u0000\u0000\u080b\u080c\u0001\u0000\u0000\u0000\u080c\u080d"+
		"\u0007\u0015\u0000\u0000\u080d\u080e\u0005\u0012\u0000\u0000\u080e\u080f"+
		"\u0005\u00a1\u0000\u0000\u080f\u0811\u0003\u00d8l\u0000\u0810\u0803";
	private static final String _serializedATNSegment1 =
		"\u0001\u0000\u0000\u0000\u0810\u080a\u0001\u0000\u0000\u0000\u0811\u0081"+
		"\u0001\u0000\u0000\u0000\u0812\u0813\u0005l\u0000\u0000\u0813\u0814\u0005"+
		"\u001a\u0000\u0000\u0814\u0819\u0003\u0084B\u0000\u0815\u0816\u0005\u0004"+
		"\u0000\u0000\u0816\u0818\u0003\u0084B\u0000\u0817\u0815\u0001\u0000\u0000"+
		"\u0000\u0818\u081b\u0001\u0000\u0000\u0000\u0819\u0817\u0001\u0000\u0000"+
		"\u0000\u0819\u081a\u0001\u0000\u0000\u0000\u081a\u083a\u0001\u0000\u0000"+
		"\u0000\u081b\u0819\u0001\u0000\u0000\u0000\u081c\u081d\u0005l\u0000\u0000"+
		"\u081d\u081e\u0005\u001a\u0000\u0000\u081e\u0823\u0003\u00d0h\u0000\u081f"+
		"\u0820\u0005\u0004\u0000\u0000\u0820\u0822\u0003\u00d0h\u0000\u0821\u081f"+
		"\u0001\u0000\u0000\u0000\u0822\u0825\u0001\u0000\u0000\u0000\u0823\u0821"+
		"\u0001\u0000\u0000\u0000\u0823\u0824\u0001\u0000\u0000\u0000\u0824\u0837"+
		"\u0001\u0000\u0000\u0000\u0825\u0823\u0001\u0000\u0000\u0000\u0826\u0827"+
		"\u0005\u011b\u0000\u0000\u0827\u0838\u0005\u00d3\u0000\u0000\u0828\u0829"+
		"\u0005\u011b\u0000\u0000\u0829\u0838\u00055\u0000\u0000\u082a\u082b\u0005"+
		"m\u0000\u0000\u082b\u082c\u0005\u00e1\u0000\u0000\u082c\u082d\u0005\u0002"+
		"\u0000\u0000\u082d\u0832\u0003\u008aE\u0000\u082e\u082f\u0005\u0004\u0000"+
		"\u0000\u082f\u0831\u0003\u008aE\u0000\u0830\u082e\u0001\u0000\u0000\u0000"+
		"\u0831\u0834\u0001\u0000\u0000\u0000\u0832\u0830\u0001\u0000\u0000\u0000"+
		"\u0832\u0833\u0001\u0000\u0000\u0000\u0833\u0835\u0001\u0000\u0000\u0000"+
		"\u0834\u0832\u0001\u0000\u0000\u0000\u0835\u0836\u0005\u0003\u0000\u0000"+
		"\u0836\u0838\u0001\u0000\u0000\u0000\u0837\u0826\u0001\u0000\u0000\u0000"+
		"\u0837\u0828\u0001\u0000\u0000\u0000\u0837\u082a\u0001\u0000\u0000\u0000"+
		"\u0837\u0838\u0001\u0000\u0000\u0000\u0838\u083a\u0001\u0000\u0000\u0000"+
		"\u0839\u0812\u0001\u0000\u0000\u0000\u0839\u081c\u0001\u0000\u0000\u0000"+
		"\u083a\u0083\u0001\u0000\u0000\u0000\u083b\u083e\u0003\u0086C\u0000\u083c"+
		"\u083e\u0003\u00d0h\u0000\u083d\u083b\u0001\u0000\u0000\u0000\u083d\u083c"+
		"\u0001\u0000\u0000\u0000\u083e\u0085\u0001\u0000\u0000\u0000\u083f\u0840"+
		"\u0007\u0016\u0000\u0000\u0840\u0841\u0005\u0002\u0000\u0000\u0841\u0846"+
		"\u0003\u008aE\u0000\u0842\u0843\u0005\u0004\u0000\u0000\u0843\u0845\u0003"+
		"\u008aE\u0000\u0844\u0842\u0001\u0000\u0000\u0000\u0845\u0848\u0001\u0000"+
		"\u0000\u0000\u0846\u0844\u0001\u0000\u0000\u0000\u0846\u0847\u0001\u0000"+
		"\u0000\u0000\u0847\u0849\u0001\u0000\u0000\u0000\u0848\u0846\u0001\u0000"+
		"\u0000\u0000\u0849\u084a\u0005\u0003\u0000\u0000\u084a\u0859\u0001\u0000"+
		"\u0000\u0000\u084b\u084c\u0005m\u0000\u0000\u084c\u084d\u0005\u00e1\u0000"+
		"\u0000\u084d\u084e\u0005\u0002\u0000\u0000\u084e\u0853\u0003\u0088D\u0000"+
		"\u084f\u0850\u0005\u0004\u0000\u0000\u0850\u0852\u0003\u0088D\u0000\u0851"+
		"\u084f\u0001\u0000\u0000\u0000\u0852\u0855\u0001\u0000\u0000\u0000\u0853"+
		"\u0851\u0001\u0000\u0000\u0000\u0853\u0854\u0001\u0000\u0000\u0000\u0854"+
		"\u0856\u0001\u0000\u0000\u0000\u0855\u0853\u0001\u0000\u0000\u0000\u0856"+
		"\u0857\u0005\u0003\u0000\u0000\u0857\u0859\u0001\u0000\u0000\u0000\u0858"+
		"\u083f\u0001\u0000\u0000\u0000\u0858\u084b\u0001\u0000\u0000\u0000\u0859"+
		"\u0087\u0001\u0000\u0000\u0000\u085a\u085d\u0003\u0086C\u0000\u085b\u085d"+
		"\u0003\u008aE\u0000\u085c\u085a\u0001\u0000\u0000\u0000\u085c\u085b\u0001"+
		"\u0000\u0000\u0000\u085d\u0089\u0001\u0000\u0000\u0000\u085e\u0867\u0005"+
		"\u0002\u0000\u0000\u085f\u0864\u0003\u00d0h\u0000\u0860\u0861\u0005\u0004"+
		"\u0000\u0000\u0861\u0863\u0003\u00d0h\u0000\u0862\u0860\u0001\u0000\u0000"+
		"\u0000\u0863\u0866\u0001\u0000\u0000\u0000\u0864\u0862\u0001\u0000\u0000"+
		"\u0000\u0864\u0865\u0001\u0000\u0000\u0000\u0865\u0868\u0001\u0000\u0000"+
		"\u0000\u0866\u0864\u0001\u0000\u0000\u0000\u0867\u085f\u0001\u0000\u0000"+
		"\u0000\u0867\u0868\u0001\u0000\u0000\u0000\u0868\u0869\u0001\u0000\u0000"+
		"\u0000\u0869\u086c\u0005\u0003\u0000\u0000\u086a\u086c\u0003\u00d0h\u0000"+
		"\u086b\u085e\u0001\u0000\u0000\u0000\u086b\u086a\u0001\u0000\u0000\u0000"+
		"\u086c\u008b\u0001\u0000\u0000\u0000\u086d\u086e\u0005\u00b5\u0000\u0000"+
		"\u086e\u086f\u0005\u0002\u0000\u0000\u086f\u0870\u0003\u00c6c\u0000\u0870"+
		"\u0871\u0005b\u0000\u0000\u0871\u0872\u0003\u008eG\u0000\u0872\u0873\u0005"+
		"s\u0000\u0000\u0873\u0874\u0005\u0002\u0000\u0000\u0874\u0879\u0003\u0090"+
		"H\u0000\u0875\u0876\u0005\u0004\u0000\u0000\u0876\u0878\u0003\u0090H\u0000"+
		"\u0877\u0875\u0001\u0000\u0000\u0000\u0878\u087b\u0001\u0000\u0000\u0000"+
		"\u0879\u0877\u0001\u0000\u0000\u0000\u0879\u087a\u0001\u0000\u0000\u0000"+
		"\u087a\u087c\u0001\u0000\u0000\u0000\u087b\u0879\u0001\u0000\u0000\u0000"+
		"\u087c\u087d\u0005\u0003\u0000\u0000\u087d\u087e\u0005\u0003\u0000\u0000"+
		"\u087e\u008d\u0001\u0000\u0000\u0000\u087f\u088c\u0003\u011a\u008d\u0000"+
		"\u0880\u0881\u0005\u0002\u0000\u0000\u0881\u0886\u0003\u011a\u008d\u0000"+
		"\u0882\u0883\u0005\u0004\u0000\u0000\u0883\u0885\u0003\u011a\u008d\u0000"+
		"\u0884\u0882\u0001\u0000\u0000\u0000\u0885\u0888\u0001\u0000\u0000\u0000"+
		"\u0886\u0884\u0001\u0000\u0000\u0000\u0886\u0887\u0001\u0000\u0000\u0000"+
		"\u0887\u0889\u0001\u0000\u0000\u0000\u0888\u0886\u0001\u0000\u0000\u0000"+
		"\u0889\u088a\u0005\u0003\u0000\u0000\u088a\u088c\u0001\u0000\u0000\u0000"+
		"\u088b\u087f\u0001\u0000\u0000\u0000\u088b\u0880\u0001\u0000\u0000\u0000"+
		"\u088c\u008f\u0001\u0000\u0000\u0000\u088d\u0892\u0003\u00d0h\u0000\u088e"+
		"\u0890\u0005\u0012\u0000\u0000\u088f\u088e\u0001\u0000\u0000\u0000\u088f"+
		"\u0890\u0001\u0000\u0000\u0000\u0890\u0891\u0001\u0000\u0000\u0000\u0891"+
		"\u0893\u0003\u011a\u008d\u0000\u0892\u088f\u0001\u0000\u0000\u0000\u0892"+
		"\u0893\u0001\u0000\u0000\u0000\u0893\u0091\u0001\u0000\u0000\u0000\u0894"+
		"\u0895\u0005\u0082\u0000\u0000\u0895\u0897\u0005\u0115\u0000\u0000\u0896"+
		"\u0898\u0005\u00a9\u0000\u0000\u0897\u0896\u0001\u0000\u0000\u0000\u0897"+
		"\u0898\u0001\u0000\u0000\u0000\u0898\u0899\u0001\u0000\u0000\u0000\u0899"+
		"\u089a\u0003\u0114\u008a\u0000\u089a\u08a3\u0005\u0002\u0000\u0000\u089b"+
		"\u08a0\u0003\u00d0h\u0000\u089c\u089d\u0005\u0004\u0000\u0000\u089d\u089f"+
		"\u0003\u00d0h\u0000\u089e\u089c\u0001\u0000\u0000\u0000\u089f\u08a2\u0001"+
		"\u0000\u0000\u0000\u08a0\u089e\u0001\u0000\u0000\u0000\u08a0\u08a1\u0001"+
		"\u0000\u0000\u0000\u08a1\u08a4\u0001\u0000\u0000\u0000\u08a2\u08a0\u0001"+
		"\u0000\u0000\u0000\u08a3\u089b\u0001\u0000\u0000\u0000\u08a3\u08a4\u0001"+
		"\u0000\u0000\u0000\u08a4\u08a5\u0001\u0000\u0000\u0000\u08a5\u08a6\u0005"+
		"\u0003\u0000\u0000\u08a6\u08b2\u0003\u011a\u008d\u0000\u08a7\u08a9\u0005"+
		"\u0012\u0000\u0000\u08a8\u08a7\u0001\u0000\u0000\u0000\u08a8\u08a9\u0001"+
		"\u0000\u0000\u0000\u08a9\u08aa\u0001\u0000\u0000\u0000\u08aa\u08af\u0003"+
		"\u011a\u008d\u0000\u08ab\u08ac\u0005\u0004\u0000\u0000\u08ac\u08ae\u0003"+
		"\u011a\u008d\u0000\u08ad\u08ab\u0001\u0000\u0000\u0000\u08ae\u08b1\u0001"+
		"\u0000\u0000\u0000\u08af\u08ad\u0001\u0000\u0000\u0000\u08af\u08b0\u0001"+
		"\u0000\u0000\u0000\u08b0\u08b3\u0001\u0000\u0000\u0000\u08b1\u08af\u0001"+
		"\u0000\u0000\u0000\u08b2\u08a8\u0001\u0000\u0000\u0000\u08b2\u08b3\u0001"+
		"\u0000\u0000\u0000\u08b3\u0093\u0001\u0000\u0000\u0000\u08b4\u08b5\u0007"+
		"\u0017\u0000\u0000\u08b5\u0095\u0001\u0000\u0000\u0000\u08b6\u08b8\u0005"+
		"\u0082\u0000\u0000\u08b7\u08b6\u0001\u0000\u0000\u0000\u08b7\u08b8\u0001"+
		"\u0000\u0000\u0000\u08b8\u08b9\u0001\u0000\u0000\u0000\u08b9\u08bd\u0003"+
		"\u00aeW\u0000\u08ba\u08bc\u0003\u0098L\u0000\u08bb\u08ba\u0001\u0000\u0000"+
		"\u0000\u08bc\u08bf\u0001\u0000\u0000\u0000\u08bd\u08bb\u0001\u0000\u0000"+
		"\u0000\u08bd\u08be\u0001\u0000\u0000\u0000\u08be\u0097\u0001\u0000\u0000"+
		"\u0000\u08bf\u08bd\u0001\u0000\u0000\u0000\u08c0\u08c1\u0003\u009aM\u0000"+
		"\u08c1\u08c3\u0005\u007f\u0000\u0000\u08c2\u08c4\u0005\u0082\u0000\u0000"+
		"\u08c3\u08c2\u0001\u0000\u0000\u0000\u08c3\u08c4\u0001\u0000\u0000\u0000"+
		"\u08c4\u08c5\u0001\u0000\u0000\u0000\u08c5\u08c7\u0003\u00aeW\u0000\u08c6"+
		"\u08c8\u0003\u009cN\u0000\u08c7\u08c6\u0001\u0000\u0000\u0000\u08c7\u08c8"+
		"\u0001\u0000\u0000\u0000\u08c8\u08d2\u0001\u0000\u0000\u0000\u08c9\u08ca"+
		"\u0005\u009c\u0000\u0000\u08ca\u08cb\u0003\u009aM\u0000\u08cb\u08cd\u0005"+
		"\u007f\u0000\u0000\u08cc\u08ce\u0005\u0082\u0000\u0000\u08cd\u08cc\u0001"+
		"\u0000\u0000\u0000\u08cd\u08ce\u0001\u0000\u0000\u0000\u08ce\u08cf\u0001"+
		"\u0000\u0000\u0000\u08cf\u08d0\u0003\u00aeW\u0000\u08d0\u08d2\u0001\u0000"+
		"\u0000\u0000\u08d1\u08c0\u0001\u0000\u0000\u0000\u08d1\u08c9\u0001\u0000"+
		"\u0000\u0000\u08d2\u0099\u0001\u0000\u0000\u0000\u08d3\u08d5\u0005v\u0000"+
		"\u0000\u08d4\u08d3\u0001\u0000\u0000\u0000\u08d4\u08d5\u0001\u0000\u0000"+
		"\u0000\u08d5\u08ec\u0001\u0000\u0000\u0000\u08d6\u08ec\u00054\u0000\u0000"+
		"\u08d7\u08d9\u0005\u0085\u0000\u0000\u08d8\u08da\u0005\u00a9\u0000\u0000"+
		"\u08d9\u08d8\u0001\u0000\u0000\u0000\u08d9\u08da\u0001\u0000\u0000\u0000"+
		"\u08da\u08ec\u0001\u0000\u0000\u0000\u08db\u08dd\u0005\u0085\u0000\u0000"+
		"\u08dc\u08db\u0001\u0000\u0000\u0000\u08dc\u08dd\u0001\u0000\u0000\u0000"+
		"\u08dd\u08de\u0001\u0000\u0000\u0000\u08de\u08ec\u0005\u00da\u0000\u0000"+
		"\u08df\u08e1\u0005\u00ce\u0000\u0000\u08e0\u08e2\u0005\u00a9\u0000\u0000"+
		"\u08e1\u08e0\u0001\u0000\u0000\u0000\u08e1\u08e2\u0001\u0000\u0000\u0000"+
		"\u08e2\u08ec\u0001\u0000\u0000\u0000\u08e3\u08e5\u0005g\u0000\u0000\u08e4"+
		"\u08e6\u0005\u00a9\u0000\u0000\u08e5\u08e4\u0001\u0000\u0000\u0000\u08e5"+
		"\u08e6\u0001\u0000\u0000\u0000\u08e6\u08ec\u0001\u0000\u0000\u0000\u08e7"+
		"\u08e9\u0005\u0085\u0000\u0000\u08e8\u08e7\u0001\u0000\u0000\u0000\u08e8"+
		"\u08e9\u0001\u0000\u0000\u0000\u08e9\u08ea\u0001\u0000\u0000\u0000\u08ea"+
		"\u08ec\u0005\u000e\u0000\u0000\u08eb\u08d4\u0001\u0000\u0000\u0000\u08eb"+
		"\u08d6\u0001\u0000\u0000\u0000\u08eb\u08d7\u0001\u0000\u0000\u0000\u08eb"+
		"\u08dc\u0001\u0000\u0000\u0000\u08eb\u08df\u0001\u0000\u0000\u0000\u08eb"+
		"\u08e3\u0001\u0000\u0000\u0000\u08eb\u08e8\u0001\u0000\u0000\u0000\u08ec"+
		"\u009b\u0001\u0000\u0000\u0000\u08ed\u08ee\u0005\u00a2\u0000\u0000\u08ee"+
		"\u08f2\u0003\u00d4j\u0000\u08ef\u08f0\u0005\u0112\u0000\u0000\u08f0\u08f2"+
		"\u0003\u00a2Q\u0000\u08f1\u08ed\u0001\u0000\u0000\u0000\u08f1\u08ef\u0001"+
		"\u0000\u0000\u0000\u08f2\u009d\u0001\u0000\u0000\u0000\u08f3\u08f4\u0005"+
		"\u00f3\u0000\u0000\u08f4\u08f6\u0005\u0002\u0000\u0000\u08f5\u08f7\u0003"+
		"\u00a0P\u0000\u08f6\u08f5\u0001\u0000\u0000\u0000\u08f6\u08f7\u0001\u0000"+
		"\u0000\u0000\u08f7\u08f8\u0001\u0000\u0000\u0000\u08f8\u08fd\u0005\u0003"+
		"\u0000\u0000\u08f9\u08fa\u0005\u00c8\u0000\u0000\u08fa\u08fb\u0005\u0002"+
		"\u0000\u0000\u08fb\u08fc\u0005\u0139\u0000\u0000\u08fc\u08fe\u0005\u0003"+
		"\u0000\u0000\u08fd\u08f9\u0001\u0000\u0000\u0000\u08fd\u08fe\u0001\u0000"+
		"\u0000\u0000\u08fe\u009f\u0001\u0000\u0000\u0000\u08ff\u0901\u0005\u0128"+
		"\u0000\u0000\u0900\u08ff\u0001\u0000\u0000\u0000\u0900\u0901\u0001\u0000"+
		"\u0000\u0000\u0901\u0902\u0001\u0000\u0000\u0000\u0902\u0903\u0007\u0018"+
		"\u0000\u0000\u0903\u0918\u0005\u00b4\u0000\u0000\u0904\u0905\u0003\u00d0"+
		"h\u0000\u0905\u0906\u0005\u00d5\u0000\u0000\u0906\u0918\u0001\u0000\u0000"+
		"\u0000\u0907\u0908\u0005\u0018\u0000\u0000\u0908\u0909\u0005\u0139\u0000"+
		"\u0000\u0909\u090a\u0005\u00a8\u0000\u0000\u090a\u090b\u0005\u00a1\u0000"+
		"\u0000\u090b\u0914\u0005\u0139\u0000\u0000\u090c\u0912\u0005\u00a2\u0000"+
		"\u0000\u090d\u0913\u0003\u011a\u008d\u0000\u090e\u090f\u0003\u0114\u008a"+
		"\u0000\u090f\u0910\u0005\u0002\u0000\u0000\u0910\u0911\u0005\u0003\u0000"+
		"\u0000\u0911\u0913\u0001\u0000\u0000\u0000\u0912\u090d\u0001\u0000\u0000"+
		"\u0000\u0912\u090e\u0001\u0000\u0000\u0000\u0913\u0915\u0001\u0000\u0000"+
		"\u0000\u0914\u090c\u0001\u0000\u0000\u0000\u0914\u0915\u0001\u0000\u0000"+
		"\u0000\u0915\u0918\u0001\u0000\u0000\u0000\u0916\u0918\u0003\u00d0h\u0000"+
		"\u0917\u0900\u0001\u0000\u0000\u0000\u0917\u0904\u0001\u0000\u0000\u0000"+
		"\u0917\u0907\u0001\u0000\u0000\u0000\u0917\u0916\u0001\u0000\u0000\u0000"+
		"\u0918\u00a1\u0001\u0000\u0000\u0000\u0919\u091a\u0005\u0002\u0000\u0000"+
		"\u091a\u091b\u0003\u00a4R\u0000\u091b\u091c\u0005\u0003\u0000\u0000\u091c"+
		"\u00a3\u0001\u0000\u0000\u0000\u091d\u0922\u0003\u0116\u008b\u0000\u091e"+
		"\u091f\u0005\u0004\u0000\u0000\u091f\u0921\u0003\u0116\u008b\u0000\u0920"+
		"\u091e\u0001\u0000\u0000\u0000\u0921\u0924\u0001\u0000\u0000\u0000\u0922"+
		"\u0920\u0001\u0000\u0000\u0000\u0922\u0923\u0001\u0000\u0000\u0000\u0923"+
		"\u00a5\u0001\u0000\u0000\u0000\u0924\u0922\u0001\u0000\u0000\u0000\u0925"+
		"\u0926\u0005\u0002\u0000\u0000\u0926\u092b\u0003\u00a8T\u0000\u0927\u0928"+
		"\u0005\u0004\u0000\u0000\u0928\u092a\u0003\u00a8T\u0000\u0929\u0927\u0001"+
		"\u0000\u0000\u0000\u092a\u092d\u0001\u0000\u0000\u0000\u092b\u0929\u0001"+
		"\u0000\u0000\u0000\u092b\u092c\u0001\u0000\u0000\u0000\u092c\u092e\u0001"+
		"\u0000\u0000\u0000\u092d\u092b\u0001\u0000\u0000\u0000\u092e\u092f\u0005"+
		"\u0003\u0000\u0000\u092f\u00a7\u0001\u0000\u0000\u0000\u0930\u0932\u0003"+
		"\u0116\u008b\u0000\u0931\u0933\u0007\u0011\u0000\u0000\u0932\u0931\u0001"+
		"\u0000\u0000\u0000\u0932\u0933\u0001\u0000\u0000\u0000\u0933\u00a9\u0001"+
		"\u0000\u0000\u0000\u0934\u0935\u0005\u0002\u0000\u0000\u0935\u093a\u0003"+
		"\u00acV\u0000\u0936\u0937\u0005\u0004\u0000\u0000\u0937\u0939\u0003\u00ac"+
		"V\u0000\u0938\u0936\u0001\u0000\u0000\u0000\u0939\u093c\u0001\u0000\u0000"+
		"\u0000\u093a\u0938\u0001\u0000\u0000\u0000\u093a\u093b\u0001\u0000\u0000"+
		"\u0000\u093b\u093d\u0001\u0000\u0000\u0000\u093c\u093a\u0001\u0000\u0000"+
		"\u0000\u093d\u093e\u0005\u0003\u0000\u0000\u093e\u00ab\u0001\u0000\u0000"+
		"\u0000\u093f\u0941\u0003\u011a\u008d\u0000\u0940\u0942\u0003 \u0010\u0000"+
		"\u0941\u0940\u0001\u0000\u0000\u0000\u0941\u0942\u0001\u0000\u0000\u0000"+
		"\u0942\u00ad\u0001\u0000\u0000\u0000\u0943\u0945\u0003\u00ba]\u0000\u0944"+
		"\u0946\u0003\u0080@\u0000\u0945\u0944\u0001\u0000\u0000\u0000\u0945\u0946"+
		"\u0001\u0000\u0000\u0000\u0946\u0948\u0001\u0000\u0000\u0000\u0947\u0949"+
		"\u0003\u009eO\u0000\u0948\u0947\u0001\u0000\u0000\u0000\u0948\u0949\u0001"+
		"\u0000\u0000\u0000\u0949\u094a\u0001\u0000\u0000\u0000\u094a\u094b\u0003"+
		"\u00b4Z\u0000\u094b\u095f\u0001\u0000\u0000\u0000\u094c\u094d\u0005\u0002"+
		"\u0000\u0000\u094d\u094e\u0003\"\u0011\u0000\u094e\u0950\u0005\u0003\u0000"+
		"\u0000\u094f\u0951\u0003\u009eO\u0000\u0950\u094f\u0001\u0000\u0000\u0000"+
		"\u0950\u0951\u0001\u0000\u0000\u0000\u0951\u0952\u0001\u0000\u0000\u0000"+
		"\u0952\u0953\u0003\u00b4Z\u0000\u0953\u095f\u0001\u0000\u0000\u0000\u0954"+
		"\u0955\u0005\u0002\u0000\u0000\u0955\u0956\u0003\u0096K\u0000\u0956\u0958"+
		"\u0005\u0003\u0000\u0000\u0957\u0959\u0003\u009eO\u0000\u0958\u0957\u0001"+
		"\u0000\u0000\u0000\u0958\u0959\u0001\u0000\u0000\u0000\u0959\u095a\u0001"+
		"\u0000\u0000\u0000\u095a\u095b\u0003\u00b4Z\u0000\u095b\u095f\u0001\u0000"+
		"\u0000\u0000\u095c\u095f\u0003\u00b0X\u0000\u095d\u095f\u0003\u00b2Y\u0000"+
		"\u095e\u0943\u0001\u0000\u0000\u0000\u095e\u094c\u0001\u0000\u0000\u0000"+
		"\u095e\u0954\u0001\u0000\u0000\u0000\u095e\u095c\u0001\u0000\u0000\u0000"+
		"\u095e\u095d\u0001\u0000\u0000\u0000\u095f\u00af\u0001\u0000\u0000\u0000"+
		"\u0960\u0961\u0005\u0113\u0000\u0000\u0961\u0966\u0003\u00d0h\u0000\u0962"+
		"\u0963\u0005\u0004\u0000\u0000\u0963\u0965\u0003\u00d0h\u0000\u0964\u0962"+
		"\u0001\u0000\u0000\u0000\u0965\u0968\u0001\u0000\u0000\u0000\u0966\u0964"+
		"\u0001\u0000\u0000\u0000\u0966\u0967\u0001\u0000\u0000\u0000\u0967\u0969"+
		"\u0001\u0000\u0000\u0000\u0968\u0966\u0001\u0000\u0000\u0000\u0969\u096a"+
		"\u0003\u00b4Z\u0000\u096a\u00b1\u0001\u0000\u0000\u0000\u096b\u096c\u0003"+
		"\u0112\u0089\u0000\u096c\u0975\u0005\u0002\u0000\u0000\u096d\u0972\u0003"+
		"\u00d0h\u0000\u096e\u096f\u0005\u0004\u0000\u0000\u096f\u0971\u0003\u00d0"+
		"h\u0000\u0970\u096e\u0001\u0000\u0000\u0000\u0971\u0974\u0001\u0000\u0000"+
		"\u0000\u0972\u0970\u0001\u0000\u0000\u0000\u0972\u0973\u0001\u0000\u0000"+
		"\u0000\u0973\u0976\u0001\u0000\u0000\u0000\u0974\u0972\u0001\u0000\u0000"+
		"\u0000\u0975\u096d\u0001\u0000\u0000\u0000\u0975\u0976\u0001\u0000\u0000"+
		"\u0000\u0976\u0977\u0001\u0000\u0000\u0000\u0977\u0978\u0005\u0003\u0000"+
		"\u0000\u0978\u0979\u0003\u00b4Z\u0000\u0979\u00b3\u0001\u0000\u0000\u0000"+
		"\u097a\u097c\u0005\u0012\u0000\u0000\u097b\u097a\u0001\u0000\u0000\u0000"+
		"\u097b\u097c\u0001\u0000\u0000\u0000\u097c\u097d\u0001\u0000\u0000\u0000"+
		"\u097d\u097f\u0003\u011c\u008e\u0000\u097e\u0980\u0003\u00a2Q\u0000\u097f"+
		"\u097e\u0001\u0000\u0000\u0000\u097f\u0980\u0001\u0000\u0000\u0000\u0980"+
		"\u0982\u0001\u0000\u0000\u0000\u0981\u097b\u0001\u0000\u0000\u0000\u0981"+
		"\u0982\u0001\u0000\u0000\u0000\u0982\u00b5\u0001\u0000\u0000\u0000\u0983"+
		"\u0984\u0005\u00d4\u0000\u0000\u0984\u0985\u0005d\u0000\u0000\u0985\u0986"+
		"\u0005\u00dc\u0000\u0000\u0986\u098a\u0005\u0135\u0000\u0000\u0987\u0988"+
		"\u0005\u011b\u0000\u0000\u0988\u0989\u0005\u00dd\u0000\u0000\u0989\u098b"+
		"\u0003<\u001e\u0000\u098a\u0987\u0001\u0000\u0000\u0000\u098a\u098b\u0001"+
		"\u0000\u0000\u0000\u098b\u09b5\u0001\u0000\u0000\u0000\u098c\u098d\u0005"+
		"\u00d4\u0000\u0000\u098d\u098e\u0005d\u0000\u0000\u098e\u0998\u0005E\u0000"+
		"\u0000\u098f\u0990\u0005]\u0000\u0000\u0990\u0991\u0005\u00f6\u0000\u0000"+
		"\u0991\u0992\u0005\u001a\u0000\u0000\u0992\u0996\u0005\u0135\u0000\u0000"+
		"\u0993\u0994\u0005R\u0000\u0000\u0994\u0995\u0005\u001a\u0000\u0000\u0995"+
		"\u0997\u0005\u0135\u0000\u0000\u0996\u0993\u0001\u0000\u0000\u0000\u0996"+
		"\u0997\u0001\u0000\u0000\u0000\u0997\u0999\u0001\u0000\u0000\u0000\u0998"+
		"\u098f\u0001\u0000\u0000\u0000\u0998\u0999\u0001\u0000\u0000\u0000\u0999"+
		"\u099f\u0001\u0000\u0000\u0000\u099a\u099b\u0005(\u0000\u0000\u099b\u099c"+
		"\u0005~\u0000\u0000\u099c\u099d\u0005\u00f6\u0000\u0000\u099d\u099e\u0005"+
		"\u001a\u0000\u0000\u099e\u09a0\u0005\u0135\u0000\u0000\u099f\u099a\u0001"+
		"\u0000\u0000\u0000\u099f\u09a0\u0001\u0000\u0000\u0000\u09a0\u09a6\u0001"+
		"\u0000\u0000\u0000\u09a1\u09a2\u0005\u0092\u0000\u0000\u09a2\u09a3\u0005"+
		"\u0080\u0000\u0000\u09a3\u09a4\u0005\u00f6\u0000\u0000\u09a4\u09a5\u0005"+
		"\u001a\u0000\u0000\u09a5\u09a7\u0005\u0135\u0000\u0000\u09a6\u09a1\u0001"+
		"\u0000\u0000\u0000\u09a6\u09a7\u0001\u0000\u0000\u0000\u09a7\u09ac\u0001"+
		"\u0000\u0000\u0000\u09a8\u09a9\u0005\u0089\u0000\u0000\u09a9\u09aa\u0005"+
		"\u00f6\u0000\u0000\u09aa\u09ab\u0005\u001a\u0000\u0000\u09ab\u09ad\u0005"+
		"\u0135\u0000\u0000\u09ac\u09a8\u0001\u0000\u0000\u0000\u09ac\u09ad\u0001"+
		"\u0000\u0000\u0000\u09ad\u09b2\u0001\u0000\u0000\u0000\u09ae\u09af\u0005"+
		"\u009f\u0000\u0000\u09af\u09b0\u0005C\u0000\u0000\u09b0\u09b1\u0005\u0012"+
		"\u0000\u0000\u09b1\u09b3\u0005\u0135\u0000\u0000\u09b2\u09ae\u0001\u0000"+
		"\u0000\u0000\u09b2\u09b3\u0001\u0000\u0000\u0000\u09b3\u09b5\u0001\u0000"+
		"\u0000\u0000\u09b4\u0983\u0001\u0000\u0000\u0000\u09b4\u098c\u0001\u0000"+
		"\u0000\u0000\u09b5\u00b7\u0001\u0000\u0000\u0000\u09b6\u09bb\u0003\u00ba"+
		"]\u0000\u09b7\u09b8\u0005\u0004\u0000\u0000\u09b8\u09ba\u0003\u00ba]\u0000"+
		"\u09b9\u09b7\u0001\u0000\u0000\u0000\u09ba\u09bd\u0001\u0000\u0000\u0000"+
		"\u09bb\u09b9\u0001\u0000\u0000\u0000\u09bb\u09bc\u0001\u0000\u0000\u0000"+
		"\u09bc\u00b9\u0001\u0000\u0000\u0000\u09bd\u09bb\u0001\u0000\u0000\u0000"+
		"\u09be\u09c3\u0003\u0116\u008b\u0000\u09bf\u09c0\u0005\u0005\u0000\u0000"+
		"\u09c0\u09c2\u0003\u0116\u008b\u0000\u09c1\u09bf\u0001\u0000\u0000\u0000"+
		"\u09c2\u09c5\u0001\u0000\u0000\u0000\u09c3\u09c1\u0001\u0000\u0000\u0000"+
		"\u09c3\u09c4\u0001\u0000\u0000\u0000\u09c4\u00bb\u0001\u0000\u0000\u0000"+
		"\u09c5\u09c3\u0001\u0000\u0000\u0000\u09c6\u09cb\u0003\u00be_\u0000\u09c7"+
		"\u09c8\u0005\u0004\u0000\u0000\u09c8\u09ca\u0003\u00be_\u0000\u09c9\u09c7"+
		"\u0001\u0000\u0000\u0000\u09ca\u09cd\u0001\u0000\u0000\u0000\u09cb\u09c9"+
		"\u0001\u0000\u0000\u0000\u09cb\u09cc\u0001\u0000\u0000\u0000\u09cc\u00bd"+
		"\u0001\u0000\u0000\u0000\u09cd\u09cb\u0001\u0000\u0000\u0000\u09ce\u09d1"+
		"\u0003\u00ba]\u0000\u09cf\u09d0\u0005\u00a5\u0000\u0000\u09d0\u09d2\u0003"+
		"<\u001e\u0000\u09d1\u09cf\u0001\u0000\u0000\u0000\u09d1\u09d2\u0001\u0000"+
		"\u0000\u0000\u09d2\u00bf\u0001\u0000\u0000\u0000\u09d3\u09d4\u0003\u0116"+
		"\u008b\u0000\u09d4\u09d5\u0005\u0005\u0000\u0000\u09d5\u09d7\u0001\u0000"+
		"\u0000\u0000\u09d6\u09d3\u0001\u0000\u0000\u0000\u09d6\u09d7\u0001\u0000"+
		"\u0000\u0000\u09d7\u09d8\u0001\u0000\u0000\u0000\u09d8\u09d9\u0003\u0116"+
		"\u008b\u0000\u09d9\u00c1\u0001\u0000\u0000\u0000\u09da\u09db\u0003\u0116"+
		"\u008b\u0000\u09db\u09dc\u0005\u0005\u0000\u0000\u09dc\u09de\u0001\u0000"+
		"\u0000\u0000\u09dd\u09da\u0001\u0000\u0000\u0000\u09dd\u09de\u0001\u0000"+
		"\u0000\u0000\u09de\u09df\u0001\u0000\u0000\u0000\u09df\u09e0\u0003\u0116"+
		"\u008b\u0000\u09e0\u00c3\u0001\u0000\u0000\u0000\u09e1\u09e9\u0003\u00d0"+
		"h\u0000\u09e2\u09e4\u0005\u0012\u0000\u0000\u09e3\u09e2\u0001\u0000\u0000"+
		"\u0000\u09e3\u09e4\u0001\u0000\u0000\u0000\u09e4\u09e7\u0001\u0000\u0000"+
		"\u0000\u09e5\u09e8\u0003\u0116\u008b\u0000\u09e6\u09e8\u0003\u00a2Q\u0000"+
		"\u09e7\u09e5\u0001\u0000\u0000\u0000\u09e7\u09e6\u0001\u0000\u0000\u0000"+
		"\u09e8\u09ea\u0001\u0000\u0000\u0000\u09e9\u09e3\u0001\u0000\u0000\u0000"+
		"\u09e9\u09ea\u0001\u0000\u0000\u0000\u09ea\u00c5\u0001\u0000\u0000\u0000"+
		"\u09eb\u09f0\u0003\u00c4b\u0000\u09ec\u09ed\u0005\u0004\u0000\u0000\u09ed"+
		"\u09ef\u0003\u00c4b\u0000\u09ee\u09ec\u0001\u0000\u0000\u0000\u09ef\u09f2"+
		"\u0001\u0000\u0000\u0000\u09f0\u09ee\u0001\u0000\u0000\u0000\u09f0\u09f1"+
		"\u0001\u0000\u0000\u0000\u09f1\u00c7\u0001\u0000\u0000\u0000\u09f2\u09f0"+
		"\u0001\u0000\u0000\u0000\u09f3\u09f4\u0005\u0002\u0000\u0000\u09f4\u09f9"+
		"\u0003\u00cae\u0000\u09f5\u09f6\u0005\u0004\u0000\u0000\u09f6\u09f8\u0003"+
		"\u00cae\u0000\u09f7\u09f5\u0001\u0000\u0000\u0000\u09f8\u09fb\u0001\u0000"+
		"\u0000\u0000\u09f9\u09f7\u0001\u0000\u0000\u0000\u09f9\u09fa\u0001\u0000"+
		"\u0000\u0000\u09fa\u09fc\u0001\u0000\u0000\u0000\u09fb\u09f9\u0001\u0000"+
		"\u0000\u0000\u09fc\u09fd\u0005\u0003\u0000\u0000\u09fd\u00c9\u0001\u0000"+
		"\u0000\u0000\u09fe\u0a01\u0003\u00ccf\u0000\u09ff\u0a01\u0003\u00fe\u007f"+
		"\u0000\u0a00\u09fe\u0001\u0000\u0000\u0000\u0a00\u09ff\u0001\u0000\u0000"+
		"\u0000\u0a01\u00cb\u0001\u0000\u0000\u0000\u0a02\u0a10\u0003\u0114\u008a"+
		"\u0000\u0a03\u0a04\u0003\u011a\u008d\u0000\u0a04\u0a05\u0005\u0002\u0000"+
		"\u0000\u0a05\u0a0a\u0003\u00ceg\u0000\u0a06\u0a07\u0005\u0004\u0000\u0000"+
		"\u0a07\u0a09\u0003\u00ceg\u0000\u0a08\u0a06\u0001\u0000\u0000\u0000\u0a09"+
		"\u0a0c\u0001\u0000\u0000\u0000\u0a0a\u0a08\u0001\u0000\u0000\u0000\u0a0a"+
		"\u0a0b\u0001\u0000\u0000\u0000\u0a0b\u0a0d\u0001\u0000\u0000\u0000\u0a0c"+
		"\u0a0a\u0001\u0000\u0000\u0000\u0a0d\u0a0e\u0005\u0003\u0000\u0000\u0a0e"+
		"\u0a10\u0001\u0000\u0000\u0000\u0a0f\u0a02\u0001\u0000\u0000\u0000\u0a0f"+
		"\u0a03\u0001\u0000\u0000\u0000\u0a10\u00cd\u0001\u0000\u0000\u0000\u0a11"+
		"\u0a14\u0003\u0114\u008a\u0000\u0a12\u0a14\u0003\u00deo\u0000\u0a13\u0a11"+
		"\u0001\u0000\u0000\u0000\u0a13\u0a12\u0001\u0000\u0000\u0000\u0a14\u00cf"+
		"\u0001\u0000\u0000\u0000\u0a15\u0a16\u0003\u00d4j\u0000\u0a16\u00d1\u0001"+
		"\u0000\u0000\u0000\u0a17\u0a1c\u0003\u00d0h\u0000\u0a18\u0a19\u0005\u0004"+
		"\u0000\u0000\u0a19\u0a1b\u0003\u00d0h\u0000\u0a1a\u0a18\u0001\u0000\u0000"+
		"\u0000\u0a1b\u0a1e\u0001\u0000\u0000\u0000\u0a1c\u0a1a\u0001\u0000\u0000"+
		"\u0000\u0a1c\u0a1d\u0001\u0000\u0000\u0000\u0a1d\u00d3\u0001\u0000\u0000"+
		"\u0000\u0a1e\u0a1c\u0001\u0000\u0000\u0000\u0a1f\u0a20\u0006j\uffff\uffff"+
		"\u0000\u0a20\u0a21\u0005\u009e\u0000\u0000\u0a21\u0a2c\u0003\u00d4j\u0005"+
		"\u0a22\u0a23\u0005U\u0000\u0000\u0a23\u0a24\u0005\u0002\u0000\u0000\u0a24"+
		"\u0a25\u0003\"\u0011\u0000\u0a25\u0a26\u0005\u0003\u0000\u0000\u0a26\u0a2c"+
		"\u0001\u0000\u0000\u0000\u0a27\u0a29\u0003\u00d8l\u0000\u0a28\u0a2a\u0003"+
		"\u00d6k\u0000\u0a29\u0a28\u0001\u0000\u0000\u0000\u0a29\u0a2a\u0001\u0000"+
		"\u0000\u0000\u0a2a\u0a2c\u0001\u0000\u0000\u0000\u0a2b\u0a1f\u0001\u0000"+
		"\u0000\u0000\u0a2b\u0a22\u0001\u0000\u0000\u0000\u0a2b\u0a27\u0001\u0000"+
		"\u0000\u0000\u0a2c\u0a35\u0001\u0000\u0000\u0000\u0a2d\u0a2e\n\u0002\u0000"+
		"\u0000\u0a2e\u0a2f\u0005\r\u0000\u0000\u0a2f\u0a34\u0003\u00d4j\u0003"+
		"\u0a30\u0a31\n\u0001\u0000\u0000\u0a31\u0a32\u0005\u00a6\u0000\u0000\u0a32"+
		"\u0a34\u0003\u00d4j\u0002\u0a33\u0a2d\u0001\u0000\u0000\u0000\u0a33\u0a30"+
		"\u0001\u0000\u0000\u0000\u0a34\u0a37\u0001\u0000\u0000\u0000\u0a35\u0a33"+
		"\u0001\u0000\u0000\u0000\u0a35\u0a36\u0001\u0000\u0000\u0000\u0a36\u00d5"+
		"\u0001\u0000\u0000\u0000\u0a37\u0a35\u0001\u0000\u0000\u0000\u0a38\u0a3a"+
		"\u0005\u009e\u0000\u0000\u0a39\u0a38\u0001\u0000\u0000\u0000\u0a39\u0a3a"+
		"\u0001\u0000\u0000\u0000\u0a3a\u0a3b\u0001\u0000\u0000\u0000\u0a3b\u0a3c"+
		"\u0005\u0016\u0000\u0000\u0a3c\u0a3d\u0003\u00d8l\u0000\u0a3d\u0a3e\u0005"+
		"\r\u0000\u0000\u0a3e\u0a3f\u0003\u00d8l\u0000\u0a3f\u0a8b\u0001\u0000"+
		"\u0000\u0000\u0a40\u0a42\u0005\u009e\u0000\u0000\u0a41\u0a40\u0001\u0000"+
		"\u0000\u0000\u0a41\u0a42\u0001\u0000\u0000\u0000\u0a42\u0a43\u0001\u0000"+
		"\u0000\u0000\u0a43\u0a44\u0005s\u0000\u0000\u0a44\u0a45\u0005\u0002\u0000"+
		"\u0000\u0a45\u0a4a\u0003\u00d0h\u0000\u0a46\u0a47\u0005\u0004\u0000\u0000"+
		"\u0a47\u0a49\u0003\u00d0h\u0000\u0a48\u0a46\u0001\u0000\u0000\u0000\u0a49"+
		"\u0a4c\u0001\u0000\u0000\u0000\u0a4a\u0a48\u0001\u0000\u0000\u0000\u0a4a"+
		"\u0a4b\u0001\u0000\u0000\u0000\u0a4b\u0a4d\u0001\u0000\u0000\u0000\u0a4c"+
		"\u0a4a\u0001\u0000\u0000\u0000\u0a4d\u0a4e\u0005\u0003\u0000\u0000\u0a4e"+
		"\u0a8b\u0001\u0000\u0000\u0000\u0a4f\u0a51\u0005\u009e\u0000\u0000\u0a50"+
		"\u0a4f\u0001\u0000\u0000\u0000\u0a50\u0a51\u0001\u0000\u0000\u0000\u0a51"+
		"\u0a52\u0001\u0000\u0000\u0000\u0a52\u0a53\u0005s\u0000\u0000\u0a53\u0a54"+
		"\u0005\u0002\u0000\u0000\u0a54\u0a55\u0003\"\u0011\u0000\u0a55\u0a56\u0005"+
		"\u0003\u0000\u0000\u0a56\u0a8b\u0001\u0000\u0000\u0000\u0a57\u0a59\u0005"+
		"\u009e\u0000\u0000\u0a58\u0a57\u0001\u0000\u0000\u0000\u0a58\u0a59\u0001"+
		"\u0000\u0000\u0000\u0a59\u0a5a\u0001\u0000\u0000\u0000\u0a5a\u0a5b\u0005"+
		"\u00cf\u0000\u0000\u0a5b\u0a8b\u0003\u00d8l\u0000\u0a5c\u0a5e\u0005\u009e"+
		"\u0000\u0000\u0a5d\u0a5c\u0001\u0000\u0000\u0000\u0a5d\u0a5e\u0001\u0000"+
		"\u0000\u0000\u0a5e\u0a5f\u0001\u0000\u0000\u0000\u0a5f\u0a60\u0007\u0019"+
		"\u0000\u0000\u0a60\u0a6e\u0007\u001a\u0000\u0000\u0a61\u0a62\u0005\u0002"+
		"\u0000\u0000\u0a62\u0a6f\u0005\u0003\u0000\u0000\u0a63\u0a64\u0005\u0002"+
		"\u0000\u0000\u0a64\u0a69\u0003\u00d0h\u0000\u0a65\u0a66\u0005\u0004\u0000"+
		"\u0000\u0a66\u0a68\u0003\u00d0h\u0000\u0a67\u0a65\u0001\u0000\u0000\u0000"+
		"\u0a68\u0a6b\u0001\u0000\u0000\u0000\u0a69\u0a67\u0001\u0000\u0000\u0000"+
		"\u0a69\u0a6a\u0001\u0000\u0000\u0000\u0a6a\u0a6c\u0001\u0000\u0000\u0000"+
		"\u0a6b\u0a69\u0001\u0000\u0000\u0000\u0a6c\u0a6d\u0005\u0003\u0000\u0000"+
		"\u0a6d\u0a6f\u0001\u0000\u0000\u0000\u0a6e\u0a61\u0001\u0000\u0000\u0000"+
		"\u0a6e\u0a63\u0001\u0000\u0000\u0000\u0a6f\u0a8b\u0001\u0000\u0000\u0000"+
		"\u0a70\u0a72\u0005\u009e\u0000\u0000\u0a71\u0a70\u0001\u0000\u0000\u0000"+
		"\u0a71\u0a72\u0001\u0000\u0000\u0000\u0a72\u0a73\u0001\u0000\u0000\u0000"+
		"\u0a73\u0a74\u0007\u0019\u0000\u0000\u0a74\u0a77\u0003\u00d8l\u0000\u0a75"+
		"\u0a76\u0005Q\u0000\u0000\u0a76\u0a78\u0005\u0135\u0000\u0000\u0a77\u0a75"+
		"\u0001\u0000\u0000\u0000\u0a77\u0a78\u0001\u0000\u0000\u0000\u0a78\u0a8b"+
		"\u0001\u0000\u0000\u0000\u0a79\u0a7b\u0005}\u0000\u0000\u0a7a\u0a7c\u0005"+
		"\u009e\u0000\u0000\u0a7b\u0a7a\u0001\u0000\u0000\u0000\u0a7b\u0a7c\u0001"+
		"\u0000\u0000\u0000\u0a7c\u0a7d\u0001\u0000\u0000\u0000\u0a7d\u0a8b\u0005"+
		"\u009f\u0000\u0000\u0a7e\u0a80\u0005}\u0000\u0000\u0a7f\u0a81\u0005\u009e"+
		"\u0000\u0000\u0a80\u0a7f\u0001\u0000\u0000\u0000\u0a80\u0a81\u0001\u0000"+
		"\u0000\u0000\u0a81\u0a82\u0001\u0000\u0000\u0000\u0a82\u0a8b\u0007\u001b"+
		"\u0000\u0000\u0a83\u0a85\u0005}\u0000\u0000\u0a84\u0a86\u0005\u009e\u0000"+
		"\u0000\u0a85\u0a84\u0001\u0000\u0000\u0000\u0a85\u0a86\u0001\u0000\u0000"+
		"\u0000\u0a86\u0a87\u0001\u0000\u0000\u0000\u0a87\u0a88\u0005K\u0000\u0000"+
		"\u0a88\u0a89\u0005f\u0000\u0000\u0a89\u0a8b\u0003\u00d8l\u0000\u0a8a\u0a39"+
		"\u0001\u0000\u0000\u0000\u0a8a\u0a41\u0001\u0000\u0000\u0000\u0a8a\u0a50"+
		"\u0001\u0000\u0000\u0000\u0a8a\u0a58\u0001\u0000\u0000\u0000\u0a8a\u0a5d"+
		"\u0001\u0000\u0000\u0000\u0a8a\u0a71\u0001\u0000\u0000\u0000\u0a8a\u0a79"+
		"\u0001\u0000\u0000\u0000\u0a8a\u0a7e\u0001\u0000\u0000\u0000\u0a8a\u0a83"+
		"\u0001\u0000\u0000\u0000\u0a8b\u00d7\u0001\u0000\u0000\u0000\u0a8c\u0a8d"+
		"\u0006l\uffff\uffff\u0000\u0a8d\u0a91\u0003\u00dcn\u0000\u0a8e\u0a8f\u0007"+
		"\u001c\u0000\u0000\u0a8f\u0a91\u0003\u00d8l\u0007\u0a90\u0a8c\u0001\u0000"+
		"\u0000\u0000\u0a90\u0a8e\u0001\u0000\u0000\u0000\u0a91\u0aa7\u0001\u0000"+
		"\u0000\u0000\u0a92\u0a93\n\u0006\u0000\u0000\u0a93\u0a94\u0007\u001d\u0000"+
		"\u0000\u0a94\u0aa6\u0003\u00d8l\u0007\u0a95\u0a96\n\u0005\u0000\u0000"+
		"\u0a96\u0a97\u0007\u001e\u0000\u0000\u0a97\u0aa6\u0003\u00d8l\u0006\u0a98"+
		"\u0a99\n\u0004\u0000\u0000\u0a99\u0a9a\u0005\u012d\u0000\u0000\u0a9a\u0aa6"+
		"\u0003\u00d8l\u0005\u0a9b\u0a9c\n\u0003\u0000\u0000\u0a9c\u0a9d\u0005"+
		"\u0130\u0000\u0000\u0a9d\u0aa6\u0003\u00d8l\u0004\u0a9e\u0a9f\n\u0002"+
		"\u0000\u0000\u0a9f\u0aa0\u0005\u012e\u0000\u0000\u0aa0\u0aa6\u0003\u00d8"+
		"l\u0003\u0aa1\u0aa2\n\u0001\u0000\u0000\u0aa2\u0aa3\u0003\u00e0p\u0000"+
		"\u0aa3\u0aa4\u0003\u00d8l\u0002\u0aa4\u0aa6\u0001\u0000\u0000\u0000\u0aa5"+
		"\u0a92\u0001\u0000\u0000\u0000\u0aa5\u0a95\u0001\u0000\u0000\u0000\u0aa5"+
		"\u0a98\u0001\u0000\u0000\u0000\u0aa5\u0a9b\u0001\u0000\u0000\u0000\u0aa5"+
		"\u0a9e\u0001\u0000\u0000\u0000\u0aa5\u0aa1\u0001\u0000\u0000\u0000\u0aa6"+
		"\u0aa9\u0001\u0000\u0000\u0000\u0aa7\u0aa5\u0001\u0000\u0000\u0000\u0aa7"+
		"\u0aa8\u0001\u0000\u0000\u0000\u0aa8\u00d9\u0001\u0000\u0000\u0000\u0aa9"+
		"\u0aa7\u0001\u0000\u0000\u0000\u0aaa\u0aab\u0007\u001f\u0000\u0000\u0aab"+
		"\u00db\u0001\u0000\u0000\u0000\u0aac\u0aad\u0006n\uffff\uffff\u0000\u0aad"+
		"\u0b8a\u0007 \u0000\u0000\u0aae\u0aaf\u0007!\u0000\u0000\u0aaf\u0ab0\u0005"+
		"\u0002\u0000\u0000\u0ab0\u0ab1\u0003\u00dam\u0000\u0ab1\u0ab2\u0005\u0004"+
		"\u0000\u0000\u0ab2\u0ab3\u0003\u00d8l\u0000\u0ab3\u0ab4\u0005\u0004\u0000"+
		"\u0000\u0ab4\u0ab5\u0003\u00d8l\u0000\u0ab5\u0ab6\u0005\u0003\u0000\u0000"+
		"\u0ab6\u0b8a\u0001\u0000\u0000\u0000\u0ab7\u0ab8\u0007\"\u0000\u0000\u0ab8"+
		"\u0ab9\u0005\u0002\u0000\u0000\u0ab9\u0aba\u0003\u00dam\u0000\u0aba\u0abb"+
		"\u0005\u0004\u0000\u0000\u0abb\u0abc\u0003\u00d8l\u0000\u0abc\u0abd\u0005"+
		"\u0004\u0000\u0000\u0abd\u0abe\u0003\u00d8l\u0000\u0abe\u0abf\u0005\u0003"+
		"\u0000\u0000\u0abf\u0b8a\u0001\u0000\u0000\u0000\u0ac0\u0ac2\u0005\u001d"+
		"\u0000\u0000\u0ac1\u0ac3\u0003\u0104\u0082\u0000\u0ac2\u0ac1\u0001\u0000"+
		"\u0000\u0000\u0ac3\u0ac4\u0001\u0000\u0000\u0000\u0ac4\u0ac2\u0001\u0000"+
		"\u0000\u0000\u0ac4\u0ac5\u0001\u0000\u0000\u0000\u0ac5\u0ac8\u0001\u0000"+
		"\u0000\u0000\u0ac6\u0ac7\u0005O\u0000\u0000\u0ac7\u0ac9\u0003\u00d0h\u0000"+
		"\u0ac8\u0ac6\u0001\u0000\u0000\u0000\u0ac8\u0ac9\u0001\u0000\u0000\u0000"+
		"\u0ac9\u0aca\u0001\u0000\u0000\u0000\u0aca\u0acb\u0005P\u0000\u0000\u0acb"+
		"\u0b8a\u0001\u0000\u0000\u0000\u0acc\u0acd\u0005\u001d\u0000\u0000\u0acd"+
		"\u0acf\u0003\u00d0h\u0000\u0ace\u0ad0\u0003\u0104\u0082\u0000\u0acf\u0ace"+
		"\u0001\u0000\u0000\u0000\u0ad0\u0ad1\u0001\u0000\u0000\u0000\u0ad1\u0acf"+
		"\u0001\u0000\u0000\u0000\u0ad1\u0ad2\u0001\u0000\u0000\u0000\u0ad2\u0ad5"+
		"\u0001\u0000\u0000\u0000\u0ad3\u0ad4\u0005O\u0000\u0000\u0ad4\u0ad6\u0003"+
		"\u00d0h\u0000\u0ad5\u0ad3\u0001\u0000\u0000\u0000\u0ad5\u0ad6\u0001\u0000"+
		"\u0000\u0000\u0ad6\u0ad7\u0001\u0000\u0000\u0000\u0ad7\u0ad8\u0005P\u0000"+
		"\u0000\u0ad8\u0b8a\u0001\u0000\u0000\u0000\u0ad9\u0ada\u0007#\u0000\u0000"+
		"\u0ada\u0adb\u0005\u0002\u0000\u0000\u0adb\u0adc\u0003\u00d0h\u0000\u0adc"+
		"\u0add\u0005\u0012\u0000\u0000\u0add\u0ade\u0003\u00f6{\u0000\u0ade\u0adf"+
		"\u0005\u0003\u0000\u0000\u0adf\u0b8a\u0001\u0000\u0000\u0000\u0ae0\u0ae1"+
		"\u0005\u00eb\u0000\u0000\u0ae1\u0aea\u0005\u0002\u0000\u0000\u0ae2\u0ae7"+
		"\u0003\u00c4b\u0000\u0ae3\u0ae4\u0005\u0004\u0000\u0000\u0ae4\u0ae6\u0003"+
		"\u00c4b\u0000\u0ae5\u0ae3\u0001\u0000\u0000\u0000\u0ae6\u0ae9\u0001\u0000"+
		"\u0000\u0000\u0ae7\u0ae5\u0001\u0000\u0000\u0000\u0ae7\u0ae8\u0001\u0000"+
		"\u0000\u0000\u0ae8\u0aeb\u0001\u0000\u0000\u0000\u0ae9\u0ae7\u0001\u0000"+
		"\u0000\u0000\u0aea\u0ae2\u0001\u0000\u0000\u0000\u0aea\u0aeb\u0001\u0000"+
		"\u0000\u0000\u0aeb\u0aec\u0001\u0000\u0000\u0000\u0aec\u0b8a\u0005\u0003"+
		"\u0000\u0000\u0aed\u0aee\u0005`\u0000\u0000\u0aee\u0aef\u0005\u0002\u0000"+
		"\u0000\u0aef\u0af2\u0003\u00d0h\u0000\u0af0\u0af1\u0005q\u0000\u0000\u0af1"+
		"\u0af3\u0005\u00a0\u0000\u0000\u0af2\u0af0\u0001\u0000\u0000\u0000\u0af2"+
		"\u0af3\u0001\u0000\u0000\u0000\u0af3\u0af4\u0001\u0000\u0000\u0000\u0af4"+
		"\u0af5\u0005\u0003\u0000\u0000\u0af5\u0b8a\u0001\u0000\u0000\u0000\u0af6"+
		"\u0af7\u0005\u0081\u0000\u0000\u0af7\u0af8\u0005\u0002\u0000\u0000\u0af8"+
		"\u0afb\u0003\u00d0h\u0000\u0af9\u0afa\u0005q\u0000\u0000\u0afa\u0afc\u0005"+
		"\u00a0\u0000\u0000\u0afb\u0af9\u0001\u0000\u0000\u0000\u0afb\u0afc\u0001"+
		"\u0000\u0000\u0000\u0afc\u0afd\u0001\u0000\u0000\u0000\u0afd\u0afe\u0005"+
		"\u0003\u0000\u0000\u0afe\u0b8a\u0001\u0000\u0000\u0000\u0aff\u0b00\u0005"+
		"\u00b7\u0000\u0000\u0b00\u0b01\u0005\u0002\u0000\u0000\u0b01\u0b02\u0003"+
		"\u00d8l\u0000\u0b02\u0b03\u0005s\u0000\u0000\u0b03\u0b04\u0003\u00d8l"+
		"\u0000\u0b04\u0b05\u0005\u0003\u0000\u0000\u0b05\u0b8a\u0001\u0000\u0000"+
		"\u0000\u0b06\u0b8a\u0003\u00deo\u0000\u0b07\u0b8a\u0005\u0129\u0000\u0000"+
		"\u0b08\u0b09\u0003\u0114\u008a\u0000\u0b09\u0b0a\u0005\u0005\u0000\u0000"+
		"\u0b0a\u0b0b\u0005\u0129\u0000\u0000\u0b0b\u0b8a\u0001\u0000\u0000\u0000"+
		"\u0b0c\u0b0d\u0005\u0002\u0000\u0000\u0b0d\u0b10\u0003\u00c4b\u0000\u0b0e"+
		"\u0b0f\u0005\u0004\u0000\u0000\u0b0f\u0b11\u0003\u00c4b\u0000\u0b10\u0b0e"+
		"\u0001\u0000\u0000\u0000\u0b11\u0b12\u0001\u0000\u0000\u0000\u0b12\u0b10"+
		"\u0001\u0000\u0000\u0000\u0b12\u0b13\u0001\u0000\u0000\u0000\u0b13\u0b14"+
		"\u0001\u0000\u0000\u0000\u0b14\u0b15\u0005\u0003\u0000\u0000\u0b15\u0b8a"+
		"\u0001\u0000\u0000\u0000\u0b16\u0b17\u0005\u0002\u0000\u0000\u0b17\u0b18"+
		"\u0003\"\u0011\u0000\u0b18\u0b19\u0005\u0003\u0000\u0000\u0b19\u0b8a\u0001"+
		"\u0000\u0000\u0000\u0b1a\u0b1b\u0003\u0112\u0089\u0000\u0b1b\u0b27\u0005"+
		"\u0002\u0000\u0000\u0b1c\u0b1e\u0003\u0094J\u0000\u0b1d\u0b1c\u0001\u0000"+
		"\u0000\u0000\u0b1d\u0b1e\u0001\u0000\u0000\u0000\u0b1e\u0b1f\u0001\u0000"+
		"\u0000\u0000\u0b1f\u0b24\u0003\u00d0h\u0000\u0b20\u0b21\u0005\u0004\u0000"+
		"\u0000\u0b21\u0b23\u0003\u00d0h\u0000\u0b22\u0b20\u0001\u0000\u0000\u0000"+
		"\u0b23\u0b26\u0001\u0000\u0000\u0000\u0b24\u0b22\u0001\u0000\u0000\u0000"+
		"\u0b24\u0b25\u0001\u0000\u0000\u0000\u0b25\u0b28\u0001\u0000\u0000\u0000"+
		"\u0b26\u0b24\u0001\u0000\u0000\u0000\u0b27\u0b1d\u0001\u0000\u0000\u0000"+
		"\u0b27\u0b28\u0001\u0000\u0000\u0000\u0b28\u0b29\u0001\u0000\u0000\u0000"+
		"\u0b29\u0b30\u0005\u0003\u0000\u0000\u0b2a\u0b2b\u0005^\u0000\u0000\u0b2b"+
		"\u0b2c\u0005\u0002\u0000\u0000\u0b2c\u0b2d\u0005\u0119\u0000\u0000\u0b2d"+
		"\u0b2e\u0003\u00d4j\u0000\u0b2e\u0b2f\u0005\u0003\u0000\u0000\u0b2f\u0b31"+
		"\u0001\u0000\u0000\u0000\u0b30\u0b2a\u0001\u0000\u0000\u0000\u0b30\u0b31"+
		"\u0001\u0000\u0000\u0000\u0b31\u0b34\u0001\u0000\u0000\u0000\u0b32\u0b33"+
		"\u0007$\u0000\u0000\u0b33\u0b35\u0005\u00a0\u0000\u0000\u0b34\u0b32\u0001"+
		"\u0000\u0000\u0000\u0b34\u0b35\u0001\u0000\u0000\u0000\u0b35\u0b38\u0001"+
		"\u0000\u0000\u0000\u0b36\u0b37\u0005\u00ab\u0000\u0000\u0b37\u0b39\u0003"+
		"\u010a\u0085\u0000\u0b38\u0b36\u0001\u0000\u0000\u0000\u0b38\u0b39\u0001"+
		"\u0000\u0000\u0000\u0b39\u0b8a\u0001\u0000\u0000\u0000\u0b3a\u0b3b\u0003"+
		"\u011a\u008d\u0000\u0b3b\u0b3c\u0005\u0132\u0000\u0000\u0b3c\u0b3d\u0003"+
		"\u00d0h\u0000\u0b3d\u0b8a\u0001\u0000\u0000\u0000\u0b3e\u0b3f\u0005\u0002"+
		"\u0000\u0000\u0b3f\u0b42\u0003\u011a\u008d\u0000\u0b40\u0b41\u0005\u0004"+
		"\u0000\u0000\u0b41\u0b43\u0003\u011a\u008d\u0000\u0b42\u0b40\u0001\u0000"+
		"\u0000\u0000\u0b43\u0b44\u0001\u0000\u0000\u0000\u0b44\u0b42\u0001\u0000"+
		"\u0000\u0000\u0b44\u0b45\u0001\u0000\u0000\u0000\u0b45\u0b46\u0001\u0000"+
		"\u0000\u0000\u0b46\u0b47\u0005\u0003\u0000\u0000\u0b47\u0b48\u0005\u0132"+
		"\u0000\u0000\u0b48\u0b49\u0003\u00d0h\u0000\u0b49\u0b8a\u0001\u0000\u0000"+
		"\u0000\u0b4a\u0b8a\u0003\u011a\u008d\u0000\u0b4b\u0b4c\u0005\u0002\u0000"+
		"\u0000\u0b4c\u0b4d\u0003\u00d0h\u0000\u0b4d\u0b4e\u0005\u0003\u0000\u0000"+
		"\u0b4e\u0b8a\u0001\u0000\u0000\u0000\u0b4f\u0b50\u0005Z\u0000\u0000\u0b50"+
		"\u0b51\u0005\u0002\u0000\u0000\u0b51\u0b52\u0003\u011a\u008d\u0000\u0b52"+
		"\u0b53\u0005f\u0000\u0000\u0b53\u0b54\u0003\u00d8l\u0000\u0b54\u0b55\u0005"+
		"\u0003\u0000\u0000\u0b55\u0b8a\u0001\u0000\u0000\u0000\u0b56\u0b57\u0007"+
		"%\u0000\u0000\u0b57\u0b58\u0005\u0002\u0000\u0000\u0b58\u0b59\u0003\u00d8"+
		"l\u0000\u0b59\u0b5a\u0007&\u0000\u0000\u0b5a\u0b5d\u0003\u00d8l\u0000"+
		"\u0b5b\u0b5c\u0007\'\u0000\u0000\u0b5c\u0b5e\u0003\u00d8l\u0000\u0b5d"+
		"\u0b5b\u0001\u0000\u0000\u0000\u0b5d\u0b5e\u0001\u0000\u0000\u0000\u0b5e"+
		"\u0b5f\u0001\u0000\u0000\u0000\u0b5f\u0b60\u0005\u0003\u0000\u0000\u0b60"+
		"\u0b8a\u0001\u0000\u0000\u0000\u0b61\u0b62\u0005\u0102\u0000\u0000\u0b62"+
		"\u0b64\u0005\u0002\u0000\u0000\u0b63\u0b65\u0007(\u0000\u0000\u0b64\u0b63"+
		"\u0001\u0000\u0000\u0000\u0b64\u0b65\u0001\u0000\u0000\u0000\u0b65\u0b67"+
		"\u0001\u0000\u0000\u0000\u0b66\u0b68\u0003\u00d8l\u0000\u0b67\u0b66\u0001"+
		"\u0000\u0000\u0000\u0b67\u0b68\u0001\u0000\u0000\u0000\u0b68\u0b69\u0001"+
		"\u0000\u0000\u0000\u0b69\u0b6a\u0005f\u0000\u0000\u0b6a\u0b6b\u0003\u00d8"+
		"l\u0000\u0b6b\u0b6c\u0005\u0003\u0000\u0000\u0b6c\u0b8a\u0001\u0000\u0000"+
		"\u0000\u0b6d\u0b6e\u0005\u00ad\u0000\u0000\u0b6e\u0b6f\u0005\u0002\u0000"+
		"\u0000\u0b6f\u0b70\u0003\u00d8l\u0000\u0b70\u0b71\u0005\u00b6\u0000\u0000"+
		"\u0b71\u0b72\u0003\u00d8l\u0000\u0b72\u0b73\u0005f\u0000\u0000\u0b73\u0b76"+
		"\u0003\u00d8l\u0000\u0b74\u0b75\u0005b\u0000\u0000\u0b75\u0b77\u0003\u00d8"+
		"l\u0000\u0b76\u0b74\u0001\u0000\u0000\u0000\u0b76\u0b77\u0001\u0000\u0000"+
		"\u0000\u0b77\u0b78\u0001\u0000\u0000\u0000\u0b78\u0b79\u0005\u0003\u0000"+
		"\u0000\u0b79\u0b8a\u0001\u0000\u0000\u0000\u0b7a\u0b7b\u0007)\u0000\u0000"+
		"\u0b7b\u0b7c\u0005\u0002\u0000\u0000\u0b7c\u0b7d\u0003\u00d8l\u0000\u0b7d"+
		"\u0b7e\u0005\u0003\u0000\u0000\u0b7e\u0b7f\u0005\u011c\u0000\u0000\u0b7f"+
		"\u0b80\u0005l\u0000\u0000\u0b80\u0b81\u0005\u0002\u0000\u0000\u0b81\u0b82"+
		"\u0005\u00a7\u0000\u0000\u0b82\u0b83\u0005\u001a\u0000\u0000\u0b83\u0b84"+
		"\u0003Z-\u0000\u0b84\u0b87\u0005\u0003\u0000\u0000\u0b85\u0b86\u0005\u00ab"+
		"\u0000\u0000\u0b86\u0b88\u0003\u010a\u0085\u0000\u0b87\u0b85\u0001\u0000"+
		"\u0000\u0000\u0b87\u0b88\u0001\u0000\u0000\u0000\u0b88\u0b8a\u0001\u0000"+
		"\u0000\u0000\u0b89\u0aac\u0001\u0000\u0000\u0000\u0b89\u0aae\u0001\u0000"+
		"\u0000\u0000\u0b89\u0ab7\u0001\u0000\u0000\u0000\u0b89\u0ac0\u0001\u0000"+
		"\u0000\u0000\u0b89\u0acc\u0001\u0000\u0000\u0000\u0b89\u0ad9\u0001\u0000"+
		"\u0000\u0000\u0b89\u0ae0\u0001\u0000\u0000\u0000\u0b89\u0aed\u0001\u0000"+
		"\u0000\u0000\u0b89\u0af6\u0001\u0000\u0000\u0000\u0b89\u0aff\u0001\u0000"+
		"\u0000\u0000\u0b89\u0b06\u0001\u0000\u0000\u0000\u0b89\u0b07\u0001\u0000"+
		"\u0000\u0000\u0b89\u0b08\u0001\u0000\u0000\u0000\u0b89\u0b0c\u0001\u0000"+
		"\u0000\u0000\u0b89\u0b16\u0001\u0000\u0000\u0000\u0b89\u0b1a\u0001\u0000"+
		"\u0000\u0000\u0b89\u0b3a\u0001\u0000\u0000\u0000\u0b89\u0b3e\u0001\u0000"+
		"\u0000\u0000\u0b89\u0b4a\u0001\u0000\u0000\u0000\u0b89\u0b4b\u0001\u0000"+
		"\u0000\u0000\u0b89\u0b4f\u0001\u0000\u0000\u0000\u0b89\u0b56\u0001\u0000"+
		"\u0000\u0000\u0b89\u0b61\u0001\u0000\u0000\u0000\u0b89\u0b6d\u0001\u0000"+
		"\u0000\u0000\u0b89\u0b7a\u0001\u0000\u0000\u0000\u0b8a\u0b95\u0001\u0000"+
		"\u0000\u0000\u0b8b\u0b8c\n\t\u0000\u0000\u0b8c\u0b8d\u0005\u0006\u0000"+
		"\u0000\u0b8d\u0b8e\u0003\u00d8l\u0000\u0b8e\u0b8f\u0005\u0007\u0000\u0000"+
		"\u0b8f\u0b94\u0001\u0000\u0000\u0000\u0b90\u0b91\n\u0007\u0000\u0000\u0b91"+
		"\u0b92\u0005\u0005\u0000\u0000\u0b92\u0b94\u0003\u011a\u008d\u0000\u0b93"+
		"\u0b8b\u0001\u0000\u0000\u0000\u0b93\u0b90\u0001\u0000\u0000\u0000\u0b94"+
		"\u0b97\u0001\u0000\u0000\u0000\u0b95\u0b93\u0001\u0000\u0000\u0000\u0b95"+
		"\u0b96\u0001\u0000\u0000\u0000\u0b96\u00dd\u0001\u0000\u0000\u0000\u0b97"+
		"\u0b95\u0001\u0000\u0000\u0000\u0b98\u0ba5\u0005\u009f\u0000\u0000\u0b99"+
		"\u0ba5\u0003\u00e8t\u0000\u0b9a\u0b9b\u0003\u011a\u008d\u0000\u0b9b\u0b9c"+
		"\u0005\u0135\u0000\u0000\u0b9c\u0ba5\u0001\u0000\u0000\u0000\u0b9d\u0ba5"+
		"\u0003\u0120\u0090\u0000\u0b9e\u0ba5\u0003\u00e6s\u0000\u0b9f\u0ba1\u0005"+
		"\u0135\u0000\u0000\u0ba0\u0b9f\u0001\u0000\u0000\u0000\u0ba1\u0ba2\u0001"+
		"\u0000\u0000\u0000\u0ba2\u0ba0\u0001\u0000\u0000\u0000\u0ba2\u0ba3\u0001"+
		"\u0000\u0000\u0000\u0ba3\u0ba5\u0001\u0000\u0000\u0000\u0ba4\u0b98\u0001"+
		"\u0000\u0000\u0000\u0ba4\u0b99\u0001\u0000\u0000\u0000\u0ba4\u0b9a\u0001"+
		"\u0000\u0000\u0000\u0ba4\u0b9d\u0001\u0000\u0000\u0000\u0ba4\u0b9e\u0001"+
		"\u0000\u0000\u0000\u0ba4\u0ba0\u0001\u0000\u0000\u0000\u0ba5\u00df\u0001"+
		"\u0000\u0000\u0000\u0ba6\u0ba7\u0007*\u0000\u0000\u0ba7\u00e1\u0001\u0000"+
		"\u0000\u0000\u0ba8\u0ba9\u0007+\u0000\u0000\u0ba9\u00e3\u0001\u0000\u0000"+
		"\u0000\u0baa\u0bab\u0007,\u0000\u0000\u0bab\u00e5\u0001\u0000\u0000\u0000"+
		"\u0bac\u0bad\u0007-\u0000\u0000\u0bad\u00e7\u0001\u0000\u0000\u0000\u0bae"+
		"\u0bb1\u0005{\u0000\u0000\u0baf\u0bb2\u0003\u00eau\u0000\u0bb0\u0bb2\u0003"+
		"\u00eew\u0000\u0bb1\u0baf\u0001\u0000\u0000\u0000\u0bb1\u0bb0\u0001\u0000"+
		"\u0000\u0000\u0bb1\u0bb2\u0001\u0000\u0000\u0000\u0bb2\u00e9\u0001\u0000"+
		"\u0000\u0000\u0bb3\u0bb5\u0003\u00ecv\u0000\u0bb4\u0bb6\u0003\u00f0x\u0000"+
		"\u0bb5\u0bb4\u0001\u0000\u0000\u0000\u0bb5\u0bb6\u0001\u0000\u0000\u0000"+
		"\u0bb6\u00eb\u0001\u0000\u0000\u0000\u0bb7\u0bb8\u0003\u00f2y\u0000\u0bb8"+
		"\u0bb9\u0003\u011a\u008d\u0000\u0bb9\u0bbb\u0001\u0000\u0000\u0000\u0bba"+
		"\u0bb7\u0001\u0000\u0000\u0000\u0bbb\u0bbc\u0001\u0000\u0000\u0000\u0bbc"+
		"\u0bba\u0001\u0000\u0000\u0000\u0bbc\u0bbd\u0001\u0000\u0000\u0000\u0bbd"+
		"\u00ed\u0001\u0000\u0000\u0000\u0bbe\u0bc1\u0003\u00f0x\u0000\u0bbf\u0bc2"+
		"\u0003\u00ecv\u0000\u0bc0\u0bc2\u0003\u00f0x\u0000\u0bc1\u0bbf\u0001\u0000"+
		"\u0000\u0000\u0bc1\u0bc0\u0001\u0000\u0000\u0000\u0bc1\u0bc2\u0001\u0000"+
		"\u0000\u0000\u0bc2\u00ef\u0001\u0000\u0000\u0000\u0bc3\u0bc4\u0003\u00f2"+
		"y\u0000\u0bc4\u0bc5\u0003\u011a\u008d\u0000\u0bc5\u0bc6\u0005\u00fc\u0000"+
		"\u0000\u0bc6\u0bc7\u0003\u011a\u008d\u0000\u0bc7\u00f1\u0001\u0000\u0000"+
		"\u0000\u0bc8\u0bca\u0007.\u0000\u0000\u0bc9\u0bc8\u0001\u0000\u0000\u0000"+
		"\u0bc9\u0bca\u0001\u0000\u0000\u0000\u0bca\u0bcb\u0001\u0000\u0000\u0000"+
		"\u0bcb\u0bcc\u0007/\u0000\u0000\u0bcc\u00f3\u0001\u0000\u0000\u0000\u0bcd"+
		"\u0bd1\u0005`\u0000\u0000\u0bce\u0bcf\u0005\t\u0000\u0000\u0bcf\u0bd1"+
		"\u0003\u0116\u008b\u0000\u0bd0\u0bcd\u0001\u0000\u0000\u0000\u0bd0\u0bce"+
		"\u0001\u0000\u0000\u0000\u0bd1\u00f5\u0001\u0000\u0000\u0000\u0bd2\u0bd3"+
		"\u0005\u0011\u0000\u0000\u0bd3\u0bd4\u0005\u0123\u0000\u0000\u0bd4\u0bd5"+
		"\u0003\u00f6{\u0000\u0bd5\u0bd6\u0005\u0125\u0000\u0000\u0bd6\u0c01\u0001"+
		"\u0000\u0000\u0000\u0bd7\u0bd8\u0005\u0092\u0000\u0000\u0bd8\u0bd9\u0005"+
		"\u0123\u0000\u0000\u0bd9\u0bda\u0003\u00f6{\u0000\u0bda\u0bdb\u0005\u0004"+
		"\u0000\u0000\u0bdb\u0bdc\u0003\u00f6{\u0000\u0bdc\u0bdd\u0005\u0125\u0000"+
		"\u0000\u0bdd\u0c01\u0001\u0000\u0000\u0000\u0bde\u0be5\u0005\u00eb\u0000"+
		"\u0000\u0bdf\u0be1\u0005\u0123\u0000\u0000\u0be0\u0be2\u0003\u0100\u0080"+
		"\u0000\u0be1\u0be0\u0001\u0000\u0000\u0000\u0be1\u0be2\u0001\u0000\u0000"+
		"\u0000\u0be2\u0be3\u0001\u0000\u0000\u0000\u0be3\u0be6\u0005\u0125\u0000"+
		"\u0000\u0be4\u0be6\u0005\u0121\u0000\u0000\u0be5\u0bdf\u0001\u0000\u0000"+
		"\u0000\u0be5\u0be4\u0001\u0000\u0000\u0000\u0be6\u0c01\u0001\u0000\u0000"+
		"\u0000\u0be7\u0be8\u0005{\u0000\u0000\u0be8\u0beb\u00070\u0000\u0000\u0be9"+
		"\u0bea\u0005\u00fc\u0000\u0000\u0bea\u0bec\u0005\u0098\u0000\u0000\u0beb"+
		"\u0be9\u0001\u0000\u0000\u0000\u0beb\u0bec\u0001\u0000\u0000\u0000\u0bec"+
		"\u0c01\u0001\u0000\u0000\u0000\u0bed\u0bee\u0005{\u0000\u0000\u0bee\u0bf1"+
		"\u00071\u0000\u0000\u0bef\u0bf0\u0005\u00fc\u0000\u0000\u0bf0\u0bf2\u0007"+
		"2\u0000\u0000\u0bf1\u0bef\u0001\u0000\u0000\u0000\u0bf1\u0bf2\u0001\u0000"+
		"\u0000\u0000\u0bf2\u0c01\u0001\u0000\u0000\u0000\u0bf3\u0bfe\u0003\u011a"+
		"\u008d\u0000\u0bf4\u0bf5\u0005\u0002\u0000\u0000\u0bf5\u0bfa\u0005\u0139"+
		"\u0000\u0000\u0bf6\u0bf7\u0005\u0004\u0000\u0000\u0bf7\u0bf9\u0005\u0139"+
		"\u0000\u0000\u0bf8\u0bf6\u0001\u0000\u0000\u0000\u0bf9\u0bfc\u0001\u0000"+
		"\u0000\u0000\u0bfa\u0bf8\u0001\u0000\u0000\u0000\u0bfa\u0bfb\u0001\u0000"+
		"\u0000\u0000\u0bfb\u0bfd\u0001\u0000\u0000\u0000\u0bfc\u0bfa\u0001\u0000"+
		"\u0000\u0000\u0bfd\u0bff\u0005\u0003\u0000\u0000\u0bfe\u0bf4\u0001\u0000"+
		"\u0000\u0000\u0bfe\u0bff\u0001\u0000\u0000\u0000\u0bff\u0c01\u0001\u0000"+
		"\u0000\u0000\u0c00\u0bd2\u0001\u0000\u0000\u0000\u0c00\u0bd7\u0001\u0000"+
		"\u0000\u0000\u0c00\u0bde\u0001\u0000\u0000\u0000\u0c00\u0be7\u0001\u0000"+
		"\u0000\u0000\u0c00\u0bed\u0001\u0000\u0000\u0000\u0c00\u0bf3\u0001\u0000"+
		"\u0000\u0000\u0c01\u00f7\u0001\u0000\u0000\u0000\u0c02\u0c07\u0003\u00fa"+
		"}\u0000\u0c03\u0c04\u0005\u0004\u0000\u0000\u0c04\u0c06\u0003\u00fa}\u0000"+
		"\u0c05\u0c03\u0001\u0000\u0000\u0000\u0c06\u0c09\u0001\u0000\u0000\u0000"+
		"\u0c07\u0c05\u0001\u0000\u0000\u0000\u0c07\u0c08\u0001\u0000\u0000\u0000"+
		"\u0c08\u00f9\u0001\u0000\u0000\u0000\u0c09\u0c07\u0001\u0000\u0000\u0000"+
		"\u0c0a\u0c0b\u0003\u00ba]\u0000\u0c0b\u0c0e\u0003\u00f6{\u0000\u0c0c\u0c0d"+
		"\u0005\u009e\u0000\u0000\u0c0d\u0c0f\u0005\u009f\u0000\u0000\u0c0e\u0c0c"+
		"\u0001\u0000\u0000\u0000\u0c0e\u0c0f\u0001\u0000\u0000\u0000\u0c0f\u0c11"+
		"\u0001\u0000\u0000\u0000\u0c10\u0c12\u0003 \u0010\u0000\u0c11\u0c10\u0001"+
		"\u0000\u0000\u0000\u0c11\u0c12\u0001\u0000\u0000\u0000\u0c12\u0c14\u0001"+
		"\u0000\u0000\u0000\u0c13\u0c15\u0003\u00f4z\u0000\u0c14\u0c13\u0001\u0000"+
		"\u0000\u0000\u0c14\u0c15\u0001\u0000\u0000\u0000\u0c15\u00fb\u0001\u0000"+
		"\u0000\u0000\u0c16\u0c1b\u0003\u00fe\u007f\u0000\u0c17\u0c18\u0005\u0004"+
		"\u0000\u0000\u0c18\u0c1a\u0003\u00fe\u007f\u0000\u0c19\u0c17\u0001\u0000"+
		"\u0000\u0000\u0c1a\u0c1d\u0001\u0000\u0000\u0000\u0c1b\u0c19\u0001\u0000"+
		"\u0000\u0000\u0c1b\u0c1c\u0001\u0000\u0000\u0000\u0c1c\u00fd\u0001\u0000"+
		"\u0000\u0000\u0c1d\u0c1b\u0001\u0000\u0000\u0000\u0c1e\u0c1f\u0003\u0116"+
		"\u008b\u0000\u0c1f\u0c22\u0003\u00f6{\u0000\u0c20\u0c21\u0005\u009e\u0000"+
		"\u0000\u0c21\u0c23\u0005\u009f\u0000\u0000\u0c22\u0c20\u0001\u0000\u0000"+
		"\u0000\u0c22\u0c23\u0001\u0000\u0000\u0000\u0c23\u0c25\u0001\u0000\u0000"+
		"\u0000\u0c24\u0c26\u0003 \u0010\u0000\u0c25\u0c24\u0001\u0000\u0000\u0000"+
		"\u0c25\u0c26\u0001\u0000\u0000\u0000\u0c26\u00ff\u0001\u0000\u0000\u0000"+
		"\u0c27\u0c2c\u0003\u0102\u0081\u0000\u0c28\u0c29\u0005\u0004\u0000\u0000"+
		"\u0c29\u0c2b\u0003\u0102\u0081\u0000\u0c2a\u0c28\u0001\u0000\u0000\u0000"+
		"\u0c2b\u0c2e\u0001\u0000\u0000\u0000\u0c2c\u0c2a\u0001\u0000\u0000\u0000"+
		"\u0c2c\u0c2d\u0001\u0000\u0000\u0000\u0c2d\u0101\u0001\u0000\u0000\u0000"+
		"\u0c2e\u0c2c\u0001\u0000\u0000\u0000\u0c2f\u0c31\u0003\u011a\u008d\u0000"+
		"\u0c30\u0c32\u0005\u0131\u0000\u0000\u0c31\u0c30\u0001\u0000\u0000\u0000"+
		"\u0c31\u0c32\u0001\u0000\u0000\u0000\u0c32\u0c33\u0001\u0000\u0000\u0000"+
		"\u0c33\u0c36\u0003\u00f6{\u0000\u0c34\u0c35\u0005\u009e\u0000\u0000\u0c35"+
		"\u0c37\u0005\u009f\u0000\u0000\u0c36\u0c34\u0001\u0000\u0000\u0000\u0c36"+
		"\u0c37\u0001\u0000\u0000\u0000\u0c37\u0c39\u0001\u0000\u0000\u0000\u0c38"+
		"\u0c3a\u0003 \u0010\u0000\u0c39\u0c38\u0001\u0000\u0000\u0000\u0c39\u0c3a"+
		"\u0001\u0000\u0000\u0000\u0c3a\u0103\u0001\u0000\u0000\u0000\u0c3b\u0c3c"+
		"\u0005\u0118\u0000\u0000\u0c3c\u0c3d\u0003\u00d0h\u0000\u0c3d\u0c3e\u0005"+
		"\u00f7\u0000\u0000\u0c3e\u0c3f\u0003\u00d0h\u0000\u0c3f\u0105\u0001\u0000"+
		"\u0000\u0000\u0c40\u0c41\u0005\u011a\u0000\u0000\u0c41\u0c46\u0003\u0108"+
		"\u0084\u0000\u0c42\u0c43\u0005\u0004\u0000\u0000\u0c43\u0c45\u0003\u0108"+
		"\u0084\u0000\u0c44\u0c42\u0001\u0000\u0000\u0000\u0c45\u0c48\u0001\u0000"+
		"\u0000\u0000\u0c46\u0c44\u0001\u0000\u0000\u0000\u0c46\u0c47\u0001\u0000"+
		"\u0000\u0000\u0c47\u0107\u0001\u0000\u0000\u0000\u0c48\u0c46\u0001\u0000"+
		"\u0000\u0000\u0c49\u0c4a\u0003\u0116\u008b\u0000\u0c4a\u0c4b\u0005\u0012"+
		"\u0000\u0000\u0c4b\u0c4c\u0003\u010a\u0085\u0000\u0c4c\u0109\u0001\u0000"+
		"\u0000\u0000\u0c4d\u0c7c\u0003\u0116\u008b\u0000\u0c4e\u0c4f\u0005\u0002"+
		"\u0000\u0000\u0c4f\u0c50\u0003\u0116\u008b\u0000\u0c50\u0c51\u0005\u0003"+
		"\u0000\u0000\u0c51\u0c7c\u0001\u0000\u0000\u0000\u0c52\u0c75\u0005\u0002"+
		"\u0000\u0000\u0c53\u0c54\u0005$\u0000\u0000\u0c54\u0c55\u0005\u001a\u0000"+
		"\u0000\u0c55\u0c5a\u0003\u00d0h\u0000\u0c56\u0c57\u0005\u0004\u0000\u0000"+
		"\u0c57\u0c59\u0003\u00d0h\u0000\u0c58\u0c56\u0001\u0000\u0000\u0000\u0c59"+
		"\u0c5c\u0001\u0000\u0000\u0000\u0c5a\u0c58\u0001\u0000\u0000\u0000\u0c5a"+
		"\u0c5b\u0001\u0000\u0000\u0000\u0c5b\u0c76\u0001\u0000\u0000\u0000\u0c5c"+
		"\u0c5a\u0001\u0000\u0000\u0000\u0c5d\u0c5e\u00073\u0000\u0000\u0c5e\u0c5f"+
		"\u0005\u001a\u0000\u0000\u0c5f\u0c64\u0003\u00d0h\u0000\u0c60\u0c61\u0005"+
		"\u0004\u0000\u0000\u0c61\u0c63\u0003\u00d0h\u0000\u0c62\u0c60\u0001\u0000"+
		"\u0000\u0000\u0c63\u0c66\u0001\u0000\u0000\u0000\u0c64\u0c62\u0001\u0000"+
		"\u0000\u0000\u0c64\u0c65\u0001\u0000\u0000\u0000\u0c65\u0c68\u0001\u0000"+
		"\u0000\u0000\u0c66\u0c64\u0001\u0000\u0000\u0000\u0c67\u0c5d\u0001\u0000"+
		"\u0000\u0000\u0c67\u0c68\u0001\u0000\u0000\u0000\u0c68\u0c73\u0001\u0000"+
		"\u0000\u0000\u0c69\u0c6a\u00074\u0000\u0000\u0c6a\u0c6b\u0005\u001a\u0000"+
		"\u0000\u0c6b\u0c70\u0003Z-\u0000\u0c6c\u0c6d\u0005\u0004\u0000\u0000\u0c6d"+
		"\u0c6f\u0003Z-\u0000\u0c6e\u0c6c\u0001\u0000\u0000\u0000\u0c6f\u0c72\u0001"+
		"\u0000\u0000\u0000\u0c70\u0c6e\u0001\u0000\u0000\u0000\u0c70\u0c71\u0001"+
		"\u0000\u0000\u0000\u0c71\u0c74\u0001\u0000\u0000\u0000\u0c72\u0c70\u0001"+
		"\u0000\u0000\u0000\u0c73\u0c69\u0001\u0000\u0000\u0000\u0c73\u0c74\u0001"+
		"\u0000\u0000\u0000\u0c74\u0c76\u0001\u0000\u0000\u0000\u0c75\u0c53\u0001"+
		"\u0000\u0000\u0000\u0c75\u0c67\u0001\u0000\u0000\u0000\u0c76\u0c78\u0001"+
		"\u0000\u0000\u0000\u0c77\u0c79\u0003\u010c\u0086\u0000\u0c78\u0c77\u0001"+
		"\u0000\u0000\u0000\u0c78\u0c79\u0001\u0000\u0000\u0000\u0c79\u0c7a\u0001"+
		"\u0000\u0000\u0000\u0c7a\u0c7c\u0005\u0003\u0000\u0000\u0c7b\u0c4d\u0001"+
		"\u0000\u0000\u0000\u0c7b\u0c4e\u0001\u0000\u0000\u0000\u0c7b\u0c52\u0001"+
		"\u0000\u0000\u0000\u0c7c\u010b\u0001\u0000\u0000\u0000\u0c7d\u0c7e\u0005"+
		"\u00bf\u0000\u0000\u0c7e\u0c8e\u0003\u010e\u0087\u0000\u0c7f\u0c80\u0005"+
		"\u00d5\u0000\u0000\u0c80\u0c8e\u0003\u010e\u0087\u0000\u0c81\u0c82\u0005"+
		"\u00bf\u0000\u0000\u0c82\u0c83\u0005\u0016\u0000\u0000\u0c83\u0c84\u0003"+
		"\u010e\u0087\u0000\u0c84\u0c85\u0005\r\u0000\u0000\u0c85\u0c86\u0003\u010e"+
		"\u0087\u0000\u0c86\u0c8e\u0001\u0000\u0000\u0000\u0c87\u0c88\u0005\u00d5"+
		"\u0000\u0000\u0c88\u0c89\u0005\u0016\u0000\u0000\u0c89\u0c8a\u0003\u010e"+
		"\u0087\u0000\u0c8a\u0c8b\u0005\r\u0000\u0000\u0c8b\u0c8c\u0003\u010e\u0087"+
		"\u0000\u0c8c\u0c8e\u0001\u0000\u0000\u0000\u0c8d\u0c7d\u0001\u0000\u0000"+
		"\u0000\u0c8d\u0c7f\u0001\u0000\u0000\u0000\u0c8d\u0c81\u0001\u0000\u0000"+
		"\u0000\u0c8d\u0c87\u0001\u0000\u0000\u0000\u0c8e\u010d\u0001\u0000\u0000"+
		"\u0000\u0c8f\u0c90\u0005\u0108\u0000\u0000\u0c90\u0c97\u00075\u0000\u0000"+
		"\u0c91\u0c92\u00056\u0000\u0000\u0c92\u0c97\u0005\u00d4\u0000\u0000\u0c93"+
		"\u0c94\u0003\u00d0h\u0000\u0c94\u0c95\u00075\u0000\u0000\u0c95\u0c97\u0001"+
		"\u0000\u0000\u0000\u0c96\u0c8f\u0001\u0000\u0000\u0000\u0c96\u0c91\u0001"+
		"\u0000\u0000\u0000\u0c96\u0c93\u0001\u0000\u0000\u0000\u0c97\u010f\u0001"+
		"\u0000\u0000\u0000\u0c98\u0c9d\u0003\u0114\u008a\u0000\u0c99\u0c9a\u0005"+
		"\u0004\u0000\u0000\u0c9a\u0c9c\u0003\u0114\u008a\u0000\u0c9b\u0c99\u0001"+
		"\u0000\u0000\u0000\u0c9c\u0c9f\u0001\u0000\u0000\u0000\u0c9d\u0c9b\u0001"+
		"\u0000\u0000\u0000\u0c9d\u0c9e\u0001\u0000\u0000\u0000\u0c9e\u0111\u0001"+
		"\u0000\u0000\u0000\u0c9f\u0c9d\u0001\u0000\u0000\u0000\u0ca0\u0ca5\u0003"+
		"\u0114\u008a\u0000\u0ca1\u0ca5\u0005^\u0000\u0000\u0ca2\u0ca5\u0005\u0085"+
		"\u0000\u0000\u0ca3\u0ca5\u0005\u00ce\u0000\u0000\u0ca4\u0ca0\u0001\u0000"+
		"\u0000\u0000\u0ca4\u0ca1\u0001\u0000\u0000\u0000\u0ca4\u0ca2\u0001\u0000"+
		"\u0000\u0000\u0ca4\u0ca3\u0001\u0000\u0000\u0000\u0ca5\u0113\u0001\u0000"+
		"\u0000\u0000\u0ca6\u0cab\u0003\u011a\u008d\u0000\u0ca7\u0ca8\u0005\u0005"+
		"\u0000\u0000\u0ca8\u0caa\u0003\u011a\u008d\u0000\u0ca9\u0ca7\u0001\u0000"+
		"\u0000\u0000\u0caa\u0cad\u0001\u0000\u0000\u0000\u0cab\u0ca9\u0001\u0000"+
		"\u0000\u0000\u0cab\u0cac\u0001\u0000\u0000\u0000\u0cac\u0115\u0001\u0000"+
		"\u0000\u0000\u0cad\u0cab\u0001\u0000\u0000\u0000\u0cae\u0caf\u0003\u011a"+
		"\u008d\u0000\u0caf\u0cb0\u0003\u0118\u008c\u0000\u0cb0\u0117\u0001\u0000"+
		"\u0000\u0000\u0cb1\u0cb2\u0005\u0128\u0000\u0000\u0cb2\u0cb4\u0003\u011a"+
		"\u008d\u0000\u0cb3\u0cb1\u0001\u0000\u0000\u0000\u0cb4\u0cb5\u0001\u0000"+
		"\u0000\u0000\u0cb5\u0cb3\u0001\u0000\u0000\u0000\u0cb5\u0cb6\u0001\u0000"+
		"\u0000\u0000\u0cb6\u0cb9\u0001\u0000\u0000\u0000\u0cb7\u0cb9\u0001\u0000"+
		"\u0000\u0000\u0cb8\u0cb3\u0001\u0000\u0000\u0000\u0cb8\u0cb7\u0001\u0000"+
		"\u0000\u0000\u0cb9\u0119\u0001\u0000\u0000\u0000\u0cba\u0cbe\u0003\u011c"+
		"\u008e\u0000\u0cbb\u0cbc\u0004\u008d\u0010\u0000\u0cbc\u0cbe\u0003\u0126"+
		"\u0093\u0000\u0cbd\u0cba\u0001\u0000\u0000\u0000\u0cbd\u0cbb\u0001\u0000"+
		"\u0000\u0000\u0cbe\u011b\u0001\u0000\u0000\u0000\u0cbf\u0cc6\u0005\u013f"+
		"\u0000\u0000\u0cc0\u0cc6\u0003\u011e\u008f\u0000\u0cc1\u0cc2\u0004\u008e"+
		"\u0011\u0000\u0cc2\u0cc6\u0003\u0124\u0092\u0000\u0cc3\u0cc4\u0004\u008e"+
		"\u0012\u0000\u0cc4\u0cc6\u0003\u0128\u0094\u0000\u0cc5\u0cbf\u0001\u0000"+
		"\u0000\u0000\u0cc5\u0cc0\u0001\u0000\u0000\u0000\u0cc5\u0cc1\u0001\u0000"+
		"\u0000\u0000\u0cc5\u0cc3\u0001\u0000\u0000\u0000\u0cc6\u011d\u0001\u0000"+
		"\u0000\u0000\u0cc7\u0cc8\u0005\u0140\u0000\u0000\u0cc8\u011f\u0001\u0000"+
		"\u0000\u0000\u0cc9\u0ccb\u0004\u0090\u0013\u0000\u0cca\u0ccc\u0005\u0128"+
		"\u0000\u0000\u0ccb\u0cca\u0001\u0000\u0000\u0000\u0ccb\u0ccc\u0001\u0000"+
		"\u0000\u0000\u0ccc\u0ccd\u0001\u0000\u0000\u0000\u0ccd\u0cf5\u0005\u013a"+
		"\u0000\u0000\u0cce\u0cd0\u0004\u0090\u0014\u0000\u0ccf\u0cd1\u0005\u0128"+
		"\u0000\u0000\u0cd0\u0ccf\u0001\u0000\u0000\u0000\u0cd0\u0cd1\u0001\u0000"+
		"\u0000\u0000\u0cd1\u0cd2\u0001\u0000\u0000\u0000\u0cd2\u0cf5\u0005\u013b"+
		"\u0000\u0000\u0cd3\u0cd5\u0004\u0090\u0015\u0000\u0cd4\u0cd6\u0005\u0128"+
		"\u0000\u0000\u0cd5\u0cd4\u0001\u0000\u0000\u0000\u0cd5\u0cd6\u0001\u0000"+
		"\u0000\u0000\u0cd6\u0cd7\u0001\u0000\u0000\u0000\u0cd7\u0cf5\u00076\u0000"+
		"\u0000\u0cd8\u0cda\u0005\u0128\u0000\u0000\u0cd9\u0cd8\u0001\u0000\u0000"+
		"\u0000\u0cd9\u0cda\u0001\u0000\u0000\u0000\u0cda\u0cdb\u0001\u0000\u0000"+
		"\u0000\u0cdb\u0cf5\u0005\u0139\u0000\u0000\u0cdc\u0cde\u0005\u0128\u0000"+
		"\u0000\u0cdd\u0cdc\u0001\u0000\u0000\u0000\u0cdd\u0cde\u0001\u0000\u0000"+
		"\u0000\u0cde\u0cdf\u0001\u0000\u0000\u0000\u0cdf\u0cf5\u0005\u0136\u0000"+
		"\u0000\u0ce0\u0ce2\u0005\u0128\u0000\u0000\u0ce1\u0ce0\u0001\u0000\u0000"+
		"\u0000\u0ce1\u0ce2\u0001\u0000\u0000\u0000\u0ce2\u0ce3\u0001\u0000\u0000"+
		"\u0000\u0ce3\u0cf5\u0005\u0137\u0000\u0000\u0ce4\u0ce6\u0005\u0128\u0000"+
		"\u0000\u0ce5\u0ce4\u0001\u0000\u0000\u0000\u0ce5\u0ce6\u0001\u0000\u0000"+
		"\u0000\u0ce6\u0ce7\u0001\u0000\u0000\u0000\u0ce7\u0cf5\u0005\u0138\u0000"+
		"\u0000\u0ce8\u0cea\u0005\u0128\u0000\u0000\u0ce9\u0ce8\u0001\u0000\u0000"+
		"\u0000\u0ce9\u0cea\u0001\u0000\u0000\u0000\u0cea\u0ceb\u0001\u0000\u0000"+
		"\u0000\u0ceb\u0cf5\u0005\u013d\u0000\u0000\u0cec\u0cee\u0005\u0128\u0000"+
		"\u0000\u0ced\u0cec\u0001\u0000\u0000\u0000\u0ced\u0cee\u0001\u0000\u0000"+
		"\u0000\u0cee\u0cef\u0001\u0000\u0000\u0000\u0cef\u0cf5\u0005\u013c\u0000"+
		"\u0000\u0cf0\u0cf2\u0005\u0128\u0000\u0000\u0cf1\u0cf0\u0001\u0000\u0000"+
		"\u0000\u0cf1\u0cf2\u0001\u0000\u0000\u0000\u0cf2\u0cf3\u0001\u0000\u0000"+
		"\u0000\u0cf3\u0cf5\u0005\u013e\u0000\u0000\u0cf4\u0cc9\u0001\u0000\u0000"+
		"\u0000\u0cf4\u0cce\u0001\u0000\u0000\u0000\u0cf4\u0cd3\u0001\u0000\u0000"+
		"\u0000\u0cf4\u0cd9\u0001\u0000\u0000\u0000\u0cf4\u0cdd\u0001\u0000\u0000"+
		"\u0000\u0cf4\u0ce1\u0001\u0000\u0000\u0000\u0cf4\u0ce5\u0001\u0000\u0000"+
		"\u0000\u0cf4\u0ce9\u0001\u0000\u0000\u0000\u0cf4\u0ced\u0001\u0000\u0000"+
		"\u0000\u0cf4\u0cf1\u0001\u0000\u0000\u0000\u0cf5\u0121\u0001\u0000\u0000"+
		"\u0000\u0cf6\u0cf7\u0005\u0106\u0000\u0000\u0cf7\u0cfe\u0003\u00f6{\u0000"+
		"\u0cf8\u0cfe\u0003 \u0010\u0000\u0cf9\u0cfe\u0003\u00f4z\u0000\u0cfa\u0cfb"+
		"\u00077\u0000\u0000\u0cfb\u0cfc\u0005\u009e\u0000\u0000\u0cfc\u0cfe\u0005"+
		"\u009f\u0000\u0000\u0cfd\u0cf6\u0001\u0000\u0000\u0000\u0cfd\u0cf8\u0001"+
		"\u0000\u0000\u0000\u0cfd\u0cf9\u0001\u0000\u0000\u0000\u0cfd\u0cfa\u0001"+
		"\u0000\u0000\u0000\u0cfe\u0123\u0001\u0000\u0000\u0000\u0cff\u0d00\u0007"+
		"8\u0000\u0000\u0d00\u0125\u0001\u0000\u0000\u0000\u0d01\u0d02\u00079\u0000"+
		"\u0000\u0d02\u0127\u0001\u0000\u0000\u0000\u0d03\u0d04\u0007:\u0000\u0000"+
		"\u0d04\u0129\u0001\u0000\u0000\u0000\u01bb\u012e\u0147\u0154\u015b\u0163"+
		"\u0165\u0179\u017d\u0183\u0186\u0189\u0190\u0193\u0197\u019a\u01a1\u01ac"+
		"\u01ae\u01b6\u01b9\u01bd\u01c0\u01c6\u01d1\u01d7\u01dc\u01fe\u020b\u0224"+
		"\u022d\u0231\u0237\u023b\u0240\u0246\u0252\u025a\u0260\u026d\u0272\u0282"+
		"\u0289\u028d\u0293\u02a2\u02a6\u02ac\u02b2\u02b5\u02b8\u02be\u02c2\u02ca"+
		"\u02cc\u02d5\u02d8\u02e1\u02e6\u02ec\u02f3\u02f6\u02fc\u0307\u030a\u030e"+
		"\u0313\u0318\u031f\u0322\u0325\u032c\u0331\u033a\u0342\u0348\u034b\u034e"+
		"\u0354\u0358\u035d\u0360\u0364\u0366\u036e\u0376\u0379\u037e\u0384\u038a"+
		"\u038d\u0391\u0394\u0398\u03b4\u03b7\u03bb\u03c1\u03c4\u03c7\u03cd\u03d5"+
		"\u03da\u03e0\u03e6\u03ee\u03f5\u03fd\u040e\u041c\u041f\u0425\u042e\u0437"+
		"\u043f\u0444\u0449\u0450\u0456\u045b\u0463\u0466\u0472\u0476\u047d\u04f1"+
		"\u04f9\u0501\u050a\u0514\u0518\u051b\u0521\u0527\u0533\u053f\u0544\u054d"+
		"\u0555\u055c\u055e\u0561\u0566\u056a\u056f\u0572\u0577\u057c\u057f\u0584"+
		"\u0588\u058d\u058f\u0593\u059c\u05a4\u05af\u05b6\u05bf\u05c4\u05c7\u05dd"+
		"\u05df\u05e8\u05ef\u05f2\u05f9\u05fd\u0603\u060b\u0616\u0621\u0628\u062e"+
		"\u063a\u0644\u064c\u064f\u0658\u0660\u0664\u0670\u0676\u0679\u067c\u067f"+
		"\u0681\u068a\u068d\u0696\u0699\u06a2\u06a5\u06ae\u06b1\u06b4\u06b9\u06bb"+
		"\u06c7\u06ce\u06d5\u06d8\u06da\u06e6\u06ea\u06ee\u06f4\u06f8\u0700\u0704"+
		"\u0707\u070a\u070d\u0711\u0715\u071a\u071e\u0721\u0724\u0727\u072b\u0730"+
		"\u0734\u0737\u073a\u073d\u073f\u0745\u074c\u0751\u0754\u0757\u075b\u0765"+
		"\u0769\u076b\u076e\u0772\u0778\u077c\u0780\u0788\u0792\u079c\u07aa\u07ac"+
		"\u07bb\u07c0\u07c7\u07d7\u07dc\u07e9\u07ee\u07f6\u07fc\u0800\u0803\u080a"+
		"\u0810\u0819\u0823\u0832\u0837\u0839\u083d\u0846\u0853\u0858\u085c\u0864"+
		"\u0867\u086b\u0879\u0886\u088b\u088f\u0892\u0897\u08a0\u08a3\u08a8\u08af"+
		"\u08b2\u08b7\u08bd\u08c3\u08c7\u08cd\u08d1\u08d4\u08d9\u08dc\u08e1\u08e5"+
		"\u08e8\u08eb\u08f1\u08f6\u08fd\u0900\u0912\u0914\u0917\u0922\u092b\u0932"+
		"\u093a\u0941\u0945\u0948\u0950\u0958\u095e\u0966\u0972\u0975\u097b\u097f"+
		"\u0981\u098a\u0996\u0998\u099f\u09a6\u09ac\u09b2\u09b4\u09bb\u09c3\u09cb"+
		"\u09d1\u09d6\u09dd\u09e3\u09e7\u09e9\u09f0\u09f9\u0a00\u0a0a\u0a0f\u0a13"+
		"\u0a1c\u0a29\u0a2b\u0a33\u0a35\u0a39\u0a41\u0a4a\u0a50\u0a58\u0a5d\u0a69"+
		"\u0a6e\u0a71\u0a77\u0a7b\u0a80\u0a85\u0a8a\u0a90\u0aa5\u0aa7\u0ac4\u0ac8"+
		"\u0ad1\u0ad5\u0ae7\u0aea\u0af2\u0afb\u0b12\u0b1d\u0b24\u0b27\u0b30\u0b34"+
		"\u0b38\u0b44\u0b5d\u0b64\u0b67\u0b76\u0b87\u0b89\u0b93\u0b95\u0ba2\u0ba4"+
		"\u0bb1\u0bb5\u0bbc\u0bc1\u0bc9\u0bd0\u0be1\u0be5\u0beb\u0bf1\u0bfa\u0bfe"+
		"\u0c00\u0c07\u0c0e\u0c11\u0c14\u0c1b\u0c22\u0c25\u0c2c\u0c31\u0c36\u0c39"+
		"\u0c46\u0c5a\u0c64\u0c67\u0c70\u0c73\u0c75\u0c78\u0c7b\u0c8d\u0c96\u0c9d"+
		"\u0ca4\u0cab\u0cb5\u0cb8\u0cbd\u0cc5\u0ccb\u0cd0\u0cd5\u0cd9\u0cdd\u0ce1"+
		"\u0ce5\u0ce9\u0ced\u0cf1\u0cf4\u0cfd";
	public static final String _serializedATN = Utils.join(
		new String[] {
			_serializedATNSegment0,
			_serializedATNSegment1
		},
		""
	);
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}
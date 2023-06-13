<?php

/**
 * @author  Dmitriy Lukin <lukin.d87@gmail.com>
 */

namespace XrTools;

use \XrTools\Utils\DebugMessages;
use \PDOException;

/**
 * Adapter for \XrTools\DatabaseManager Interface (Proxy with caching)
 */
class DBMCached implements DatabaseManager
{
	/**
	 * [$databaseManager description]
	 * @var DatabaseManager
	 */
	protected $databaseManager;

	/**
	 * [$cacheManager description]
	 * @var CacheManager
	 */
	protected $cacheManager;

	/**
	 * [$isCollectingQueries description]
	 * @var boolean
	 */
	protected $isCollectingQueries = false;

	/**
	 * [$queryCollector description]
	 * @var array
	 */
	protected $queryCollector = [];
	
	/**
	 * Debugger
	 * @var DebugMessages
	 */
	protected $dbg;

	/**
	 * @var int
	 */
	protected $transactionLevel = 0;

	/**
	 * @var array
	 */
	protected $lastQueryFetch = [];
	/**
	 * @var string
	 */
	private $lastError = '';
	/**
	 * @var string
	 */
	private $lastErrorCode = '';

	/**
	 * DBMCached constructor.
	 * @param DatabaseManager $databaseManager
	 * @param CacheManager $cacheManager
	 * @param DebugMessages $dbg
	 * @param array $opt
	 */
	function __construct(
		DatabaseManager $databaseManager,
		CacheManager $cacheManager,
		DebugMessages $dbg,
		array $opt = []
	){
		// set instances
		$this->databaseManager = $databaseManager;
		$this->cacheManager = $cacheManager;
		$this->dbg = $dbg;

		// set options
		$this->setOptions($opt);		
	}

	/**
	 * @return array
	 */
	function getLastQueryFetch(): array
	{
		return $this->lastQueryFetch;
	}

	/**
	 * @return string
	 */
	function getLastError(): string
	{
		return $this->lastError;
	}

	/**
	 * @return string
	 */
	function getLastErrorCode(): string
	{
		return $this->lastErrorCode;
	}

	/**
	 * @return mixed
	 */
	public function getAffectedRows()
	{
		return $this->databaseManager->getAffectedRows();
	}

	/**
	 * @param string $query
	 * @param array|null $params
	 * @param array $opt
	 * @return bool
	 */
	protected function exec(string $query, array $params = null, array $opt = [])
	{
		$res = $this->query($query, $params, $opt);

		return ! empty($res['status']);
	}
	
	/**
	 * [query description]
	 *
	 * @param string     $query  [description]
	 * @param array|null $params [description]
	 * @param array      $opt    [description]
	 *
	 * @return null|array|mixed [description]
	 */
	public function query(string $query, array $params = null, array $opt = [])
	{
		$this->resetLastError();

		// prepare result array
		$result = [
			'status' => false,
			'message' => '',
			'affected' => null
		];

		// debug mode
		$debug = ! empty($opt['debug']);

		if ($debug) {
			$this->dbg->log($this->getQueryDebugInfo($query, $params), __METHOD__);
		}
		
		// Executing the request SQL
		try {
			// log query
			$this->collectQuery($query, $params);

			$status_or_insert_id = $this->databaseManager->query($query, $params, $opt);

			$result['insert_id'] = is_bool($status_or_insert_id) ? null : $status_or_insert_id;
			$result['affected'] = $this->databaseManager->getAffectedRows();
			$result['status'] = true;
			$result['status_or_insert_id'] = $status_or_insert_id;
		}
		catch(PDOException $e)
		{
			$this->setLastError($e);

			$result['message'] = $e->getMessage();
			$result['errcode'] = $e->getCode();

			if ($debug) {
				$this->dbg->log($result['message'], __METHOD__);
			}
		}

		return ! empty($opt['return'])
			? ($result[$opt['return']] ?? null)
			: $result;
	}
	
	/**
	 * [setOptions description]
	 * @param array $opt [description]
	 */
	protected function setOptions(array $opt)
	{
		// collect all queries to $queryCollector
		if(isset($opt['collect_queries'])){
			$this->isCollectingQueries = !empty($opt['collect_queries']);
		}
	}

	/**
	 * [setConnectionParams description]
	 * @param array $settings [description]
	 */
	public function setConnectionParams(array $settings)
	{
		$this->databaseManager->setConnectionParams($settings);
	}

	/**
	 * [collectQuery description]
	 * @param string     $query  [description]
	 * @param array|null $params [description]
	 */
	protected function collectQuery(string $query, array $params = null)
	{
		// don't collect
		if (! $this->isCollectingQueries){
			return;
		}

		// add to collection
		$this->queryCollector []= $this->getQueryDebugInfo($query, $params);
	}

	/**
	 * [getQueryCollection description]
	 * @return [type] [description]
	 */
	public function getQueryCollection()
	{
		if (! $this->isCollectingQueries) {
			return false;
		}

		return $this->queryCollector;
	}

	/**
	 * [getQueryDebugInfo description]
	 * @param  string     $query  [description]
	 * @param  array|null $params [description]
	 * @return [type]             [description]
	 */
	protected function getQueryDebugInfo(string $query, array $params = null)
	{
		// set message info
		$message = "Query:<br><br>\n\n<span class='query-sql'>{$query}</span>";

		if (! empty($params)) {
			$message .= "<br><br>\n\nBound values:<br>\n<pre>" . htmlspecialchars(print_r($params, true), ENT_QUOTES) . '</pre>';
		}
		
		return $message;
	}

	/**
	 * @param string $query
	 * @param array|null $params
	 * @param array $opt
	 * @return array|false
	 */
	public function fetchArray(string $query, array $params = null, array $opt = [])
	{
		$this->resetLastError();
		$this->setLastQueryFetch($query, $params, $opt);

		// debug mode
		$debug = ! empty($opt['debug']);

		if ($debug) {
			$this->dbg->log( $this->getQueryDebugInfo($query, $params), __METHOD__);
		}

		// error on empty query
		if (! $query) {
			return false;
		}

		// modes: separate multiple keys (0 = cache_prefix) or whole-list key (1 = cache_key)
		$cache_mode = null;

		// collect keys found in cache
		$found_in_cache = [];

		// try to get cached data first
		if (! empty($opt['cache']))
		{
			$cache_time = $opt['cache_time'] ?? null;

			// Caching each row separately
			//
			// Important! Query MUST END with "WHERE"
			// because this caching mode modifies query by appending missing keys to it
			if ($params && is_array($params) && ! empty($opt['cache_prefix']))
			{
				// set cache mode
				$cache_mode = 1;

				// default cache postfix given by column value (each row cache key is {prefix}_{id} by default)
				$cache_by_column = $opt['cache_bycol'] ?? 'id';
				
				// collect cache keys
				$mc_keys = [];
				
				// collect keys NOT found in cache
				$db_check = [];

				foreach ($params as $val)
				{
					// generate key name
					$key = $opt['cache_prefix'] . $val;

					// add to collection
					$mc_keys[$val] = $key;
				}

				// renew or get cached results
				$cached = empty($opt['renew_cache'])
					? $this->cacheManager->getMulti(array_values($mc_keys), true)
					: false;
				
				// something found in cache
				if ($cached)
				{
					// collect missing keys for retreiving from db
					foreach ($mc_keys as $val => $mc_key)
					{
						// found in cache
						if (isset($cached[$mc_key]) && $cached[$mc_key] !== false) {
							$found_in_cache[$val] = $cached[$mc_key];
						}
						// not found in cache
						else {
							$db_check []= $val;
						}
					}
					
					// omit db query and return cached result
					if (! $db_check)
					{
						if ($debug)
						{
							$this->dbg->log(
								'Cached results found. Skipping query... ['.implode(', ', array_values($mc_keys)).']',
								__METHOD__
							);
						}

						// indexing result by selected column
						if (! empty($opt['arr_index']))
						{
							// already indexed if selected columns are the same in both cases
							if ($opt['arr_index'] == $cache_by_column) {
								return $found_in_cache;
							}
							
							return $this->indexArrayByKey($found_in_cache, $opt['arr_index']);
						}

						return array_values($found_in_cache);
					}
				}
				// nothing found in cache, querying all keys from database
				else {
					$db_check = array_keys($mc_keys);
				}

				// 
				// setting column name for query
				// 				
				// select it manually if table must be specified, e.g. `content`.`id`
				// !Important! There is no filtering so it should not be used with raw user inputs
				// 
				// if not specified then cache postfix setting is used
				// 
				$query_column = $opt['cache_bycol_sql'] ?? '`' . $cache_by_column . '`';

				//
				// adjusting query and params
				//

				// replace params for db querying
				$params = $db_check;
				
				// adding missing keys to the query (that's why query MUST must end with "WHERE")
				$query .= ' ' . $query_column . ' IN (' . implode(',', array_fill(1, count($params), '?')) . ')';
				
				if(!empty($opt['cache_prefix_add_query'])){
					$query .= ' ' .$opt['cache_prefix_add_query'];
				}
			}
			
			// 
			// Caching whole list (all rows together) with one cache key
			// 
			elseif (! empty($opt['cache_key']) && is_string($opt['cache_key']))
			{
				// set cache mode
				$cache_mode = 2;

				// multiple page caching with versioning key
				if (! empty($opt['cache_version_key']))
				{
					// get list version
					$list_version = $this->cacheManager->get($opt['cache_version_key']);
					
					// generate new version if not found
					if (! $list_version)
					{
						$list_version = time();

						// save version to cache
						$this->cacheManager->set($opt['cache_version_key'], $list_version, $cache_time);
					}

					// append version to cache key name
					$opt['cache_key'] .= '_' . $list_version;
				}

				// renew or get result from cache
				$cached = empty($opt['renew_cache'])
					? $this->cacheManager->get($opt['cache_key'], true)
					: false;
				
				if ($cached !== false)
				{
					if ($debug)
					{
						$this->dbg->log(
							'Cached result found via key "' . $opt['cache_key'] . '". Skipping query...',
							__METHOD__
						);
					}

					// get result (may be indexed)
					return empty($opt['arr_index']) ? $cached : $this->indexArrayByKey($cached, $opt['arr_index']);
				}
			}
		}

		//
		// if cache not found or only part is present, query the database
		//
		try {
			// log query
			$this->collectQuery($query, $params);
			
			// get result
			$result = $this->databaseManager->fetchArray($query, $params);

			// cache helper array for collecting missing cache items to save
			if ($result && $cache_mode == 1)
			{
				// result can be grouped by selected column
				if (! empty($opt['cache_bycol_group']))
				{
					// group data
					$db_data = $this->groupArrayByKey($result, $cache_by_column, $opt['cache_bycol_group'], [
						'direct_value' => ! empty($opt['cache_bycol_group_value'])
					]);
					
					// result array needs to be replaced because cache data are also already grouped
					$result = $db_data;
				}
				// renaming keys for convinient array searching
				else {
					$db_data = $this->indexArrayByKey($result, $cache_by_column);
				}
			}
			else {
				$db_data = [];
			}

			// add found cached items to final result
			if (! empty($found_in_cache))
			{
				if ($debug) {
					$this->dbg->log('Loaded from cache: ' . "\n" . '<pre>' . print_r($found_in_cache, true) . '</pre>', __METHOD__);
				}

				foreach ($found_in_cache as $key => $val)
				{
					// group data
					if (empty($opt['cache_bycol_group'])) {
						$result []= $val;
					}
					// without grouping
					else {
						$result[ $key ] = $val;
					}
				}
			}

			// allow array indexing (confilcts with data grouping)
			if (! empty($opt['arr_index']) && empty($opt['cache_bycol_group'])) {
				$result = $this->indexArrayByKey($result, $opt['arr_index']);
			}

			// collect items for saving to cache		
			$to_cache = [];

			// collect missing cache items found in database
			if ($cache_mode == 1)
			{
				foreach ($db_check as $val)
				{
					// get cache key
					$mc_key = $mc_keys[ $val ];
					
					// keep empty entries in cache
					if (! isset($db_data[ $val ]) ) {
						$db_data[ $val ] = [];
					}
					// skip invalid items
					elseif($db_data[ $val ] === false){
						continue;
					}

					$to_cache[ $mc_key ] = $db_data[ $val ];
				}
			}
			// collect whole list to cache
			elseif ($cache_mode == 2 && $result !== false) {
				$to_cache[ $opt['cache_key'] ] = $result;
			}

			// if there is anything to save in cache
			if ($to_cache)
			{
				if ($debug) {
					$this->dbg->log('Saving in cache: ' . "\n" . '<pre>' . print_r($to_cache, true) . '</pre>', __METHOD__);
				}

				$this->cacheManager->setMulti($to_cache, $cache_time, true);
			}
		}
		catch (PDOException $e)
		{
			$this->setLastError($e);

			$result = false;

			if ($debug) {
				$this->dbg->log($e->getMessage(), __METHOD__);
			}
		}

		return $result;
	}

	/**
	 * Getting data and the number of all rows
	 * ps. DISTINCT() not yet provided
	 * @param string $query
	 * @param array|null $params
	 * @param array $opt
	 * @return mixed
	 */
	public function fetchArrayWithCount(string $query, array $params = null, array $opt = [])
	{
		$this->resetLastError();

		$cache = ! empty($opt['cache']) && ! empty($opt['cache_key']);
		$debug = ! empty($opt['debug']);

		if ($debug) {
			$this->dbg->log($this->getQueryDebugInfo($query, $params), __METHOD__);
		}

		if (! $query) {
			return false;
		}
		
		// get cache
		if ($cache)
		{
			$result = $this->cacheManager->get($opt['cache_key'], true);

			if ($result !== false)
			{
				if ($debug)
				{
					$this->dbg->log(
						'Cached result found via key "' . $opt['cache_key'] . '". Skipping query...' . "\n",
						__METHOD__
					);
				}

				return $result;
			}
		}
		
		try {
			// log query
			$this->collectQuery($query, $params);

			$result = $this->databaseManager->fetchArrayWithCount($query, $params);
		}
		catch(PDOException $e)
		{
			$this->setLastError($e);

			if ($debug) {
				$this->dbg->log($e->getMessage(), __METHOD__);
			}
			
			return false;
		}

		// set cache
		if ($cache)
		{
			if ($debug)
			{
				$this->dbg->log(
					'Saving cache via key "' . $opt['cache_key'] . '"'. "\nValue:\n" . '<pre>'.print_r($result, true).'</pre>',
					__METHOD__
				);
			}

			$this->cacheManager->set($opt['cache_key'], $result, $opt['cache_time'] ?? null, true);
		}
		
		return $result;
	}

	/**
	 * @param string $query
	 * @param array|null $params
	 * @param array $opt
	 * @return mixed
	 */
	public function fetchColumn(string $query, array $params = null, array $opt = [])
	{
		$this->resetLastError();
		$this->setLastQueryFetch($query, $params, $opt);

		// debug mode
		$debug = !empty($opt['debug']);

		// cache key is also needed for cache mode to enable
		$use_cache = !empty($opt['cache']) && !empty($opt['cache_key']);

		if ($debug) {
			$this->dbg->log($this->getQueryDebugInfo($query, $params), __METHOD__);
		}

		// error on empty query
		if (! $query) {
			return false;
		}
		
		if ($use_cache)
		{
			// renew or get cached result
			$cached = empty($opt['renew_cache'])
				? $this->cacheManager->get($opt['cache_key'])
				: false;
			
			// cache found
			if ($cached !== false)
			{
				if ($debug)
				{
					$this->dbg->log(
						'Cached result found via key "' . $opt['cache_key'] . '". Skipping query...' . "\n",
						__METHOD__
					);
				}
				
				return $cached;
			}
		}

		try {

			// log query
			$this->collectQuery($query, $params);

			// get result
			$result = $this->databaseManager->fetchColumn($query, $params);

			if ($use_cache)
			{
				$cache_time = $opt['cache_time'] ?? null;

				$this->cacheManager->set($opt['cache_key'], $result, $cache_time);

				if ($debug)
				{
					$this->dbg->log(
						'Saving cache via key "' . $opt['cache_key'] . '"'. "\nValue:\n" . '<pre>'.print_r($result, true).'</pre>',
						__METHOD__
					);
				}
			}
		}
		catch (PDOException $e)
		{
			$this->setLastError($e);

			$result = false;

			if ($debug) {
				$this->dbg->log($e->getMessage(), __METHOD__);
			}
		}

		return $result;
	}

	/**
	 * @param string $query
	 * @param array|null $params
	 * @param array $opt
	 * @return mixed
	 */
	public function fetchRow(string $query, array $params = null, array $opt = [])
	{
		$this->resetLastError();
		$this->setLastQueryFetch($query, $params, $opt);

		// debug mode
		$debug = ! empty($opt['debug']);

		// cache key is also needed for cache mode to enable
		$use_cache = ! empty($opt['cache']) && ! empty($opt['cache_key']);

		if ($debug) {
			$this->dbg->log( $this->getQueryDebugInfo($query, $params), __METHOD__);
		}

		// error on empty query
		if (! $query) {
			return false;
		}
		
		if ($use_cache)
		{
			// renew or get cached result
			$cached = empty($opt['renew_cache']) ? $this->cacheManager->get($opt['cache_key'], true) : false;
			
			// cache found
			if ($cached !== false)
			{
				if ($debug)
				{
					$this->dbg->log(
						'Cached result found via key "' . $opt['cache_key'] . '". Skipping query...' . "\n",
						__METHOD__
					);
				}
				
				return $cached;
			}
		}
		
		try {

			// log query
			$this->collectQuery($query, $params);

			// get result
			$result = $this->databaseManager->fetchRow($query, $params);

			if ($use_cache)
			{
				$cache_time = $opt['cache_time'] ?? null;

				$this->cacheManager->set($opt['cache_key'], $result, $cache_time, true);

				if ($debug)
				{
					$this->dbg->log(
						'Saving cache via key "' . $opt['cache_key'] . '"'. "\nValue:\n" . '<pre>'.print_r($result, true).'</pre>',
						__METHOD__
					);
				}
			}
		}
		catch (PDOException $e)
		{
			$this->setLastError($e);

			$result = false;

			if ($debug) {
				$this->dbg->log($e->getMessage(), __METHOD__);
			}
		}

		return $result;
	}

	/**
	 * @param bool $inheritCache
	 * @param array $opt
	 * @return mixed
	 */
	function getCalcFoundRows(bool $inheritCache = true, array $opt = [])
	{
		$debug = ! empty($opt['debug']);

		if (empty($this->lastQueryFetch))
		{
			if ($debug) {
				$this->dbg->log('No previous select query found', __METHOD__);
			}

			return false;
		}

		$lastOpt = $this->lastQueryFetch['opt'] ?? [];

		if ($inheritCache && isset($lastOpt['cache']) && ! empty($lastOpt['cache_key']))
		{
			$opt = array_merge($opt, [
				'cache'      => $lastOpt['cache'],
				'cache_key'  => $lastOpt['cache_key'].'_count',
				'cache_time' => $lastOpt['cache_time'] ?? 1200,
			]);
		}

		return $this->fetchColumn(
			$this->databaseManager->getExtractCountSQL($this->lastQueryFetch['sql']),
			$this->lastQueryFetch['params'],
			$opt
		);
	}

	/**
	 * @param string $sql
	 * @param array|null $params
	 * @param array $opt
	 */
	protected function setLastQueryFetch(string $sql, array $params = null, array $opt = [])
	{
		$this->lastQueryFetch = [
			'sql'    => $sql,
			'params' => $params,
			'opt'    => $opt,
		];
	}

	/**
	 * Последняя ошибка
	 * @param \Exception $exception
	 */
	protected function setLastError(\Exception $exception)
	{
		$this->lastError = (string) $exception->getMessage();
		$this->lastErrorCode = (string) $exception->getCode();
	}

	/**
	 * Сброс последней ошибки
	 */
	protected function resetLastError()
	{
		$this->lastError = '';
		$this->lastErrorCode = '';
	}

	/**
	 * arr_index()
	 * 
	 * Index array by selected key (index)
	 * @param  array  $arr    [description]
	 * @param  string $by_key [description]
	 * @return [type]         [description]
	 */
	protected function indexArrayByKey(array $arr, string $by_key)
	{
		// init result
		$result = [];

		foreach ($arr as $item)
		{
			if (! isset($item[ $by_key ])) {
				return $arr;
			}

			$result[ $item[ $by_key] ] = $item;
		}

		return $result;
	}

	/**
	 * array_groupby()
	 * 
	 * Group array by selected key (index)
	 * @param  array   $arr       Input array
	 * @param  string  $index     Selected key name to group array by
	 * @param  array   $selective Selective mode: filter result array by selected keys
	 * @param  array   $opt       Settings: 
	 *                             <ul>
	 *                             		<li> <strong> direct_value </strong> bool (false)
	 *                             		 - Saves direct value instead of array. 
	 *                             		 Works only in SELECTIVE mode with ONE SELECTED column! 
	 *                             </ul>
	 * @return array             grouped array
	 */
	protected function groupArrayByKey(array $arr, string $index, array $selective = [], $opt = [])
	{
		// 
		// example $arr:
		// [
		// 	 0 => ['id' => '100', 'name' => 'foo', 'catalog_id' => '1'],
		// 	 1 => ['id' => '101', 'name' => 'bar', 'catalog_id' => '1']
		// 	 2 => ['id' => '103', 'name' => 'baz', 'catalog_id' => '2']
		// ]
		// 
		// $index: 'catalog_id'
		// 
		// result:
		// [
		// 	 '1' => [
		// 	 	['id' => '100', 'name' => 'foo', 'catalog_id' => '1'],
		// 	 	['id' => '101', 'name' => 'bar', 'catalog_id' => '1']
		// 	 ],
		// 	 '2' => [
		// 	 	['id' => '103', 'name' => 'baz', 'catalog_id' => '2']
		// 	 ],
		// ]
		// 

		// init result
		$result = [];	

		// quick validate
		if (empty($arr)) {
			return $result;
		}

		// selective mode example: ['id']
		// result:
		// [
		// 	 '1' => [
		// 	 	['id' => '100'],
		// 	 	['id' => '101']
		// 	 ]
		// 	 '2' => [
		// 	 	['id' => '103']
		// 	 ]
		// ]
		$save_full_row = empty($selective);

		// selective mode: ['id'] 
		// with direct_value = true
		// result:
		// [
		// 	 '1' => ['100', '101'],
		// 	 '2' => ['103']
		// ]
		$direct_value = ! empty($opt['direct_value']);

		foreach ($arr as $row)
		{
			// validate existing key
			if (! isset($row[ $index ])) {
				break;
			}

			// full row mode
			if ($save_full_row) {
				$result[ $row[ $index ] ] []= $row;
			}
			// selective mode
			else
			{
				$tmp = [];
				
				foreach ($selective as $col)
				{
					if (! isset($row[ $col ])) {
						continue;
					}

					// break cycle on first found selected key
					if ($direct_value) {
						$tmp = $row[ $col ];
						break;
					}

					$tmp[ $col ] = $row[ $col ];
				}

				$result[ $row[ $index ] ] []= $tmp;
			}
		}

		return $result;
	}
	
	/**
	 * Insert / Update row(s) into table via params
	 * @param array  $data       Table data
	 * @param string $table_name Table name
	 * @param mixed  $index      Table update key (id or opt.index_key)
	 * @param array  $opt        Options
	 */
	public function set(array $data, string $table_name, $index = null, array $opt = [])
	{
		$query_data = $this->databaseManager->getInsertUpdateQuery($data, $table_name, $index, $opt);

		if (empty($query_data['query'])) {
			return ['status' => false, 'message' => 'Empty query!'];
		}

		// log query
		$this->collectQuery($query_data['query'], $query_data['params']);
		
		return $this->query(
			$query_data['query'],
			$query_data['params'],
			[
				'debug' => !empty($opt['debug']),
				'return' => $opt['return'] ?? null
			]
		);
	}

	/**
	 * Создание части sql запроса из массива
	 * @param array $data
	 * @param string $glue
	 * @return array
	 */
	public function genPartSQL(array $data = [], string $glue = ', '): array
	{
		return $this->databaseManager->genPartSQL($data, $glue);
	}

	/**
	 * @param bool $debug
	 * @return bool
	 */
	function start(bool $debug = false): bool
	{
		$this->resetLastError();

		if ($this->transactionLevel < 1) {
			$this->transactionLevel = 1;
		}

		$result = false;

		if ($this->transactionLevel == 1)
		{
			try {
				$result = $this->databaseManager->start();
			}
			catch (PDOException $e)
			{
				$this->setLastError($e);
				$this->dbg->log2($e->getMessage(), ['debug' => $debug]);
			}
		}
		else {
			$result = $this->exec('SAVEPOINT dbmc_'.$this->transactionLevel);
		}

		if (! $result) {
			return false;
		}

		$this->dbg->log2('Transaction Start ( '.$this->transactionLevel.' )', ['debug' => $debug]);

		$this->transactionLevel++;

		return true;
	}

	/**
	 * @param bool $debug
	 * @return bool
	 */
	function rollback(bool $debug = false): bool
	{
		$this->resetLastError();

		if ($this->transactionLevel < 1) {
			return false;
		}

		$level = $this->transactionLevel;
		$level--;

		$result = false;

		if ($level == 1)
		{
			try {
				$result = $this->databaseManager->rollback();
			}
			catch (PDOException $e)
			{
				$this->setLastError($e);
				$this->dbg->log2($e->getMessage(), ['debug' => $debug]);
			}
		}
		else {
			$result = $this->exec('ROLLBACK TO SAVEPOINT dbmc_'.$level);
		}

		if (! $result) {
			return false;
		}

		$this->dbg->log2('Transaction Rollback ( '.$level.' )', ['debug' => $debug]);

		$this->transactionLevel = $level;

		return true;
	}

	/**
	 * @param bool $debug
	 * @return bool
	 */
	function commit(bool $debug = false): bool
	{
		$this->resetLastError();

		if ($this->transactionLevel < 1) {
			return false;
		}

		$level = $this->transactionLevel;
		$level--;

		$result = false;

		if ($level == 1)
		{
			try {
				$result = $this->databaseManager->commit();
			}
			catch (PDOException $e)
			{
				$this->setLastError($e);
				$this->dbg->log2($e->getMessage(), ['debug' => $debug]);
			}
		}
		else {
			$result = $this->exec('RELEASE SAVEPOINT dbmc_'.$level);
		}

		if (! $result) {
			return false;
		}

		$this->dbg->log2('Transaction Commit ( '.$level.' )', ['debug' => $debug]);

		$this->transactionLevel = $level;

		return true;
	}
}


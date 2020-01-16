<?php
/**
 * @author  Dmitriy Lukin <lukin.d87@gmail.com>
 */

namespace XrTools;

/**
 * Adapter for \XrTools\DatabaseManager Interface (Proxy with caching)
 */
class DBMCached implements DatabaseManager {

	/**
	 * [$databaseManager description]
	 * @var DatabaseManager
	 */
	private $databaseManager;

	/**
	 * [$cacheManager description]
	 * @var CacheManager
	 */
	private $cacheManager;

	/**
	 * [$isCollectingQueries description]
	 * @var boolean
	 */
	private $isCollectingQueries = false;

	/**
	 * [$queryCollector description]
	 * @var array
	 */
	private $queryCollector = [];
	
	/**
	 * [$debugMessages description]
	 * @var array
	 */
	private $debugMessages = [];

	/**
	 * [__construct description]
	 * @param DatabaseManager $databaseManager [description]
	 * @param CacheManager    $cacheManager [description]
	 * @param array           $opt        [description]
	 */
	function __construct(DatabaseManager $databaseManager, CacheManager $cacheManager, array $opt = []){
		// set instances
		$this->databaseManager = $databaseManager;
		$this->cacheManager = $cacheManager;

		// set options
		$this->setOptions($opt);		
	}

	public function query(string $query, array $params = null, array $opt = []){

		// prepare result array
		$result = [
			'status' => false,
			'message' => __METHOD__ . ': ' . $query . '; ',
			'affected' => 0
		];
		
		// Executing the request SQL
		try{
			$result_query = $this->databaseManager->query($query, $params, $opt);

			$result['insert_id'] = is_bool($result_query) ? null : $result_query;
			$result['affected'] = $this->databaseManager->getAffectedRows();
			$result['status'] = true;
			$result['result_query'] = $result_query;
		}catch(\Exception $e){
			$result['message'] .= $e->getMessage();
			$result['errcode'] = $e->getCode();
		}

		// :TODO: 
		// mysql_do (regex пройти код и поправить все места где $params = false и переписывать на mysql_do($query, null, ['return' => 'result_query']))
		
		return !empty($opt['return']) ? ($result[$opt['return']] ?? null) : $result;

	}
	
	public function connect(array $settings){
		return $this->databaseManager->connect($settings);
	}

	public function setOptions(array $opt){
		// collect all queries to $queryCollector
		if(isset($opt['collect_queries'])){
			$this->isCollectingQueries = !empty($opt['collect_queries']);
		}
	}

	/**
	 * [debugMessage description]
	 * @param  string $str    [description]
	 * @param  string $method [description]
	 * @return [type]         [description]
	 */
	private function debugMessage(string $str, string $method){
		// fill message array
		$this->debugMessages[] = $method . ': ' . $str;
	}

	/**
	 * [clearDebugMessages description]
	 * @return [type] [description]
	 */
	private function clearDebugMessages(){
		$this->debugMessages = [];
	}

	/**
	 * [getDebugMessages description]
	 * @return [type] [description]
	 */
	public function getDebugMessages(){
		return $this->debugMessages;
	}

	/**
	 * [collectQuery description]
	 * @param string     $query  [description]
	 * @param array|null $params [description]
	 */
	private function collectQuery(string $query, array $params = null){
		// don't collect
		if(!$this->isCollectingQueries){
			return;
		}

		// add to collection
		$this->queryCollector[] = $this->getQueryDebugInfo($query, $params);
	}

	/**
	 * [getQueryCollection description]
	 * @return [type] [description]
	 */
	public function getQueryCollection(){
		return $this->queryCollector;
	}

	/**
	 * [getQueryDebugInfo description]
	 * @param  string     $query  [description]
	 * @param  array|null $params [description]
	 * @return [type]             [description]
	 */
	private function getQueryDebugInfo(string $query, array $params = null){
		// set message info
		$message = "Query:\n" . $query;

		if(isset($params)){
			$message .= "\nBound values:\n" . '<pre>' . print_r($params, true) . '</pre>';
		}
		
		return $message;
	}

	/**
	 * [fetchArray description]
	 * @param  string $query  [description]
	 * @param  array  $params [description]
	 * @param  array  $opt    [description]
	 * @return [type]         [description]
	 */
	public function fetchArray(string $query, array $params = null, array $opt = []){
		// error on empty query
		if(!$query){
			throw new \Exception('Empty query!');
		}

		// debug mode
		$debug = !empty($opt['debug']);

		if($debug){
			$this->clearDebugMessages();
			$this->debugMessage($this->getQueryDebugInfo($query, $params), __METHOD__);
		}

		// modes: separate multiple keys (0 = cache_prefix) or whole-list key (1 = cache_key)
		$cache_mode = null;

		// collect keys found in cache
		$found_in_cache = [];

		// try to get cached data first
		if(!empty($opt['cache'])){

			$cache_time = $opt['cache_time'] ?? null;
			
			//
			// Caching each row separately
			//
			// Important! Query MUST END with "WHERE"
			// because this caching mode modifies query by appending missing keys to it
			// 
			if($params && is_array($params) && !empty($opt['cache_prefix'])){

				// set cache mode
				$cache_mode = 1;

				// default cache postfix given by column value (each row cache key is {prefix}_{id} by default)
				$cache_by_column = $opt['cache_bycol'] ?? 'id';
				
				// collect cache keys
				$mc_keys = [];
				
				// collect keys NOT found in cache
				$db_check = [];

				foreach ($params as $val){
					// generate key name
					$key = $opt['cache_prefix'] . $val;

					// add to collection
					$mc_keys[$val] = $key;
				}

				// renew or get cached results
				$cached = empty($opt['renew_cache']) ? $this->cacheManager->getMulti(array_values($mc_keys), true) : false;
				
				// something found in cache
				if($cached){
					
					// collect missing keys for retreiving from db
					foreach ($mc_keys as $val => $mc_key){

						// found in cache
						if(isset($cached[$mc_key]) && $cached[$mc_key] !== false){
							$found_in_cache[$val] = $cached[$mc_key];
						}
						// not found in cache
						else {
							$db_check[] = $val;
						}
					}
					
					// omit db query and return cached result
					if(!$db_check){

						if($debug){
							$this->debugMessage(
								'Cached results found. Skipping query...',
								__METHOD__
							);
						}

						// indexing result by selected column
						if(!empty($opt['arr_index'])){

							// already indexed if selected columns are the same in both cases
							if($opt['arr_index'] == $cache_by_column){
								return $found_in_cache;
							}
							
							return $this->indexArrayByKey($found_in_cache, $opt['arr_index']);
						}

						return array_values($found_in_cache);
					}
				}
				// nothing found in cache, querying all keys from database
				else{
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
			}
			
			// 
			// Caching whole list (all rows together) with one cache key
			// 
			elseif(!empty($opt['cache_key']) && is_string($opt['cache_key'])){
				
				// set cache mode
				$cache_mode = 2;

				// multiple page caching with versioning key
				if(!empty($opt['cache_version_key'])){
					
					// get list version
					$list_version = $this->cacheManager->get($opt['cache_version_key']);
					
					// generate new version if not found
					if(!$list_version){
						$list_version = time();

						// save version to cache
						$this->cacheManager->set($opt['cache_version_key'], $list_version, $cache_time);
					}

					// append version to cache key name
					$opt['cache_key'] .= '_' . $list_version;
				}

				// renew or get result from cache
				$cached = empty($opt['renew_cache']) ? $this->cacheManager->get($opt['cache_key'], true) : false;
				
				if($cached !== false){
					
					if($debug){
						$this->debugMessage(
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
		$result = $this->databaseManager->fetchArray($query, $params);

		// log query
		$this->collectQuery($query, $params);
		

		// cache helper array for collecting missing cache items to save
		if($result && $cache_mode == 1){
			
			// result can be grouped by selected column
			if(!empty($opt['cache_bycol_group'])){
				
				// group data
				$db_data = $this->groupArrayByKey($result, $cache_by_column, $opt['cache_bycol_group'], [
					'direct_value' => !empty($opt['cache_bycol_group_value'])
				]);
				
				// result array needs to be replaced because cache data are also already grouped
				$result = $db_data;
			}
			// renaming keys for convinient array searching
			else {
				$db_data = $this->indexArrayByKey($result, $cache_by_column);
			}
		} else {
			$db_data = [];
		}

		// add found cached items to final result
		if(!empty($found_in_cache)){

			if($debug){
				$this->debugMessage('Loaded from cache: ' . "\n" . '<pre>' . print_r($found_in_cache, true) . '</pre>', __METHOD__);
			}

			foreach ($found_in_cache as $key => $val){
				// group data
				if(empty($opt['cache_bycol_group'])){
					$result[] = $val;
				}
				// without grouping
				else {
					$result[$key] = $val;
				}
			}
		}

		// allow array indexing (confilcts with data grouping)
		if(!empty($opt['arr_index']) && empty($opt['cache_bycol_group'])){
			$result = $this->indexArrayByKey($result, $opt['arr_index']);
		}

		// collect items for saving to cache		
		$to_cache = [];

		// collect missing cache items found in database
		if($cache_mode == 1){
			foreach ($db_check as $val){

				// get cache key
				$mc_key = $mc_keys[$val];
				
				// skip invalid items
				if(!isset($db_data[$val]) || $db_data[$val] === false){
					continue;
				}

				$to_cache[$mc_key] = $db_data[$val];
			}
		}
		// collect whole list to cache
		elseif($cache_mode == 2 && $result !== false){
			$to_cache[$opt['cache_key']] = $result;
		}

		// if there is anything to save in cache
		if($to_cache){
			if($debug){
				$this->debugMessage('Saving in cache: ' . "\n" . '<pre>' . print_r($to_cache, true) . '</pre>', __METHOD__);
			}

			$this->cacheManager->setMulti($to_cache, $cache_time, true);
		}
	
		return $result;
	}

	/**
	 * [fetchColumn description]
	 * @param  string     $query  [description]
	 * @param  array|null $params [description]
	 * @param  array      $opt    [description]
	 * @return [type]             [description]
	 */
	public function fetchColumn(string $query, array $params = null, array $opt = []){
		
		// debug mode
		$debug = !empty($opt['debug']);

		// cache key is also needed for cache mode to enable
		$use_cache = !empty($opt['cache']) && !empty($opt['cache_key']);

		if($debug){
			$this->clearDebugMessages();
			$this->debugMessage($this->getQueryDebugInfo($query, $params), __METHOD__);
		}
		
		if($use_cache){
			
			// renew or get cached result
			$cached = empty($opt['renew_cache']) ? $this->cacheManager->get($opt['cache_key']) : false;
			
			// cache found
			if($cached !== false){

				if($debug){
					$this->debugMessage(
						'Cached result found via key "' . $opt['cache_key'] . '". Skipping query...' . "\n",
						__METHOD__
					);
				}
				
				return $cached;
			}
		}
		
		// get result
		$result = $this->databaseManager->fetchColumn($query, $params);

		// log query
		$this->collectQuery($query, $params);
				
		if($use_cache){

			$cache_time = $opt['cache_time'] ?? null;

			$this->cacheManager->set($opt['cache_key'], $result, $cache_time);

			if($debug){
				$this->debugMessage(
					'Saving cache via key "' . $opt['cache_key'] . '"'. "\nValue:\n" . '<pre>'.print_r($result, true).'</pre>',
					__METHOD__
				);
			}
		}
			
		return $result;
	}

	/**
	 * [fetchRow description]
	 * @param  string     $query [description]
	 * @param  array|null $opt   [description]
	 * @param  array      $opt   [description]
	 * @return [type]            [description]
	 */
	public function fetchRow(string $query, array $params = null, array $opt = []){

		// debug mode
		$debug = !empty($opt['debug']);

		// cache key is also needed for cache mode to enable
		$use_cache = !empty($opt['cache']) && !empty($opt['cache_key']);

		if($debug){
			$this->clearDebugMessages();
			$this->debugMessage($this->getQueryDebugInfo($query, $params), __METHOD__);
		}
		
		if($use_cache){
			
			// renew or get cached result
			$cached = empty($opt['renew_cache']) ? $this->cacheManager->get($opt['cache_key'], true) : false;
			
			// cache found
			if($cached !== false){

				if($debug){
					$this->debugMessage(
						'Cached result found via key "' . $opt['cache_key'] . '". Skipping query...' . "\n",
						__METHOD__
					);
				}
				
				return $cached;
			}
		}
		
		// get result
		$result = $this->databaseManager->fetchRow($query, $params);

		// log query
		$this->collectQuery($query, $params);
				
		if($use_cache){

			$cache_time = $opt['cache_time'] ?? null;

			$this->cacheManager->set($opt['cache_key'], $result, $cache_time, true);

			if($debug){
				$this->debugMessage(
					'Saving cache via key "' . $opt['cache_key'] . '"'. "\nValue:\n" . '<pre>'.print_r($result, true).'</pre>',
					__METHOD__
				);
			}
		}
			
		return $result;
	}

	/**
	 * arr_index()
	 * 
	 * Index array by selected key (index)
	 * @param  array  $arr    [description]
	 * @param  string $by_key [description]
	 * @return [type]         [description]
	 */
	public function indexArrayByKey(array $arr, string $by_key){
		// init result
		$result = [];

		foreach ($arr as $item){
			if(!isset($item[$by_key])){
				return $arr;
			}
			$result[$item[$by_key]] = $item;
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
	public function groupArrayByKey(array $arr, string $index, array $selective = [], $opt = []){

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
		if(empty($arr)){
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
		$direct_value = !empty($opt['direct_value']);


		foreach ($arr as $row){
			
			// validate existing key
			if(!isset($row[$index])){
				break;
			}

			// full row mode
			if($save_full_row){
				$result[$row[$index]][] = $row;
			}
			// selective mode
			else {
				
				$tmp = [];
				
				foreach ($selective as $col){
					
					if(!isset($row[$col])){
						continue;
					}

					// break cycle on first found selected key
					if($direct_value){
						$tmp = $row[$col];
						break;
					}

					$tmp[$col] = $row[$col];
				}

				$result[$row[$index]][] = $tmp;
			}
		}

		return $result;
	}

}

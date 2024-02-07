<?php

/**
 * @author Oleg Isaev (PandCar)
 * @contacts vk.com/id50416641, t.me/pandcar, github.com/pandcar
 */

namespace XrTools;

/**
 * Class DBExt
 * @package XrTools
 */
class DBExt
{
	/**
	 * @var DBMCached
	 */
	protected $db;
	/**
	 * @var MemcachedAdapter
	 */
	private $mc;
	/**
	 * @var Utils
	 */
	protected $utils;
	/**
	 * @var bool
	 */
	protected $debug;
	/**
	 * @var string
	 */
	protected $columnQuote = '`';
	/**
	 * @var array
	 */
	protected $opt = [];

	/**
	 * DBExt constructor.
	 * @param DBMCached $db
	 * @param MemcachedAdapter $mc
	 * @param Utils $utils
	 * @param array $opt
	 */
	function __construct(
		DBMCached $db,
		MemcachedAdapter $mc,
		Utils $utils,
		array $opt = []
	){
		$this->db = $db;
		$this->mc = $mc;
		$this->utils = $utils;
		$this->opt = $opt;

		if (isset($opt['debug'])) {
			$this->debug = ! empty($opt['debug']);
		}
	}

	/**
	 * @return DBMCached
	 */
	function db(): DBMCached
	{
		return $this->db;
	}

	/////////////////////////////////
	/// Основа
	/////////////////////////////////

	/**
	 * @param string $query
	 * @param array|null $params
	 * @param array $opt
	 * @return mixed
	 */
	function query(string $query, array $params = null, array $opt = [])
	{
		return $this->db->query($query, $params, $this->opt($opt));
	}

	/**
	 * @param string $query
	 * @param array|null $params
	 * @param array $opt
	 * @return bool
	 */
	function exec(string $query, array $params = null, array $opt = []): bool
	{
		$res = $this->db->query($query, $params, $this->opt($opt));

		return ! empty($res['status']);
	}

	/**
	 * @param string $query
	 * @param array|null $params
	 * @param array $opt
	 * @return mixed
	 */
	function fetchArray(string $query, array $params = null, array $opt = [])
	{
		$opt = $this->cacheOpt($query, $params, $this->opt($opt));

		return $this->db->fetchArray($query, $params, $opt);
	}

	/**
	 * @param string $query
	 * @param array|null $params
	 * @param array $opt
	 * @return mixed
	 */
	function fetchRow(string $query, array $params = null, array $opt = [])
	{
		$opt = $this->cacheOpt($query, $params, $this->opt($opt));

		return $this->db->fetchRow($query, $params, $opt);
	}

	/**
	 * @param string $query
	 * @param array|null $params
	 * @param array $opt
	 * @return mixed
	 */
	function fetchColumn(string $query, array $params = null, array $opt = [])
	{
		$opt = $this->cacheOpt($query, $params, $this->opt($opt));

		return $this->db->fetchColumn($query, $params, $opt);
	}

	/**
	 * @param bool $inheritCache
	 * @param array $opt
	 * @return mixed
	 */
	function getCalcFoundRows(bool $inheritCache = true, array $opt = [])
	{
		return $this->db->getCalcFoundRows($inheritCache, $this->opt($opt));
	}

	/**
	 * @param string $query
	 * @param array|null $params
	 * @param array $opt
	 * @return mixed
	 */
	function fetchArrayWithCount(string $query, array $params = null, array $opt = [])
	{
		$opt = $this->cacheOpt($query, $params, $this->opt($opt));

		return $this->db->fetchArrayWithCount($query, $params, $opt);
	}

	/**
	 * @param string $query
	 * @param array|null $params
	 * @param array $opt
	 * @return mixed
	 */
	function fetchCount(string $query, array $params = null, array $opt = [])
	{
		$opt = $this->cacheOpt($query, $params, $this->opt($opt));

		return $this->fetchColumn(
			$this->db()->getExtractCountSQL($query),
			$params,
			$opt
		);
	}

	/**
	 * @param array $opt
	 * @return Transaction
	 */
	function transaction(array $opt = []): Transaction
	{
		return new Transaction(
			$this,
			$this->utils,
			$this->opt($opt)
		);
	}

	/**
	 * @param array $opt
	 * @return bool
	 */
	function beginTransaction(array $opt = []): bool
	{
		$opt = $this->opt($opt);
		$debug = ! empty($opt['debug']);

		return $this->db->start($debug);
	}

	/**
	 * @param array $opt
	 * @return bool
	 */
	function rollback(array $opt = []): bool
	{
		$opt = $this->opt($opt);
		$debug = ! empty($opt['debug']);

		return $this->db->rollback($debug);
	}

	/**
	 * @param array $opt
	 * @return bool
	 */
	function commit(array $opt = []): bool
	{
		$opt = $this->opt($opt);
		$debug = ! empty($opt['debug']);

		return $this->db->commit($debug);
	}

	/**
	 * @return array
	 */
	function getLastQueryFetch(): array
	{
		return $this->db->getLastQueryFetch();
	}

	/**
	 * @return string
	 */
	function getLastError(): string
	{
		return $this->db->getLastError();
	}

	/**
	 * @return string
	 */
	function getLastErrorCode(): string
	{
		return $this->db->getLastErrorCode();
	}

	/////////////////////////////////
	/// Получение
	/////////////////////////////////

	/**
	 * @param string|array $source
	 * @param array|null $whereAnd
	 * @param array $opt
	 * @return mixed
	 */
	function getWhereAnd( $source, array $whereAnd = null, array $opt = [])
	{
		// Для использования $this->source()
		[$tableFrom, $opt] = $this->sourceWorkingInGetWhereAnd($source, $opt);

		if (is_null($tableFrom)) {
			return false;
		}

		// Что бы можно было юзать null в whereAnd
		if (is_null($whereAnd)) {
			$whereAnd = [];
		}

		[$where, $params] = $this->partSQLWhereAnd($whereAnd);

		if (is_null($where)) {
			return false;
		}

		return $this->fetchArray(
			'SELECT
			  '.$this->fields($opt).'
			FROM
			  '.$tableFrom.'
			'.($where ? 'WHERE ' . $where : '').'
			'.$this->groupBy($opt).'
			'.$this->orderBy($opt).'
			'.$this->limitOffset($opt),
			$params,
			$this->opt($opt)
		);
	}

	/**
	 * @param string|array $source
	 * @param string $column
	 * @param $val
	 * @param array $opt
	 * @return mixed
	 */
	function getByColumn($source, string $column, $val, array $opt = [])
	{
		$whereAnd = [
			$column => $val
		];

		return $this->getWhereAnd($source, $whereAnd, $opt);
	}

	/**
	 * @param string|array $source
	 * @param array $whereAnd
	 * @param array $opt
	 * @return mixed
	 */
	function getRowWhereAnd($source, array $whereAnd, array $opt = [])
	{
		$opt['limit'] = 1;

		$res = $this->getWhereAnd($source, $whereAnd, $opt);

		if (! $res) {
			return null;
		}

		return $res[0] ?? null;
	}

	/**
	 * @param string|array $source
	 * @param string $column
	 * @param $val
	 * @param array $opt
	 * @return mixed
	 */
	function getRowByColumn($source, string $column, $val, array $opt = [])
	{
		$whereAnd = [
			$column => $val
		];

		return $this->getRowWhereAnd($source, $whereAnd, $opt);
	}

	/**
	 * @param string|array $source
	 * @param int $id
	 * @param array $opt
	 * @return mixed
	 */
	function getById($source, int $id, array $opt = [])
	{
		$column = 'id';

		if (is_array($source))
		{
			if (empty($source['main_table'])) {
				return false;
			}

			$column = $source['main_table'].'.id';
		}

		return $this->getRowByColumn($source, $column, $id, $opt);
	}

	/**
	 * @param string|array $source
	 * @param string $field
	 * @param array $whereAnd
	 * @param array $opt
	 * @return mixed
	 */
	function getFieldWhereAnd($source, string $field, array $whereAnd = [], array $opt = [])
	{
		$opt['fields'] = [
			$field
		];

		$res = $this->getRowWhereAnd($source, $whereAnd, $opt);

		if (! $res) {
			return null;
		}

		$res = array_values($res);

		return $res[0] ?? null;
	}

	/**
	 * @param string|array $source
	 * @param array $whereAnd
	 * @param array $opt
	 * @return mixed
	 */
	function getCountWhereAnd($source, array $whereAnd = [], array $opt = [])
	{
		return $this->getFieldWhereAnd($source, 'COUNT(*)', $whereAnd, $opt);
	}

	/////////////////////////////////
	/// Обновление
	/////////////////////////////////

	/**
	 * @param string $table
	 * @param array $whereAnd
	 * @param array $set
	 * @param array $opt
	 * @return bool
	 */
	function updateWhereAnd(string $table, array $whereAnd, array $set, array $opt = []): bool
	{
		[$set, $params] = $this->partSQLSet($set);
		[$where, $params2] = $this->partSQLWhereAnd($whereAnd);

		if (is_null($where) || is_null($set)) {
			return false;
		}

		if (empty($set)) {
			$this->err('Data for updating has not been sent');
			return false;
		}

		array_push($params, ...$params2);

		$opt = $this->opt($opt);

		$res = $this->exec(
			'UPDATE
			  '.$this->escapeName($table).'
			SET
			  '.$set.'
			'.($where ? 'WHERE ' . $where : '').'
			'.$this->limitOffset($opt),
			$params,
			$opt
		);

		if ($res) {
			$this->cacheDeleteOpt($opt);
		}

		return $res;
	}

	/**
	 * @param string $table
	 * @param string $column
	 * @param $val
	 * @param array $set
	 * @param array $opt
	 * @return bool
	 */
	function updateByColumn(string $table, string $column, $val, array $set, array $opt = []): bool
	{
		$whereAnd = [
			$column => $val
		];

		return $this->updateWhereAnd($table, $whereAnd, $set, $opt);
	}

	/**
	 * @param string $table  Редактируемая таблица
	 * @param int $id        Id строки
	 * @param array $set     Колонки с данными для обновления
	 * @param array $opt     Опции
	 * @return bool
	 */
	function updateById(string $table, int $id, array $set, array $opt = [])
	{
		return $this->updateByColumn($table, 'id', $id, $set, $opt);
	}

	/////////////////////////////////
	/// Удаление
	/////////////////////////////////

	/**
	 * @param string $table
	 * @param array $whereAnd
	 * @param array $opt
	 * @return bool
	 */
	function deleteWhereAnd(string $table, array $whereAnd, array $opt = []): bool
	{
		[$where, $params] = $this->partSQLWhereAnd($whereAnd);

		if (is_null($where)) {
			return false;
		}

		$opt = $this->opt($opt);

		$res = $this->exec(
			'DELETE FROM
			  '.$this->escapeName($table).'
			'.($where ? 'WHERE ' . $where : '').'
			'.$this->limitOffset($opt),
			$params,
			$opt
		);

		if ($res) {
			$this->cacheDeleteOpt($opt);
		}

		return $res;
	}

	/**
	 * @param string $table
	 * @param string $column
	 * @param $val
	 * @param array $opt
	 * @return bool
	 */
	function deleteByColumn(string $table, string $column, $val, array $opt = []): bool
	{
		$whereAnd = [
			$column => $val
		];

		return $this->deleteWhereAnd($table, $whereAnd, $opt);
	}

	/**
	 * @param string $table
	 * @param int $id
	 * @param array $opt
	 * @return bool
	 */
	function deleteById(string $table, int $id, array $opt = []): bool
	{
		return $this->deleteByColumn($table, 'id', $id, $opt);
	}

	/////////////////////////////////
	/// Добавление
	/////////////////////////////////

	/**
	 * @param string $table
	 * @param array $setList
	 * @param array $opt
	 *    -insertID = true
	 *    -chunkSize = 100
	 *    -columns = ['id','title']
	 * @return bool|int
	 */
	function insertList(string $table, array $setList, array $opt = [])
	{
		if (! $table || ! $setList) {
			$this->err('No data to write');
			return false;
		}

		$useChunk = ! empty($opt['chunkSize']);
		$chunkSize = $opt['chunkSize'] ?? 10000000;

		$row = current($setList);

		if (! $row) {
			return false;
		}

		$insertColumns = (array) ($opt['columns'] ?? []);

		if (empty($insertColumns)) {
			$insertColumns = array_keys($row ?? []);
		}

		$setList = array_chunk($setList, $chunkSize);

		if ($useChunk)
		{
			if (! $this->beginTransaction($opt)) {
				return false;
			}
		}

		$res = false;

		foreach ($setList as $chunk)
		{
			[$partSql, $params] = $this->partSQLInsert($insertColumns, $chunk);

			$res = $this->query(
				'INSERT INTO
				  '.$this->escapeName($table).' (
					'.$this->escapeNameArray($insertColumns).'
				  )
				VALUES
				  '.$partSql.'
				'.$this->indexConflict($opt, $insertColumns),
				$params,
				$this->opt($opt)
			);

			if (empty($res['status'])) {
				break;
			}
		}

		$status = ! empty($res['status']);

		// Если результат false
		if (! $status)
		{
			// Rollback если используются чанки
			if ($useChunk) {
				$this->rollback($opt);
			}

			return false;
		}

		// Успех

		// Commit если используются чанки
		if ($useChunk)
		{
			if (! $this->commit($opt)) {
				return false;
			}
		}

		$this->cacheDeleteOpt($opt);

		// Если нужен insert id
		if (! empty($opt['insertID']) && ! empty($res['insert_id'])) {
			return (int) $res['insert_id'];
		}

		return true;
	}

	/**
	 * @param string $table
	 * @param array $set
	 * @param array $opt
	 * @return bool|int
	 */
	function insert(string $table, array $set, array $opt = [])
	{
		$setList = [
			$set
		];

		$opt['insertID'] = true;
		$opt['chunkSize'] = null;

		return $this->insertList($table, $setList, $opt);
	}

	/////////////////////////////////
	/// Добавление или Обновление
	/////////////////////////////////

	/**
	 * @param string $table
	 * @param array $setList
	 * @param array $conflictUpdate
	 * @param array $opt
	 * @return bool|int
	 */
	function upsertList(string $table, array $setList, array $conflictUpdate = [], array $opt = [])
	{
		$opt['conflictUpdate'] = $conflictUpdate;

		return $this->insertList($table, $setList, $opt);
	}

	/**
	 * @param string $table
	 * @param array $set
	 * @param array $conflictUpdate
	 * @param array $opt
	 * @return bool|int
	 */
	function upsert(string $table, array $set, array $conflictUpdate = [], array $opt = [])
	{
		$opt['conflictUpdate'] = $conflictUpdate;

		return $this->insert($table, $set, $opt);
	}

	/////////////////////////////////
	/// Доп. инструменты
	/////////////////////////////////

	/**
	 * @param false|array $items
	 * @param bool $inheritCache
	 * @param array $opt
	 * @return array|false
	 */
	function formatCountAndItems( $items, bool $inheritCache = true, array $opt = [])
	{
		if (! is_array($items)) {
			return false;
		}

		return [
			'count' => $this->getCalcFoundRows($inheritCache, $opt),
			'items' => $items,
		];
	}

	/**
	 * @param array $from
	 * @return false|array[]
	 */
	function source(array $from)
	{
		$from_res = [];
		$fields_res = [];
		$mainTable = null;
		$aliasUse = [];

		foreach ($from as $source => $fields)
		{
			if (is_int($source)) {
				$source = $fields;
				$fields = true;
			}

			preg_match(
				'~^([a-z0-9_-]+)\(([a-z0-9_-]+)\)(?= ([a-z0-9_-]+) *= *([a-z0-9_-]+\.[a-z0-9_-]+)|)~i',
				$source,
				$pregSource
			);

			if (! $pregSource) {
				$this->err('Link is not valid');
				return false;
			}

			[$_, $table, $tableAlias, $columnTable, $columnBind] = array_pad($pregSource, 5, '');

			if (in_array($tableAlias, $aliasUse)) {
				$this->err('Table with the selected alias is already in the set');
				return false;
			}

			$aliasUse []= $tableAlias;

			$columnTable = $this->escapeName($tableAlias.'.'.$columnTable);
			$tableAndAlias = $this->escapeName($table) . (! empty($tableAlias) ? ' AS '.$this->escapeName($tableAlias) : '');

			if (empty($columnBind))
			{
				array_unshift($from_res, $tableAndAlias);

				if ($table || $tableAlias) {
					$mainTable = $tableAlias ? $tableAlias : $table;
				}
			}
			else
			{
				$from_res []= 'LEFT JOIN '.$tableAndAlias.' ON '.$columnTable.' = '.$this->escapeName($columnBind);
			}

			if (is_array($fields))
			{
				foreach ($fields as $field)
				{
					preg_match(
						'~^([a-z0-9_-]+)(?=\(([a-z0-9_-]+)\)|)~i',
						$field,
						$pregField
					);

					if (! $pregField) {
						$this->err('Field is not valid');
						return false;
					}

					[$_, $column, $alias] = array_pad($pregField, 3, '');

					$fields_res []= $this->escapeName($tableAlias .'.'. $column) . (! empty($alias) ? ' AS '.$this->escapeName($alias) : '');
				}
			}
			elseif ($fields)
			{
				$fields_res []= $this->escapeName($tableAlias).'.*';
			}
		}

		return [
			'from'       => $from_res,
			'fields'     => $fields_res,
			'main_table' => $mainTable,
		];
	}

	/**
	 * @param array $whereAnd1
	 * @param array $whereAnd2
	 * @return false|string[]
	 */
	function or(array $whereAnd1, array $whereAnd2)
	{
		[$where1, $params1] = $this->partSQLWhereAnd($whereAnd1);
		[$where2, $params2] = $this->partSQLWhereAnd($whereAnd2);

		if (is_null($where1) || is_null($where2)) {
			return false;
		}

		$return = [
			'('.$where1.' OR '.$where2.')'
		];

		array_push($return, ...$params1);
		array_push($return, ...$params2);

		return $return;
	}

	/**
	 * @param array $arr
	 * @return string
	 */
	function valuesIn(array $arr): string
	{
		$arr = array_fill(0, count($arr), '?');
		return implode(',', $arr);
	}

	/**
	 * @param string $val
	 * @return string
	 */
	function escapeName(string $val): string
	{
		$val = str_replace($this->columnQuote, '',  $val);
		$val = addslashes($val);
		$exp = explode('.', $val, 2);

		return $this->columnQuote . implode($this->columnQuote .'.'. $this->columnQuote, $exp) . $this->columnQuote;
	}

	/**
	 * @param string $val
	 * @return string
	 */
	function escapeNameAndAlias(string $val): string
	{
		preg_match('~^([a-z0-9._-]+)\(([a-z0-9_-]+)\)~i', $val, $preg);

		if (! $preg) {
			return $this->escapeName($val);
		}

		return $this->escapeName($preg[1]).' AS '.$this->escapeName($preg[2]);
	}

	/**
	 * @param array $arr
	 * @return string
	 */
	function escapeNameArray(array $arr): string
	{
		$arr = array_map(function ($item){
			return $this->escapeName($item);
		}, $arr);

		return implode(', ', $arr);
	}

	/**
	 * @param array $opt
	 *    -fields = [
	 *      'title', 'COUNT(`id`)', 'SUM(`num`) AS `sum_num`',
	 *    ]
	 *    -columns = [
	 *      'id', 'type'
	 *    ]
	 * @return string
	 */
	function fields(array $opt): string
	{
		$default = '*';
		$fields = $opt['fields'] ?? [];
		$columns = $opt['columns'] ?? [];

		if ($columns && is_array($columns))
		{
			foreach ($columns as $column) {
				$fields []= $this->escapeNameAndAlias($column);
			}
		}

		if (! $fields || ! is_array($fields)) {
			return $default;
		}

		// Не даём накидывать левую логику
		//$test = preg_replace('/\((.*?)\)/is', '', implode(' _ ', $fields));
		//preg_match('~(,)~i', $test, $preg);

		//if ($preg) {
		//	return $default;
		//}

		return implode(", \n", $fields);
	}

	/**
	 * @param array $opt
	 *    -orderBy = [
	 *      $column => $isDesc,
	 *    ]
	 * @return string
	 */
	function orderBy(array $opt): string
	{
		$order = $opt['orderBy'] ?? [];

		if (empty($order)) {
			return '';
		}

		if (! is_array($order)) {
			$order = [
				$order => false
			];
		}

		$part = [];

		foreach ($order as $column => $desc) {
			$part []= $this->escapeName($column).' '.($desc ? 'DESC' : 'ASC');
		}

		return 'ORDER BY '.implode(', ', $part);
	}

	/**
	 * @param array $opt
	 * @return string
	 */
	function groupBy(array $opt): string
	{
		$group = (array) ($opt['groupBy'] ?? []);

		if (empty($group)) {
			return '';
		}

		return 'GROUP BY '.$this->escapeNameArray($group);
	}

	/**
	 * @param array $opt
	 *    -limit = 10
	 *    -offset = 20
	 *    -page = 1
	 * @return string
	 */
	function limitOffset(array $opt): string
	{
		$limit = intval($opt['limit'] ?? 0);
		$offset = intval($opt['offset'] ?? 0);

		if (empty($limit)) {
			return '';
		}

		if (isset($opt['page'])) {
			$page = intval($opt['page'] ?? 1);
			$offset = ($page - 1) * $limit;
		}

		return 'LIMIT ' . ($offset ? $offset . ', ' : '') . $limit;
	}

	/**
	 * @param array $opt
	 *    -conflictUpdate = ['name','time']
	 * @param array $insertColumns
	 * @return string
	 */
	function indexConflict(array $opt, array $insertColumns = []): string
	{
		if (! isset($opt['conflictUpdate'])) {
			return '';
		}

		$update = (array) ($opt['conflictUpdate'] ?? []);

		if (empty($update)) {
			$update = $insertColumns;
		}

		if (empty($update)) {
			return '';
		}

		$update = array_map(
			function ($column) {
				return $this->escapeName($column).' = VALUES('.$this->escapeName($column).')';
			},
			$update
		);

		return 'ON DUPLICATE KEY UPDATE '.implode(', ', $update);
	}

	/**
	 * Создание части sql запроса Insert
	 * @param array $columns
	 * @param array $dataRows
	 * @return array
	 */
	function partSQLInsert(array $columns, array $dataRows): array
	{
		$partSql = [];
		$params = [];

		$vars = '('.$this->valuesIn($columns).')';

		foreach ($dataRows as $item) {
			$partSql []= $vars;
			array_push($params, ...array_values($item));
		}

		return [
			implode(', ', $partSql),
			$params,
		];
	}

	/**
	 * Создание части sql запроса Set из массива
	 * @param array $data
	 * @return array
	 */
	function partSQLSet(array $data = []): array
	{
		$partSql = [];
		$params = [];

		foreach ($data as $key => $value)
		{
			$column = $this->escapeName($key);

			if (is_null($value)) {
				$partSql []= $column.' = NULL';
			} else {
				$partSql []= $column.' = ?';
				$params []= $value;
			}
		}

		return [
			implode(", \n", $partSql),
			$params,
		];
	}

	/**
	 * Создание части sql запроса WhereAnd из массива
	 * @param array $data
	 * @return array|false
	 */
	function partSQLWhereAnd(array $data = [])
	{
		$partSql = [];
		$params = [];

		// Если требуются именованные параметры
		$nameParams = false;

		foreach ($data as $key => $value)
		{
			$column = $this->escapeName($key);

			if (is_null($value))
			{
				$partSql []= $column.' IS NULL';
			}
			elseif (is_array($value))
			{
				$way = array_shift($value);

				// Если идёт вставка чистого sql
				if (is_int($key))
				{
					$partSql []= $way;
				}
				else
				{
					// Не даём накидывать левую логику
					preg_match("~(&&|\|\||(AND|OR)[ \n\r\t]+)~i", $way, $preg);

					if ($preg) {
						$this->err('Parameter contains forbidden elements');
						return false;
					}

					if (substr_count($way, '?...') && is_array($value[0]))
					{
						$inArr = array_shift($value);
						$way = str_replace('?...', $this->valuesIn($inArr), $way);
						array_push($params, ...$inArr);
					}

					$partSql []= $column.' '.$way;
				}

				if (! empty($value)) {
					array_push($params, ...$value);
				}
			}
			// Если $this->or() вернёт false то принимаем за ошибку
			elseif ($value === false)
			{
				return false;
			}
			// Если требуются именованные параметры
			elseif (substr($key, 0, 1) == ':')
			{
				$params[ $key ] = $value;
				$nameParams = true;
			}
			else
				{
				$partSql []= $column.' = ?';
				$params []= $value;
			}
		}

		$partSql = implode("\n AND ", $partSql);

		// Если требуются именованные параметры
		if ($nameParams)
		{
			$paramsNew = [];

			foreach ($params as $key => $val)
			{
				if (! is_numeric($key)) {
					$paramsNew[ $key ] = $val;
					continue;
				}

				$name = ':'.$key;
				$paramsNew[ $name ] = $params[ $key ];
				$partSql = preg_replace('/\?/', $name, $partSql, 1);
			}

			$params = $paramsNew;
		}

		return [
			$partSql,
			$params,
		];
	}

	/////////////////////////////////
	/// Внутреннее
	/////////////////////////////////

	/**
	 * Для использования $this->source() в $this->getWhereAnd()
	 * @param $source
	 * @param array $opt
	 * @return array|false
	 */
	protected function sourceWorkingInGetWhereAnd($source, array $opt)
	{
		if (is_array($source))
		{
			if (empty($source['from']) || empty($source['main_table']) || ! isset($source['fields'])) {
				return false;
			}

			$fields = [];
			array_push($fields, ...$source['fields']);
			array_push($fields, ...$opt['fields'] ?? []);

			$opt['fields'] = $fields;

			$tableFrom = implode(" \n", $source['from']);
		}
		elseif (is_string($source)) {
			$tableFrom = $this->escapeNameAndAlias($source);
		}
		else {
			return false;
		}

		return [$tableFrom, $opt];
	}

	/**
	 * @param string $sql
	 * @param array|null $params
	 * @param array $opt
	 * @return array
	 */
	protected function cacheOpt(string $sql, array $params = null, array $opt = []): array
	{
		$cache = $opt['cache'] ?? [];

		if (! $cache || ! is_array($cache)) {
			return $opt;
		}

		$key = $cache['key'] ?? null;
		$versions = $cache['versions'] ?? $cache['version'] ?? null;
		$exp = $cache['exp'] ?? $cache['sec'] ?? 1200;

		if ($versions)
		{
			if (! is_array($versions)) {
				$versions = (array) $versions;
			}

			sort($versions);
		}

		// Если нет ключа, создаём
		if (! $key)
		{
			$draft = $sql;

			if (! is_null($params)) {
				$draft .= '__'.json_encode($params, JSON_UNESCAPED_UNICODE);
			}

			$key = sha1($draft);

			// Добавляем версию
			if ($versions)
			{
				$list = [];

				foreach ($versions as $version)
				{
					if (empty($version)) {
						continue;
					}

					$list []= $this->mc->getStamp($version, 3600 * 30);
				}

				if ($list) {
					$key .= '_'.implode(':', $list);
				}
			}
		}

		$opt['cache'] = $cache['use'] ?? true;
		$opt['cache_key'] = $key;
		$opt['cache_time'] = $exp;

		return $opt;
	}

	/**
	 * @param array $opt
	 * @return void
	 */
	protected function cacheDeleteOpt(array $opt): void
	{
		$keys = $opt['cache_delete'] ?? [];

		if (empty($keys)) {
			return;
		}

		$keys = (array) $keys;

		$this->mc->del($keys);
	}

	/**
	 * @param string $message
	 * @param array $opt
	 */
	protected function err(string $message, array $opt = []): void
	{
		$this->utils->dbg()->log2($message, $this->opt($opt));
	}

	/**
	 * @param array $opt
	 * @param array $opt2
	 * @return array
	 */
	protected function opt(array $opt = [], array $opt2 = []): array
	{
		$opt['debug'] = $this->debug || ! empty($opt['debug']);

		// Функция обратного вызова для определения доступности дебага
		if (! $opt['debug'] && ! empty($this->opt['debugMiddleware']) && is_callable($this->opt['debugMiddleware'])) {
			$opt['debug'] = $this->opt['debugMiddleware']();
		}

		return array_merge($opt, $opt2);
	}
}


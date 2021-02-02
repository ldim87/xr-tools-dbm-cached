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
	 * @var DatabaseManager
	 */
	protected $db;

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
	 * DBExt constructor.
	 * @param DatabaseManager $db
	 * @param Utils $utils
	 * @param array $opt
	 */
	function __construct(
		DatabaseManager $db,
		Utils $utils,
		array $opt = []
	){
		$this->db = $db;
		$this->utils = $utils;

		if (isset($opt['debug'])) {
			$this->debug = ! empty($opt['debug']);
		}
	}

	/**
	 * @return DatabaseManager
	 */
	function db(): DatabaseManager
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
		return $this->db->query($query, $params, $this->getOpt($opt));
	}

	/**
	 * @param string $query
	 * @param array|null $params
	 * @param array $opt
	 * @return bool
	 */
	function exec(string $query, array $params = null, array $opt = [])
	{
		$res = $this->db->query($query, $params, $this->getOpt($opt));

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
		return $this->db->fetchArray($query, $params, $this->getOpt($opt));
	}

	/**
	 * @param string $query
	 * @param array|null $params
	 * @param array $opt
	 * @return mixed
	 */
	function fetchRow(string $query, array $params = null, array $opt = [])
	{
		return $this->db->fetchRow($query, $params, $this->getOpt($opt));
	}

	/**
	 * @param string $query
	 * @param array|null $params
	 * @param array $opt
	 * @return mixed
	 */
	function fetchColumn(string $query, array $params = null, array $opt = [])
	{
		return $this->db->fetchColumn($query, $params, $this->getOpt($opt));
	}

	/**
	 * @param bool $inheritCache
	 * @param array $opt
	 * @return mixed
	 */
	function getCalcFoundRows(bool $inheritCache = true, array $opt = [])
	{
		return $this->db->getCalcFoundRows($inheritCache, $this->getOpt($opt));
	}

	/**
	 * @param string $query
	 * @param array|null $params
	 * @param array $opt
	 * @return mixed
	 */
	function fetchArrayWithCount(string $query, array $params = null, array $opt = [])
	{
		return $this->db->fetchArrayWithCount($query, $params, $this->getOpt($opt));
	}

	/**
	 * @param array $opt
	 * @return bool
	 */
	function transaction(array $opt = []): bool
	{
		$opt = $this->getOpt($opt);
		$debug = ! empty($opt['debug']);

		return $this->db->start($debug);
	}

	/**
	 * @param array $opt
	 * @return bool
	 */
	function rollback(array $opt = []): bool
	{
		$opt = $this->getOpt($opt);
		$debug = ! empty($opt['debug']);

		return $this->db->rollback($debug);
	}

	/**
	 * @param array $opt
	 * @return bool
	 */
	function commit(array $opt = []): bool
	{
		$opt = $this->getOpt($opt);
		$debug = ! empty($opt['debug']);

		return $this->db->commit($debug);
	}

	/////////////////////////////////
	/// Получение
	/////////////////////////////////

	/**
	 * @param string|array $table
	 * @param array $whereAnd
	 * @param array $opt
	 * @return mixed
	 */
	function getWhereAnd( $table, array $whereAnd = [], array $opt = [])
	{
		// Для использования $this->source()
		if (is_array($table))
		{
			if (empty($table['from']) || empty($table['fields'])) {
				return false;
			}

			// Создаём массив $opt['fields'] если его нет
			if (empty($opt['fields']) || ! is_array($opt['fields'])) {
				$opt['fields'] = [];
			}

			array_push($opt['fields'], ...$table['fields']);

			$from = implode(" \n", $table['from']);
		}
		elseif (is_string($table)) {
			$from = $this->escapeName($table);
		}
		else {
			return false;
		}

		[$where, $params] = $this->partSQL($whereAnd, true);

		if (is_null($where)) {
			return false;
		}

		return $this->fetchArray(
			'SELECT
			  '.$this->fields($opt).'
			FROM
			  '.$from.'
			'.($where ? 'WHERE ' . $where : '').'
			'.$this->groupBy($opt).'
			'.$this->orderBy($opt).'
			'.$this->limitOffset($opt),
			$params,
			$this->getOpt($opt)
		);
	}

	/**
	 * @param string|array $table
	 * @param string $column
	 * @param $val
	 * @param array $opt
	 * @return mixed
	 */
	function getByColumn($table, string $column, $val, array $opt = [])
	{
		$whereAnd = [
			$column => $val
		];

		return $this->getWhereAnd($table, $whereAnd, $opt);
	}

	/**
	 * @param string|array $table
	 * @param array $whereAnd
	 * @param array $opt
	 * @return mixed
	 */
	function getRowWhereAnd($table, array $whereAnd, array $opt = [])
	{
		$opt['limit'] = 1;

		$res = $this->getWhereAnd($table, $whereAnd, $opt);

		if (! $res) {
			return null;
		}

		return $res[0] ?? null;
	}

	/**
	 * @param string|array $table
	 * @param string $column
	 * @param $val
	 * @param array $opt
	 * @return mixed
	 */
	function getRowByColumn($table, string $column, $val, array $opt = [])
	{
		$whereAnd = [
			$column => $val
		];

		return $this->getRowWhereAnd($table, $whereAnd, $opt);
	}

	/**
	 * @param string|array $table
	 * @param int $id
	 * @param array $opt
	 * @return mixed
	 */
	function getById($table, int $id, array $opt = [])
	{
		return $this->getRowByColumn($table, 'id', $id, $opt);
	}

	/**
	 * @param string|array $table
	 * @param string $field
	 * @param array $whereAnd
	 * @param array $opt
	 * @return mixed
	 */
	function getFieldWhereAnd($table, string $field, array $whereAnd = [], array $opt = [])
	{
		$opt['fields'] = [
			$field
		];

		$res = $this->getRowWhereAnd($table, $whereAnd, $opt);

		if (! $res) {
			return null;
		}

		$res = array_values($res);

		return $res[0] ?? null;
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
		[$set, $params] = $this->partSQL($set);
		[$where, $params2] = $this->partSQL($whereAnd, true);

		if (is_null($where) || is_null($set)) {
			return false;
		}

		if (empty($set)) {
			$this->err('Не переданы данные для обновления');
			return false;
		}

		array_push($params, ...$params2);

		return $this->exec(
			'UPDATE
			  '.$this->escapeName($table).'
			SET
			  '.$set.'
			'.($where ? 'WHERE ' . $where : '').'
			'.$this->limitOffset($opt),
			$params,
			$this->getOpt($opt)
		);
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
	 * @param string $table
	 * @param int $id
	 * @param array $set
	 * @param array $opt
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
		[$where, $params] = $this->partSQL($whereAnd, true);

		if (is_null($where)) {
			return false;
		}

		return $this->exec(
			'DELETE FROM
			  '.$this->escapeName($table).'
			'.($where ? 'WHERE ' . $where : '').'
			'.$this->limitOffset($opt),
			$params,
			$this->getOpt($opt)
		);
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
		if (empty($setList) || ! is_array($setList)) {
			$this->err('Отсутствуют данные для записи');
			return false;
		}

		$chunkSize = $opt['chunkSize'] ?? false;

		$row = $setList[0] ?? [];
		$temp = '('.$this->valuesIn($row).')';

		$insertColumns = (array) ($opt['columns'] ?? []);

		if (empty($insertColumns)) {
			$insertColumns = array_keys($row ?? []);
		}

		if ($chunkSize)
		{
			$setList = array_chunk($setList, $chunkSize);

			$trans = $this->transaction($opt);

			if (! $trans) {
				return false;
			}
		}
		else
		{
			$setList = [
				$setList
			];
		}

		$res = false;

		foreach ($setList as $chunk)
		{
			$parts  = [];
			$params = [];

			foreach ($chunk as $item) {
				$parts []= $temp;
				array_push($params, ...array_values($item));
			}

			$res = $this->query(
				'INSERT INTO
				  '.$this->escapeName($table).' (
					'.$this->escapeNameArray($insertColumns).'
				  )
				VALUES
				  '.implode(',', $parts).'
				'.$this->indexConflict($insertColumns, $opt),
				$params,
				$this->getOpt($opt)
			);

			if (! $res) {
				break;
			}
		}

		$status = ! empty($res['status']);

		if ($chunkSize)
		{
			if ($status) {
				$trans = $this->commit($opt);
			} else {
				$trans = $this->rollback($opt);
			}

			if (! $trans) {
				return false;
			}
		}
		else
		{
			if (! empty($opt['insertID'])) {
				return $res['insert_id'] > 0 ? (int) $res['insert_id'] : false;
			}
		}

		return $status;
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

		$insertID = $this->insertList($table, $setList, $opt);

		if (! $insertID) {
			return false;
		}

		return $insertID;
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
	 * @return array
	 */
	function formatCountAndItems( $items, bool $inheritCache = true, array $opt = []): array
	{
		$result = [
			'count' => 0,
			'items' => [],
		];

		if ($items) {
			$result['count'] = $this->getCalcFoundRows($inheritCache, $opt);
			$result['items'] = $items;
		}

		return $result;
	}

	/**
	 * @param array $from
	 * @return false|array[]
	 */
	function source(array $from)
	{
		$from_res = [];
		$fields_res = [];
		$aliasUse = [];

		foreach ($from as $source => $fields)
		{
			if (is_int($source)) {
				$source = $fields;
				$fields = true;
			}

			preg_match('~^([a-z0-9_-]+)\(([a-z0-9_-]+)\)(?= ([a-z0-9_-]+) *= *([a-z0-9_-]+\.[a-z0-9_-]+)|)~i', $source, $pregSource);

			if (! $pregSource) {
				$this->err('Связь не валидна');
				return false;
			}

			[$_, $table, $tableAlias, $columnTable, $columnBind] = array_pad($pregSource, 5, '');

			if (in_array($tableAlias, $aliasUse)) {
				$this->err('Таблица выбранным алиасом уже есть в наборе');
				return false;
			}

			$aliasUse []= $tableAlias;

			$table = $this->escapeName($table);
			$columnTable = $this->escapeName($tableAlias.'.'.$columnTable);

			$tableAndAlias = $table . (! empty($tableAlias) ? ' AS '.$this->escapeName($tableAlias) : '');

			if (empty($columnBind)) {
				array_unshift($from_res, $tableAndAlias);
			} else {
				$from_res []= 'LEFT JOIN '.$tableAndAlias.' ON '.$columnTable.' = '.$this->escapeName($columnBind);
			}

			if (is_bool($fields) && $fields) {
				$fields_res []= $this->escapeName($tableAlias).'.*';
			}
			else
			{
				foreach ($fields as $field)
				{
					preg_match('~^([a-z0-9_-]+)(?=\(([a-z0-9_-]+)\)|)~i', $field, $pregField);

					if (! $pregField) {
						$this->err('Поле не валидно');
						return false;
					}

					[$_, $column, $alias] = array_pad($pregField, 3, '');

					$fields_res []= $this->escapeName($tableAlias .'.'. $column) . (! empty($alias) ? ' AS '.$this->escapeName($alias) : '');
				}
			}
		}

		return [
			'from'   => $from_res,
			'fields' => $fields_res,
		];
	}

	/////////////////////////////////
	/// Хэлперы
	/////////////////////////////////

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
	 * @return string
	 */
	function fields(array $opt): string
	{
		$fields = $opt['fields'] ?? [];
		$default = '*';

		if (empty($fields) || ! is_array($fields)) {
			return $default;
		}

		preg_match('~(,)~i', implode($fields), $preg);

		if ($preg) {
			return $default;
		}

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
	 * @return string
	 */
	function limitOffset(array $opt): string
	{
		$limit = (int) ($opt['limit'] ?? 0);
		$offset = (int) ($opt['offset'] ?? 0);

		if (empty($limit)) {
			return '';
		}

		return 'LIMIT ' . ($offset ? $offset . ', ' : '') . $limit;
	}

	/**
	 * @param array $insertColumns
	 * @param array $opt
	 *    -conflictUpdate = ['name','time']
	 * @return string
	 */
	function indexConflict(array $insertColumns, array $opt): string
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
	 * Создание части sql запроса из массива
	 * @param array $data
	 * @param bool $whereAnd (where and or update set)
	 * @return false|array
	 */
	function partSQL(array $data = [], bool $whereAnd = false)
	{
		$part_sql = [];
		$params = [];

		foreach ($data as $key => $value)
		{
			$key = $this->escapeName($key);

			if (is_null($value)) {
				$part_sql []= $key.' = NULL';
			}
			elseif ($whereAnd && is_array($value))
			{
				$way = array_shift($value);

				preg_match("~(,|&&|\|\||(AND|OR)[ \n\r\t]+)~i", $way, $preg);

				if ($preg) {
					$this->err('Параметр содержит запрещённые элементы');
					return false;
				}

				if (substr_count($way, '?...') && is_array($value[0]))
				{
					$inArr = array_shift($value);
					$way = str_replace('?...', $this->valuesIn($inArr), $way);
					array_push($params, ...$inArr);
				}

				if (! empty($value)) {
					array_push($params, ...$value);
				}

				$part_sql []= $key.' '.$way;
			}
			else {
				$part_sql []= $key.' = ?';
				$params []= $value;
			}
		}

		// where and or update set
		$glue = $whereAnd ? ' AND ' : ', ';

		return [
			implode($glue, $part_sql),
			$params,
		];
	}

	/////////////////////////////////
	/// Внутреннее
	/////////////////////////////////

	/**
	 * @param string $message
	 * @param array $opt
	 */
	protected function err(string $message, array $opt = []): void
	{
		$this->utils->dbg()->log2($message, $this->getOpt($opt));
	}

	/**
	 * @param array $opt
	 * @param array $opt2
	 * @return array
	 */
	protected function getOpt(array $opt = [], array $opt2 = []): array
	{
		$opt['debug'] = $this->debug || ! empty($opt['debug']);

		return array_merge($opt, $opt2);
	}
}


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

	/**
	 * @return Utils\Arrays
	 */
	function arrays(): Utils\Arrays
	{
		return $this->utils->arrays();
	}

	/**
	 * @return Utils\Strings
	 */
	function strings(): Utils\Strings
	{
		return $this->utils->strings();
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
		return $this->db->query($query, $params, $opt);
	}

	/**
	 * @param string $query
	 * @param array|null $params
	 * @param array $opt
	 * @return bool
	 */
	function exec(string $query, array $params = null, array $opt = [])
	{
		$res = $this->db->query($query, $params, $opt);

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
		return $this->db->fetchColumn($query, $params, $opt);
	}

	/**
	 * @param string $query
	 * @param array|null $params
	 * @param array $opt
	 * @return mixed
	 */
	function fetchArrayWithCount(string $query, array $params = null, array $opt = [])
	{
		return $this->db->fetchArrayWithCount($query, $params, $opt);
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
	 * @param string $table
	 * @param array $whereAnd
	 * @param array $opt
	 * @return mixed
	 */
	function getWhereAnd(string $table, array $whereAnd, array $opt = [])
	{
		list($where, $params) = $this->partSql($whereAnd, ' AND ');

		return $this->fetchArray(
			'SELECT
			  '.$this->fields($opt).'
			FROM
			  `'.$this->escapeName($table).'`
			'.($where ? 'WHERE ' . $where : '').'
			'.$this->orderBy($opt).'
			'.$this->offsetLimit($opt),
			$params,
			$this->getOpt($opt)
		);
	}

	/**
	 * @param string $table
	 * @param string $column
	 * @param $val
	 * @param array $opt
	 * @return mixed
	 */
	function getByColumn(string $table, string $column, $val, array $opt = [])
	{
		$whereAnd = [
			$column => $val
		];

		return $this->getWhereAnd($table, $whereAnd, $opt);
	}

	/**
	 * @param string $table
	 * @param array $whereAnd
	 * @param array $opt
	 * @return mixed|null
	 */
	function getRowWhereAnd(string $table, array $whereAnd, array $opt = [])
	{
		$opt['limit'] = 1;

		$res = $this->getWhereAnd($table, $whereAnd, $opt);

		if (! $res) {
			return null;
		}

		return $res[0] ?? null;
	}

	/**
	 * @param string $table
	 * @param string $column
	 * @param $val
	 * @param array $opt
	 * @return mixed|null
	 */
	function getRowByColumn(string $table, string $column, $val, array $opt = [])
	{
		$whereAnd = [
			$column => $val
		];

		return $this->getRowWhereAnd($table, $whereAnd, $opt);
	}

	/**
	 * @param string $table
	 * @param int $id
	 * @param array $opt
	 * @return mixed|null
	 */
	function getByID(string $table, int $id, array $opt = [])
	{
		return $this->getRowByColumn($table, 'id', $id, $opt);
	}

	/**
	 * @param string $table
	 * @param string $field
	 * @param array $whereAnd
	 * @param array $opt
	 * @return mixed|null
	 */
	function getFieldWhereAnd(string $table, string $field, array $whereAnd = [], array $opt = [])
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

	/**
	 * @param string $table
	 * @param string $field
	 * @param int $id
	 * @param array $opt
	 * @return mixed|null
	 */
	function getFieldByID(string $table, string $field, int $id, array $opt = [])
	{
		$whereAnd = [
			'id' => $id
		];

		return $this->getFieldWhereAnd($table, $field, $whereAnd, $opt);
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
	function updateWhereAnd(string $table, array $whereAnd, array $set, array $opt = [])
	{
		list($set, $params) = $this->partSql($set, ', ');
		list($where, $params2) = $this->partSql($whereAnd, ' AND ');

		array_push($params, ...$params2);

		return $this->exec(
			'UPDATE
			  `'.$this->escapeName($table).'`
			SET
			  '.$set.'
			WHERE
			  '.$where.'
			'.$this->offsetLimit($opt),
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
	function updateByColumn(string $table, string $column, $val, array $set, array $opt = [])
	{
		$whereAnd = [
			$column => $val
		];

		return $this->updateWhereAnd($table, $whereAnd, $set, $opt);
	}

	/**
	 * @param string $table
	 * @param $id
	 * @param array $set
	 * @param array $opt
	 * @return bool
	 */
	function updateByID(string $table, $id, array $set, array $opt = [])
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
	 * @return mixed
	 */
	function deleteWhereAnd(string $table, array $whereAnd, array $opt = [])
	{
		list($part, $params) = $this->partSql($whereAnd, ' AND ');

		return $this->exec(
			'DELETE FROM
			  `'.$this->escapeName($table).'`
			WHERE
			  '.$part.'
			'.$this->offsetLimit($opt),
			$params,
			$this->getOpt($opt)
		);
	}

	/**
	 * @param string $table
	 * @param string $column
	 * @param $val
	 * @param array $opt
	 * @return mixed
	 */
	function deleteByColumn(string $table, string $column, $val, array $opt = [])
	{
		$whereAnd = [
			$column => $val
		];

		return $this->deleteWhereAnd($table, $whereAnd, $opt);
	}

	/**
	 * @param string $table
	 * @param $id
	 * @param array $opt
	 * @return mixed
	 */
	function deleteByID(string $table, $id, array $opt = [])
	{
		return $this->deleteByColumn($table, 'id', $id, $opt);
	}

	/////////////////////////////////
	/// Добавление
	/////////////////////////////////

	/**
	 * @param string $table
	 * @param array $setList
	 * @param array $opt     -insertID = true , -chunkSize = 100 , -columns = ['id','title']
	 * @return bool|int
	 */
	function insertList(string $table, array $setList, array $opt = [])
	{
		if (empty($setList) || ! is_array($setList)) {
			return false;
		}

		$chunkSize = $opt['chunkSize'] ?? false;

		$row = $setList[0] ?? [];
		$temp = '('.$this->valuesIn($row).')';

		$columns = (array) ($opt['columns'] ?? []);

		if (empty($columns)) {
			$columns = array_keys($row ?? []);
		}

		if ($chunkSize)
		{
			$setList = array_chunk($setList, $chunkSize);

			$this->transaction($opt);
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
			$parts = [];
			$params = [];

			foreach ($chunk as $item) {
				$parts []= $temp;
				array_push($params, ...array_values($item));
			}

			$res = $this->query(
				'INSERT INTO
				  `'.$this->escapeName($table).'` (
					'.$this->escapeNameArr($columns, true).'
				  )
				VALUES
				  '.implode(',', $parts).'
				'.$this->duplicKeyColumns($columns, $opt),
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
				$this->commit($opt);
			} else {
				$this->rollback($opt);
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

	function insertListDuplicKey(string $table, array $setList, array $columns = [], array $opt = [])
	{
		$opt['duplicKeyColumns'] = $columns;

		return $this->insertList($table, $setList, $opt);
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

	function insertDuplicKey(string $table, array $set, array $columns = [], array $opt = [])
	{
		$opt['duplicKeyColumns'] = $columns;

		return $this->insert($table, $set, $opt);
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
		$val = addslashes($val);
		return str_replace('`', '', $val);
	}

	/**
	 * @param array $arr
	 * @param bool $implode
	 * @return array|string
	 */
	function escapeNameArr(array $arr, bool $implode = false)
	{
		$arr = array_map(function ($item){
			return $this->escapeName($item);
		}, $arr);

		if ($implode) {
			$arr = '`'.implode('`,`', $arr).'`';
		}

		return $arr;
	}

	/**
	 * @param array $opt -limit = 10 , -offset = 20
	 * @return string
	 */
	function offsetLimit(array $opt): string
	{
		$limit = isset($opt['limit']) ? (int) $opt['limit'] : null;
		$offset = isset($opt['offset']) ? (int) $opt['offset'] : null;

		return ($limit ? 'LIMIT ' . ($offset ? $offset . ', ' : '') . $limit : '');
	}

	/**
	 * @param array $opt -fields = ['title','num']
	 * @return string
	 */
	function fields(array $opt): string
	{
		$fields = (array) ($opt['fields'] ?? ['*']);

		preg_match('~(,|SELECT)~i', implode($fields), $preg);

		if ($preg) {
			return '*';
		}

		return implode(', ', $fields);
	}

	/**
	 * @param array $opt -orderBy = ['id', 'desc'] or ['id', 1]
	 * @return string
	 */
	function orderBy(array $opt): string
	{
		$order = (array) ($opt['orderBy'] ?? []);

		list($column, $type) = array_pad($order, 2, '');

		$type = (mb_strtolower($type) == 'desc' || $type ? 'DESC' : 'ASC');

		return ($column ? 'ORDER BY `'.$this->escapeName($column).'` '.$type : '');
	}

	/**
	 * @param array $setColumns
	 * @param array $opt        -duplicKeyColumns = ['title','num']
	 * @return string
	 */
	function duplicKeyColumns(array $setColumns, array $opt): string
	{
		if (! isset($opt['duplicKeyColumns'])) {
			return '';
		}

		$dkc = (array) ($opt['duplicKeyColumns'] ?? []);

		if (empty($dkc)) {
			$dkc = $setColumns;
		}

		$dkc = array_map(
			function ($column) {
				return '`'.$this->escapeName($column).'` = VALUES(`'.$this->escapeName($column).'`)';
			},
			$dkc
		);

		return ($dkc ? 'ON DUPLICATE KEY UPDATE '.implode(', ', $dkc) : '');
	}

	/**
	 * Создание части sql запроса из массива
	 * @param array $data
	 * @param string $glue
	 * @return array
	 */
	function partSql(array $data = [], string $glue = ', '): array
	{
		$part_sql = [];
		$params = [];

		foreach ($data as $key => $value)
		{
			$key = '`'.$this->escapeName($key).'`';

			if (is_null($value)) {
				$part_sql []= $key.' = NULL';
			}
			elseif (is_array($value)) {
				$part_sql []= $key.' IN ('.$this->valuesIn($value).')';
				array_push($params, ...$value);
			}
			else {
				$part_sql []= $key.' = ?';
				$params []= $value;
			}
		}

		return [
			implode($glue, $part_sql),
			$params
		];
	}

	/////////////////////////////////
	/// Внутреннее
	/////////////////////////////////

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

	/**
	 * @param string $message
	 * @param array $opt
	 */
	protected function err(string $message, array $opt): void
	{
		$this->utils->dbg()->log2($message, $opt);
	}
}


<?php

/**
 * @author Oleg Isaev (PandCar)
 * @contacts vk.com/id50416641, t.me/pandcar, github.com/pandcar
 */

namespace XrTools;

/**
 * Class Transaction
 * @package XrTools
 */
class Transaction
{
	/**
	 * @var DBExt
	 */
	private $dbe;

	/**
	 * @var Utils
	 */
	private $utils;

	/**
	 * @var bool
	 */
	private $start = false;

	/**
	 * Transaction constructor.
	 * @param DBExt $dbe
	 * @param Utils $utils
	 * @param array $opt
	 */
	function __construct(
		DBExt $dbe,
		Utils $utils,
		array $opt = []
	){
		$this->dbe = $dbe;
		$this->utils = $utils;

		$res = $this->dbe->beginTransaction($opt);

		if ($res){
			$this->start = true;
		} else {
			$this->utils->dbg()->log2('The transaction did not start', $opt);
		}
	}

	/**
	 * @return bool
	 */
	function is(): bool
	{
		return $this->start;
	}

	/**
	 * @return bool
	 */
	function rollback(): bool
	{
		if (! $this->start) {
			return false;
		}

		$this->start = false;

		return $this->dbe->rollback();
	}

	/**
	 * @return bool
	 */
	function commit(): bool
	{
		if (! $this->start) {
			return false;
		}

		$this->start = false;

		return $this->dbe->commit();
	}

	/**
	 * Destruct
	 */
	function __destruct()
	{
		$this->rollback();
	}
}


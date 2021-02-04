<?php

/**
 * @author Oleg Isaev (PandCar)
 * @contacts vk.com/id50416641, t.me/pandcar, github.com/pandcar
 *
 * @var \XrTools\DBExt $dbe
 */

// Всё стандартно
//
// $dbe->exec() является обёрткой над $dbe->query() и возвращает только bool

$bool = $dbe->exec('SELECT * FROM `table`');

$res = $dbe->query('SELECT * FROM `table`');


$value = $dbe->fetchColumn('SELECT COUNT(`id`) FROM `table`');

$row = $dbe->fetchRow('SELECT * FROM `table` WHERE `id` = 123');

$list = $dbe->fetchArray('SELECT * FROM `table`');



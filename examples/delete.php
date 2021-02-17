<?php

/**
 * @author Oleg Isaev (PandCar)
 * @contacts vk.com/id50416641, t.me/pandcar, github.com/pandcar
 *
 * @var \XrTools\DBExt $dbe
 */

// Различные опции доступные для всех функций ниже
$opt = [
	'limit' => 20,
];

// Удаление по столбцу id
$bool = $dbe->deleteById('table', 123);

// Удаление по столбцу id
$bool = $dbe->deleteByColumn('table', 'id', 123);

// Удаление по столбцу id
$bool = $dbe->deleteWhereAnd('table', [
	'id' => 123
]);


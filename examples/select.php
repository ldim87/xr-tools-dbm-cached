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
	'offset' => 40,

	'groupBy' => [
		'type', 'id'
	],

	// true в значении эквивалентно DESC
	'orderBy' => [
		'type' => true
	],

	// Столбцы попадают в sql запрос в чистом виде,
	// потому в некоторых случаях может потребоваться экранирование кавычками
	'fields' => [
		'`id`',
		'`type`',
		'COUNT(*) as count_all'
	]

];


// Получение одной строки

$row = $dbe->getById('table', 123);

$row = $dbe->getRowByColumn('table', 'id', 123);

$row = $dbe->getRowWhereAnd('table', [
	'id' => 123
]);


// Получение списка

$list = $dbe->getByColumn('table', 'type', 9);

$list = $dbe->getWhereAnd('table', [
	'type' => 9
]);


// Получение одного столбца

$value = $dbe->getFieldWhereAnd('table', 'COUNT(`id`)', [
	'type' => 9
]);



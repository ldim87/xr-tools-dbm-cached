<?php

/**
 * @author Oleg Isaev (PandCar)
 * @contacts vk.com/id50416641, t.me/pandcar, github.com/pandcar
 *
 * @var \XrTools\DBExt $dbe
 */

// Различные опции доступные для всех функций ниже
$opt = [
	// Разобъёт запрос на транзакционные блоки
	// дабы не привышать лимит размера данных запроса
	'chunkSize' => 100,
];

// Добавление одной строки
// return int id or bool
$id = $dbe->insert('table', [
	'name' => 'Jack',
	'desc' => 'Big text'
]);

// Добавление нескольких строк
// return bool
$bool = $dbe->insertList('table', [
	[
		'name' => 'Jack',
		'desc' => 'Text about Jack'
	],
	[
		'name' => 'Rick',
		'desc' => 'Text about Rick'
	]
]);


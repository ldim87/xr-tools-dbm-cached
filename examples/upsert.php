<?php

/**
 * @author Oleg Isaev (PandCar)
 * @contacts vk.com/id50416641, t.me/pandcar, github.com/pandcar
 *
 * @var \XrTools\DBExt $dbe
 */

// Необязательный параметр $conflictUpdate у обоих функций
// принимает массив с именами столбцов для ON DUPLICATE KEY UPDATE
// Поумолчанию извлекаются все автоматически

// Различные опции доступные для всех функций ниже
$opt = [
	// Разобъёт запрос на транзакционные блоки
	// дабы не привышать лимит размера данных запроса
	'chunkSize' => 100,
];

// Добавление или обновление одной строки
// return int id or bool
$id = $dbe->upsert('table', [
	'name' => 'Jack',
	'desc' => 'Big text'
]);

// Добавление или обновление нескольких строк
// return bool
$bool = $dbe->upsertList('table', [
	[
		'name' => 'Jack',
		'desc' => 'Text about Jack'
	],
	[
		'name' => 'Rick',
		'desc' => 'Text about Rick'
	]
]);


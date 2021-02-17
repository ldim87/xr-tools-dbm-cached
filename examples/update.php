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

// Обновление по столбцу id
$bool = $dbe->updateById('table', 123, [
	'column' => 'value'
]);

// Обновление по столбцу id
$bool = $dbe->updateByColumn('table', 'id', 123, [
	'column' => 'value'
]);

// Обновление по столбцу id
$bool = $dbe->updateWhereAnd('table', [
	'id' => 123
], [
	'column' => 'value'
]);



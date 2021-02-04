<?php

/**
 * @author Oleg Isaev (PandCar)
 * @contacts vk.com/id50416641, t.me/pandcar, github.com/pandcar
 *
 * @var \XrTools\DBExt $dbe
 */

// После выполнения запросов на выборку, можно получить
// можно получить общее количество строк одной функцией.
//
// По сути она "помнит" последний sql с данными и опциями, и преобразует
// его в запрос типа COUNT(*).


// Пример 1

$list = $dbe->getWhereAnd('table', [
	'type' => 9
]);

$count = $dbe->getCalcFoundRows();


// Пример 2

$list = $dbe->fetchArray('SELECT * FROM `table` WHERE `type` = 9');

$count = $dbe->getCalcFoundRows();


// Упрощение для возврата массива count and items
// На вход принимает $list последней выборки
// И внутри себя выполняет $dbe->getCalcFoundRows()
// return [
// 	'count' => 10,
// 	'items' => [...]
// ]
$countAndItems = $dbe->formatCountAndItems($list);




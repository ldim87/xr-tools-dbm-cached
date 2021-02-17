<?php

/**
 * @author Oleg Isaev (PandCar)
 * @contacts vk.com/id50416641, t.me/pandcar, github.com/pandcar
 *
 * @var \XrTools\DBExt $dbe
 */

// $dbe->source() метод для создания left join-ов
//
// Описание идёт в определённом стиле
//
// Всё что в скобках принимается как алиас, то есть:
// name(user_name) эквивалентно `name` AS `user_name`
//
// Первым элементом массива идёт основная таблица, дальше связи
// Если значения нет или является true то принимается как эквивалент table.*
// Если значение массив, в него передаются столбцы
// ps. Это не полноценный "fields", принимаются только имена столбцов

$source = $dbe->source([
	'user(u)',
	'content(c) user_id = u.id' => ['name', 'info(meta_info)']
]);


$row = $dbe->getRowWhereAnd( $source, [
	'u.id' => 123
]);


$list = $dbe->getWhereAnd( $source, [
	'u.id' => 123
]);



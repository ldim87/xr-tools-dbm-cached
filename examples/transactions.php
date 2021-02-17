<?php

/**
 * @author Oleg Isaev (PandCar)
 * @contacts vk.com/id50416641, t.me/pandcar, github.com/pandcar
 *
 * @var \XrTools\DBExt $dbe
 */

// Поддерживаются многомерные транзакции
// НО!!!
// Транзакция ДОЛЖНА стартовать и закрываться в одном контексте
// То есть Если стартовала в начале функции, то завершение должно быть в конце
// Не в коем случае не допускать стиля типа: запускать в конструкторе класса и закрывать деструктором

$dbe->transaction();

$dbe->rollback();

$dbe->commit();


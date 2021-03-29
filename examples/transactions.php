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


// Стандартное использование

$dbe->beginTransaction();

$dbe->rollback();

$dbe->commit();


// Для использования внутри методов и функций
// Предполагает автоматическое срабатывание rollback() деструктором класса если не был вызван commit()

$transaction = $dbe->transaction();

$transaction->is();

$transaction->rollback();

$transaction->commit();


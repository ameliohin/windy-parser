Структура приложения задумана для запуска контейнере (докер, кубер) по одному инстансу на каждую иную модель.
Это позволит запускать и контролировать импорт из разных источников независимо.
Все переменные конфигурации могут определяться через env.

Приложение постоянно смотрит на список файлов в источнике и осуществляет импорт.

В работе приложения есть три стадии, которые выполняются последовательно:

- проверка и получение списка файлов
- скачивание и импорт, если появились новые данные
- запись данных по целевым директориям

Итерация расчета модели названа эпохой.

В силу синхронности pygrib параллелизм участка импорта формата grib основан на потоках.

В преобразовании формата также есть блокирующий код, но в силу условия вычитания разности,
эти операции невозможно выполнить независимо.

===============================================================================

В процессе работы над заданием сделаны следующие допущения: 

- Отсутствие тестов

- Конфигурация сделана исходя из анализа одной конкретной модели icon_d2

- Никак не осуществляется удаление старых файлов, информации о предыдущих эпохах

- В работе с целевым источником не допускается наличия в папке более одного расчета модели, 
чего не было до времени отправки ТЗ, в этом случае падает с ошибкой:
  File "windy-parser/downloader.py", line 53, in check_for_new_epoch
    raise Exception('More than one epoch in set!')
Exception: More than one epoch in set!

- Не проверено достоверно как ведёт себя np.substract с masked arrays, думаю это выходит за рамки ТЗ.

- Почему-то set_fill_value() не сработало и нулевые значения заполняются 9999.0, нужно отдельно разобраться.



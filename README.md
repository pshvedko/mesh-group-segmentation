пИдОРаМ из https://hh.ru/employer/2576051 не понравилось мое решение, сказали уровень джуниор =(

## MESH GROUP Golang тестовое задание

Требуется разработать go lang модуль (golang best practices)  
который будет импортировать данные из сторонней ERP системы в таблицу БД  
Таблица будет содержать такие поля -  
автоинкрементируемое уникальное поле  
address_sap_id (varchar 255)  
adr_segment (varchar 16)  
segment_id (bigint)

Кроме того, в схеме таблицы предусмотреть уникальность колонки address_sap_id  
При повторном импорте (при добавлении данных поверх существующих) предусмотреть обновление существующих данных (on conflict address_sap_id)

Модуль должен поддерживать возможность конфигурирования переменных при помощи расширения github.com/kelseyhightower/envconfig   
Конфигурационные переменные такие:  
DB_HOST - IP адрес DB сервера, значение по-умолч. "127.0.0.1"  
DB_PORT - TCP порт DB сервера, значение по-умолч. "5432"  
DB_NAME - Название DB, значение по-умолч. "mesh_group"  
DB_USER - Имя пользователя DB, , значение по-умолч. "postgres"  
DB_PASSWORD - Пароль пользователя DB, значение по-умолч. "postgres"

CONN_URI - Адрес для подключения к внешнему API, значение по-умолч. "http://bsm.api.iql.ru/ords/bsm/segmentation/get_segmentation"  
CONN_AUTH_LOGIN_PWD - Аутентификационные логин и пароль, значение по-умолч. "4Dfddf5:jKlljHGH"  
CONN_USER_AGENT - Юзер агент для подключения к САПу, значение по-умолч. "spacecount-test"  
CONN_TIMEOUT - Время таймаута подключения к внешнему API,в секундах, значение по-умолч. "5"  
CONN_INTERVAL - Задержка между получением очередной пачки данных из САП, значение по-умолч. 1500мс

IMPORT_BATCH_SIZE - Размер пачки данных для получения при каждом запросе к внешнему API, значение по-умолч. "50"  
LOG_CLEANUP_MAX_AGE - Время, после которого удаляются старые логи, в днях, значение по-умолч. "7"

При импорте данных нужно делать логгирование, по какому endpoint запрашивались данные, логгирование должно быть в консоль,а  
также в файл /log/segmentation_import.log  
При запуске модуля должна быть проверка - если в папке /log/ есть файлы старше LOG_CLEANUP_MAX_AGE дней, то удалять их.  
Если в процессе импорта (получение JSON данных,их добавление в БД) возникла какая либо ошибка - она должна логгироваться.

Подключение к БД при помощи библиотеки github.com/jmoiron/sqlx, данные для подключения брать из  
DB_HOST, DB_PORT, DB_NAME, DB_USER и DB_PASSWORD

При подключении к API эндпоинту нужно указывать User-Agent равным CONN_USER_AGENT,  
а также базово аутентифицироваться:  
Authorization: Basic XXX  
где XXX - это base64 кодировка CONN_AUTH_LOGIN_PWD

При подключении go http client должен иметь таймаут подключения в CONN_TIMEOUT милисекунд.

Эндпоинт для получения данных от ERP - в переменной CONN_URI (http://bsm.api.iql.ru/ords/bsm/segmentation/get_segmentation)  
Данные эндпоинт поддерживает 2 параметра:  
p_limit - лимит данных, берем из IMPORT_BATCH_SIZE  
p_offset - смещение  
Пример: [http://bsm.api.iql.ru/ords/bsm/segmentation/get_segmentation?p_limit=50&p_offset=1](http://bsm.api.iql.ru/ords/bsm/segmentation/get_segmentation?p_limit=50&p_offset=1) 

Получение данных нужно реализовать в цикле, при каждом обращении получать пачку данных размером IMPORT_BATCH_SIZE, то есть:  
http://bsm.api.iql.ru/ords/bsm/segmentation/get_segmentation?p_limit=50&p_offset=1  
http://bsm.api.iql.ru/ords/bsm/segmentation/get_segmentation?p_limit=50&p_offset=50  
http://bsm.api.iql.ru/ords/bsm/segmentation/get_segmentation?p_limit=50&p_offset=100  
http://bsm.api.iql.ru/ords/bsm/segmentation/get_segmentation?p_limit=50&p_offset=150 ...  
Получать данные до тех пор,пока в ответе (тело ответа) не будет пусто.

Реализация модуля должна содержать модель model/Segmentation.go  
которая использует библиотеку github.com/jmoiron/sqlx 

Выполненное задание должно содержать файл setup/install.sql  
в котором будет SQL-миграция создания таблицы segmentation c учётов требований выше.

Модуль должен иметь название sap_segmentation, а основной файл должен находиться по пути cmd/sap_segmentationd/main.go 

Необходимо использовать СУБД Postgres

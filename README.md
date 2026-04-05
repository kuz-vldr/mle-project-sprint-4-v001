# Подготовка виртуальной машины

## Склонируйте репозиторий

Склонируйте репозиторий проекта:

```
git clone https://github.com/yandex-praktikum/mle-project-sprint-4-v001.git
```

## Активируйте виртуальное окружение

Используйте то же самое виртуальное окружение, что и созданное для работы с уроками. Если его не существует, то его следует создать.

Создать новое виртуальное окружение можно командой:

```
python3 -m venv env_recsys_start
```

После его инициализации следующей командой

```
. env_recsys_start/bin/activate
```

установите в него необходимые Python-пакеты следующей командой

```
pip install -r requirements.txt
```

### Скачайте файлы с данными

Для начала работы понадобится три файла с данными:
- [tracks.parquet](https://storage.yandexcloud.net/mle-data/ym/tracks.parquet)
- [catalog_names.parquet](https://storage.yandexcloud.net/mle-data/ym/catalog_names.parquet)
- [interactions.parquet](https://storage.yandexcloud.net/mle-data/ym/interactions.parquet)
 
Скачайте их в директорию локального репозитория. Для удобства вы можете воспользоваться командой wget:

```
wget https://storage.yandexcloud.net/mle-data/ym/tracks.parquet

wget https://storage.yandexcloud.net/mle-data/ym/catalog_names.parquet

wget https://storage.yandexcloud.net/mle-data/ym/interactions.parquet
```

## Запустите Jupyter Lab

Запустите Jupyter Lab в командной строке

```
jupyter lab --ip=0.0.0.0 --no-browser
```

# Расчёт рекомендаций

Код для выполнения первой части проекта находится в файле `recommendations.ipynb`. Изначально, это шаблон. Используйте его для выполнения первой части проекта.

# Сервис рекомендаций

**Рабочая директория:** все команды ниже выполняются из корня репозитория `mle-project-sprint-4-v001`.

Код сервиса рекомендаций находится в файле **`recommendations_service.py`**.

Данные для сервиса: **`data/prepared/events.parquet`**, **`data/recommendations/top_popular.parquet`**, персональный ALS — **`data/recommendations/als_recommendations.parquet`**.
(Данные появятся после выполнения всех ячеек `recommendations.ipynb`.)
## Стратегия смешивания

Сервис выбирает одну из трёх веток (в ответе поле `strategy`):

- **Нет персональных рекомендаций (ALS)** — отдаём топ-популярное, убирая треки, которые пользователь уже слушал (по `events`).
- **Есть ALS, но нет истории в `events`** — отдаём только персональный список ALS.
- **Есть и ALS, и история** (`strategy`: **`hybrid_interleaved`**) — **чередование**: по очереди берётся трек из персонального ALS и трек из топ-популярного (уже без прослушанного из `events`). Повторы пропускаются. Если до `k` не хватает — добор по порядку из популярного.

## Инструкции для запуска сервиса рекомендаций

```bash
# из корня mle-project-sprint-4-v001
uvicorn recommendations_service:app --reload --port 8000 --host 0.0.0.0
```

По умолчанию в коде также можно запустить:

```bash
python3 recommendations_service.py
```

Документация API в браузере: `http://127.0.0.1:8000/docs`.

## Инструкции для тестирования сервиса

Код для тестирования находится в файле **`test_service.py`**.

```bash
python3 test_service.py
```

Сохранить вывод тестов в файл:

```bash
python3 test_service.py 2>&1 | tee test_service.log
```

## Примеры curl-запросов

Проверка:

```bash
curl -s "http://localhost:8000/health"
```

Рекомендации (**GET**; полный гибрид офлайн + онлайн по истории из `events.parquet`):

```bash
curl -s "http://localhost:8000/recommendations?user_id=1291250&k=10"
```

Рекомендации (**POST**, тело JSON):

```bash
curl -s -X POST "http://localhost:8000/recommendations" \
  -H "accept: application/json" \
  -H "Content-Type: application/json" \
  -d '{"user_id": 1291250, "k": 10}'
```

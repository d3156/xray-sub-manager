# xray-sub-manager

`xray-sub-manager` — сервис на Rust для загрузки подписок Xray/V2Ray, парсинга популярных форматов, дедупликации нод, проверки доступности через асинхронный TCP connect, проверки поднятия тоннеля через `sing-box` и публикации итоговой подписки через веб-панель и `/sub`-эндпоинт.

## Возможности

- Поддержка base64-списков URI, plaintext-списков, sing-box JSON и SIP008 JSON
- Поддержка протоколов `vmess`, `vless`, `trojan`, `ss`, `ssr`, `hysteria2`, `tuic`
- Асинхронный планировщик обновлений с ручным запуском, кешем и graceful shutdown
- Сортировка итоговых нод по задержке поднятого тоннеля и выдача top-N через `limit` в `/sub`
- Встроенный одностраничный веб-интерфейс без внешних зависимостей
- Атомарное сохранение `config.json` и кеша итоговой подписки

![Пример развертывания](images/image.png)

## Запуск локально

```bash
cargo run --release
```

По умолчанию приложение использует конфиг:

```text
/opt/xray-sub-manager/config.json
```

Путь можно переопределить через `CONFIG_PATH` или аргумент командной строки:

```bash
CONFIG_PATH=/path/to/config.json cargo run --release
# или
cargo run --release -- /path/to/config.json
```

Если конфиг отсутствует, приложение создаёт его автоматически и генерирует:

- `admin_token` — токен входа в веб-панель
- `subscription_token` — токен для `/sub?token=...`

## Веб-панель

- Откройте `http://127.0.0.1:8080/`
- Авторизуйтесь с помощью `admin_token` из `config.json`
- Добавьте URL подписок, сохраните настройки и запустите `Update Now`

## Эндпоинт подписки

После успешного обновления итоговая base64-подписка доступна по адресу:

```text
http://127.0.0.1:8080/sub?token=<subscription_token>&limit=50
```

`limit` определяет, сколько самых быстрых по задержке тоннеля нод попадёт в экспорт.

Если кеш ещё не сформирован, `/sub` вернёт `503 Service Unavailable`.

## Установка как systemd-сервис

```bash
sudo ./install.sh
```

Скрипт установки для Ubuntu:

- автоматически устанавливает необходимые пакеты (`cargo`, `rustc`, `python3`, `build-essential`, `pkg-config`, `libssl-dev` и другие)
- собирает release-бинарь
- устанавливает бинарь в `/opt/xray-sub-manager/bin/xray-sub-manager`
- копирует `static/index.html` в `/opt/xray-sub-manager/static/index.html`
- создаёт конфиг по умолчанию в `/opt/xray-sub-manager/config.json`, если его ещё нет
- создаёт и запускает systemd-сервис `xray-sub-manager.service`
- выводит URL панели и токены из `/opt/xray-sub-manager/config.json`

## Структура файлов после установки

- Бинарь: `/opt/xray-sub-manager/bin/xray-sub-manager`
- Конфиг: `/opt/xray-sub-manager/config.json`
- Кеш подписки: `/opt/xray-sub-manager/subscription.cache`
- Статика: `/opt/xray-sub-manager/static/index.html`

## Примечания

- Проверка нод выполняется через TCP connect, а затем через локально поднятый `sing-box`-тоннель
- Для этапа проверки тоннелей в системе должен быть доступен `sing-box` (путь можно переопределить через `SING_BOX_BIN`)
- Веб-интерфейс вшит в бинарь через `include_str!`
- Уровень логов можно настроить через `RUST_LOG`, например `RUST_LOG=info cargo run --release`

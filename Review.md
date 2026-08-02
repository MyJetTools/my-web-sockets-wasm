# Code review — `my-web-sockets-wasm`

Ревью коммита `431a316` (первая версия крейта).

Проверено: `cargo check --target wasm32-unknown-unknown` проходит чисто, **0 warnings**.
Всё, что ниже, — семантика и документация, а не компиляция. Отдельно скомпилированы
примеры из README (см. раздел «README против кода»).

---

## Что сделано правильно

- **`Drop for ManagedWs`** (`src/managed_ws.rs:156-168`) — отцепление всех четырёх листенеров
  **до** освобождения `Closure`. Это действительно лечит баг gloo-net / reqwasm
  («closure invoked recursively or after being dropped») и является главной причиной
  существования крейта.
- **`performance.now()`** вместо `Date.now()` для измерения таймаутов — верно.
- **Раздельные control / data каналы** — правильная идея (см., впрочем, баг №5 про приоритет).
- **Cancellation-safety корректна**: `gloo_timers::TimeoutFuture` отменяется на drop,
  `StreamExt::next` не теряет сообщение при проигрыше в `select!`.
- Стамп `last_msg_ms` **после** `on_data` — осознанный и задокументированный компромисс.

---

## Реальные баги

### 1. `stop()` → `start()` = два живых цикла и два сокета (high)

`src/ws_client.rs:51-61`. `start()` безусловно делает `working.set(true)` + `spawn(...)`.
Нет ни generation-счётчика, ни сохранённого `Task`. Флаг читается ровно в двух местах
(`:90`, `:161`), а между ними цикл паркуется надолго: `sleep(3s)`, `wait_for_open` (до 10 с),
`on_connected` / `on_disconnected` (await приложения), таймер `select!` (до 13 с).

**Сценарий:** `stop()` + сразу `start()` при рефреше токена. Старый цикл просыпается через
~10 с, читает уже заново выставленный `true` и продолжает жить. Приложение получает каждый
фрейм **дважды** и держит два сокета. Каждый цикл stop/start добавляет ещё один.

Отдельно плохо то, что rustdoc сам это рекомендует:
*«calling it again after `stop()` resumes the loop»* (`src/ws_client.rs:49-50`).

**Фикс:** `generation: Cell<u64>` в `WebSocketClientInner`; `start()` инкрементит и захватывает
своё поколение, оба условия цикла становятся
`inner.generation.get() == my_gen && inner.is_working()`; `stop()` тоже бампает поколение.
Альтернатива — хранить `Task`, возвращаемый `spawn`, и звать `cancel()` в `stop()`.

### 2. `stop()` никого не будит (medium-high)

`src/ws_client.rs:59-61` — только `working.set(false)`. Последствия:

- На молчащем сокете браузерный WebSocket остаётся **OPEN до 13 секунд** после `stop()`
  (закрывается только в `ManagedWs::drop`, когда `managed` дропается на `:129`).
- Ещё один `on_data` может прилететь уже **после** `stop()` — `:204` не перепроверяет
  `is_working()` перед вызовом колбэка.
- Между backoff-сном `:92` и `ManagedWs::open` `:104` проверки нет вообще: `stop()` во время
  3-секундного backoff приводит к тому, что цикл **откроет новое соединение**, прополлит его
  до 10 с и вызовет `on_connected` + `on_disconnected` — всё это уже после `stop()`.

**Фикс:** oneshot / mpsc как третья ветка `select!` + гонка с backoff-сном и `wait_for_open`;
минимум — перепроверять `is_working()` перед `open()` и перед `on_data`.

### 3. Зависший `on_connected` вешает клиента навсегда (medium)

`src/ws_client.rs:120` — `await` без таймаута и без гонки с чем-либо. `init_timeout` его не
покрывает: `started_ms` берётся уже внутри `run_read_loop` (`:158`). Если пользовательский
`on_connected` ждёт рефреш токена, который не приходит, цикл мёртв, и `stop()` не помогает.

### 4. `wait_for_open` считает номинальные тики, а не время (medium)

`src/ws_client.rs:133-148`: `elapsed += poll` (100 мс). В фоновой вкладке `setTimeout`
троттлится до 1 с (при интенсивном троттлинге — до минуты), так что «10-секундный»
`connect_timeout` растягивается до минут. Нужно мерить `now_ms()`, как в read-loop.
Плюс `wait_for_open` не смотрит на `is_working()`.

### 5. Приоритет control-канала молча съедает хвост данных (medium)

`src/managed_ws.rs:143-152` — control проверяется первым **всегда**. Когда сервер шлёт
10 фреймов и сразу close, все 11 событий успевают попасть в каналы до того, как проснётся
Rust-таск; `poll_next` возвращает `Err(Closed)`, read-loop делает `break`, и **10 забуференных
фреймов теряются молча**.

Мотивация («close не должен стоять за данными») валидна только для `SlowConsumer`.
Для `Closed` / `ConnectionError` правильнее сначала осушить `data_rx` и отдавать control
только когда данные вернули `Pending`.

Смежное: фрейм, переполнивший буфер, тоже выбрасывается (`src/managed_ws.rs:81`), и любой
не-`String` / не-`ArrayBuffer` фрейм дропается **без единого лога** (`src/managed_ws.rs:77`).

### 6. Путь `stop()` оставляет `is_connected() == true` (medium)

Все выходы из read-loop делают `conn.disconnect()` — кроме выхода по `!is_working()`
(`src/ws_client.rs:161`). В итоге `on_disconnected` получает соединение, которое считает себя
живым, а сохранённый `ws_handle` продолжает «успешно» слать в закрытый сокет: браузер в
состоянии CLOSED не бросает исключение, а молча отбрасывает данные.

### 7. Приложение не может узнать причину дисконнекта (design gap)

`WsError::SlowConsumer` / `ConnectionError` / `Closed { code, reason }` строятся, но никогда
не покидают крейт — только `console_log`. `on_disconnected(conn)` не принимает причину.
Для трейдингового терминала «1006 vs 4001 invalid token» — разные реакции. Это не баг,
но дыру стоит закрыть до того, как крейт разойдётся по проектам.

### 8. Мелкая гонка в `select!` (low)

`futures::select!` рандомизирует порядок веток. Ровно на границе таймаута готовый фрейм может
проиграть таймеру → лишний реконнект. Окно узкое; при желании — `select_biased!` с `next_msg`
первой веткой.

---

## README против кода

Все пункты перепроверены по исходникам; пп. 7-8 — скомпилированы.

| # | README | Реальность |
|---|---|---|
| 1 | `:328` «`stop()` — **Terminal**, рестарт невозможен» | `src/ws_client.rs:49-58` говорит ровно обратное, и код рестарт разрешает — вместе с багом №1 |
| 2 | `:183-184` «работает, пока не будет dropped» | `Drop` не реализован. Жизнь цикла привязана к Dioxus-скоупу (`dioxus-core` убивает `spawned_tasks` при unmount — `runtime.rs:187`), а не к `WebSocketClient`. Дроп самой структуры не делает **ничего** |
| 3 | `:41-44` + quick start: «a component body» — валидное место для `start()` | Каждый ре-рендер = новый клиент + новый цикл + новый сокет. Нужен `use_hook` / `use_coroutine` / `use_effect(once)` |
| 4 | `:26` таблица: «Binary send: via raw handle only» | `WsConnection::send_bytes` существует и документирован тут же ниже |
| 5 | `:287` «never silently drops or reorders frames» | Неверно: см. баг №5 — дропается и переполнивший фрейм, и весь хвост буфера при close |
| 6 | `:364` «sleep/resume не вызывает spurious reconnects» | Поведение `performance.now()` при suspend платформозависимо; и после resume реконнект как раз **нужен**. Переобещание |
| 7 | `:141-181` quick start | **Не компилируется**: 3× `E0433: cannot find crate log` (нет в deps сниппета). После добавления `log` — `warning: unused Result that must be used` на `conn.send_text(...)` |
| 8 | `:197-200` `conn.send_text("ping")?;` | В колбэке не компилируется: `E0277: the trait From<WsError> is not implemented for ()` |
| 9 | `:65` диаграмма | Нет ребра для мгновенного отказа. `wait_for_open` возвращает `false` и по таймауту, и по CLOSED, а лог всегда пишет `WS: connect timeout` — при опечатке в URL это печатается каждые 3 с и уводит в ложном направлении |

Расхождения в rustdoc:

- `src/ws_callback.rs:32` — «You must call `mark_initialized` **here**» (в `on_data`) против
  README `:134-135`, который разрешает `on_connected`. Код никаких ограничений не накладывает.
- `src/error.rs:20` — «while the connection was not open»: на деле проверяется логический флаг
  `is_connected`, а не `readyState`. Сокет в CLOSING/CLOSED без вызова `disconnect()` пройдёт
  guard и вернёт `SendFailed`, а не `NotConnected`.

---

## Cargo.toml

- **`wasm-bindgen = "*"`, `web-sys = "*"`, `js-sys = "*"`, `futures = "*"`** — для WASM особенно
  плохо: версия `wasm-bindgen` обязана совпадать с версией `wasm-bindgen-cli` у потребителя.
  `*` рано или поздно даст classic mismatch-ошибку у того, кто подключит крейт.
  Нужны caret-диапазоны.
- **`license = "MIT"`, но LICENSE-файла в репозитории нет** (только `.gitignore`, `Cargo.lock`,
  `Cargo.toml`, `README.md`, `src/`). README `:372-374` ещё и хеджирует
  «MIT unless stated otherwise», а `Cargo.toml` заявляет безусловно.
- **Полный publish-набор метаданных, но `cargo publish` невозможен** — git-зависимость
  `dioxus-utils` + wildcards. Либо `publish = false`, либо доводить до публикуемого состояния.
- **Ни одного теста, ни CI.** Для крейта, который шарится между `mt-client` и `mt-admin`,
  стоит хотя бы `wasm-bindgen-test` на `ManagedWs` и на арифметику таймаутов.

---

## Мелочи

- `_on_open` — пустая заглушка (`src/managed_ws.rs:62`). Через oneshot из `onopen` можно убрать
  100-мс поллинг в `wait_for_open` целиком.
- `INCOMING_BUFFER_SIZE = 1024` → реальная ёмкость 1025 (`futures` mpsc даёт `n + 1`
  на отправителя).
- `WebSocketClient` не `Clone`, хотя внутри просто `Rc` (проверено: `E0308`).
  `Debug` нет ни на одном публичном типе.
- Ветка `Poll::Ready(None)` недостижима — сендеры живут в замыканиях внутри `ManagedWs`,
  так что `src/ws_client.rs:198` — мёртвый код.

---

## Резюме

Ядро — `ManagedWs` и его `Drop` — сделано правильно и решает настоящую проблему.
Дыры сосредоточены в **управлении жизненным циклом**: `start` / `stop` / `Drop` не образуют
согласованную модель, а README описывает три взаимоисключающие её версии.

Порядок починки:

1. Generation-счётчик (или `Task::cancel`) в `start` / `stop` + проверки `is_working()`
   перед `open()` и перед `on_data`.
2. Будильник для `stop()` (oneshot в `select!`).
3. README: убрать «component body», починить `stop()` / drop / binary send / «never drops»,
   добавить `log` в сниппет.
4. Зафиксировать версии зависимостей, добавить LICENSE.
5. Дренаж `data_rx` перед control-ошибками + передача причины дисконнекта в `on_disconnected`.

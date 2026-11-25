# Tapio

Tapio は、Java 25 で Apache Pekko Classic Actor のローカルランタイムを読み解き、段階的に移植するプロジェクトです。実装は [Pekko のコミット `85ab376242`](https://github.com/apache/pekko/tree/85ab376242152046740a63307304a2e062990f67/actor/src/main) を基準にし、2 つのコードベースを並べて読めるよう、上流のクラス名、メソッド名、呼び出し順序をできるだけ維持しています。

Tapio は現在、移植の初期段階にあります。ソースコードの学習と継続的な実装を目的としており、Pekko の全機能を提供するものではありません。

[English](README.md) | [简体中文](README.zh-CN.md) | [日本語](README.ja.md)

## 対象範囲

現在のモジュールは `tapio-actor` です。ローカル Actor の作成、メッセージ Envelope、Mailbox と Dispatcher、基本的なライフサイクル処理、基本的な One-for-One supervision、DeathWatch メッセージ、`Patterns.ask`、一回限りのスケジューリング、初期のログイベント対応を含みます。

次の領域は、まだ上流実装と差異があります。

- `ChildrenContainer` は Pekko に合わせて empty、normal、waiting-for-termination、terminated の状態に分割されています。restart と termination の後も lifecycle 処理を継続し、`ChildRestartStats` は child UID、再試行回数、時間ウィンドウを追跡します。
- supervision の経路は、Pekko のデフォルト例外分類、One-for-One と All-for-One supervision、再試行制限、escalation、失敗ログに対応しています。guardian の supervision lifecycle 全体はまだ移植されていません。
- 一回限りのスケジューリングには `LightArrayRevolverScheduler` を使用し、設定は `pekko.scheduler` にあります。周期的なスケジューリング API はまだ移植されていません。
- ローカルの `Patterns.ask` はシステム Scheduler と一時的な `PromiseActorRef` を使用します。Classic の `Status.Success` / `Status.Failure`、一時参照の DeathWatch、遅れて到着した応答の dead letters、Java `CompletionStage` を返すミリ秒単位のオーバーロードに対応しています。
- EventStream は ActorRef の購読者と class channel を使用し、channel の親子型関係に従って配信します。デフォルトの dead letters ロガーはカウントと一時停止に対応しています。system guardian、`systemActorOf`、`CoordinatedShutdown` の phase 終了、JVM shutdown hook、`LogMarker`/MDC bridge はローカルランタイムに含まれています。`System.exit`、クラスタタスク、完全なログサービスはまだ移植されていません。receive timeout、ActorSelection、Stash、routing、serialization、I/O、Typed、Remote、Sharding は未実装です。

上流の各概念に対応するソース位置と Java 実装の対応関係は [docs/actor-porting.md](docs/actor-porting.md) に記録しています。

## 必要環境

- JDK 25
- Maven 3.9 以降

このリポジトリには Maven Wrapper はありません。`java -version` と `mvn -version` を確認した後、リポジトリのルートで次を実行してください。

```shell
mvn test
```

単体テストはローカルランタイムの基本的な呼び出し経路を対象としており、Pekko のテストスイートに合格したことを意味しません。テストを実行せずにパッケージする場合は `mvn -DskipTests package` を実行します。

## ActorSystem の設定

`ActorSystem.create(name)` は `application.conf` と `reference.conf` を読み込みます。`ActorSystem.create(name, config)` は渡された設定を使用し、`reference.conf` からデフォルト値を補います。有効な設定は `system.settings().config()` から取得できます。

`pekko.actor.default-dispatcher.throughput` のデフォルト値は `5` で、1 回のスケジューリングバッチで Mailbox が処理する通常メッセージ数を制御します。
`pekko.actor.default-dispatcher.throughput-deadline-time` のデフォルト値は `0ms` で、バッチの時間制限を無効にします。
`system.dispatchers().lookup(id)` では名前付きの通常 Dispatcher を検索でき、複数の alias から同じインスタンスを参照できます。Actor の作成には現在、デフォルト Dispatcher を使用します。

## Logging

`ActorSystem` と `AbstractActor` は `log()` を提供し、`debug`、`info`、`warning`、`error` と `{}` テンプレートを利用できます。ログ呼び出しは source path、source class、level、message、cause を含む `LogEvent` になってから `ActorSystem.eventStream()` に公開されます。デフォルトの SLF4J logger Actor はメッセージの前に source path を付加します。アプリケーションは ActorRef と `LogEvent.class` を使って構造化イベントを購読できます。

ランタイムは `slf4j-api` のみに依存し、ログ実装を同梱しません。アプリケーション側で Logback、Log4j2、SLF4J Simple、JUL provider などの SLF4J provider を実行時に追加し、レベルと出力形式を設定してください。provider がない場合、SLF4J はその旨を通知して NOP logger を使用します。

ActorSystem は `application.conf` から Pekko と互換性のあるログ設定を読み込みます。

```hocon
pekko.loglevel = "DEBUG"
pekko.actor.debug.lifecycle = on
pekko.actor.debug.unhandled = on
```

ライフサイクルと未処理メッセージの DEBUG ログを出力するには、`pekko.loglevel` を `DEBUG` に設定し、選択した SLF4J provider で該当 logger の DEBUG を有効にします。従来の `pekko.actor.loglevel` キーもフォールバックとして受け付けます。ライフサイクルログには Actor の作成、supervision、restart、stop、DeathWatch の変更が含まれ、デフォルトでは無効です。未処理メッセージは常に `UnhandledMessage` として公開され、`debug.unhandled = on` にすると DEBUG ログにもなります。`debug.receive` と `debug.autoreceive` のキーは保持されていますが、関連する receive decorator と AutoReceive API はまだ移植されていません。

## モジュール構成

```text
tapio-actor/
└── src/main/
    ├── java/io/github/espen7/tapio/
    │   ├── actor/       # ActorSystem、ActorRef、ActorCell、ライフサイクル
    │   ├── dispatch/    # Dispatcher、Mailbox、Envelope、SystemMessage
    │   ├── event/       # LogEvent、EventStream、LoggingAdapter、SLF4J bridge
    │   ├── japi/        # Java Creator と ReceiveBuilder
    │   └── pattern/     # ask パターン
    └── resources/
        └── reference.conf
```

Java パッケージは Pekko の相対階層に合わせ、`io.github.espen7.tapio` をプレフィックスとして使用します。エントリポイントとクラスの対応関係は Pekko のコミット `85ab376242` を基準にしています。

## ライセンス

Tapio は Apache License 2.0 で公開しています。詳細は [LICENSE](LICENSE) を参照してください。上流コードの参照元を記録し、Pekko から直接派生またはコピーしたコードには、適用される著作権、ライセンス、NOTICE 情報を残してください。

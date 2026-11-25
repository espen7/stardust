# Tapio

**高並行分散システム向けの軽量な Java 25 Actor プラットフォーム。**

[![Java 25](https://img.shields.io/badge/Java-25-orange.svg)](https://openjdk.org/projects/jdk/25/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

Tapio は、Akka 2.6 と Apache Pekko の基盤にある Classic Actor モデルを Java ネイティブに書き直すプロジェクトです。最新の Java を前提に小さな runtime と仮想スレッドを中心とした並行実行モデルを提供し、ゲームやその他の高並行サービスに必要な分散コンポーネントへ集中します。

Tapio は学びやすく、組み込みやすく、公開 API から runtime の実装まで Java 開発者が読みやすいことを目指します。Akka エコシステムの有用な部分を残しながら、すべてのサービスに必要ではない大規模な統合や抽象化は含めません。

[English](README.md) | [简体中文](README.zh-CN.md) | [日本語](README.ja.md)

## Akka、Pekko、Tapio

Akka 2.6 は Classic Actor モデルの API と設計の系譜です。Apache Pekko は、プロジェクトの方向性が変わった後に生まれた Akka の Apache-2.0 コミュニティ fork であり、その継続プロジェクトです。Tapio はこの系譜から着想を得た独立した Java 実装です。

- `ActorSystem`、`ActorRef`、Mailbox、supervision、EventStream、`ask` など、なじみのある Actor の概念を維持します。
- Java 25 API を基盤に、仮想スレッドを含む現代的な並行処理を意識して runtime を設計します。
- Akka や Pekko の全エコシステムを複製せず、意図的に範囲を絞ります。

Tapio は Lightbend や Apache Pekko プロジェクトと関係がなく、Akka または Pekko のバイナリ互換代替ではありません。

## Tapio の目的

- **Java ネイティブ**：アプリケーションと runtime の開発に Scala ツールチェーンを必要としません。
- **現代的な並行処理**：Java 25 を基準にし、スケーラブルなワークロードでは仮想スレッドを中心に考えます。
- **小さく集中した構成**：Actor runtime と実サービスに必要な分散コンポーネントを残します。
- **読みやすい実装**：明確な Java コードと Classic Actor モデルにより、学習とソースコード分析を容易にします。
- **高並行処理向け**：ゲームサーバー、ゲートウェイ、シミュレーションなどの状態を持つサービスの基盤になります。

## 範囲とステータス

Tapio は現在、初期開発段階です。最初の安定版までは公開 API と設定が変更される可能性があります。

| 領域 | ステータス |
| --- | --- |
| ローカル Classic Actor runtime | `tapio-actor` で提供中 |
| Actor lifecycle、Mailbox、supervision、DeathWatch | `tapio-actor` で提供中 |
| 通常 Dispatcher のライフサイクル、lazy executor の再生成、inhabitants、アイドル時 shutdown | `tapio-actor` で提供中 |
| Dispatcher/Mailbox の Props・デプロイ設定による結合、無制限/上限付き mailbox | `tapio-actor` で提供中 |
| EventStream、構造化ログ、`ask`、一回限りのスケジューリング | `tapio-actor` で提供中 |
| Routing | 計画中 |
| Cluster Sharding | 計画中 |
| Kubernetes によるクラスタディスカバリ | 計画中 |
| クラスタ機能に必要な transport と membership | Tapio の限定的なコンポーネントとして計画中 |

「提供中」は、ローカル runtime の契約が実装され、Tapio のテストでカバーされていることを意味します。Pekko ソースとの完全な一致や、Pekko の全テストスイートの通過を意味するものではありません。internal と PinnedDispatcher、executor 設定、高度な mailbox、周期スケジューリング、完全なロギングライフサイクルは未実装のままです。

初期範囲には、Akka/Pekko の完全なエコシステムは含めません。Typed API、Streams、Persistence、大規模な serialization 統合、完全な remote 互換性、無関係なエンタープライズ統合は対象外です。Actor プラットフォームと対象ワークロードに直接役立つ機能を優先します。

## クイックスタート

### 必要環境

- JDK 25
- Maven 3.9 以降

### ソースからビルド

```shell
git clone https://github.com/espen7/tapio.git
cd tapio
mvn test
```

テストを実行せずに Actor モジュールをパッケージする場合：

```shell
mvn -DskipTests package
```

### 最小 Actor の例

```java
import io.github.espen7.tapio.actor.AbstractActor;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.pattern.Patterns;

import java.time.Duration;

public final class EchoActor extends AbstractActor {
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(String.class, message -> getSender().tell(message, getSelf()))
                .build();
    }

    public static void main(String[] args) {
        try (ActorSystem system = ActorSystem.create("example")) {
            ActorRef echo = system.actorOf(Props.create(EchoActor.class), "echo");
            Object reply = Patterns.ask(echo, "hello", Duration.ofSeconds(1))
                    .toCompletableFuture()
                    .join();
            System.out.println(reply);
        }
    }
}
```

現在はソースからビルドします。最初のリリース時に正式な依存関係座標を追加します。

## アーキテクチャ

ローカルの Actor ツリーはルート guardian（`/`）から始まります。その `/user` と `/system` の子が、アプリケーション Actor とシステム Actor をそれぞれ監督します。ルートは、一時参照用の `/temp` と、配信できなかったメッセージの受け先 `/deadLetters` も解決します。

`ActorSystem` と `ActorContext` は `ActorRefFactory` を実装し、名前付きおよび自動命名による Actor 生成、ローカル ActorSelection、非同期 stop を提供します。システム経由でアタッチされた Actor は、guardian が mailbox を起動するまでメッセージをバッファします。

```text
tapio-actor/
└── src/main/
    ├── java/io/github/espen7/tapio/
    │   ├── actor/       # ActorSystem、ActorRef、lifecycle、supervision
    │   ├── dispatch/    # Dispatcher、Mailbox、Envelope、system message
    │   ├── event/       # EventStream、LogEvent、SLF4J bridge
    │   ├── japi/        # Java Creator と ReceiveBuilder API
    │   └── pattern/     # ask パターン
    └── resources/       # reference.conf のデフォルト設定
```

現在の `tapio-actor` はローカル runtime を提供します。Routing、Sharding、Kubernetes ディスカバリは同じ Actor モデルを中心とした限定的なモジュールとして追加し、Pekko の全エコシステムを要求しない構成にします。

## 設定とログ

`ActorSystem.create(name)` は `application.conf` とモジュールの `reference.conf` を読み込みます。runtime は `ActorSystem` を通じて名前付き Dispatcher、Scheduler、協調シャットダウン、dead-letter ログ、SLF4J ベースの構造化イベントを提供します。

Tapio は `slf4j-api` のみに依存します。アプリケーション側で Logback、Log4j2、SLF4J Simple、JUL provider などを選択して設定してください。

詳細な設定とソースレベルの設計記録は [docs/actor-porting.md](docs/actor-porting.md) を参照してください。

## ロードマップ

1. ローカル Actor runtime を完成させ、公開 API を安定化します。
2. サービスやゲームサーバーの一般的なトポロジー向け Routing を追加します。
3. クラスタ membership と Kubernetes ディスカバリを追加します。
4. 状態を持つ Actor の水平スケールに Cluster Sharding を追加します。
5. 実際の高並行ワークロードで Java 25 と仮想スレッドの実行モデルを検証します。

## コントリビューション

Issue と Pull Request を歓迎します。大きな API または runtime の変更は、動作と互換性への影響を話し合うため、先に Issue を開いてください。新しい runtime の動作には絞った JUnit 5 テストを追加し、公開契約が変わる場合はドキュメントも更新してください。

## ソースの系譜とライセンス

Tapio は [Apache License 2.0](LICENSE) で公開しています。Actor runtime は Classic Actor のセマンティクスに従い、Apache Pekko を実装上の参考にしています。ソースの対応関係、Java 固有の適応、適用される上流 NOTICE は [docs/actor-porting.md](docs/actor-porting.md) とソースヘッダーに記録しています。

Tapio は独立したプロジェクトであり、Lightbend、Akka、Apache Pekko による承認や所属関係はありません。

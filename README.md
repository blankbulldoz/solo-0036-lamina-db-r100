# LaminaDB

LaminaDB 是一个 Go 实现的单进程关系数据库内核和 HTTP 服务，面向数据库工程学习、存储实验和可复现的语义研究。它实现自己的类型系统、快照版本、提交校验、二级索引、查询执行、日志和恢复；不依赖 SQLite、PostgreSQL 或在线服务。

领域：**基础设施 / 数据库**。主要技术：Go 标准库、HTTP、版本链、CRC32 帧日志、SHA-256 图像。

## 构建和启动

要求 Go 1.25+，Linux 或 macOS。生产工作流演示另外使用 Python 3 标准库。

```sh
go build -o bin/laminad ./cmd/laminad
./bin/laminad -data var/lamina -listen 127.0.0.1:7386
```

数据目录只能由一个进程打开。默认仅监听回环地址；服务没有身份认证，不应直接暴露在不可信网络。进程收到 SIGTERM/SIGINT 后停止接受新请求，等待 HTTP 请求结束，再关闭数据库。没有在退出时强制生成检查点，已成功返回的提交依赖日志恢复。

```sh
python3 examples/walkthrough.py --workflow all
```

该命令启动临时实例，执行 17 条真实生产入口工作流，输出逐条观测 JSON，最后关闭实例并删除本次临时目录。可用 `--workflow query-plan` 等方式只运行一条。它是有界运行演示，不是基线测试套件。测试代码和自动化测试基础设施有意留待后续工作，本基线 `testing=deferred`。

## 一个事务

所有业务接口前缀为 `/v1/lamina`，JSON 标量仅支持有符号 64 位整数、文本、布尔值和 null。

```sh
curl -s http://127.0.0.1:7386/v1/lamina/relations -H 'Content-Type: application/json' -d '{"name":"spectra","primary":"sample","columns":[{"name":"sample","type":"int"},{"name":"band","type":"text"},{"name":"amplitude","type":"int","nullable":true}]}'
curl -s http://127.0.0.1:7386/v1/lamina/transactions -H 'Content-Type: application/json' -d '{}'
```

取返回的事务 `id`，在下面 URL 中替换 `<id>`：

```sh
curl -s http://127.0.0.1:7386/v1/lamina/transactions/<id>/put -H 'Content-Type: application/json' -d '{"relation":"spectra","row":{"sample":1,"band":"infrared","amplitude":40},"insert_only":true}'
curl -s -X POST http://127.0.0.1:7386/v1/lamina/transactions/<id>/commit
curl -s http://127.0.0.1:7386/v1/lamina/sql -H 'Content-Type: application/json' -d '{"sql":"SELECT sample, amplitude FROM spectra WHERE amplitude >= 20 ORDER BY amplitude DESC LIMIT 10;"}'
```

`put` 是完整行替换；缺失的可空列补为 null，缺失的非空列拒绝。主键从新行中取得，不提供原主键重命名操作。删除接口为事务 URL 下的 `delete`，正文为 `{"relation":"spectra","key":1}`；不存在的键也可以形成墓碑。DDL 自身是单个耐久操作，不在用户事务中暂存。

## 查询与隔离

`POST /query` 接收 `relation`、`columns`、`where`、`order`、`limit`，以及可选的 `transaction` 或 `at`。两种快照选择互斥。`where` 是 AND 连接的比较，支持 `= != < <= > >= is_null not_null`；`order` 指定列及 `desc`。

```json
{"relation":"spectra","where":[{"column":"amplitude","op":">=","value":20}],"order":[{"column":"amplitude","desc":true}],"limit":10}
```

读事务固定开始时的提交序号，重复读稳定，支持读自己的暂存写入。普通读使用当前序号。按 `at` 查询只接受保留区间内的序号，并要求关系在该序号已存在。事务无法访问在它开始之后创建的关系。无显式排序时按类型化主键排序；排序相同时也用主键确定次序。升序 null 在前，降序 null 在后。null 普通比较不匹配；使用 `is_null` 检查。

数据库提供快照隔离和同键 first-committer-wins，不承诺串行化、谓词锁或跨行写偏差防护。提交前对最终可见状态检查所有受影响关系的唯一索引；一次事务内可以置换唯一值。失败提交会关闭该事务，不发布任何私有行。只读事务通过 `{"read_only":true}` 开始。

## 行级排他锁

`POST /transactions/<id>/locks` 让调用方在执行昂贵编辑前先取得指定行的修改资格。正文为 `{"relation":"spectra","key":1,"wait_ms":500,"lease_ms":30000}`：

```sh
curl -s http://127.0.0.1:7386/v1/lamina/transactions/<id>/locks -H 'Content-Type: application/json' \
  -d '{"relation":"spectra","key":1,"wait_ms":500,"lease_ms":30000}'
```

成功返回 `id`、`relation`、`key`、`transaction`、`lease_ms`、`acquired_at`、`expires_at`，以及本次是否只是续约（`renewed`）和实际等待时长 `waited_ms`。同一事务再次请求同一行是续约；已持有该行的其他事务得到 409 `locked` 并携带当前持有者，`wait_ms>0` 时先排队等待，而不是把整段编辑算完才在提交时发现冲突。

锁是按键的排他修改资格，不是范围或谓词锁，不改变默认快照隔离：它不阻止其他事务读取该行，也不使本事务的读变成串行化。多行编辑请用 `POST /transactions/<id>/locks/batch`，正文 `{"rows":[{"relation":"spectra","key":1},{"relation":"spectra","key":2}],"wait_ms":1000,"lease_ms":30000}`。批量请求按 (relation, key) 规范化顺序一次性获取，全部成功才保留；任一键失败会释放本次新取得的前序键并返回错误，因此相反获取顺序不会互相占住对方需要的行。

边界与解除：

- `wait_ms` 取值 0..10000，0 表示不排队立即拒绝；超时返回 409 `locked`，等待时间有上限。
- `lease_ms` 取值 100..600000，默认 60000。租约到期后锁自动消失并计入 `expired_since_open`；后续使用该资格会得到 409 `lock_expired`，不会静默接受过期快照。
- 等待期检测环形依赖，形成环的等待以 409 `deadlock` 立即拒绝，未成环的等待继续受 `wait_ms` 约束。
- `POST /transactions/<id>/locks/cancel` 只取消用户本事务正在进行的等待，返回 409 `canceled`，与超时、死锁和租约到期区分。
- `POST /transactions/<id>/locks/release` 用 `{"relation":"spectra","key":1}` 释放单行，`POST /transactions/<id>/locks/release_all` 释放全部；重复释放无副作用。显式释放后，之后的提交就是普通快照隔离提交。
- 提交、回滚和实例关闭都会释放该事务的全部行锁。锁只存在于进程内，不写日志、不进入备份；重启后占用状态清零，需要重新取得资格。这正是重启、回收和带着锁提交要分别核对的理由。
- 未使用新接口的写入同样受约束：`put`/`delete`/`commit` 在目标行被其他活跃事务持锁时返回 409 `locked`。若事务自己持有该行的锁但租约已过期或该行在事务快照之后已被提交，则返回 409 `lock_expired` 或 409 `conflict`，绝不把基于旧快照的值当作新值发布。
- 只有持锁的写入会额外检查“取得锁之后该行未被改动”。无锁写入维持原有的提交期同键检查。
- 每个事务最多 1000 个行锁，进程最多 4096 个持有中的锁；超限返回 429 `capacity`。

`GET /locks` 返回当前真实持有（`held`）与等待（`waiting`）中的占用，以及 `acquired/renewed/released/expired/waited/timed_out/canceled/aborted_waits/deadlocks` 计数。`GET /stats` 另有 `row_locks_held`、`row_lock_waits` 与同样的 `lock_counters`；这些进程事实在重启后归零。锁不保证写偏差防护，也不把同键排他升级为范围串行化。

SQL 只支持单条 SELECT：投影或 `count(*)`、`count(column)`、`sum(integer_column)`，可选 WHERE AND、ORDER BY、LIMIT。字符串使用单引号，内部单引号写两次；关键字及未引号标识符转为小写。没有 JOIN、GROUP BY、别名、表达式、SQL DDL/DML。语义检查与结构化查询共用绑定和执行。`sum` 忽略 null，空输入返回 null，溢出报错；`count(column)` 忽略 null，`count(*)` 计算全部匹配行。

## 索引

`POST /indexes` 的示例正文：

```json
{"name":"by_amplitude","relation":"spectra","column":"amplitude","unique":false}
```

单列索引由类型化有序切片维护，保留历史非空值到主键的候选关联。唯一约束允许多个 null。索引在当前提交序号同步构建；构建失败不会发布目录。规划器在可用索引中选择候选键最少的一条，之后读取指定快照的真实版本并执行全部谓词。对早于索引创建时刻的查询使用扫描；含私有写入的事务查询也使用扫描。回收后重建历史候选。

结果的 `plan.candidates` 是访问路径候选键数，`scanned` 是存在可见行并进入残余判断的行数，`matched` 是限制输出前的匹配数。保留历史候选可能导致这些数字差异很大。`force_scan:true` 可用于对照观测。`plan` 是执行事实，不是 I/O 耗时估计。

## 日志、检查点、还原

提交顺序为校验、完整帧写入、fsync、内存发布。每个日志帧包括长度、CRC32 和完整提交记录。日志序号连续，记录格式版本为 1。写盘或同步失败后实例拒绝继续服务；调用方可能无法确定该次提交最终是否落盘，应重启后核对实际状态，基线没有请求级去重协议。

启动先验证检查点，再按序重放剩余记录。只自动截断最后一个不完整帧；完整帧校验不匹配、非法长度或序号异常都会拒绝启动。完整日志一直保留，检查点不会截短日志。数据库状态在内存中，日志和镜像会随数据量增长。

`POST /checkpoint` 在数据库锁内捕获并写入图像，临时文件同步后原子替换检查点并同步目录，因此会暂停提交和查询。`GET /backup` 生成包含目录、所有保留版本、提交序号和回收水位的冻结图像，带 SHA-256 摘要；之后的源库提交不会改变已返回的图像。

```sh
curl -s http://127.0.0.1:7386/v1/lamina/backup -o /tmp/lamina-backup.lmi
./bin/laminad -restore /tmp/lamina-backup.lmi -data var/lamina-restored -listen 127.0.0.1:7387
```

还原目标必须不存在。启动前验证图像摘要、格式、目录、版本顺序及行类型，再安装图像和带恢复基点的日志头。还原保留图像内的历史读版本，但不包含源库历史日志记录；变更消费者必须从恢复基点重新对齐。禁止将备份文件当成一个新根日志目录手动拼接。

## 变更与回收

`GET /changes?after=0&limit=100` 返回按提交序号递增的目录、提交和回收记录。分页单位是完整记录，不拆分事务，`cursor` 是已返回的最后序号，`head` 是观测时的数据库头部序号。还原实例的 `history_base` 之前没有变更历史，旧游标返回 410。

`POST /vacuum` 正文为 `{"horizon":<sequence>}`。实际水位取请求值和所有活动事务快照的最小值；活动读者阻挡时 `pinned=true`。每条版本链保留水位时的最后版本和所有更新版本；水位前的最终墓碑可以全部移除。低于持久回收水位的按序号查询返回 410，水位不能回退。回收并不清理日志磁盘占用。

## 资源与错误

请求体上限 1MiB，列数 64，文本值 64KiB，活动事务 128，每事务最终写集 1000 个键，单日志帧 16MiB，镜像 128MiB。查询最多 32 个 AND 谓词、8 个排序项，默认输出 1000 行，可显式设到 10000；截断时 `truncated=true`。查询执行仍可能物化全部匹配行，事务没有自动过期，排序和备份没有外溢机制，应显式关闭不用的会话并为演练设置合理数据规模。

错误 JSON 为 `{"error":{"code":"...","message":"..."}}`。无效输入 400，已关闭或未知事务及缺失行 404，写冲突、唯一冲突、锁占用 `locked`、等待环 `deadlock`、租约过期 `lock_expired` 和用户取消 `canceled` 均为 409，快照和变更历史不可用 410，容量拒绝 429，存储不确定或已关闭 503。`GET /stats` 返回会话、版本、候选索引大小、恢复尾字节等领域事实，进程计数在重启后归零。

## 容器

```sh
docker build -t lamina-db .
docker run --rm -p 127.0.0.1:7386:7386 -v lamina-volume:/data lamina-db
```

镜像使用非特权用户 10001。容器工作流需本机镜像构建可用；原生运行不依赖 Docker。持久目录使用 Unix 文件锁，当前不声明 Windows 支持。备份只能缓解存储故障；没有复制、自动切换或加密。

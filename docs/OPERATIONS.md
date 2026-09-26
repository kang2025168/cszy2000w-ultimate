# 升级与运行手册

## 当前服务

MySQL 保存交易状态；split-bots 的四个独立服务负责 B/F 买卖；ultimate 服务提供网页并监管其余机器人。网页通过 ULTIMATE_EXTERNAL_BOTS 排除已有独立容器的机器人，防止重复启动。

```bash
docker compose --profile split-bots --profile ultimate up -d --build
docker compose logs -f buybot sellbot ultimate_v1
```

启动网页可能按数据库已有开关启动机器人；升级前先关闭买入并停止工作进程。不要将检查命令等同于安全的 paper 演练。

## 升级顺序

1. 备份数据库和当前镜像版本，关闭交易机器人，保留旧配置。
2. 在独立测试库运行迁移，确认旧 stock_operations 支持所需字段及股票/策略唯一键。不要用生产库运行测试脚本。
3. 配置真实网页密码、MYSQL_ROOT_PASSWORD。示例 CHANGE_ME 密码不能登录。A 账户需要 RETIREMENT 专属 key/secret；配置不完整会阻断，不能再回退到 B/C/D 账户。
4. 建议配置 RETIREMENT_EXPECTED_ACCOUNT_ID / TRADING_EXPECTED_ACCOUNT_ID；系统会校验返回的账户身份，且拒绝两个 profile 指向同一已识别账户。
5. 用 paper 配置验证登录、预览、下单、部分成交、撤单、重启恢复与持仓对账。确认后再安排 live 部署；不应在有未核对订单时切换凭证。
6. 启动时版本迁移在数据库命名锁下执行，版本写入 schema_migrations。版本 4 将 app_settings.setting_value 扩容为 MEDIUMTEXT，以保存 A 类自选分配配置。新增 execution_orders 和 d_grid_cycles.pending_client_order_id。

只执行迁移：`python -m ultimate_v1.schema`。迁移修改数据库；不是只读检查。DDL 有 MySQL 隐式提交语义，升级失败应检查备份与迁移版本，不能假定全部自动回滚。

## 安全边界

- 数据库宿主端口绑定 127.0.0.1；容器内部仍通过 mysql:3306 通信。
- 看板/手机页面默认发布到 127.0.0.1。远程访问建议通过 HTTPS 代理；代理终止 HTTPS 后设置 DASHBOARD_COOKIE_SECURE=1。需要改变监听范围时显式设置 DASHBOARD_BIND_IP，并配置网络访问控制。
- 会话默认 8 小时到期；更换登录密码或签名密钥会令旧会话失效。登录每个来源每分钟最多 10 次尝试；多进程/代理部署还应在代理层限速。
- 写接口接受 JSON，并校验存在的 Origin 与 Host；代理需要保留外部 Host。
- 凭证存储仍兼容原 app_settings 配置。数据库备份必须按敏感文件管理。

## 订单故障恢复

- 手动下单预览生成 request_id。重复确认/网络重试保留同一 request_id，不创建新订单。
- execution_orders 先记录意图及预占，再访问券商。submitting/unknown 状态只查询原 client_order_id，不盲目重发。
- 网页后台每 5 秒核对手动订单；部分成交按累计成交量差额入账。持仓和入账进度在同一事务更新，终态且已完成入账才释放预占。
- 未终结手动订单存在时，该账户的聚合持仓同步暂缓，避免把待入账成交先写入持仓造成重复计数；其他账户继续同步。
- D 网格 BUY_SUBMITTING/SELL_SUBMITTING 可在重启后恢复查询；错误记录独立提交，不因异常丢失。网格买入预占保留到该轮卖出完成或买单确认零成交终结。
- unknown 持续存在时核对券商订单与本地记录；不要删除 execution_orders 或清除预占来“恢复额度”。如果券商确认从未收到订单，需要人工核实后处理，系统选择保守阻断。

## 性能与观察

- 网页资金、风险、持仓读取后台快照；约 15 秒刷新，超过 60 秒拒绝当作新鲜数据。交易执行独立实时校验，不使用展示缓存。
- MySQL 使用有界连接池（DB_POOL_SIZE 默认 8），带连接、读写与池等待超时；Alpaca 客户端按线程和账户复用并设置请求超时。
- 登录后访问 `/api/health` 查看本进程 HTTP/DB/券商请求计数、p50/p95、待核对订单和机器人心跳。多进程指标需分别汇总；它不是完整的告警平台。
- 关注心跳停止更新、unknown/submitting 长时间不消失、数据库连接池耗尽、磁盘容量与数据库备份失败。对公网告警渠道的发送需另外配置。

## 测试与依赖

`requirements.txt` 是直接依赖，`requirements.lock` 固定完整解析版本，Docker 和 CI 使用锁文件。修改依赖后重新解析并在 Python 3.12 Linux 容器运行测试，不直接升级生产依赖。

```bash
.venv/bin/python scripts/test_offline.py
```

该脚本清空运行环境、不加载 .env，并阻断网络。当前覆盖 74 项单元测试；集成测试只允许本机 13379/cszy_test，清理其中的测试数据：

```bash
docker run --rm -d --name cszy-test-mysql \
  -e MYSQL_ALLOW_EMPTY_PASSWORD=yes -e MYSQL_DATABASE=cszy_test \
  -p 127.0.0.1:13379:3306 --tmpfs /var/lib/mysql mysql:8.0
.venv/bin/python scripts/test_execution_mysql.py
docker stop cszy-test-mysql
```

等待测试 MySQL 就绪后运行脚本。不要把 13379 映射到现有交易数据库。

## 备份与恢复演练

使用 mysqldump --single-transaction 备份 InnoDB，备份包含执行日志、持仓、资金状态和 schema_migrations；文件设置仅管理员可读，保留离机副本。定期恢复到隔离 MySQL，核对表行数、schema 版本和 execution_orders，再运行只读校验。恢复出的机器人保持关闭，隔离环境不得使用 live 凭证。恢复备份不会撤销券商端已执行订单，恢复服务前必须重新对账。

本次代码优化没有替你执行生产备份恢复、重启交易容器或实盘订单演练。

### D 收盘退出与策略归属修复

D 在券商实际收盘时间前 10 分钟进入退出阶段（正常交易日洛杉矶 12:50，提前收盘按券商时钟）。撤销原止盈单并确认终态后，以最新现价提交 DAY 限价卖单；没有有效现价时等待重试，不回退到止盈目标价。退出单超过 30 秒仍未成交时请求撤销，确认撤销及实际成交量后，按新现价重挂剩余数量。历史部分卖出的成交金额一并计入闭环收益。限价单不能保证成交；收盘后仍有仓位保留异常状态，下一交易时段继续处理，不记作已闭合。

同步仓位时，以 D 当前循环的真实券商买卖成交量验证剩余仓位。只有全部券商持仓数量与 D 剩余数量一致、且没有多策略持仓记录时，才纠正 C 等错误归属。混合或数量冲突停止该次同步并输出复核错误，养老金不参与修复。

更新 B 规则时，必须一并重建并重启 B 买卖机器人。仅重启网页容器不会刷新已运行 Python 进程的配置。用户在服务器项目目录部署时执行：

```bash
docker compose up -d --build --force-recreate ultimate_v1 buybot sellbot
docker compose ps ultimate_v1 buybot sellbot
```

这会启动实际策略进程；部署后查看 D 收盘退出记录和持仓归属。上述命令不自动启动 F 服务。

D 最后一分钟兜底：以券商实际收盘时钟为准，收盘前 60 秒内检查剩余仓位。先撤销旧限价卖单，等待终态并汇总全部已成交数量，再按 D 剩余数量提交 DAY 市价卖单；市价单未完成时持续对账，不重复提交。支持提前收盘；闭环仍以真实成交确认。停牌、券商拒单等情况下保留异常，不把未成交当作卖出。

D 极端止损：本轮实际买入均价下跌达到 5% 时，以新鲜报价触发退出；先撤销原卖单并确认终态，再按扣除全部已卖数量后的剩余量提交市价单。触发状态按买入订单编号持久化，反弹或进程重启不会取消本轮退出。闭环记录标注 STOP_LOSS_5PCT；新循环不继承旧止损状态。5% 是触发阈值，非成交损失保证。配置页显示固定 5% 止损、收盘前10分钟限价退出和前1分钟市价兜底；旧固定平仓时间配置不再影响执行。

### D 收盘清仓与重合持仓（2026-09-26）

D 在券商实际收盘前 10 分钟撤销自己的限价卖单，确认撤单及已成交量后，市价卖出本周期剩余数量。市价单未完成时继续跟踪，不重复提交；支持提前收盘。ERROR 状态和被停用的标的仍进入订单管理，旧的卖单过期仓位在市场开盘时恢复退出。停牌、券商拒单或服务离线仍可能导致无法当日成交。

账户中的同名股票按策略份额管理。同步时按 D 买单实际成交减去 D 所有卖单成交拆出 D，剩余份额交给 B/C/F 的已有分配逻辑；不把账户总股数写入 D。B 的卖后余额及加仓成本仅使用 B 原持仓和自己的订单成交，撤单仅针对 B 已记录订单。

更新涉及 D 网页容器与 B 买卖容器，部署时需一起更新：
`docker compose up -d --build --force-recreate ultimate_v1 buybot sellbot`

# mcbot bridge 当前进度

## 0.3.2：自动阶段与非阻塞发送（2026-10-25，Asia/Shanghai）

- 22:34:56任务已派发，22:36:02游戏核实10个橡木原木、22:36:35木镐入包，
  但没有节点事件；不是0.3.1任务入口回归，是游戏模型没主动调用report_milestone。
  原成功progress静默到HUD，宿主没有收到respond指令。原件私有副本在
  build/automatic-evidence-20261025，游戏SHA256
  99d49be3e20d9a342148cc8756fff4ffb3a77dc0a556f3bad309bcd1bfe4a300；
  桥日志SHA256 2ff772f57145e795b3e051c7dbae3d186986c1904076967b779beca793c8c24d。
- 配套新JAR直接从gather最终完整背包/真实基线及craft实际提交/成品存量/工具耐久属性
  生成相同progress.milestone。模型不用记得调用，Lua未结束即可通知并继续。
  无物品升级清单、额外模型、轮询或游戏主线程网络/语音等待；查询/失败/受理/旧身体/迟到不报，
  缺报告证据不改变原动作结果。资源集合/工具及等价显式节点共原持久去重账。
  重要位置/建筑语义节点仍可显式核验，未扩成自动识别任意自然语言阶段。
- 游戏SSE单订阅队列256，生产者offer不等socket；慢端满则断开后沿原游标重连。
  原200条环仍不是无限断线持久队列。插件保留原可靠待送及同任务节点/终态顺序；
  新增节点delivery=submitted/queued与补送受理日志，不把submitted称作声音已播放。
  新任务仍只有后台普通mcbot_task入口，宿主源码和真实安装实例未修改。
- 游戏强制全量2082项/216 suites、1分17秒/21任务全执行，全部失败/错误/跳过0。
  真实runner+HTTP/SSE样本复制到tests/fixtures/automatic-milestones.json，
  跨端回归证明自动两节点及最终结果拒收后保序补送且不重派。
  build/automatic-milestone-final-tests-20261025.log/XML共205项，0.92秒，
  failures/errors/skipped全0，Ruff通过。包括慢socket仍发布3000条且控制口响应、
  Lua未结束先发/原预算继续、自动节点恢复去重、取消迟到与缺证据边界。
- 官方CLI构建及tools/verify_delivery通过，日志
  build/automatic-package-build-20261025.log、build/automatic-package-verify-20261025.log，
  记录build/delivery-verification-0.3.2-20261025.json；验证真实SDK工具/普通入口、
  payload哈希/源码指纹、独立vendor导入及JAR/内嵌核心一致。
  dist/mcbot_bridge-0.3.2.neko-plugin为692351 bytes，SHA256
  10abd4a73d39c7e279079be564486fbd46ef2a3bc4302d0f9f33b4140e22c181。
  dist/mcbot-0.1.0.jar为1893002 bytes，SHA256
  687f9b63f3a07d8527c846e3d6acf27336fd0c71ad2608b46fb637aa7d6e8ab2；
  内嵌核心285952 bytes，SHA256 ff2dbaaf96e5c59d3fe58e6ce8d0cf52190d4bbe7cffe6aac133fd7c9d53f731。
  此次两端都要更新。未安装/启动实例、游戏或真实模型，不自动重放用户任务；
  实际模型/语音/画面仍待维护者新包测试。

## 0.3.1：后台识别任务但无法派发（2026-10-25，Asia/Shanghai）

- 22:06:21事件流已经连接，22:06:57身体正常召唤；22:07:33和22:08:26后台连续报
  `entry_id 'mcbot_task' does not exist`，仅见重载、状态、停止三个入口。
  游戏该段只有状态查询，没有新任务指令或执行动作。已装插件确为0.3.0，
  游戏JAR的SHA256与上一轮caa0e8bc…一致；不是旧JAR或任务中途寻路失败。
  宿主api_runtime的插件提供器主动过滤__llm_tool__入口，先前只注册为对话工具的任务
  无法供后台调用，造成口头应答后无动作。已核实日志及机制见docs/REPORTING-2026-10-25。
- 新任务统一使用普通plugin_entry id=mcbot_task，后台可见，移除同名对话工具派发面，
  避免两模型竞态重复执行。各任务仍传用户原话到原游戏执行器，不增行为模板。
  宿主提供最新原话时优先使用，角色只取宿主调用作用域；实际受理后返回任务编号，
  忙碌、未知和失败不给accepted，原锁、任务槽及未知结果不重派机制保持。
  受理沿finish silent/result_kind=event，不充当游戏完成；原节点与done可靠播报保持。
- 后台另有普通mcbot_answer入口，与既有答复方法/角色及问题期限守卫共用，
  让无对话工具调用时的用户确认也可回传，不将答复重新派成任务。
  官方包共5个普通入口：task、answer、status、stop、reload_config；
  3个对话控制工具：ask、answer、cancel。需启用宿主后台任务和用户插件能力。
  当前宿主对话工具仍缺可信角色载荷时沿原守卫拒绝跨角色，不读取最近调用者猜身份。
- build/routing-final-tests-20261025.log/XML共202项，1.03秒，failures/errors/skipped全0。
  新增13项覆盖宿主真实插件过滤函数（只编译该函数，HTTP受控）、任务入口及原话、
  角色/实际编号/静默受理、不同任务类型复用、忙碌/未知不重派、坏参数不受理、
  后台答复返回原任务和其他角色不能代批。Ruff、面板TypeScript/解码检查通过。
  不启动宿主服务/游戏或真实模型，不自动执行玩家请求，也不修改宿主/实例或mod生产代码。
- 官方构建及tools/verify_delivery通过，验证实际SDK产生的普通入口Schema、
  对话工具集合、payload哈希/源码指纹、独立vendor导入及原JAR/内嵌核心。
  日志build/routing-package-build-20261025.log、build/routing-package-verify-20261025.log，
  指纹build/delivery-verification-0.3.1-20261025.json。正式包
  dist/mcbot_bridge-0.3.1.neko-plugin为691193 bytes，SHA256
  46f92e3c825bbd478ef3df9129a3e8355a0940c35019339c9030f51b50f97702。
  只更新插件即可，JAR未重建且与0.3.0逐字节相同；真实后台选入口、游戏执行及口播仍需联调。
- 证据私有副本位于build/routing-evidence-20261025，构建允许清单排除它们。
  host-agent.log为23110 bytes，SHA256
  dd7962a6604487bb184d85f818abc54d1593c2e051cd42777c12d0c7e0270c1b；
  bridge.log为103165 bytes，SHA256
  dc57c958e8a457907e3769b44a9db05578a555852fe1c891d545d20c2356db6d；
  game.log为23035 bytes，SHA256
  3524a8fc11ca80c3e61e841d24828ddc1411aac16eccac0dcb4a4398eecf3917。

## 0.3.0：通用关键节点主动汇报（2026-10-25，Asia/Shanghai）

- 21:34:14木镐、21:35:26石镐已真实制作成功，旧成功progress只进后台。
  本轮同时修改mcbot及桥接器，不改宿主源码或安装实例；需要两端一起更新。
  模型通过通用report_milestone选择节点，游戏端核实新鲜背包/到达位置/建筑阶段格集，
  通过progress.milestone传入。没有钻石镐固定链、逐路线模板或额外模型分类器。
- 服务只接已关联任务的verified节点，POST编号未返回先暂存；角色关联后再发。
  每个节点独立respond、规范ID去重随运行账保存，普通进度不清记录，
  失败进入原可靠待送队列。节点及最终结果同优先级并按任务顺序提交，不释放待办或重派工作。
  8语提示强调第一人称、准确阶段事实与最新整体状态；不把库存归因成制作或把节点说成整项完成。
  宿主仍可能合并同批相邻通知为一轮回复，submitted只证明本地受理，不保证语音已播放。
- build/milestone-final-tests-20261025.log/XML共189项，0.92秒，failures/errors/skipped全0。
  新增16项回归覆盖节点去重与多个节点保留、普通查询静默、错误数据不播、
  POST竞态/无关任务、重载可靠补送与去重、取消/会话变化、终态不超车及失败顺序保持、8语模板。
  Ruff通过，面板TypeScript及真实解码函数检查通过；没有真实模型/语音/游戏自动运行。
- 官方CLI build及tools/verify_delivery通过，核对payload哈希、真实SDK元数据与四工具、
  源码指纹、独立vendor导入和配套JAR/嵌入核心。日志为
  build/milestone-package-build-20261025.log、build/milestone-package-verify-20261025.log，
  完整指纹见build/delivery-verification-0.3.0-20261025.json。
- 配套mcbot强制全量2069项/215 suites全过，JAR为1886513 bytes，
  SHA256 caa0e8bc71d0b30fb7ad1debb85e9641ecb1e96d94fb3799b8335cedd5e74ac7。
  正式dist/mcbot_bridge-0.3.0.neko-plugin为688263 bytes，SHA256
  ee9b06fa5be4b91128d91e82e4743283b86ea36c4c7a259ed539eef42b1f90e2；
  dist同时包含新JAR、对接说明、汇报说明及0.3.0交付核验记录。
  新版游戏大脑是否及时选节点与猫娘口播仍待维护者实测；当前宿主远程工具载荷缺可信角色，
  多角色路由限制沿前节，不猜角色或声称已经修复。

## 0.2.1：Steam 首次联调汇报修复（2026-10-25，Asia/Shanghai）

- 真实宿主日志位于 Windows 用户本地应用数据的 N.E.K.O/logs；
  本轮只读定位主/后台分析器/插件日志、桥接运行账和游戏 latest.log。
  20:58:21 原文“去采集两个橡木原木”已派发为 task_id=1；
  20:58:45 gather 核基线0、当前2、净新增2、尚缺0；
  20:58:48 最终结果已转发并被宿主调度。不是本段采集未执行。
- 同轮后台分析器错误选 mcbot_reload_config 响应采集意图，20:58:26
  将配置重载当 UserPlugin completed，20:58:29 触发提前播报。
  原普通 Ok 数据没有 SDK finish 的静默/事件语义，这是已复现的误报路径。
  20:58:49 最终回复仅日志文本长度8，无法据此还原完整口播。
- 三项管理入口使用公开 SDK finish(data, delivery="silent")，静态与运行时
  result_kind="event"，不再把管理成功主动播报为游戏目标完成。
  入口描述拒绝充当采集/建造入口；错误仍走错误回执。
  done 加原始指令与实际结果上下文，保留部分完成、失败、未知和去重/通知待送；
  接单明确第一人称自己执行。面板兼容旧响应与新 finish 信封。
- 对照当前宿主实际远程工具载荷，未携带可信调用角色；因此本段
  target_lanlan 为空，日志回退到 YUI。此项不猜模型参数/最近面板角色，
  不修改宿主，本轮没有声称解决多角色精确回传。
  完整时间线、源码机制及复测边界见 docs/REPORTING-2026-10-25.md。
- 最终 build/reporting-tests-20261025.log/XML：173 项，0.81秒，
  failures/errors/skipped全0。14项新增回归使用真实 SDK finish、AST 读取的
  宿主实际纯函数、真实插件管理入口和 service，覆盖采集期间重载不抢完成、
  不重派、最终原指令/数量、部分结果与8语上下文。
  Ruff 通过；面板 TypeScript 与真实解码函数的旧/新/嵌套/取消响应检查通过。
- 官方 CLI build 与 verify_delivery 检查成功，含真实 SDK 三项管理元数据、
  四项 LLM 工具、payload hash、源码指纹及 -I -S 独立 vendor 导入。
  证据为 build/reporting-package-build-20261025.log、
  build/reporting-package-verify-20261025.log 和
  build/delivery-verification-0.2.1-20261025.json。
- dist/mcbot_bridge-0.2.1.neko-plugin：684580 bytes，SHA256
  9bc48a62aaef155185b7d3a3c999f1b8cb12f2c585d9d44d52ba816779b9c4e0。
  本轮不改 mod，沿用上一节 JAR；不启动游戏/宿主服务/真实模型，
  不安装或改写用户正在使用的插件，不改宿主源码/参考/游戏实例。
  修复后的实际口播、用户聊天与多角色路由仍需复测，离线通过不替代它们。
- 私有日志副本在 build/reporting-evidence-20261025，构建允许清单不包含它们。
  host-agent.log SHA256
  c61b98fa2e3a867806ef37d34cf2a27d1662b5f4a04db6615b5021ed96a6b24f；
  game-latest.log SHA256
  26e7044dcacd9f7f2f2280aa2f541212f2b10a24fed0fd484eb1657bfc445203。

## 正式导入包与配套 JAR（2026-10-25，Asia/Shanghai）

- 按本地源码构建，不拉取或覆盖现有改动。使用宿主官方 CLI 对外部插件目录
  执行 sync/build，Python 3.11，生成含 HTTP 依赖的 vendor、正式 manifest、
  payload/profiles、依赖清单及真实 SDK 探测的 plugin.meta.json。
  pyproject 构建允许清单排除运行数据、凭据、测试、工具、缓存及安装锁文件。
- dist/mcbot_bridge-0.2.0.neko-plugin：679425 bytes，SHA256
  f5056915d90747d469ed3260e5e73d78d798d8f44403896b1b3aa4b3b1251278。
  这是正式包，与上一节普通源码 ZIP 不同。
- 20:46 后完成最终正式包检查；build/package-build-20261025.log、
  build/package-verify-20261025.log 和 build/delivery-verification-20261025.json
  保存证据。tools/verify_delivery.py 调用真实宿主 inspect_package 验 payload hash，
  解包核源码指纹、面板、三项管理入口，并用真实 SDK 检查四项 LLM 工具声明。
  独立 -I -S 解释器仅用包内 vendor 成功导入 HTTP 依赖并创建/关闭无网络客户端。
  未执行插件启动生命周期；这不证明模型已调用工具或宿主已播放语音。
- 本轮 Python 3.11 离线回归 159 项全过，失败/错误/跳过全 0，0.74 秒；
  build/package-tests-20261025.log/XML 留存。验证工具 Ruff 与面板类型检查通过。
- 配套 dist/mcbot-0.1.0.jar 为 mod 强制全量离线 build 新产物副本，
  1878486 bytes，SHA256
  e06fb2cc14cfa76efc8de6d23a3c270ad94d2ede9742012e9b1fb8ed0509011e。
  2054 项/213 suites 全过，21 任务全执行；内嵌 agent-core 与独立产物逐字节相同。
  mod 构建证据见其 STATUS 顶部。
- docs/CONNECT-AND-TEST.md 随正式包交付，dist 也有独立副本和校验报告。
  未安装到游戏实例，未启动游戏、宿主服务或真实模型，真实联调由维护者按步骤测试。
- 首次加载官方 CLI 时宿主日志模块自动清理了 2026-09-17 至 09-23 的旧插件日志；
  后续构建将存储、配置与日志定向至本插件 build/package-runtime。
  没有修改宿主源码或参考项目；上述日志清理是本轮已发生的运行副作用。

## 0.2.0 源码候选：任务与沟通可靠性

日期：2026-10-25，Asia/Shanghai。

本轮只修改独立桥接插件。没有修改宿主、mcbot 或游戏实例，没有安装包、启动
游戏/宿主/服务器或调用真实模型。本地源码优先，没有拉取远端覆盖。

已实现：

- 等待超时及受理未知仍保留任务关联；不自动重投，明确停止仍发取消。
- 用户工具入口串行派发与取消，原编号未知时不猜归属；旧会话响应不改变新会话状态。
- 普通 completed 改用中性结果回执，失败保留部分事实；恢复别名使用受理回执。
- mcbot_ask 只读状态及近期事实，不再创建游戏任务。
- 重要通知持久待送，识别 SDK 的 submitted 拒收；问题过期/取消/换会话后不补送。
- 任务、有限历史/进展、问题和通知可在同会话重载后恢复观察；不重放游戏动作。
- 取得宿主调用角色时绑定原角色；面板增加执行状态、近期观察和待送通知。
- 回环 HTTP 校验、禁用系统代理环境、显式声明 httpx 运行依赖。

验证与打包：

- 2026-10-25 20:35（Asia/Shanghai）完成最终离线回归：
  159 项，failures/errors/skipped 均为 0，pytest 显示 0.72 秒。
  日志为 build/bridge-reliability-20261025.log，
  XML 为 build/bridge-reliability-20261025.xml。
  同轮 Ruff 检查和面板对照宿主声明的 TypeScript 检查均通过。
- 详细测试命令、最终 XML、面板类型检查及源码包由本地 build/dist 保存。
- 实际 HTTP/SSE 客户端用 MockTransport 验证 task、提前 question、answer、done 闭环；
  不是在真实游戏中取得的任务成功证据。
- 宿主专项只读加载真实工具元数据和消息翻译函数，检查四个工具和角色通知格式；
  不证明完整宿主工具注册、模型路由或语音播放已经验收。
- 面板对照宿主 hosted UI 声明单独进行 TypeScript 检查。
  宿主原有完整 TSX 检查器测试最初因宿主源码环境未安装 typescript 而无法运行；
  没有为此修改或安装依赖到宿主目录。
- tools/package_bridge.py 使用允许清单打包，排除 data、日志、token、密钥、
  node_modules、测试和解释器；压缩包内含逐文件 SHA256 清单。

仍待真实联合验收：

- 猫娘在挖矿期间正常聊天/查询进度，收到真实反问后正确回答，并主动汇报实际结果。
- 用户叫停、插件重载、游戏换世界和宿主背压情况下的实际体验。
- 面板在宿主中的显示及长回执排版。

本轮没有新增实时背包/截图、任务历史查询、父子任务关系或统一目标成功字段。
观察缺失仍为未知；SDK 的提交成功不等于宿主已经消费或语音已播放。
详细边界见 docs/BRIDGE-RELIABILITY-2026-10-25.md。

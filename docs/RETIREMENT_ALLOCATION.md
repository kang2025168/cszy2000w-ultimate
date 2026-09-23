# A 养老金账户目标配置

基金部分固定为 QQQ 20%、VOO 20%、XLV 10%。主题部分初始为 MSFT 15%、NVDA 15%、ISRG 10%、TER 5%、IBIT 5%。IBIT 是比特币 ETF，算在主题部分；全部默认标的来自现有 C 类名单。A 与 C 使用各自账户、独立持仓和权重。

在「配置 → A 养老金 → A 养老金目标配置」添加或移除主题股票并编辑比例。主题合计必须为 50%；股票代码不得重复，新添加标的须经券商确认是可交易的美股或 ETF。配置保存在 app_settings.RETIREMENT_ALLOCATION_V1，重启不会覆盖。移除目标保留已有持仓，不自动卖出。

月投以养老金账户总权益计算各标的目标市值，扣除已有持仓市值，再按缺口分配可用现金。预算不超过 A 池额度或账户现金；按整股向下取整，剩余现金保留。现有持仓明显偏离目标时，通过后续补仓逐步接近目标，不保证保存后立即达到 50/50。手动暂停买入的标的跳过，其预算不转给其他股票。最大标的数量小于配置数量时拒绝生成计划，防止静默截断。

保存配置不下单、不改变月投自动执行开关。基金本身也持有部分所选科技公司，因此基金/主题各 50% 不等于风险平均分散；IBIT 与个股仍有显著波动。

主题分类参考发行人资料：
- MSFT：https://www.microsoft.com/investor/reports/ar25/
- NVDA：https://nvidianews.nvidia.com/news/nvidia-and-global-robotics-leaders-take-physical-ai-to-the-real-world
- ISRG：https://isrg.intuitive.com/investors/
- TER：https://www.teradyne.com/robotics/
- IBIT：https://www.ishares.com/us/products/333011/ishares-Bitcoin-trust-etf

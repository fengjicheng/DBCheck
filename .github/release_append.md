---

## ✨ 主要更新
- **告警邮件/Webhook 通知**：新增告警状态机，状态迁移才发送（正常→告警、等级变化、告警→恢复），同状态绝不重发；服务启动时已存在的告警实例合并发一封摘要邮件；SMTP 串行发送防并发拒信。
- **通知密码加密根修**：修复首次保存邮箱密码时加密密钥被冲掉导致密文永久解不开的问题（原子写配置、解密路径不再生成密钥）。
- **监控大屏连线重构**：合并为单主干母线（故障只标红自身支线）、粒子等间距不叠加、真实数据库 logo 图标、右上角斜三角错误角标、首页新增「监控大屏」入口。
- **采集增强**：MongoDB / Redis 原生采集通道（pymongo / redis-py）；GBase8s / DB2 / ClickHouse 补齐复制与锁等待指标；慢查询依赖缺失自动静默降级不再刷错误日志。
- **修复**：数据源编辑库名不回填且更新误变新增；大屏节点卡片错误文案溢出。

## 🐳 Docker 镜像

推荐使用 Docker Hub（国内可用）：

```powershell
docker pull jackge12345/dbcheck:v26.9.20.0
docker pull jackge12345/dbcheck:latest
```

或 GitHub Container Registry：

```powershell
docker pull ghcr.io/fiyo/dbcheck:v26.9.20.0
docker pull ghcr.io/fiyo/dbcheck:latest
```

运行示例：

```powershell
docker run -d -p 5003:5003 --name raccoonx jackge12345/dbcheck:v26.9.20.0
```

> 镜像同时支持 `linux/amd64` 与 `linux/arm64`（ARM64 信创主机）。

## ⚠️ 安装注意事项
- **macOS**：当前分发包**未做 Apple 公证（二进制未签名）**。首次打开若被 Gatekeeper 拦截，请右键点击 App / 可执行文件 →「打开」并在弹窗中确认即可运行；如需彻底消除提示，后续可接入 Apple 开发者证书做正式公证。
- **Windows**：解压后双击 `start.bat` 启动，浏览器访问 http://localhost:5003 。
- **Linux**：客户端安装包暂未发布，当前仅提供 Windows / macOS 安装包；Linux 用户请直接使用上方 Docker 镜像。

## 📦 二进制包
下方 Assets 提供 Windows / macOS 客户端压缩包；需要源码编译的请下载 `Source code`。

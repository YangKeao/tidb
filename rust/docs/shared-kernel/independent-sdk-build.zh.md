# 独立SDK表达式库的固定来源与声明工具链

本批只修复构建接线。SDK表达式库从固定TiDB Git提交读取原datatype；TiDB共享构建
把同一Git来源patch回本地原datatype。没有复制算法、迁移所有者或扩大SQL执行入口。
依据Designer dd2cd5b5／b981b0a4，task40的两个参数缺陷已通过限定复审；本批单独验收构建边界。

## 修改与失败证据

SDK `components/tidb_query_expr/Cargo.toml`把相邻路径改为
`https://github.com/YangKeao/tidb`的固定提交`8abb688c80bd48de688bfc792bc735682404e4dd`。
SDK锁文件只为datatype、error、hack、kvcache、mysql五个原TiDB包增加Git source字段，
未升级第三方版本。TiDB `rust/Cargo.toml`为这个URL增加本地datatype path patch；其锁文件未变。

最初独立检出的原SDK在metadata阶段因缺少相邻TiDB目录失败（`before-path.log`）。
固定来源首试dece836798又在抓取无关enterprise子模块时失败，原记录为`sdk-git-metadata.log`。
随后仅在TiDB `.gitmodules`为该可选子模块设置`update = none`，形成上述8abb固定提交。
Cargo实际记录跳过该子模块，随后发现嵌套Rust workspace及其172个原所有者文件／资源。
此设置遵循[Cargo Git依赖说明](https://doc.rust-lang.org/stable/cargo/reference/specifying-dependencies.html)。
它也改变该分支普通Git的子模块默认更新行为；需要enterprise的用户应显式运行
`git submodule update --init --checkout pkg/extension/enterprise`，并自行满足其访问条件。

SDK原声明Jan30 nightly实际选中rustc1.95.0，普通`cargo test`因五个原TiDB包要求
Rust1.97而失败（`before-msrv.log`）。没有降低MSRV；SDK声明改为nightly-2026-08-22，
实际为rustc1.100.0-nightly c656540d6、Cargo1.100.0-nightly 514c56dd7、LLVM23.1.0。
保留rustfmt、Clippy、rust-src和rust-analyzer组件；macOS Clippy工作流单独写死的
nightly-2025-02-28也对齐为Aug22。

## 两个构建图

| 构建 | datatype及四个叶子的来源 | 类型身份检查 |
| --- | --- | --- |
| 无相邻TiDB的SDK检出 | 上述固定Git提交 | 每个包各一个package ID，路径包全部位于SDK自身 |
| TiDB开启shared-kernel | 本地原datatype及叶子 | native tidb-expr和SDK tidb_query_expr指向同一个datatype package ID |

两次构建的Git与path package ID不要求字面相同。逐项比较172个拥有者文件的SHA256，
涵盖datatype及四个叶子、`rust/scripts/aggregate-tests.rs`、测试输入和排序规则二进制资源，
Git与本地内容一致。SDK构建会实际执行原聚合测试构建脚本、编译包含这些资源的datatype。
元数据、tree输出及文件哈希保存在`dependency-identity.json`和两个metadata记录中。

## 普通Cargo复现

独立目录为`independent-sdk/tikv`，其父目录没有TiDB；构建目标目录初始为空。
使用rustup正常Cargo代理，由检出内`rust-toolchain.toml`选择版本。Cargo缓存复用了已有
registry及Git下载内容；首次TiDB Git抓取为实际网络下载，不把缓存复用称作全冷下载验证。
没有RUSTC、RUSTDOC、RUSTUP_TOOLCHAIN或RUSTFLAGS覆盖，没有全局Cargo path patch。
检查到的唯一Cargo配置是TiDB自身`rust/.cargo/config.toml`，独立SDK不受其影响。
标准CARGO_HOME／RUSTUP_HOME／CARGO_TARGET_DIR仅指定安装与缓存位置。

系统前提包括C/C++、CMake、make、pkg-config、protoc和OpenSSL开发文件。此Gentoo机器使用
GCC/G++14；原native依赖需要明确的兼容设置，已写入SDK表达式库README：

```sh
export CC=gcc-14 CXX=g++-14 CXXFLAGS='-include cstdint'
export CMAKE_POLICY_VERSION_MINIMUM=3.5 CARGO_BUILD_JOBS=4
# 独立SDK根目录；正常rustup Cargo代理位于PATH
rustup show active-toolchain
rustc --version --verbose
cargo metadata --locked --format-version 1
cargo tree --locked -i tidb-datatype
cargo build --locked -p tidb_query_expr --lib
cargo test --locked -p tidb_query_expr --lib -- --nocapture
# TiDB rust/目录
cargo metadata --locked --features tidb-expr/shared-kernel --format-version 1
cargo tree --locked --features tidb-expr/shared-kernel -i tidb-datatype
cargo test --locked -p tidb-expr --features shared-kernel --lib shared_kernel_ -- --nocapture
cargo test --locked -p tidb-expr --features shared-kernel --lib constant::tests::prepared_ -- --nocapture
```

本次在Git解析成功后为上述最终Cargo命令加`--offline`，使用同一锁文件和已下载来源。
没有调用旧的绝对路径`cargo-tidb`包装器。系统编译器和版本完整记录于`compiler.log`。

## 验证与限制

独立SDK正常build通过（约127秒），429项表达式测试通过。随后独立检出切换到固定SDK提交，
工作树干净，1421项编译输入逐项相同，build及429项再次通过。TiDB的41项共享／8项prepared
测试通过，限定Clippy完成但有既有警告。根make lint和SDK的check-redact-log、check-log-style、
check-dashboards、check-docker-build、check-license五项静态检查通过。

正式SDK `make clippy`也实际尝试：预检查通过并安装其独立Rust1.92工具链，之后仍停在
scripts/deny的工具安装阶段。由本次运行器在120秒限时到达时向整个进程组发送SIGTERM，
退出−15；这是我们的限时终止，不是完整Clippy或deny通过，也不归因为外部自动取消。
本批没有继续运行完整workspace Clippy；记录和终止原因在sdk-make-clippy.log／result.json中。

工具链声明影响整个SDK及读取该文件的Docker／开发构建；macOS工作流声明也已修改，
但本批没有运行macOS CI、全部SDK workspace、FIPS／release构建或完整TiKV测试。
因此只声明表达式库及列明的TiDB共享路径通过，不能扩大为整个TiKV兼容。
原默认工具链MSRV和独立检出接线问题在此限定包范围验收；没有新性能测量、默认SQL接入、
并行worker或跨EXECUTE生命周期证明。

## 固定材料与完整增删

源码固定为Main `542d31a7978f7bbee4a6a13dc89cf790b7ef5f7f`、SDK `41c56427b68ce7518415d22f6d14e9f410b8c2e0`。
5525项本地源码输入摘要为`5842cd9ad21356f089cc144a6fbdd520ec155dc345257b46a9afc33d4b0e2697`，检查前后未变。
[验证收据](independent-sdk-build-validation.json)包含测试终态、Git来源和完整记录哈希；
既有verify-checkpoint-6.py可用此收据与本地independent-sdk-build证据目录独立核验。
Git资源哈希与metadata/tree、实际编译器、两个锁文件差异、独立检出状态和失败尝试一并保留。
统计基线为Main dece836798／SDK05130ce8；本批不改生产Rust或测试逻辑。
统计器把.gitmodules、toolchain文件和CI workflow与Cargo文件一起归为构建，脚本／报告为文档证据。
本批生产与测试增删均为0；构建+14/−3，文档／证据+364/−1；全部+378/−4，净增374，共382行增删。原始累计源码及测试7030行增删，全部+24861/−221，共25082行增删。

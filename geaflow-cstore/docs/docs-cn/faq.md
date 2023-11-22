# CStore FAQ
## 1. 代码风格 && 静态检查

遵循 rust 社区的代码风格，参见 rustfmt.toml, clippy.toml。

## 2. MacOs 执行 make update 卡住的解决方法：

   ```shell
# 执行下面命令可以换成国内源
$ echo -e '[source.crates-io]\nreplace-with = "tuna"\n[source.tuna]\nregistry = "https://mirrors.tuna.tsinghua.edu.cn/git/crates.io-index.git"' > <root>/.cargo/[con](#)[fig](#)

# 执行 makefile 中的 make update 前需要在 mac 上执行以下命令
$ brew install pkg-config && brew install OpenSSL && cargo install cargo-edit

# make update 现在的作用：
# 1. 更新 cargo.lock 中依赖版本
# 2. 更新 cargo.toml 中所有依赖版本
# 3. 更新 rust-stable toolchain 版本（包括 rustfmt、clippy）
# 4. 更新 rust-nightly toolchain 版本
   ```


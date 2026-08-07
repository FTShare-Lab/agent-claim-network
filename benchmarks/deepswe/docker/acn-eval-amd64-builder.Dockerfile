# 在宿主 Docker daemon 的原生架构上运行，生成 DeepSWE 所需的 Linux x86_64 二进制。
ARG BASE_IMAGE=debian:bookworm-slim
FROM ${BASE_IMAGE}

RUN dpkg --add-architecture amd64 \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        curl \
        gcc-x86-64-linux-gnu \
        libc6-dev-amd64-cross \
        libssl-dev:amd64 \
        pkg-config \
    && rm -rf /var/lib/apt/lists/* \
    && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o /tmp/rustup-init.sh \
    && sh /tmp/rustup-init.sh -y --default-toolchain 1.90.0 --profile minimal \
    && rm /tmp/rustup-init.sh \
    && /root/.cargo/bin/rustup target add x86_64-unknown-linux-gnu

ENV PATH=/root/.cargo/bin:$PATH
# 锁定到仓库 rust-toolchain.toml 所要求的 1.90.0，避免 rustup 将简写 "1.90"
# 解析为独立工具链而丢失上面安装的 x86_64 标准库。
ENV RUSTUP_TOOLCHAIN=1.90.0
ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-linux-gnu-gcc
ENV CC_x86_64_unknown_linux_gnu=x86_64-linux-gnu-gcc
ENV PKG_CONFIG_ALLOW_CROSS=1
ENV PKG_CONFIG_LIBDIR=/usr/lib/x86_64-linux-gnu/pkgconfig:/usr/share/pkgconfig

FROM scratch
ADD ./target/x86_64-unknown-linux-musl/debug/fs-synchronizer /
CMD ["/fs-synchronizer", "-d", "."]

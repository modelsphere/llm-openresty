# 共享 helper —— 隔离 harness 用。
# 重构后 session_route.conf 的 init_by_lua_block 只 `require "router"`,引擎代码在 openresty/lua/*.lua。
# 各 test_*.sh 从 ENGINE 抽出 init_by_lua_block 到 initblock.conf 再 include,但生成的 http{} 没有
# lua_package_path → require 找不到模块。本 helper 给 initblock 前置 lua_package_path 指向 lua/ 目录。
# 用法:  source "$HERE/lib_initblock.sh"; prepend_lua_path "$PREFIX/initblock.conf" "$HERE/../lua"
prepend_lua_path() {
    local ib="$1" luadir="$2"
    { echo "lua_package_path '$luadir/?.lua;;';"; cat "$ib"; } > "$ib.tmp" && mv "$ib.tmp" "$ib"
}

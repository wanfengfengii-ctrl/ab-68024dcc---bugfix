#!/usr/bin/env python3
"""verify 一次性服务：代码测试 + 构建检查 + 投递冒烟，退出码汇总。

退出码为位掩码：
  bit 0 (1) —— Python 构建/导入检查失败
  bit 1 (2) —— 单元测试失败
  bit 2 (4) —— 投递冒烟失败
全部通过退出 0。
"""

from __future__ import annotations

import compileall
import os
import sys
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from relay import client, scenarios  # noqa: E402

API_URL = os.getenv("API_URL", "http://api:8080")
RECEIVER_ADMIN = os.getenv("RECEIVER_ADMIN", "http://receiver:8081")


def step(title: str) -> None:
    print(f"\n=== {title} ===", flush=True)


def check_build() -> bool:
    step("1/3 构建检查：compileall + 模块导入")
    ok = compileall.compile_dir(str(ROOT / "relay"), quiet=1, maxlevels=10)
    ok = compileall.compile_dir(str(ROOT / "tests"), quiet=1) and ok
    try:
        import relay.api  # noqa: F401
        import relay.receiver  # noqa: F401
        import relay.httpclient  # noqa: F401
    except Exception as exc:
        print(f"[FAIL] 模块导入失败: {exc}")
        ok = False
    print("[PASS] 构建检查通过" if ok else "[FAIL] 构建检查失败")
    return bool(ok)


def check_unit_tests() -> bool:
    step("2/3 代码单元/集成测试（unittest）")
    loader = unittest.TestLoader()
    suite = loader.discover(str(ROOT / "tests"), pattern="test_*.py")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    ok = result.wasSuccessful()
    print("[PASS] 全部测试通过" if ok else "[FAIL] 存在失败用例")
    return ok


def check_smoke() -> bool:
    step("3/3 投递冒烟（真实容器链路）")
    try:
        client.wait_healthy(f"{API_URL}/health", timeout=60)
        client.wait_healthy(f"{RECEIVER_ADMIN}/health", timeout=60)
    except TimeoutError as exc:
        print(f"[FAIL] 服务健康检查超时: {exc}")
        return False

    # 清理其他验证可能遗留的故障脚本
    client.set_fault(RECEIVER_ADMIN, "ok")
    print(f"目标 API={API_URL} 接收端={RECEIVER_ADMIN}")
    t0 = time.time()
    results = scenarios.run_all(API_URL, RECEIVER_ADMIN)
    elapsed = time.time() - t0
    passed = sum(1 for r in results if r.ok)

    print("\n--- 值班员视角终态抽查（GET /api/alerts/{id}）---")
    for r in results:
        if r.view.get("conclusion"):
            print(f"  [{r.view.get('status')}] {r.name}\n      → {r.view['conclusion']}")

    print(f"\n冒烟结果：{passed}/{len(results)} 通过，用时 {elapsed:.1f}s")
    ok = passed == len(results)
    print("[PASS] 投递冒烟全部通过" if ok else "[FAIL] 投递冒烟存在失败场景")
    return ok


def main() -> int:
    print("地震台网告警可靠投递 —— verify 汇总")
    code = 0
    if not check_build():
        code |= 1
    if not check_unit_tests():
        code |= 2
    if not check_smoke():
        code |= 4

    step("汇总")
    if code == 0:
        print("✅ 构建检查、代码测试、投递冒烟全部通过；退出码 0")
    else:
        print(f"❌ 验证未完全通过，退出码 {code}（bit0=构建 bit1=单测 bit2=冒烟）")
    return code


if __name__ == "__main__":
    sys.exit(main())
